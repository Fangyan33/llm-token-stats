"""真实 Istio ECDS 加载故障、透传与原地恢复验收，仅用于隔离测试网关。"""
import copy
import hashlib
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from reload_gateway_filter import Reloader


def checks(h, original):
    from k8s_collection import ROOT, require, wait_for, walk, write_json
    name = original['metadata']['name']
    pod = h.gateway['metadata']['name']
    uid = h.gateway['metadata']['uid']
    # Other replicas are tested separately; fault traffic and download cache are scoped by extension name/digest.
    fixture = 'llm-token-stats-collection-fixture.' + h.namespace + '.svc.cluster.local'
    nonce = uuid.uuid4().hex[:16]
    section = ('ecds-v1-' + nonce).encode()
    variant = (ROOT / 'dist/gateway-observer-v1.wasm').read_bytes() + bytes([0, len(section)+1, len(section)]) + section
    results = []
    h.kubectl('-n', h.namespace, 'delete', 'envoyfilter', name)
    current_name = None
    try:
        for scenario in ('slow', 'not_found', 'timeout', 'checksum_mismatch', 'unavailable', 'corrupt', 'invalid_config'):
            print('验证 ECDS 故障与透传：' + scenario, flush=True)
            temporary = copy.deepcopy(original)
            current_name = 'llm-token-stats-ecds-' + scenario.replace('_', '-') + '-' + nonce[:8]
            temporary['metadata'] = {'name': current_name, 'namespace': h.namespace}
            ext = temporary['spec']['configPatches'][0]['patch']['value']
            http = temporary['spec']['configPatches'][1]['patch']['value']
            ext['name'] = http['name'] = current_name
            config = ext['typed_config']['config']
            remote = config['vm_config']['code']['remote']
            remote['http_uri'].update(uri='http://' + fixture + ':8081/wasm/e2e?scenario=' + scenario + '&nonce=' + nonce,
                                      cluster='outbound|8081||' + fixture)
            remote['sha256'] = hashlib.sha256(variant if scenario in ('slow', 'invalid_config') else (scenario + nonce).encode()).hexdigest()
            if scenario == 'corrupt':
                remote['sha256'] = hashlib.sha256(('corrupt-wasm-' + nonce).encode()).hexdigest()
            if scenario == 'invalid_config':
                config['configuration']['value']['schema_version'] = 'invalid'
            if scenario == 'unavailable':
                cluster = 'llm-token-stats-ecds-unavailable'
                remote['http_uri']['cluster'] = cluster
                temporary['spec']['configPatches'].insert(0, {
                    'applyTo': 'CLUSTER', 'match': {'context': 'GATEWAY', 'proxy': {'metadata': {'NAMESPACE': h.namespace}}},
                    'patch': {'operation': 'ADD', 'value': {'name': cluster, 'type': 'STATIC', 'connect_timeout': '0.1s',
                       'load_assignment': {'cluster_name': cluster, 'endpoints': [{'lb_endpoints': [{'endpoint': {
                          'address': {'socket_address': {'address': '127.0.0.1', 'port_value': 1}}}}]}]}}}})
            h.apply(temporary)
            discovery_name = current_name
            def installed():
                return any(v.get('name') == discovery_name and 'config_discovery' in v
                           for v in walk([v['active_state'] for v in walk(h.dump(pod)) if 'active_state' in v]))
            wait_for('故障 ECDS 的监听器无需等待下载即可激活', installed, 30)
            body = {'model': 'fixture', 'messages': [{'role': 'user', 'content': 'synthetic ECDS forwarding'}]}
            ids = []
            with h.forwarding(pod_name=pod):
                start = time.monotonic()
                for _ in range(3):
                    request_id = str(uuid.uuid4())
                    status, _, headers = h.request('/v1/chat/completions', body, request_id)
                    require(status == 200 and headers['x-test-body-sha256'] == hashlib.sha256(json.dumps(body).encode()).hexdigest(),
                            'ECDS 故障改变业务响应或原始正文')
                    ids.append(request_id)
                elapsed = time.monotonic() - start
                if scenario == 'slow':
                    require(elapsed < 3, '下载期间业务等待了 Wasm')
            stats = Reloader(h.args.context, h.namespace, current_name).stats(pod)
            require(any(('extension_config_discovery.' in k and '.' + current_name + '.' in k and k.endswith('config_fail')
                         or scenario == 'invalid_config' and k.endswith('metering_config_invalid_total')) and v > 0
                        for k, v in stats.items()), '故障未触发真实 ECDS/插件配置拒绝')
            time.sleep(1)
            quoted = ','.join("'" + i + "'" for i in ids)
            require(h.sql('SELECT COUNT(*) FROM metering.events_raw WHERE logical_request_id IN (' + quoted + ');').strip() == '0',
                    '默认透传产生了采集事件')
            result = {'scenario': scenario, 'business_requests': len(ids), 'elapsed_seconds': elapsed}
            if scenario in ('slow', 'invalid_config'):
                # The valid variant has now been fetched into the same process cache.
                if scenario == 'invalid_config':
                    try:
                        Reloader(h.args.context, h.namespace, current_name, 30).reload()
                    except RuntimeError as exc:
                        require('配置或运行时身份' in str(exc), '非法 YAML 未按预期被拒绝')
                    else:
                        raise RuntimeError('非法 YAML 被加载为有效配置')
                    config['configuration']['value'] = original['spec']['configPatches'][0]['patch']['value']['typed_config']['config']['configuration']['value']
                    h.apply(temporary)
                loaded = Reloader(h.args.context, h.namespace, current_name).reload()
                temporary = h.get('-n', h.namespace, 'get', 'envoyfilter', current_name)
                # Client-side apply must not carry the resourceVersion from before the next hot update.
                temporary['metadata'] = {'name': current_name, 'namespace': h.namespace}
                discovery_name = loaded['discovery_name']
                with h.forwarding(pod_name=pod):
                    request_id = str(uuid.uuid4())
                    status, payload, _ = h.request('/v1/chat/completions', {'model': 'fixture', 'stream': True,
                                                   'messages': [{'role': 'user', 'content': 'synthetic ECDS recovery'}]}, request_id)
                    require(status == 200 and b'[DONE]' in payload, '原地恢复后 SSE 响应不完整')
                    wait_for('恢复后的实际采集入库', lambda: h.sql("SELECT COUNT(*) FROM metering.events_raw WHERE logical_request_id='" + request_id + "' AND event_type='usage_observed' AND usage_status='known';").strip() == '1')
                    # A rejected runtime update must retain the last valid extension while traffic continues.
                    if scenario == 'slow':
                        broken = copy.deepcopy(temporary)
                        broken['spec']['configPatches'][0]['patch']['value']['typed_config']['config']['vm_config']['runtime'] = 'envoy.wasm.runtime.missing'
                        # VM cache identity omits runtime; force a distinct VM to exercise runtime rejection.
                        broken['spec']['configPatches'][0]['patch']['value']['typed_config']['config']['vm_config']['vm_id'] = 'llm-token-stats-invalid-runtime'
                        def traffic():
                            for _ in range(12):
                                require(h.request('/v1/chat/completions', body)[0] == 200, '热更新中断业务')
                                time.sleep(0.1)
                        loader = Reloader(h.args.context, h.namespace, current_name)
                        previous_fails = sum(v for k, v in loader.stats(pod).items() if loaded['discovery_name'] in k and k.endswith('config_fail'))
                        with ThreadPoolExecutor(max_workers=1) as executor:
                            task = executor.submit(traffic)
                            h.apply(broken)
                            wait_for('热更新已被 ECDS 拒绝', lambda: sum(v for k, v in loader.stats(pod).items()
                                if loaded['discovery_name'] in k and k.endswith('config_fail')) > previous_fails, 30)
                            task.result()
                        require(h.request('/v1/chat/completions', {'model': 'fixture', 'stream': True})[0] == 200,
                                '被拒绝更新未保留旧有效插件')
                        h.apply(temporary)
                result['recovery'] = loaded
            # Emergency disable uses direct Lua and does not depend on fetching or accepting an extension.
            disabled = copy.deepcopy(temporary)
            disabled['spec']['configPatches'] = [p for p in disabled['spec']['configPatches'] if p['applyTo'] == 'HTTP_FILTER']
            value = disabled['spec']['configPatches'][0]['patch']['value']
            value.pop('config_discovery')
            value['typed_config'] = {'@type': 'type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua',
                                    'inline_code': 'function envoy_on_request(handle) end'}
            h.apply(disabled)
            wait_for('关闭态移除 ECDS 依赖', lambda: not installed(), 30)
            with h.forwarding(pod_name=pod):
                disabled_id = str(uuid.uuid4())
                status, _, headers = h.request('/v1/chat/completions', body, disabled_id)
                require(status == 200 and headers['x-test-body-sha256'] == hashlib.sha256(json.dumps(body).encode()).hexdigest(),
                        '关闭态未原样转发')
                time.sleep(1)
                require(h.sql("SELECT COUNT(*) FROM metering.events_raw WHERE logical_request_id='" + disabled_id + "';").strip() == '0',
                        '关闭态仍产生采集事件')
            results.append(result)
            h.kubectl('-n', h.namespace, 'delete', 'envoyfilter', current_name)
            current_name = None
    finally:
        if current_name:
            h.kubectl('-n', h.namespace, 'delete', 'envoyfilter', current_name, '--ignore-not-found')
        h.apply(original)
        h.wait_wasm()
    require(any(p['metadata']['uid'] == uid for p in h.pods('llm-token-stats-wasm-gateway')), '原地恢复重建了网关')
    write_json(h.output / 'ecds-faults.result.json', results)
    return results
