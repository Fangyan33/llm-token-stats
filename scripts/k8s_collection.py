"""本地 Kubernetes 1.22.17 + Istio 1.13.5 部署用例；仅复用 Compose Kafka/Doris。"""
import argparse
import contextlib
import copy
import hashlib
import json
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from image_tag import resolve_image_tag

ROOT = Path(__file__).resolve().parents[1]
OWNER_KEY = 'app.kubernetes.io/part-of'
OWNER = 'llm-token-stats-verification'
RUN_KEY = 'llm-token-stats/test-run-id'
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'deploy/local/compose.yaml')]


def run(command, *, input=None, timeout=45, check=True, capture=True):
    return subprocess.run(command, input=input, text=True, cwd=ROOT, timeout=timeout,
                          check=check, capture_output=capture)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_for(description, probe, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = probe()
        if value:
            return value
        time.sleep(1)
    raise RuntimeError(description + ' 超时')


def namespace_name(value):
    if (len(value) > 63 or not re.fullmatch(r'metering-wasm-[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', value)):
        raise argparse.ArgumentTypeError('测试命名空间必须是 metering-wasm- 开头的 DNS 标签')
    return value


def isolated_names(run_id):
    require(bool(re.fullmatch(r'[0-9a-f]{12}', run_id)), '测试 run-id 无效，拒绝清理 Kafka')
    return 'llm-metering-k8s-' + run_id, 'llm-metering-k8s-' + run_id


def decode_resources(raw):
    resources = []
    while raw.strip():
        value, end = json.JSONDecoder().raw_decode(raw.lstrip())
        raw = raw.lstrip()[end:]
        resources.extend(value['items'] if value.get('kind') == 'List' else [value])
    return resources


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def wasm_configs(dump):
    # 只检查已激活的监听器，不能把 warming 中的配置当成加载成功。
    active = [v['active_state'] for v in walk(dump) if 'active_state' in v]
    return [v['typed_config']['config'] for v in walk(active)
            if v.get('name') == 'envoy.filters.http.wasm'
            and v.get('typed_config', {}).get('config', {}).get('name') == 'gateway-observer']


def local_configs(host, topic, group):
    require(bool(re.fullmatch(r'[A-Za-z0-9_.-]+', host))
            and host not in ('localhost', '0.0.0.0') and not host.startswith('127.'),
            'Compose Kafka 的 EXTERNAL 地址必须能从 Pod 访问，不能是回环地址')
    kafka = {'bootstrap_servers': [host + ':29092'], 'topic': topic}
    observer = json.loads((ROOT / 'configs/observer.production.example.json').read_text())
    observer['kafka'] = kafka
    consumer = json.loads((ROOT / 'configs/consumer.production.example.json').read_text())
    consumer['kafka'] = copy.deepcopy(kafka)
    consumer['consumer'].update(group_id=group, flush_interval_ms=1000)
    # 本地 all-in-one 的 FE 重定向到容器内 127.0.0.1；使用已发布的 BE 直连。
    consumer['doris']['fe_url'] = 'http://' + host + ':18040'
    consumer['doris']['allowed_load_hosts'] = [host + ':18040']
    credentials = {
        'DORIS_DSN': ('root@tcp(' + host + ':19030)/metering?timeout=5s&readTimeout=30s'
                      '&writeTimeout=10s&interpolateParams=true&time_zone=%27%2B00%3A00%27'),
        'DORIS_USER': 'root', 'DORIS_PASSWORD': '',
    }
    return observer, consumer, credentials


def validate_events(results, rows, gateway_uid, producer_ids, topic):
    for result in results:
        matches = [row for row in rows if row[0] == result['x_request_id']]
        require(len(matches) == 2 and {row[1] for row in matches} == {'request_started', 'usage_observed'},
                '每个请求必须恰有 request_started 和 usage_observed 两条事件')
        require(all(row[8] == gateway_uid and row[9] in producer_ids and row[10] == topic for row in matches),
                '事件必须来自本次 Kubernetes 网关、Observer 和独立 topic')
        require(len({row[11] for row in matches}) == 1
                and matches[0][11] in ('synthetic-model-route', 'unknown'),
                '事件必须记录真实 HTTP 路由名或 unknown，且请求内保持不变')
        usage = next(row for row in matches if row[1] == 'usage_observed')
        require(usage[6] == 'complete', '观察结果不完整')
        expected = ['unknown', 'NULL', 'NULL'] if result['scenario'] == 'unknown' else ['known', '10', '4']
        require(usage[3:6] == expected and usage[7] == ('0' if result['scenario'] == 'unknown' else '1'),
                '用量或 unknown/NULL 语义不符合预期')
        result.update(logical_request_id=usage[2], usage_status=usage[3],
                      input_tokens=None if usage[4] == 'NULL' else int(usage[4]),
                      output_tokens=None if usage[5] == 'NULL' else int(usage[5]),
                      gateway_instance_id=usage[8], observer_producer_id=usage[9],
                      kafka_topic=usage[10], route_name=usage[11], event_count=len(matches))


def offsets_caught_up(raw, topic):
    rows = [line.split() for line in raw.splitlines() if topic in line.split()]
    if len(rows) != 3 or any(len(row) != 9 for row in rows):
        return False
    offsets = []
    for row in rows:
        # Kafka 对从未写入记录的空分区不一定登记 committed offset / lag。
        if row[3:6] == ['-', '0', '-']:
            offsets.append(0)
        elif row[3].isdigit() and row[3] == row[4] and row[5] == '0':
            offsets.append(int(row[3]))
        else:
            return False
        if row[6] == '-':
            return False
    return sum(offsets) == 6


class Harness:
    def __init__(self, args):
        self.args = args
        self.namespace = args.namespace
        self.kube = ['kubectl', '--context', args.context]
        self.base = ROOT / '.tmp/k8s-collection' / args.context / args.namespace
        self.state_file = self.base / 'state.json'
        self.output = self.base

    def kubectl(self, *args, **kwargs):
        return run([*self.kube, *args], **kwargs).stdout

    def get(self, *args):
        return json.loads(self.kubectl(*args, '-o', 'json'))

    def apply(self, value):
        self.kubectl('apply', '-f', '-', input=json.dumps(value))

    def kafka(self, tool, *args):
        return run([*COMPOSE, 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-' + tool + '.sh',
                    '--bootstrap-server', 'kafka:9092', *args]).stdout

    def sql(self, sql):
        return run([*COMPOSE, 'exec', '-T', 'doris', 'mysql', '-uroot', '-h127.0.0.1',
                    '-P9030', '-N', '-e', sql]).stdout

    def cleanup(self):
        raw = self.kubectl('get', 'namespace', self.namespace, '--ignore-not-found', '-o', 'json')
        namespace = json.loads(raw) if raw.strip() else None
        saved = json.loads(self.state_file.read_text()) if self.state_file.exists() else {}
        if namespace:
            require(namespace['metadata'].get('labels', {}).get(OWNER_KEY) == OWNER,
                    '命名空间没有本测试的归属标签，拒绝删除：' + self.namespace)
            run_id = namespace['metadata'].get('annotations', {}).get(RUN_KEY)
        else:
            require(not saved or (saved.get('context') == self.args.context
                                  and saved.get('namespace') == self.namespace), '本地清理记录不匹配')
            run_id = None if saved.get('cleaned') else saved.get('run_id')
        topic_group = isolated_names(run_id) if run_id else None
        # 删除命名空间前留存记录；即使中途失败，下次仍能清理本测试的 topic/group。
        if run_id:
            saved = {'context': self.args.context, 'namespace': self.namespace, 'run_id': run_id}
            write_json(self.state_file, saved)
        if namespace:
            print('清理测试命名空间：' + self.namespace, flush=True)
            self.kubectl('delete', 'namespace', self.namespace, '--wait=true', '--timeout=120s', timeout=135)
        if topic_group:
            topic, group = topic_group
            groups = self.kafka('consumer-groups', '--list').splitlines()
            if group in groups:
                # Namespace 删除完成后 Consumer 已退出，才删除该测试消费组。
                self.kafka('consumer-groups', '--delete', '--group', group)
            if topic in self.kafka('topics', '--list').splitlines():
                self.kafka('topics', '--delete', '--topic', topic)
            wait_for('测试 topic 删除', lambda: topic not in self.kafka('topics', '--list').splitlines(), 45)
            print('已删除本次独立 Kafka topic / 消费组：' + topic, flush=True)
        if saved:
            write_json(self.state_file, {**saved, 'cleaned': True})
        print('清理完成；保留 Compose 服务、数据卷、Doris 验证记录和本地诊断文件。', flush=True)

    def preflight(self):
        nodes = self.get('get', 'nodes')['items']
        require(nodes and all(n['status']['nodeInfo']['kubeletVersion'] == 'v1.22.17' for n in nodes),
                '本用例固定验证 Kubernetes 1.22.17')
        versions = json.loads(run(['istioctl', '--context', self.args.context, 'version', '-o', 'json']).stdout)
        require(versions['meshVersion'] and all(v['Info']['version'] == '1.13.5' for v in versions['meshVersion']),
                '本用例固定验证 Istio 1.13.5')
        write_json(self.output / 'versions.json', {'nodes': nodes, 'istio': versions})
        services = run([*COMPOSE, 'ps', '--status', 'running', '--services']).stdout.splitlines()
        require({'kafka', 'doris'} <= set(services), '请先执行 make local-infra-up 启动 Compose Kafka/Doris')
        config = json.loads(run([*COMPOSE, 'config', '--format', 'json']).stdout)
        listeners = config['services']['kafka']['environment']['KAFKA_ADVERTISED_LISTENERS']
        endpoint = listeners.split('EXTERNAL://', 1)[1].split(',', 1)[0]
        host, port = endpoint.rsplit(':', 1)
        require(port == '29092', '本地用例要求 Kafka EXTERNAL 发布在 29092')
        local_configs(host, 'preflight', 'preflight')
        for name in ('observer', 'consumer', 'fixture', 'gateway-observer-v1.wasm'):
            require((ROOT / 'dist' / name).is_file(), '请先执行 make build wasm')
        self.sql('SELECT COUNT(*) FROM metering.consumer_partition_origins;')
        self.host = host

    def build_images(self):
        self.image_tag = resolve_image_tag(ROOT)
        self.images = {name: 'metering-' + name + ':' + self.image_tag
                       for name in ('observer', 'consumer', 'fixture')}
        write_json(self.output / 'images.json', {'image_tag': self.image_tag, 'images': self.images})
        for name, image in self.images.items():
            print('构建测试镜像：' + image, flush=True)
            dockerfile = ROOT / ('deploy/k8s-test/fixture.Dockerfile' if name == 'fixture'
                                 else 'deploy/' + name + '/Dockerfile')
            context = ROOT / 'dist' if name == 'fixture' else ROOT
            run(['docker', 'build', '--network=host', '--build-arg', 'HTTP_PROXY', '--build-arg', 'HTTPS_PROXY',
                 '--build-arg', 'NO_PROXY', '-f', str(dockerfile), '-t', image, str(context)],
                timeout=600, capture=False)
        run(['kind', 'load', 'docker-image', *self.images.values(), '--name', self.args.kind_cluster],
            timeout=180, capture=False)

    def manifest(self):
        resources = []
        observer, consumer, credentials = local_configs(self.host, self.topic, self.group)
        for name, config in (('observer', observer), ('consumer', consumer)):
            raw = self.kubectl('create', '--dry-run=client', '-f', 'deploy/' + name + '/' + name + '.yaml', '-o', 'json')
            for resource in decode_resources(raw):
                resource['metadata']['namespace'] = self.namespace
                if resource['kind'] == 'Deployment':
                    template = resource['spec']['template']
                    template['metadata']['annotations'] = {'proxy.istio.io/config': '{"concurrency":2}'}
                    spec = template['spec']
                    container = spec['containers'][0]
                    container.update(image=self.images[name], imagePullPolicy='IfNotPresent')
                    container['env'] = [e for e in container.get('env', []) if e['name'] == 'POD_UID']
                    container['volumeMounts'] = [v for v in container['volumeMounts'] if v['name'] == 'config']
                    spec['volumes'] = [v for v in spec['volumes'] if v['name'] == 'config']
                resources.append(resource)
            resources.append({'apiVersion': 'v1', 'kind': 'ConfigMap',
                              'metadata': {'name': 'metering-' + name, 'namespace': self.namespace},
                              'data': {name + '.json': json.dumps(config, indent=2)}})
        resources.append({'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
                          'metadata': {'name': 'metering-consumer-credentials', 'namespace': self.namespace},
                          'stringData': credentials})
        text = (ROOT / 'deploy/k8s-test/fixtures.yaml').read_text()
        text = text.replace('TEST_NAMESPACE', self.namespace).replace('TEST_FIXTURE_IMAGE', self.images['fixture'])
        resources.extend(decode_resources(self.kubectl('create', '--dry-run=client', '-f', '-',
                                                       '-o', 'json', input=text)))
        for resource in resources:
            resource['metadata'].setdefault('labels', {})[OWNER_KEY] = OWNER
        return {'apiVersion': 'v1', 'kind': 'List', 'items': resources}

    def pods(self, app):
        return self.get('-n', self.namespace, 'get', 'pods', '-l', 'app=' + app)['items']

    def gateway_snapshot(self):
        pods = self.get('-n', 'istio-system', 'get', 'pods', '-l', 'app=istio-ingressgateway')['items']
        return sorted([{'name': p['metadata']['name'], 'uid': p['metadata']['uid'],
                        'containers': [{'name': c['name'], 'restarts': c['restartCount']}
                                       for c in p['status'].get('containerStatuses', [])]} for p in pods],
                      key=lambda p: p['name'])

    def render_filter(self):
        pods = self.pods('metering-wasm-gateway')
        require(len(pods) == 1, '测试网关必须恰有一个 Pod')
        self.gateway = pods[0]
        self.gateway_uid = self.gateway['metadata']['uid']
        config = json.loads((ROOT / 'configs/gateway-observer.example.json').read_text())
        config['observer_cluster'] = 'outbound|9090||metering-observer.' + self.namespace + '.svc.cluster.local'
        path = self.output / 'gateway-observer.json'
        write_json(path, config)
        output = self.output / 'envoyfilter.yaml'
        dns = 'metering-observer.' + self.namespace + '.svc.cluster.local'
        run([sys.executable, 'scripts/render_gateway_filter.py', '--gateway-pod-uid', self.gateway_uid,
             '--gateway-pod-name', self.gateway['metadata']['name'], '--gateway-namespace', self.namespace,
             '--config', str(path), '--wasm-url', 'http://' + dns + ':9093/wasm/gateway-observer-v1.wasm',
             '--wasm-cluster', 'outbound|9093||' + dns, '--enable', '--output', str(output)])
        resource = json.loads(self.kubectl('create', '--dry-run=client', '-f', str(output), '-o', 'json'))
        resource['metadata'].setdefault('labels', {})[OWNER_KEY] = OWNER
        resource['spec']['workloadSelector']['labels'] = {'istio': self.namespace}
        write_json(self.output / 'envoyfilter.json', resource)
        self.apply(resource)
        return resource

    def dump(self):
        return json.loads(run(['istioctl', '--context', self.args.context, 'proxy-config', 'all',
                               self.gateway['metadata']['name'], '-n', self.namespace, '-o', 'json']).stdout)

    def wait_wasm(self, present=True):
        def probe():
            dump = self.dump()
            configs = wasm_configs(dump)
            if not present:
                return dump if not configs else None
            if configs and all(c['vm_config']['code']['remote']['sha256'] == self.sha
                               and json.loads(c['configuration']['value'])['enabled'] for c in configs):
                return dump
            return None
        return wait_for('网关已激活的 Wasm 配置收敛', probe)

    @contextlib.contextmanager
    def forwarding(self):
        # 让 kubectl 选择空闲端口，进程由本用例持有，退出时必定停止。
        log_path = self.output / 'port-forward.log'
        with log_path.open('w') as log:
            process = subprocess.Popen([*self.kube, '-n', self.namespace, 'port-forward',
                                        '--address=127.0.0.1', 'service/wasm-gateway', ':8080'],
                                       cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, text=True)
            try:
                def ready():
                    require(process.poll() is None, 'port-forward 启动失败，见 ' + str(log_path))
                    match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+)', log_path.read_text())
                    return match.group(1) if match else None
                port = wait_for('port-forward', ready, 15)
                self.url = 'http://127.0.0.1:' + port
                self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                yield
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)

    def request(self, path, body=None, request_id=None, method=None):
        headers = {'Host': 'wasm-verify.local', 'Content-Type': 'application/json'}
        if request_id:
            headers['x-request-id'] = request_id
        request = urllib.request.Request(self.url + path, headers=headers,
                                         data=json.dumps(body).encode() if body is not None else None,
                                         method=method)
        with self.http.open(request, timeout=15) as response:
            return response.status, response.read(), response.headers

    def check_match(self, resource):
        other = copy.deepcopy(resource)
        other['spec']['configPatches'][0]['match']['proxy']['metadata']['NAME'] = 'no-such-verification-pod'
        try:
            self.apply(other)
            self.wait_wasm(False)
            require(self.request('/healthz')[0] == 200, '未安装插件时业务健康请求失败')
        finally:
            self.apply(resource)
        write_json(self.output / 'config.final.json', self.wait_wasm())

    def requests(self):
        results = []
        for scenario in ('json', 'sse', 'unknown'):
            body = {'model': 'fixture', 'stream': scenario != 'json',
                    'messages': [{'role': 'user', 'content': 'synthetic Kubernetes verification'}]}
            if scenario == 'unknown':
                body['test_case'] = 'missing_usage'
            request_id = str(uuid.uuid4())
            path = '/v1/chat/completions?source=wildcard-test' if scenario == 'json' else '/v1/chat/completions'
            status, payload, headers = self.request(path, body, request_id)
            require(status == 200, '模型请求失败')
            if scenario != 'json':
                require(b'data: [DONE]' in payload, 'SSE 响应不完整')
            results.append({'scenario': scenario, 'x_request_id': request_id, 'http_status': status,
                            'content_type': headers.get('Content-Type')})
        ids = ','.join("'" + r['x_request_id'] + "'" for r in results)
        sql = ('SELECT x_request_id,event_type,logical_request_id,usage_status,input_tokens,output_tokens,'
               'observation_status,usage_eligible,gateway_instance_id,producer_id,kafka_topic,route_name '
               'FROM metering.events_raw WHERE x_request_id IN (' + ids + ') ORDER BY x_request_id,event_type;')

        def events():
            rows = [line.split('\t') for line in self.sql(sql).splitlines()]
            return rows if len(rows) >= 6 else None
        rows = wait_for('Kubernetes Consumer 写入 Doris', events, 90)
        producers = {p['metadata']['uid'] for p in self.pods('metering-observer')}
        validate_events(results, rows, self.gateway_uid, producers, self.topic)
        write_json(self.output / 'requests.result.json', results)
        return results

    def ignored_requests(self):
        def selected():
            raw = self.kubectl('-n', self.namespace, 'exec', self.gateway['metadata']['name'],
                               '-c', 'istio-proxy', '--', 'pilot-agent', 'request', 'GET',
                               'stats?filter=metering_requests_selected_total')
            values = re.findall(r'metering_requests_selected_total:\s+(\d+)', raw)
            require(bool(values), '网关 selected 计数不可读')
            return sum(int(value) for value in values)
        before = selected()
        results = []
        for method, path in [('GET', '/v1/chat/completions'), ('HEAD', '/v1/chat/completions'),
                             ('OPTIONS', '/v1/chat/completions'), ('PUT', '/v1/chat/completions'),
                             ('POST', '/v1/embeddings'), ('POST', '/v1/chat/completions/'),
                             ('POST', '/v1/chat/completions/extra'), ('GET', '/healthz')]:
            request_id = str(uuid.uuid4())
            try:
                status, _, _ = self.request(path, {'model': 'fixture', 'stream': False}, request_id, method)
            except urllib.error.HTTPError as error:
                status = error.code
                error.close()
            results.append({'method': method, 'path': path, 'http_status': status, 'x_request_id': request_id})
        after = selected()
        require(after == before, '非白名单请求触发了采集')
        ids = ','.join("'" + result['x_request_id'] + "'" for result in results)
        count = self.sql('SELECT COUNT(*) FROM metering.events_raw WHERE x_request_id IN (' + ids + ');').strip()
        require(count == '0', '非白名单请求产生了事件')
        report = {'requests': results, 'selected_before': before, 'selected_after': after, 'event_count': 0}
        write_json(self.output / 'ignored-requests.result.json', report)
        return report

    def check_consumer(self):
        consumer = self.pods('metering-consumer')
        require(len(consumer) == 1, '必须由一个独立 Kubernetes Consumer 消费')
        def committed():
            raw = self.kafka('consumer-groups', '--describe', '--group', self.group)
            return raw if offsets_caught_up(raw, self.topic) else None
        raw = wait_for('Kubernetes Consumer 消费位点提交', committed, 60)
        (self.output / 'consumer-offsets.txt').write_text(raw)
        origins = self.sql("SELECT COUNT(*) FROM metering.consumer_partition_origins WHERE group_id='"
                           + self.group + "' AND kafka_topic='" + self.topic + "';").strip()
        require(origins == '3', 'Consumer 分区起点记录不完整')
        receipts = self.sql("SELECT kafka_partition,MAX(next_offset) FROM metering.consumer_batch_receipts "
                            "WHERE kafka_topic='" + self.topic + "' GROUP BY kafka_partition;")
        require(sum(int(line.split('\t')[1]) for line in receipts.splitlines()) == 6,
                'Doris 可见的 Consumer 批次回执未覆盖全部事件')
        (self.output / 'consumer-receipts.txt').write_text(receipts)
        # kind 到 Compose 的连接有 SNAT，broker 所见 HOST 不能用于核对 Pod IP。
        # 实际停掉本次 K8s Consumer，确认该组随之变为 0 成员，再恢复并检查 1 成员。
        def group_state(state, members):
            text = self.kafka('consumer-groups', '--describe', '--group', self.group, '--state')
            rows = [line.split() for line in text.splitlines() if line.startswith(self.group + ' ')]
            return text if len(rows) == 1 and rows[0][-2:] == [state, str(members)] else None
        wait_for('独立 Consumer 消费组成员', lambda: group_state('Stable', 1), 60)
        try:
            print('核验 Consumer 归属：缩容 Kubernetes Consumer，等待消费组成员归零', flush=True)
            self.kubectl('-n', self.namespace, 'scale', 'deployment/metering-consumer', '--replicas=0')
            empty = wait_for('Kubernetes Consumer 停止后消费组归零', lambda: group_state('Empty', 0), 90)
            (self.output / 'consumer-group.stopped.txt').write_text(empty)
        finally:
            self.kubectl('-n', self.namespace, 'scale', 'deployment/metering-consumer', '--replicas=1')
        self.kubectl('-n', self.namespace, 'rollout', 'status', 'deployment/metering-consumer',
                      '--timeout=180s', timeout=195)
        stable = wait_for('Kubernetes Consumer 恢复入组', lambda: group_state('Stable', 1), 60)
        (self.output / 'consumer-group.restored.txt').write_text(stable)
        wait_for('Consumer 恢复后已提交位点保持', committed, 60)
        restored = self.pods('metering-consumer')
        restored = next(p for p in restored if not p['metadata'].get('deletionTimestamp'))
        return {'pod_name': restored['metadata']['name'], 'pod_uid': restored['metadata']['uid'],
                'initial_pod_uid': consumer[0]['metadata']['uid'], 'pod_ip': restored['status']['podIP'],
                'group_id': self.group, 'committed_events': 6,
                'members_with_k8s_consumer_stopped': 0, 'members_after_restore': 1}

    def verify_pods(self):
        pods = self.get('-n', self.namespace, 'get', 'pods')['items']
        require(len(pods) == 5, '预期 2 个 Observer、1 个 Consumer、1 个 fixture、1 个网关 Pod')
        for pod in pods:
            statuses = pod['status'].get('containerStatuses', [])
            require(statuses and all(s['ready'] and s['restartCount'] == 0 for s in statuses),
                    '测试 Pod 未 Ready 或曾重启：' + pod['metadata']['name'])
            spec = pod['spec']
            if pod['metadata']['labels'].get('app') == 'metering-wasm-gateway':
                require(len(spec['containers']) == 1 and spec['containers'][0]['image'] == 'docker.io/istio/proxyv2:1.13.5',
                        '网关必须使用原始 Istio 1.13.5 镜像')
                require('wasm' not in json.dumps(spec.get('volumes', [])), '网关不得挂载 Wasm')
            if pod['metadata']['labels'].get('app') == 'metering-observer':
                digest = self.kubectl('-n', self.namespace, 'exec', pod['metadata']['name'], '-c', 'observer',
                                      '--', 'sha256sum', '/usr/share/metering/wasm/gateway-observer-v1.wasm').split()[0]
                require(digest == self.sha, 'Observer 副本内 Wasm 摘要不一致')
        # 从有 sidecar 的 fixture 经 STRICT mTLS 下载，验证 Service 实际提供的内容。
        fixture = self.pods('collection-fixture')[0]['metadata']['name']
        url = 'http://metering-observer.' + self.namespace + '.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm'
        # 二进制结果不经文本解码，避免改变校验内容。
        data = subprocess.run([*self.kube, '-n', self.namespace, 'exec', fixture, '-c', 'fixture', '--',
                               'wget', '-qO-', url], cwd=ROOT, check=True, capture_output=True, timeout=15).stdout
        require(hashlib.sha256(data).hexdigest() == self.sha, '通过 mTLS 下载的 Wasm 摘要不一致')
        write_json(self.output / 'pods.result.json', pods)

    def diagnostics(self):
        commands = {'pods.json': ['get', 'pods', '-o', 'json'], 'events.txt': ['get', 'events'],
                    'describe.txt': ['describe', 'pods']}
        for filename, command in commands.items():
            with contextlib.suppress(Exception):
                (self.output / filename).write_text(self.kubectl('-n', self.namespace, *command))
        for name in ('metering-observer', 'metering-consumer', 'collection-fixture', 'wasm-gateway'):
            with contextlib.suppress(Exception):
                (self.output / (name + '.log')).write_text(self.kubectl(
                    '-n', self.namespace, 'logs', 'deployment/' + name, '--all-containers=true', '--tail=100'))

    def test(self):
        self.run_id = uuid.uuid4().hex[:12]
        self.output = self.base / self.run_id
        self.output.mkdir(parents=True)
        print('本地 Kubernetes / Istio 部署用例；诊断目录：' + str(self.output), flush=True)
        self.preflight()
        self.build_images()
        self.cleanup()
        self.topic, self.group = isolated_names(self.run_id)
        before = self.gateway_snapshot()
        write_json(self.output / 'existing-gateway-before.json', before)
        self.apply({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
            'name': self.namespace, 'labels': {'istio-injection': 'enabled', OWNER_KEY: OWNER},
            'annotations': {RUN_KEY: self.run_id}}})
        write_json(self.state_file, {'context': self.args.context, 'namespace': self.namespace,
                                     'run_id': self.run_id})
        self.kafka('topics', '--create', '--topic', self.topic, '--partitions', '3', '--replication-factor', '1',
                   '--config', 'retention.ms=86400000')
        try:
            manifest = self.manifest()
            write_json(self.output / 'resources.json', manifest)
            self.apply(manifest)
            for name in ('metering-observer', 'metering-consumer', 'collection-fixture', 'wasm-gateway'):
                print('等待 Kubernetes Deployment 就绪：' + name, flush=True)
                self.kubectl('-n', self.namespace, 'rollout', 'status', 'deployment/' + name,
                              '--timeout=180s', timeout=195)
            self.sha = hashlib.sha256((ROOT / 'dist/gateway-observer-v1.wasm').read_bytes()).hexdigest()
            resource = self.render_filter()
            self.wait_wasm()
            with self.forwarding():
                self.check_match(resource)
                results = self.requests()
                ignored = self.ignored_requests()
            consumer = self.check_consumer()
            self.verify_pods()
            after = self.gateway_snapshot()
            write_json(self.output / 'existing-gateway-after.json', after)
            require(before == after, '既有 istio-system 网关身份或重启次数发生变化')
            summary = {'status': 'passed', 'context': self.args.context, 'namespace': self.namespace,
                       'image_tag': self.image_tag, 'images': self.images,
                       'wasm_sha256': self.sha, 'kafka_topic': self.topic, 'consumer': consumer,
                       'requests': results, 'ignored_requests': ignored, 'existing_gateway_unchanged': True}
            write_json(self.output / 'result.json', summary)
            print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
            print('PASS：Kubernetes 网关 → Observer → Compose Kafka → Kubernetes Consumer → Compose Doris', flush=True)
            print('测试资源已保留；清理：make k8s-clean', flush=True)
        except BaseException:
            self.diagnostics()
            write_json(self.output / 'result.json', {'status': 'failed', 'namespace': self.namespace})
            print('FAIL：资源及诊断已保留，修复后可重跑或执行 make k8s-clean', file=sys.stderr)
            raise


def main(argv=None, *, action=None):
    parser = argparse.ArgumentParser(description=__doc__)
    if action is None:
        parser.add_argument('action', choices=['test', 'cleanup'])
    else:
        parser.set_defaults(action=action)
    parser.add_argument('--context', default='kind-kind')
    parser.add_argument('--kind-cluster', default='kind')
    parser.add_argument('--namespace', type=namespace_name, default='metering-wasm-verify')
    args = parser.parse_args(argv)
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', args.kind_cluster) or args.context != 'kind-' + args.kind_cluster:
        parser.error('本用例只操作指定 kind 集群，--context 必须为 kind-<kind-cluster>')
    harness = Harness(args)
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        if args.action == 'test':
            harness.test()
        else:
            harness.cleanup()
    except KeyboardInterrupt:
        print('已中断；可使用清理脚本移除保留的测试资源。', file=sys.stderr)
        return 130
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
