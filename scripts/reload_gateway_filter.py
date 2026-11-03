"""显式重推项目 ECDS 配置；不启动常驻控制器，不重启网关。"""
import argparse
import json
import re
import subprocess
import time
import uuid

from gateway_workload import DEFAULT_FILTER_NAME, filter_name, label

WASM = 'type.googleapis.com/envoy.extensions.filters.http.wasm.v3.Wasm'
ERRORS = ('metering_config_invalid_total', 'metering_gateway_identity_unavailable_total')


def extension(resource):
    name = filter_name(resource['metadata']['name'])
    matches = [(i, p['patch']['value']) for i, p in enumerate(resource['spec']['configPatches'])
               if p['applyTo'] == 'EXTENSION_CONFIG' and p['patch']['value'].get('typed_config', {}).get('config', {}).get('name') == 'gateway-observer']
    if len(matches) != 1 or matches[0][1].get('typed_config', {}).get('@type') != WASM:
        raise ValueError('过滤器未启用项目 Wasm ECDS；关闭态不需要重推')
    i, value = matches[0]
    config = value['typed_config']['config']
    vm = config['vm_config']
    if (config.get('name') != 'gateway-observer' or config.get('fail_open') is not True
            or vm.get('nack_on_code_cache_miss') is not True or 'remote' not in vm['code']):
        raise ValueError('过滤器不符合项目 ECDS fail-open 配置')
    for patch in resource['spec']['configPatches']:
        if patch.get('match', {}).get('proxy', {}).get('metadata', {}).get('NAMESPACE') != resource['metadata']['namespace']:
            raise ValueError('过滤器缺少一致的代理命名空间隔离')
    http = [p['patch']['value'] for p in resource['spec']['configPatches'] if p['applyTo'] == 'HTTP_FILTER']
    if len(http) != 1 or http[0].get('name') != value['name'] or 'config_discovery' not in http[0]:
        raise ValueError('ECDS 订阅与扩展配置不匹配')
    return i, vm


def parse_stats(raw):
    result = {}
    for line in raw.splitlines():
        key, sep, value = line.rpartition(': ')
        if sep and value.isdigit():
            result[key] = int(value)
    return result


def reload_counts(stats, name):
    token = '.' + name + '.'
    return {k: v for k, v in stats.items() if 'extension_config_discovery.' in k and token in k and k.endswith('.config_reload')}


def error_counts(stats):
    return {suffix: sum(v for k, v in stats.items() if k.endswith(suffix)) for suffix in ERRORS}


class Reloader:
    def __init__(self, context, namespace, name, timeout=120):
        self.kube = ['kubectl', '--context', context, '--request-timeout=15s', '-n', namespace]
        self.name, self.timeout = name, timeout

    def command(self, *args):
        try:
            return subprocess.run([*self.kube, *args], check=True, capture_output=True,
                                  text=True, timeout=20).stdout
        except (subprocess.SubprocessError, OSError) as from_error:
            raise RuntimeError('kubectl 操作失败；请检查目标上下文、权限和网关状态') from from_error

    def resource(self):
        return json.loads(self.command('get', 'envoyfilter', self.name, '-o', 'json'))

    def pods(self, resource):
        labels = resource['spec']['workloadSelector']['labels']
        if not labels:
            raise ValueError('拒绝对无工作负载选择器的过滤器执行重推')
        for key, value in labels.items():
            label(key + '=' + value)
        selector = ','.join(k + '=' + v for k, v in sorted(labels.items()))
        pods = json.loads(self.command('get', 'pods', '-l', selector, '-o', 'json'))['items']
        pods = [p for p in pods if not p['metadata'].get('deletionTimestamp')]
        if not pods:
            raise ValueError('没有匹配的网关 Pod')
        for pod in pods:
            containers = [c for c in pod['spec']['containers'] if c['name'] == 'istio-proxy']
            if len(containers) != 1 or not any(e.get('name') == 'ISTIO_AGENT_ENABLE_WASM_REMOTE_LOAD_CONVERSION'
                                              and e.get('value') == 'false' for e in containers[0].get('env', [])):
                raise ValueError('目标网关必须显式关闭 Agent ECDS 下载转换并完成滚动')
            statuses = [c for c in pod['status'].get('containerStatuses', []) if c['name'] == 'istio-proxy']
            if not statuses or not statuses[0].get('ready'):
                raise ValueError('目标网关尚未 Ready，请就绪后重试')
        return pods

    def stats(self, pod):
        return parse_stats(self.command('exec', pod, '-c', 'istio-proxy', '--',
                                        'pilot-agent', 'request', 'GET', 'stats'))

    def reload(self):
        resource = self.resource()
        extension(resource)
        pods = self.pods(resource)
        def identity(p):
            return (p['metadata']['uid'], tuple((c['name'], c.get('restartCount', 0))
                    for c in p.get('status', {}).get('containerStatuses', [])))
        identities = {p['metadata']['name']: identity(p) for p in pods}
        baseline = {name: self.stats(name) for name in identities}
        initial_i, _ = extension(resource)
        initial_name = resource['spec']['configPatches'][initial_i]['patch']['value']['name']
        if any(not reload_counts(sample, initial_name) for sample in baseline.values()):
            raise ValueError('目标网关版本未匹配 proxyVersion 或未导出 ECDS 指标；请核对版本，并在 proxy.istio.io/config 的 proxyStatsMatcher.inclusionRegexps 中保留 .*extension_config_discovery.* 并完成滚动')
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            i, _ = extension(resource)
            revision = uuid.uuid4().hex
            # 独立订阅避免旧 Envoy 跨监听器复用 factory context；不通过重建 VM 重试。
            discovery = 'llm-token-stats-ecds-' + revision
            http_i = next(j for j, p in enumerate(resource['spec']['configPatches']) if p['applyTo'] == 'HTTP_FILTER')
            patch = [{'op': 'test', 'path': '/metadata/resourceVersion', 'value': resource['metadata']['resourceVersion']},
                     {'op': 'replace', 'path': f'/spec/configPatches/{i}/patch/value/name', 'value': discovery},
                     {'op': 'replace', 'path': f'/spec/configPatches/{http_i}/patch/value/name', 'value': discovery}]
            resource = json.loads(self.command('patch', 'envoyfilter', self.name, '--type=json',
                                               '-p', json.dumps(patch), '-o', 'json'))
            stop = min(deadline, time.monotonic() + 15)
            while time.monotonic() < stop:
                current = self.resource()
                if current['spec'] != resource['spec']:
                    raise RuntimeError('过滤器被并发修改，停止重推；未覆盖外部变更')
                now_pods = self.pods(current)
                if {p['metadata']['name']: identity(p) for p in now_pods} != identities:
                    raise RuntimeError('目标 Pod 已变化，请重新执行重推')
                samples = {name: self.stats(name) for name in identities}
                for name, stats in samples.items():
                    if any(v > error_counts(baseline[name])[k] for k, v in error_counts(stats).items()):
                        raise RuntimeError('插件配置或运行时身份被拒绝，业务继续透传；请修正后重推')
                if all(reload_counts(s, discovery) and all(v > 0 for v in reload_counts(s, discovery).values())
                       for s in samples.values()):
                    return {'pods': sorted(identities), 'revision': revision, 'ecds_loaded': True, 'discovery_name': discovery}
                time.sleep(min(1, max(0, stop - time.monotonic())))
        raise RuntimeError('ECDS 加载超时；保留旧有效配置或透传默认配置，业务不受重推命令阻断')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--context', required=True)
    p.add_argument('--gateway-namespace', default='istio-system')
    p.add_argument('--envoyfilter-name', type=filter_name, default=DEFAULT_FILTER_NAME)
    p.add_argument('--timeout-seconds', type=int, default=120)
    args = p.parse_args()
    if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', args.gateway_namespace) or args.timeout_seconds < 1:
        p.error('命名空间或等待时间无效')
    try:
        result = Reloader(args.context, args.gateway_namespace, args.envoyfilter_name, args.timeout_seconds).reload()
    except (ValueError, RuntimeError, KeyError) as exc:
        p.exit(1, str(exc) + '\n')
    print(json.dumps(result, ensure_ascii=False))
    print('ECDS 已加载；仍须用实际采集请求及事件验证采集效果。')


if __name__ == '__main__':
    main()
