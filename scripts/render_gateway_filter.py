"""Render a gateway workload EnvoyFilter; disabled unless --enable is explicit."""
import argparse
import hashlib
import json
import re
from pathlib import Path
from validate_collection_contracts import load, validate_config
from wasm_artifact import remote_source
from yaml_block import yaml_block
from gateway_workload import DEFAULT_FILTER_NAME, filter_name, label, selector

ROOT = Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--gateway-label', type=label, action='append', help='网关标签 KEY=VALUE，可重复；默认 istio=ingressgateway')
p.add_argument('--gateway-namespace', default='istio-system')
p.add_argument('--envoyfilter-name', type=filter_name, default=DEFAULT_FILTER_NAME,
               help='EnvoyFilter 名称；同一命名空间多网关独立配置时须显式使用不同名称')
p.add_argument('--config', type=Path, default=ROOT/'configs/gateway-observer.example.json')
p.add_argument('--enable', action='store_true')
p.add_argument('--wasm-file', type=Path, default=ROOT/'dist/gateway-observer-v1.wasm')
p.add_argument('--wasm-url', default='http://llm-token-stats-observer.metering.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm')
p.add_argument('--wasm-cluster', default='outbound|9093||llm-token-stats-observer.metering.svc.cluster.local')
p.add_argument('--output', type=Path, required=True)
args = p.parse_args()
if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', args.gateway_namespace):
    p.error('--gateway-namespace must be a Kubernetes DNS label')
try:
    labels = selector(args.gateway_label)
except ValueError as exc:
    p.error(str(exc))
try:
    remote = remote_source(args.wasm_file, args.wasm_url, args.wasm_cluster)
except ValueError as exc:
    p.error(str(exc))
config = load(args.config)
config['enabled'] = args.enable
validate_config(config, release=True)
plugin_yaml = yaml_block(config)
if len(plugin_yaml.encode('utf-8')) > 65536:
    p.error('rendered plugin YAML exceeds 64 KiB')
text = (ROOT/'deploy/gateway/envoyfilter.yaml').read_text()
text = text.replace('REPLACE_ENVOYFILTER_NAME', args.envoyfilter_name)
# 旧 Envoy 的 ECDS 订阅持有监听器上下文；配置变化必须使用独立订阅，避免复用失效上下文。
ecds_key = json.dumps([args.envoyfilter_name, args.gateway_namespace, config, remote], sort_keys=True)
ecds_name = 'llm-token-stats-ecds-' + hashlib.sha256(ecds_key.encode()).hexdigest()[:16]
text = text.replace('REPLACE_ECDS_NAME', ecds_name if args.enable else args.envoyfilter_name)
text = text.replace('REPLACE_WASM_URL', json.dumps(remote['http_uri']['uri']))
text = text.replace('REPLACE_WASM_CLUSTER', json.dumps(remote['http_uri']['cluster']))
text = text.replace('REPLACE_WASM_SHA256', json.dumps(remote['sha256']))
text = text.replace('REPLACE_GATEWAY_LABELS', '\n'.join('      ' + line for line in yaml_block(labels).splitlines()))
text = text.replace('REPLACE_GATEWAY_NAMESPACE', json.dumps(args.gateway_namespace))
text = text.replace('REPLACE_PLUGIN_YAML', '\n'.join('                  ' + line for line in plugin_yaml.splitlines()))
if not args.enable:
    text = '\n'.join(line for line in text.split('\n') if 'proxyVersion:' not in line)
    start = text.index('    - applyTo: EXTENSION_CONFIG')
    end = text.index('    - applyTo: HTTP_FILTER')
    text = text[:start] + text[end:]
    text = text[:text.index('          config_discovery:')] + '''          typed_config:
            '@type': type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua
            inline_code: |
              function envoy_on_request(handle)
              end
'''
text = text.replace('# 模板需经 render_gateway_filter.py 渲染；默认关闭时直接使用 Lua 透传。',
                    '# 已渲染，可直接 kubectl apply；关闭态不依赖 Wasm 加载。')
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(text)
print(f'Rendered {args.output}; name={args.envoyfilter_name}; enabled={args.enable}; namespace={args.gateway_namespace}; labels={labels}.')
