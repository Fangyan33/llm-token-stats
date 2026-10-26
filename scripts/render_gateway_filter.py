"""Render a gateway workload EnvoyFilter; disabled unless --enable is explicit."""
import argparse
import json
import re
from pathlib import Path
from validate_collection_contracts import load, validate_config
from wasm_artifact import remote_source
from gateway_workload import filter_name, label, selector

ROOT = Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--gateway-label', type=label, action='append', help='网关标签 KEY=VALUE，可重复；默认 istio=ingressgateway')
p.add_argument('--gateway-namespace', default='istio-system')
p.add_argument('--config', type=Path, default=ROOT/'configs/gateway-observer.example.json')
p.add_argument('--enable', action='store_true')
p.add_argument('--wasm-file', type=Path, default=ROOT/'dist/gateway-observer-v1.wasm')
p.add_argument('--wasm-url', default='http://metering-observer.metering.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm')
p.add_argument('--wasm-cluster', default='outbound|9093||metering-observer.metering.svc.cluster.local')
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
text = (ROOT/'deploy/gateway/envoyfilter.yaml').read_text()
text = text.replace('REPLACE_ENVOYFILTER_NAME', filter_name(labels))
text = text.replace('REPLACE_WASM_URL', json.dumps(remote['http_uri']['uri']))
text = text.replace('REPLACE_WASM_CLUSTER', json.dumps(remote['http_uri']['cluster']))
text = text.replace('REPLACE_WASM_SHA256', json.dumps(remote['sha256']))
text = text.replace('REPLACE_GATEWAY_LABELS', json.dumps(labels))
text = text.replace('REPLACE_GATEWAY_NAMESPACE', json.dumps(args.gateway_namespace))
text = text.replace('REPLACE_WITH_DEFAULT_DISABLED_RENDERED_CONFIG_JSON', json.dumps(json.dumps(config,ensure_ascii=False,separators=(',',':')),ensure_ascii=False))
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(text)
print(f'Rendered {args.output}; name={filter_name(labels)}; enabled={args.enable}; namespace={args.gateway_namespace}; labels={labels}.')
