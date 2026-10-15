"""Render one gateway Pod's EnvoyFilter; disabled unless --enable is explicit."""
import argparse
import json
import re
import uuid
from pathlib import Path
from validate_collection_contracts import load, validate_config
from wasm_artifact import remote_source

ROOT = Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--gateway-pod-uid', required=True)
p.add_argument('--gateway-pod-name', required=True)
p.add_argument('--gateway-namespace', default='istio-system')
p.add_argument('--config', type=Path, default=ROOT/'configs/gateway-observer.example.json')
p.add_argument('--enable', action='store_true')
p.add_argument('--wasm-file', type=Path, default=ROOT/'dist/gateway-observer-v1.wasm')
p.add_argument('--wasm-url', default='http://metering-observer.metering.svc.cluster.local:9093/wasm/gateway-observer-v1.wasm')
p.add_argument('--wasm-cluster', default='outbound|9093||metering-observer.metering.svc.cluster.local')
p.add_argument('--output', type=Path, required=True)
args = p.parse_args()
for name, value, maximum in (('--gateway-pod-name', args.gateway_pod_name, 253),
                             ('--gateway-namespace', args.gateway_namespace, 63)):
    valid = (len(value) <= maximum
             and re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', value)
             and all(part and len(part) <= 63 and not part.startswith('-') and not part.endswith('-')
                     for part in value.split('.'))
             and (name != '--gateway-namespace' or '.' not in value))
    if not valid:
        p.error(name + ' must be a Kubernetes DNS name')
try:
    remote = remote_source(args.wasm_file, args.wasm_url, args.wasm_cluster)
except ValueError as exc:
    p.error(str(exc))
try:
    if str(uuid.UUID(args.gateway_pod_uid)) != args.gateway_pod_uid: raise ValueError()
except ValueError:
    p.error('--gateway-pod-uid must be a canonical lowercase Pod UUID')
config = load(args.config)
config['gateway_instance_id'] = args.gateway_pod_uid
config['enabled'] = args.enable
validate_config(config, release=True)
text = (ROOT/'deploy/gateway/envoyfilter.yaml').read_text()
text = text.replace('REPLACE_WASM_URL', json.dumps(remote['http_uri']['uri']))
text = text.replace('REPLACE_WASM_CLUSTER', json.dumps(remote['http_uri']['cluster']))
text = text.replace('REPLACE_WASM_SHA256', json.dumps(remote['sha256']))
# Startup proxy metadata is stable; labels added after startup are stale in Istio 1.13.
text = text.replace('REPLACE_GATEWAY_POD_NAME', json.dumps(args.gateway_pod_name))
text = text.replace('REPLACE_GATEWAY_NAMESPACE', json.dumps(args.gateway_namespace))
text = text.replace('name: gateway-observer, namespace:', 'name: gateway-observer-'+args.gateway_pod_uid+', namespace:')
text = text.replace('REPLACE_WITH_DEFAULT_DISABLED_RENDERED_CONFIG_JSON', json.dumps(json.dumps(config,ensure_ascii=False,separators=(',',':')),ensure_ascii=False))
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(text)
print(f'Rendered {args.output}; enabled={args.enable}; proxy={args.gateway_namespace}/{args.gateway_pod_name}; instance={args.gateway_pod_uid}.')
