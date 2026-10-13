"""Render one gateway Pod's EnvoyFilter; disabled unless --enable is explicit."""
import argparse
import json
import uuid
from pathlib import Path
from validate_collection_contracts import load, validate_config

ROOT = Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--gateway-pod-uid', required=True)
p.add_argument('--config', type=Path, default=ROOT/'configs/gateway-observer.example.json')
p.add_argument('--enable', action='store_true')
p.add_argument('--output', type=Path, required=True)
args = p.parse_args()
try:
    if str(uuid.UUID(args.gateway_pod_uid)) != args.gateway_pod_uid: raise ValueError()
except ValueError:
    p.error('--gateway-pod-uid must be a canonical lowercase Pod UUID')
config = load(args.config)
config['gateway_instance_id'] = args.gateway_pod_uid
config['enabled'] = args.enable
validate_config(config, release=True)
text = (ROOT/'deploy/gateway/envoyfilter.yaml').read_text()
# A Pod-specific selector is required: one ID cannot be shared by all gateways.
text = text.replace("metering-canary: 'true'", 'metering-instance: '+json.dumps(args.gateway_pod_uid))
text = text.replace('name: gateway-observer, namespace:', 'name: gateway-observer-'+args.gateway_pod_uid+', namespace:')
text = text.replace('REPLACE_WITH_DEFAULT_DISABLED_RENDERED_CONFIG_JSON', json.dumps(json.dumps(config,ensure_ascii=False,separators=(',',':')),ensure_ascii=False))
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(text)
print(f'Rendered {args.output}; enabled={args.enable}. Set the selected gateway Pod label metering-instance to its UID before use.')
