"""Record local release inputs without collecting credentials or payloads."""
import hashlib
import json
import subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def version(*cmd):return subprocess.check_output(cmd,text=True,cwd=ROOT).strip()
sdk=ROOT/'third_party/proxy-wasm-go-sdk'
files=sorted([sdk/'go.mod',sdk/'PATCHES.md',*sdk.glob('proxywasm/**/*.go')])
digest=hashlib.sha256()
for f in files:
 digest.update(str(f.relative_to(sdk)).encode()+b'\0'+bytes.fromhex(sha(f)))
manifest={'go':version('go','version'),'tinygo':version('tinygo','version'),'sdk_base':'v0.24.0','sdk_tree_sha256':digest.hexdigest(),'files':{str(f.relative_to(ROOT)):sha(f) for f in [ROOT/'go.mod',ROOT/'go.sum',ROOT/'api/observation/v1/observation.proto',ROOT/'api/events/v1/metering-event.schema.json',ROOT/'dist/gateway-observer-v1.wasm',ROOT/'dist/observer',ROOT/'dist/consumer',ROOT/'dist/usage-worker']}}
out=ROOT/'dist/collection-manifest.json';out.write_text(json.dumps(manifest,indent=2)+'\n');print(out)
