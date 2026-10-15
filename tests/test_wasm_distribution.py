import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from wasm_artifact import remote_source


class WasmDistributionTests(unittest.TestCase):
    def test_rendered_filter_uses_exact_artifact_and_keeps_pod_selector(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / 'plugin.wasm'
            data = b'\x00asm\x01\x00\x00\x00synthetic'
            artifact.write_bytes(data)
            output = Path(directory) / 'filter.yaml'
            uid = '7f6c2a90-4b13-4b68-9d21-67283e0c5f44'
            url = 'http://observer.example:9093/wasm/gateway-observer-v1.wasm'
            command = [sys.executable, str(ROOT / 'scripts/render_gateway_filter.py'),
                       '--gateway-pod-uid', uid, '--wasm-file', str(artifact),
                       '--wasm-url', url, '--wasm-cluster', 'download-http', '--output', str(output)]
            subprocess.run(command, cwd=ROOT, check=True, capture_output=True)
            text = output.read_text()
            self.assertIn('sha256: "' + hashlib.sha256(data).hexdigest() + '"', text)
            self.assertIn('uri: ' + json.dumps(url), text)
            self.assertIn('cluster: "download-http"', text)
            self.assertIn('metering-instance: ' + json.dumps(uid), text)
            self.assertIn('gateway-observer-' + uid, text)
            self.assertNotIn('filename:', text)
            self.assertNotIn('REPLACE_', text)
            value = next(line.strip()[len('value: '):] for line in text.splitlines() if line.strip().startswith('value: "'))
            self.assertFalse(json.loads(json.loads(value))['enabled'])
            artifact.write_bytes(b'not wasm')
            output.unlink()
            result = subprocess.run(command, cwd=ROOT, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(output.exists())

    def test_unsafe_urls_and_invalid_artifacts_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / 'plugin.wasm'
            artifact.write_bytes(b'\x00asm\x01\x00\x00\x00')
            for url in ['file:///etc/passwd', 'http://u:p@observer/p.wasm', 'http://observer:bad/p', 'http://observer/p#fragment', 'http://observer/\npath']:
                with self.subTest(url=url), self.assertRaises(ValueError):
                    remote_source(artifact, url, 'download')
            with self.assertRaises(ValueError):
                remote_source(Path(directory) / 'missing', 'http://observer/p', 'download')


if __name__ == '__main__':
    unittest.main()
