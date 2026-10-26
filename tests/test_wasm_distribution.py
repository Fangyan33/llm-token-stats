import hashlib
import json
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from wasm_artifact import remote_source


class WasmDistributionTests(unittest.TestCase):
    def test_rendered_filter_uses_exact_artifact_and_workload_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / 'plugin.wasm'
            data = b'\x00asm\x01\x00\x00\x00synthetic'
            artifact.write_bytes(data)
            output = Path(directory) / 'filter.yaml'
            url = 'http://observer.example:9093/wasm/gateway-observer-v1.wasm'
            namespace = 'gateway-test'
            command = [sys.executable, str(ROOT / 'scripts/render_gateway_filter.py'),
                       '--gateway-label', 'app=ai-gateway',
                       '--gateway-namespace', namespace, '--wasm-file', str(artifact),
                       '--wasm-url', url, '--wasm-cluster', 'download-http', '--output', str(output)]
            subprocess.run(command, cwd=ROOT, check=True, capture_output=True)
            text = output.read_text()
            self.assertIn('sha256: "' + hashlib.sha256(data).hexdigest() + '"', text)
            self.assertIn('uri: ' + json.dumps(url), text)
            self.assertIn('cluster: "download-http"', text)
            self.assertIn('proxy:\n          metadata:\n            NAMESPACE: ' + json.dumps(namespace), text)
            self.assertNotIn('            NAME:', text)
            self.assertIn('labels: {"app": "ai-gateway"}', text)
            self.assertIn('namespace: ' + json.dumps(namespace), text)
            self.assertNotIn('metering-instance', text)
            self.assertRegex(text, r'metadata: \{name: gateway-observer-[0-9a-f]{16},')
            self.assertNotIn('gateway_instance_id', text)
            self.assertNotIn('filename:', text)
            self.assertNotIn('REPLACE_', text)
            value = next(line.strip()[len('value: '):] for line in text.splitlines() if line.strip().startswith('value: "'))
            self.assertFalse(json.loads(json.loads(value))['enabled'])
            artifact.write_bytes(b'not wasm')
            output.unlink()
            result = subprocess.run(command, cwd=ROOT, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(output.exists())

    def test_namespace_guard_and_distinct_stable_workload_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / 'plugin.wasm'
            artifact.write_bytes(b'\x00asm\x01\x00\x00\x00synthetic')
            output = root / 'filter.yaml'

            def render(namespace, labels, enabled=False):
                command = [sys.executable, str(ROOT / 'scripts/render_gateway_filter.py'),
                           '--gateway-namespace', namespace, '--wasm-file', str(artifact),
                           '--output', str(output)]
                for label in labels:
                    command += ['--gateway-label', label]
                if enabled:
                    command += ['--enable']
                subprocess.run(command, cwd=ROOT, check=True, capture_output=True)
                text = output.read_text()
                name = re.search(r'^metadata: \{name: ([a-z0-9-]+),', text, re.M).group(1)
                self.assertIn('proxy:\n          metadata:\n            NAMESPACE: ' + json.dumps(namespace), text)
                self.assertNotIn('            NAME:', text)
                return name, text

            # Root-namespace filters must still restrict proxy NAMESPACE.
            production, disabled = render('istio-system', ['app=production', 'example.com/env=prd'])
            canary, _ = render('istio-system', ['app=canary', 'example.com/env=prd'])
            self.assertNotEqual(production, canary)
            # Enabling, label argument order and a new Wasm version update one resource.
            artifact.write_bytes(b'\x00asm\x01\x00\x00\x00new-version')
            enabled, text = render('istio-system', ['example.com/env=prd', 'app=production'], True)
            self.assertEqual(production, enabled)
            self.assertNotEqual(disabled, text)
            render('gateway-test', ['app=production'])

    def test_renderer_rejects_invalid_namespace_or_selector(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / 'plugin.wasm'
            artifact.write_bytes(b'\x00asm\x01\x00\x00\x00')
            command = [sys.executable, str(ROOT / 'scripts/render_gateway_filter.py'),
                       '--wasm-file', str(artifact), '--output', str(Path(directory) / 'filter.yaml')]
            for args in [['--gateway-label', 'bad/key/name=value'],
                         ['--gateway-label', 'app=a', '--gateway-label', 'app=b'],
                         ['--gateway-namespace', 'a.b']]:
                result = subprocess.run([*command, *args], cwd=ROOT, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
            self.assertFalse((Path(directory) / 'filter.yaml').exists())

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
