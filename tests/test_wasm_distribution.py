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
from yaml_support import decode_yaml, plugin_config
from yaml_block import yaml_block


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
                       '--enable', '--gateway-label', 'app=ai-gateway',
                       '--gateway-namespace', namespace, '--wasm-file', str(artifact),
                       '--wasm-url', url, '--wasm-cluster', 'download-http', '--output', str(output)]
            subprocess.run(command, cwd=ROOT, check=True, capture_output=True)
            text = output.read_text()
            self.assertIn('sha256: "' + hashlib.sha256(data).hexdigest() + '"', text)
            self.assertIn('uri: ' + json.dumps(url), text)
            self.assertIn('cluster: "download-http"', text)
            self.assertIn('proxy:\n          metadata:\n            NAMESPACE: ' + json.dumps(namespace), text)
            self.assertNotIn('            NAME:', text)
            self.assertEqual(decode_yaml(text)['spec']['workloadSelector']['labels'], {'app': 'ai-gateway'})
            self.assertIn('namespace: ' + json.dumps(namespace), text)
            self.assertNotIn('metering-instance', text)
            self.assertEqual(decode_yaml(text)['metadata']['name'], 'llm-token-stats-envoyfilter-wasm')
            self.assertNotIn('gateway_instance_id', text)
            self.assertNotIn('filename:', text)
            self.assertNotIn('REPLACE_', text)
            self.assertTrue(plugin_config(decode_yaml(text))['enabled'])
            self.assertTrue(all('proxyVersion' in p['match']['proxy'] for p in decode_yaml(text)['spec']['configPatches']))
            self.assertIn('type.googleapis.com/google.protobuf.Struct', text)
            self.assertNotIn('StringValue', text)
            self.assertNotIn('labels: {', text)
            self.assertIn('nack_on_code_cache_miss: true', text)
            self.assertIn('apply_default_config_without_warming: true', text)
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

            def render(namespace, labels, enabled=False, name=None):
                command = [sys.executable, str(ROOT / 'scripts/render_gateway_filter.py'),
                           '--gateway-namespace', namespace, '--wasm-file', str(artifact),
                           '--output', str(output)]
                for label in labels:
                    command += ['--gateway-label', label]
                if enabled:
                    command += ['--enable']
                if name is not None:
                    command += ['--envoyfilter-name', name]
                subprocess.run(command, cwd=ROOT, check=True, capture_output=True)
                text = output.read_text()
                name = decode_yaml(text)['metadata']['name']
                self.assertIn('proxy:\n          metadata:\n            NAMESPACE: ' + json.dumps(namespace), text)
                self.assertNotIn('            NAME:', text)
                return name, text

            # Root-namespace filters must still restrict proxy NAMESPACE.
            production, disabled = render('istio-system', ['app=production', 'example.com/env=prd'])
            default_canary, _ = render('istio-system', ['app=canary'])
            self.assertEqual(production, 'llm-token-stats-envoyfilter-wasm')
            self.assertEqual(production, default_canary)
            canary, _ = render('istio-system', ['app=canary', 'example.com/env=prd'],
                               name='llm-token-stats-envoyfilter-wasm-canary')
            self.assertNotEqual(production, canary)
            # Enabling, label argument order and a new Wasm version update one resource.
            artifact.write_bytes(b'\x00asm\x01\x00\x00\x00new-version')
            enabled, text = render('istio-system', ['example.com/env=prd', 'app=production'], True)
            self.assertEqual(production, enabled)
            self.assertNotEqual(disabled, text)
            enabled_canary, _ = render('istio-system', ['app=canary', 'example.com/env=prd'], True,
                                       name=canary)
            self.assertEqual(canary, enabled_canary)
            render('gateway-test', ['app=production'])

    def test_renderer_rejects_invalid_namespace_or_selector(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / 'plugin.wasm'
            artifact.write_bytes(b'\x00asm\x01\x00\x00\x00')
            command = [sys.executable, str(ROOT / 'scripts/render_gateway_filter.py'),
                       '--wasm-file', str(artifact), '--output', str(Path(directory) / 'filter.yaml')]
            for args in [['--gateway-label', 'bad/key/name=value'],
                         ['--gateway-label', 'app=a', '--gateway-label', 'app=b'],
                         ['--gateway-namespace', 'a.b'],
                         *[['--envoyfilter-name', name] for name in (
                             'other-filter', 'llm-token-stats-', 'llm-token-stats-a.b',
                             'llm-token-stats-UPPER', 'llm-token-stats-' + 'a' * 49,
                             'llm-token-stats-$(id)')]]:
                result = subprocess.run([*command, *args], cwd=ROOT, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
            self.assertFalse((Path(directory) / 'filter.yaml').exists())

    def test_yaml_strings_and_keys_round_trip_without_implicit_coercion(self):
        value = {'null': 'true', 'on': 'off', 'unicode': '中文\u0085\u2028\u2029end', 'items': []}
        self.assertEqual(decode_yaml(yaml_block(value)), value)

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
