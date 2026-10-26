import argparse
import json
import re
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import package_distribution as bundle


class DistributionTests(unittest.TestCase):
    def args(self, *values):
        if not any(v == '--accepted-request-host' or v.startswith('--accepted-request-host=') for v in values):
            values = (*values, '--accepted-request-host', 'model.example.invalid',
                      '--accepted-request-host', '*.example.invalid')
        return bundle.parser().parse_args(list(values))

    def test_invalid_explicit_host_list_is_rejected_before_packaging(self):
        for patterns in ([], [''], ['K.example.com'], ['api.example.com:443'],
                         ['model-*.example.com'], ['api.example.com', 'API.EXAMPLE.COM.'],
                         ['*'] * 1025):
            with self.subTest(patterns=str(patterns)[:80]), tempfile.TemporaryDirectory() as directory:
                args = self.args('--output', str(Path(directory) / 'package'))
                args.accepted_request_host = patterns
                with patch.object(bundle, 'prepare_images') as images, \
                     patch.object(bundle, 'run') as run:
                    with self.assertRaises(ValueError):
                        bundle.package(args)
                    images.assert_not_called()
                    run.assert_not_called()
                self.assertFalse(args.output.exists())

    def images(self):
        return {'observer': 'metering-observer:test', 'consumer': 'metering-consumer:test',
                **bundle.RUNTIME_IMAGES}

    def walk(self, value):
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from self.walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from self.walk(child)

    def test_namespace_storage_and_all_pod_images_are_self_contained(self):
        args = self.args('--namespace', 'isolated', '--storage-class', 'offline-storage')
        resources = (bundle.infrastructure(args, self.images()) + bundle.configurations(args)
                     + bundle.initialization(args, self.images()) + bundle.applications(args, self.images()))
        self.assertTrue(all(item['metadata']['namespace'] == 'isolated' for item in resources))
        pod_images = {item['image'] for item in self.walk(resources) if 'image' in item}
        self.assertEqual(pod_images, set(self.images().values()) - {self.images()['gateway']})
        objects = {(item['kind'], item['metadata']['name']) for item in resources}
        for item in self.walk(resources):
            for key, kind in (('configMap', 'ConfigMap'), ('secretRef', 'Secret')):
                if key in item:
                    self.assertIn((kind, item[key]['name']), objects)
            if 'image' in item:
                self.assertEqual(item['imagePullPolicy'], 'IfNotPresent')
        for workload in (r for r in resources if r['kind'] in ('StatefulSet', 'Deployment', 'Job')):
            annotations = workload['spec']['template']['metadata']['annotations']
            self.assertEqual(annotations['sidecar.istio.io/inject'], 'false')
        claims = [item for item in self.walk(resources) if 'storageClassName' in item]
        self.assertEqual(len(claims), 3)
        self.assertTrue(all(item['storageClassName'] == 'offline-storage' for item in claims))

    def test_configs_share_topic_database_and_migrations_in_required_order(self):
        args = self.args('--namespace', 'isolated', '--database', 'offline_data', '--topic', 'offline-events')
        objects = {r['metadata']['name']: r for r in bundle.configurations(args)}
        observer = json.loads(objects['metering-observer']['data']['observer.json'])
        consumer = json.loads(objects['metering-consumer']['data']['consumer.json'])
        self.assertEqual(observer['kafka'], consumer['kafka'])
        self.assertEqual(observer['kafka'], {'bootstrap_servers': ['kafka.isolated.svc.cluster.local:9092'],
                                            'topic': 'offline-events'})
        self.assertEqual(observer['observer']['deployment_environment'], 'dev')
        source = json.loads((bundle.ROOT / 'configs/observer.production.example.json').read_text())
        for kind in ('service_lookup', 'customer_lookup'):
            self.assertEqual(observer['observer'][kind]['cache'], source['observer'][kind]['cache'])
        for settings in (observer['observer'], consumer['consumer']):
            self.assertNotIn('accepted_service_ids', settings)
            self.assertNotIn('accepted_request_hosts', settings)
        self.assertEqual(consumer['doris']['database'], 'offline_data')
        self.assertEqual(consumer['doris']['fe_url'], 'http://doris.isolated.svc.cluster.local:8040')
        sql = objects['metering-doris-migrations']['data']
        self.assertEqual(list(sql), list(bundle.MIGRATIONS))
        self.assertIn('CREATE DATABASE IF NOT EXISTS offline_data;', sql['001_init.sql'])
        jobs = bundle.initialization(args, self.images())
        script = jobs[1]['spec']['template']['spec']['containers'][0]['command'][2]
        positions = [script.index('/migrations/' + name) for name in bundle.MIGRATIONS]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('application_id', script)
        self.assertIn('$alive=="true"', script)
        for job in jobs:
            subprocess.run(['bash', '-n'], input=job['spec']['template']['spec']['containers'][0]['command'][2],
                           text=True, check=True, capture_output=True)

    def test_yaml_scalars_preserve_booleans_numeric_strings_and_multiline(self):
        text = bundle.yaml_text({'data': {'empty': '', 'bool': 'false', 'number': '9092',
                                          'config': 'a\nb\n'}, 'enabled': False, 'items': [1, {}]})
        self.assertIn('"empty": ""\n', text)
        self.assertIn('"bool": "false"\n', text)
        self.assertIn('"number": "9092"\n', text)
        self.assertIn('"config": |\n    a\n    b\n', text)
        self.assertIn('"enabled": false\n', text)

    def test_gateway_outputs_use_workload_selector_without_static_identity(self):
        args = self.args('--namespace', 'isolated', '--gateway-label', 'app=ai-gateway',
                         '--accepted-request-host', 'API.Corp.internal.',
                         '--accepted-request-host', '*.Corp.internal.')
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            artifact = output / 'test.wasm'
            artifact.write_bytes(b'\x00asm\x01\x00\x00\x00synthetic')
            bundle.render_manifests(args, self.images(), output, wasm_file=artifact)
            names = []
            for mode in ('disabled', 'enabled'):
                text = (output / 'gateway' / mode / 'envoyfilter.yaml').read_text()
                names.append(re.search(r'^metadata: \{name: ([a-z0-9-]+),', text, re.M).group(1))
                self.assertNotIn('REPLACE_', text)
                self.assertNotIn('RENDER_', text)
                self.assertNotIn('Template only', text)
                self.assertIn('proxy:\n          metadata:\n            NAMESPACE: \"istio-system\"', text)
                self.assertNotIn('            NAME:', text)
                self.assertIn('"app": "ai-gateway"', text)
                self.assertIn(bundle.checksum(artifact), text)
                self.assertIn('metering-observer.isolated.svc.cluster.local', text)
                line = next(row.strip() for row in text.splitlines() if row.strip().startswith('value: "'))
                config = json.loads(json.loads(line[len('value: '):]))
                self.assertEqual(config['enabled'], mode == 'enabled')
                self.assertNotIn('gateway_instance_id', config)
                self.assertEqual(config['accepted_request_hosts'], ['api.corp.internal', '*.corp.internal'])
            self.assertEqual((output / 'envoyfilter.yaml').read_text(),
                             (output / 'gateway/disabled/envoyfilter.yaml').read_text())
            self.assertFalse((output / 'gateway-observer.json').exists())
            self.assertEqual(names[0], names[1])

    def test_omitted_hosts_default_all_and_explicit_hosts_replace_default(self):
        args = bundle.parser().parse_args([])
        self.assertEqual(bundle.gateway_configuration(args)['accepted_request_hosts'], ['*'])
        args = bundle.parser().parse_args(['--accepted-request-host', 'API.Corp.internal.'])
        self.assertEqual(bundle.gateway_configuration(args)['accepted_request_hosts'], ['api.corp.internal'])

    def test_default_host_and_envoyfilter_survive_packaging_and_tgz(self):
        import release_package as release
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wasm_file = root / 'test.wasm'
            wasm_file.write_bytes(b'\x00asm\x01\x00\x00\x00synthetic')
            args = bundle.parser().parse_args([
                '--output', str(root / 'package')])
            original_run = bundle.run
            original_render = bundle.render_manifests

            def run(command, **kwargs):
                if command[:2] == ['docker', 'save']:
                    Path(command[command.index('--output') + 1]).write_bytes(b'synthetic archive')
                    return
                self.assertNotEqual(command[0], 'kubectl')
                return original_run(command, **kwargs)

            def render(args, images, output):
                original_render(args, images, output, wasm_file=wasm_file)

            with patch.object(bundle, 'checksum', return_value=bundle.file_sha256(wasm_file)), \
                    patch.object(bundle, 'resolve_image_tag', return_value='version'), \
                    patch.object(bundle, 'prepare_images', return_value={}), \
                    patch.object(bundle, 'render_manifests', side_effect=render), \
                    patch.object(bundle, 'run', side_effect=run):
                bundle.package(args)
            manifest = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual(manifest['accepted_request_hosts'], ['*'])
            self.assertNotIn('gateway_pods', manifest)
            self.assertEqual(manifest['gateway_selector'], {'istio': 'ingressgateway'})
            self.assertIn('metadata: {name: ' + manifest['gateway_filter_name'] + ',',
                          (args.output / 'envoyfilter.yaml').read_text())
            text = (args.output / 'envoyfilter.yaml').read_text()
            config_line = next(line.strip() for line in text.splitlines() if line.strip().startswith('value: "'))
            config = json.loads(json.loads(config_line[len('value: '):]))
            self.assertEqual(config['accepted_request_hosts'], ['*'])
            self.assertIs(config['enabled'], False)
            self.assertIn(bundle.file_sha256(args.output / 'envoyfilter.yaml') + '  envoyfilter.yaml',
                          (args.output / 'SHA256SUMS').read_text())
            parts = release.archive_package('v1.0.0', args.output, root / 'release')
            with tarfile.open(parts[0], 'r:gz') as archive:
                self.assertEqual(archive.extractfile('package/envoyfilter.yaml').read().decode(), text)
                for mode in ('disabled', 'enabled'):
                    self.assertIn('package/gateway/' + mode + '/envoyfilter.yaml',
                                  archive.getnames())

    def test_explicit_all_hosts_is_supported(self):
        args = self.args('--accepted-request-host', '*')
        self.assertEqual(bundle.gateway_configuration(args)['accepted_request_hosts'], ['*'])

    def test_failed_docker_save_does_not_publish_partial_package(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.args('--output', str(Path(directory) / 'package'))
            with patch.object(bundle, 'checksum', return_value='wasm-sha'), \
                    patch.object(bundle, 'resolve_image_tag', return_value='version'), \
                    patch.object(bundle, 'render_manifests'), \
                    patch.object(bundle, 'prepare_images', return_value={}), \
                    patch.object(bundle, 'run', side_effect=subprocess.CalledProcessError(1, ['docker', 'save'])):
                with self.assertRaises(subprocess.CalledProcessError):
                    bundle.package(args)
            self.assertFalse(args.output.exists())
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_successful_package_saves_selected_images_and_checksums(self):
        def docker_save(command, **kwargs):
            self.assertEqual(command[:2], ['docker', 'save'])
            Path(command[command.index('--output') + 1]).write_bytes(b'synthetic archive')

        for include_dependencies in (False, True):
            with self.subTest(include_dependencies=include_dependencies), tempfile.TemporaryDirectory() as directory:
                options = ['--include-dependency-images'] if include_dependencies else []
                args = self.args('--output', str(Path(directory) / 'package'), *options)
                with patch.object(bundle, 'checksum', return_value='wasm-sha'), \
                        patch.object(bundle, 'resolve_image_tag', return_value='version'), \
                        patch.object(bundle, 'render_manifests') as render, \
                        patch.object(bundle, 'prepare_images', return_value={}) as prepare, \
                        patch.object(bundle, 'run', side_effect=docker_save) as run:
                    bundle.package(args)
                manifest = json.loads((args.output / 'manifest.json').read_text())
                self.assertEqual(manifest['accepted_request_hosts'], ['model.example.invalid', '*.example.invalid'])
                self.assertIn(json.dumps(manifest['accepted_request_hosts']), (args.output / 'README.md').read_text())
                expected = {'observer', 'consumer'} | (set(bundle.RUNTIME_IMAGES) if include_dependencies else set())
                self.assertEqual(set(manifest['archived_images']), expected)
                self.assertEqual(set(prepare.call_args.args[1]), expected)
                self.assertEqual(set(run.call_args.args[0][4:]), set(manifest['archived_images'].values()))
                self.assertEqual(set(render.call_args.args[1]), {'observer', 'consumer', *bundle.RUNTIME_IMAGES})
                self.assertEqual(set(manifest['external_images']), set(manifest['images']) - expected)
                sums = (args.output / 'SHA256SUMS').read_text()
                self.assertIn(bundle.file_sha256(args.output / 'images/images.tar') + '  images/images.tar', sums)
                with patch.object(bundle, 'prepare_images') as prepare:
                    with self.assertRaisesRegex(ValueError, '非空'):
                        bundle.package(args)
                prepare.assert_not_called()

    def test_mismatched_image_platform_is_rejected_before_save(self):
        info = [{'Os': 'linux', 'Architecture': 'arm64', 'Id': 'sha256:image'}]
        with patch.object(bundle, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps(info))):
            with self.assertRaisesRegex(ValueError, '平台'):
                bundle.prepare_images(self.args(), self.images())

    def test_workload_selector_defaults_and_validation(self):
        self.assertEqual(bundle.selector(self.args().gateway_label), {'istio': 'ingressgateway'})
        self.assertEqual(bundle.selector(self.args('--gateway-label', 'app=ai-gateway',
                          '--gateway-label', 'example.com/group=prod').gateway_label),
                         {'app': 'ai-gateway', 'example.com/group': 'prod'})
        for value in ('bad/key/name=x', '=foo', 'app=bad value', '/key=value', 'a..b/key=x', 'a'*64+'=x'):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                bundle.label(value)
        with self.assertRaises(ValueError):
            bundle.selector([('app', 'a'), ('app', 'b')])


if __name__ == '__main__':
    unittest.main()
