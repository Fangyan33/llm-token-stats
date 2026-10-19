import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import package_distribution as bundle


class DistributionTests(unittest.TestCase):
    def args(self, *values):
        return bundle.parser().parse_args(list(values))

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
        self.assertEqual(observer['observer']['deployment_environment'], 'development')
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

    def test_gateway_outputs_have_real_identity_exact_wasm_and_both_states(self):
        args = self.args('--namespace', 'isolated')
        pod = bundle.gateway_identity('ingressgateway-abc=7f6c2a90-4b13-4b68-9d21-67283e0c5f44')
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            artifact = output / 'test.wasm'
            artifact.write_bytes(b'\x00asm\x01\x00\x00\x00synthetic')
            bundle.render_manifests(args, self.images(), [pod], output, wasm_file=artifact)
            for mode in ('disabled', 'enabled'):
                text = (output / 'gateway' / mode / (pod['uid'] + '.yaml')).read_text()
                self.assertNotIn('REPLACE_', text)
                self.assertNotIn('RENDER_', text)
                self.assertNotIn('Template only', text)
                self.assertIn('NAME: "ingressgateway-abc"', text)
                self.assertIn(bundle.checksum(artifact), text)
                self.assertIn('metering-observer.isolated.svc.cluster.local', text)
                line = next(row.strip() for row in text.splitlines() if row.strip().startswith('value: "'))
                config = json.loads(json.loads(line[len('value: '):]))
                self.assertEqual(config['enabled'], mode == 'enabled')
                self.assertEqual(config['gateway_instance_id'], pod['uid'])
            self.assertFalse((output / 'gateway-observer.json').exists())

    def test_failed_docker_save_does_not_publish_partial_package(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.args('--output', str(Path(directory) / 'package'))
            with patch.object(bundle, 'checksum', return_value='wasm-sha'), \
                    patch.object(bundle, 'gateway_pods', return_value=[]), \
                    patch.object(bundle, 'resolve_image_tag', return_value='version'), \
                    patch.object(bundle, 'render_manifests'), \
                    patch.object(bundle, 'prepare_images', return_value={}), \
                    patch.object(bundle, 'run', side_effect=subprocess.CalledProcessError(1, ['docker', 'save'])):
                with self.assertRaises(subprocess.CalledProcessError):
                    bundle.package(args)
            self.assertFalse(args.output.exists())
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_successful_package_saves_all_manifest_images_and_checksums(self):
        def docker_save(command, **kwargs):
            self.assertEqual(command[:2], ['docker', 'save'])
            Path(command[command.index('--output') + 1]).write_bytes(b'synthetic archive')

        with tempfile.TemporaryDirectory() as directory:
            args = self.args('--output', str(Path(directory) / 'package'))
            with patch.object(bundle, 'checksum', return_value='wasm-sha'), \
                    patch.object(bundle, 'gateway_pods', return_value=[]), \
                    patch.object(bundle, 'resolve_image_tag', return_value='version'), \
                    patch.object(bundle, 'render_manifests'), \
                    patch.object(bundle, 'prepare_images', return_value={}), \
                    patch.object(bundle, 'run', side_effect=docker_save) as run:
                bundle.package(args)
            manifest = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual(set(run.call_args.args[0][4:]), set(manifest['images'].values()))
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

    def test_gateway_discovery_is_read_only_and_excludes_terminating_pods(self):
        uid = '7f6c2a90-4b13-4b68-9d21-67283e0c5f44'
        data = {'items': [{'metadata': {'name': 'gateway-active', 'uid': uid}},
                          {'metadata': {'name': 'gateway-old', 'uid': uid, 'deletionTimestamp': 'now'}}]}
        with patch.object(bundle, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps(data))) as run:
            pods = bundle.gateway_pods(self.args('--context', 'target'))
        self.assertEqual(pods, [{'name': 'gateway-active', 'uid': uid}])
        self.assertEqual(run.call_args.args[0], ['kubectl', '--context', 'target', '-n', 'istio-system',
                                                'get', 'pods', '-l', 'istio=ingressgateway', '-o', 'json'])


if __name__ == '__main__':
    unittest.main()
