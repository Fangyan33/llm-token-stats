import argparse
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import k8s_collection as k8s


class KubernetesCollectionTests(unittest.TestCase):
    def test_builds_and_kind_import_use_git_version_for_all_images(self):
        for version in ('v1.2.3', '0123456789ab'):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                harness = self.harness(directory)
                harness.run_id = 'unrelated-random-run-id'
                with patch.object(k8s, 'resolve_image_tag', return_value=version), \
                        patch.object(k8s, 'run') as run, \
                        patch.object(k8s.k8s_integration, 'prepare_suite'):
                    harness.build_images()
                expected = ['llm-token-stats-' + name + ':' + version for name in ('observer', 'consumer', 'fixture', 'integration')]
                builds = [call.args[0] for call in run.call_args_list if call.args[0][:2] == ['docker', 'build']]
                self.assertEqual([cmd[cmd.index('-t') + 1] for cmd in builds], expected)
                self.assertEqual(run.call_args_list[-2].args[0], ['kind', 'load', 'docker-image',
                                                                 *k8s.k8s_integration.IMAGES, '--name', 'kind'])
                self.assertEqual(run.call_args_list[-1].args[0], ['kind', 'load', 'docker-image',
                                                                 *expected, '--name', 'kind'])
                saved = json.loads((Path(directory) / 'images.json').read_text())
                self.assertEqual(saved['image_tag'], version)
                self.assertEqual(list(saved['images'].values()), expected)

    def test_match_probe_changes_selector_and_restores_shared_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = self.harness(directory)
            resource = {'spec': {'workloadSelector': {'labels': {'istio': harness.namespace}},
                                 'configPatches': [{'match': {'context': 'GATEWAY',
                                                            'proxy': {'metadata': {'NAMESPACE': harness.namespace}}}}]}}
            original = copy.deepcopy(resource)
            with patch.object(harness, 'apply') as apply, \
                    patch.object(harness, 'wait_wasm', return_value={}) as wait, \
                    patch.object(harness, 'request', return_value=(200, {}, b'')):
                harness.check_match(resource)
            self.assertEqual(apply.call_args_list[0].args[0]['spec']['workloadSelector']['labels'],
                             {'istio': 'no-such-verification-workload'})
            self.assertEqual(apply.call_args_list[1].args[0], original)
            self.assertEqual(apply.call_args_list[2].args[0]['spec']['configPatches'][0]['match']['proxy']['metadata'],
                             {'NAMESPACE': 'no-such-verification-namespace'})
            self.assertEqual(apply.call_args_list[3].args[0], original)
            self.assertEqual(resource, original)
            self.assertEqual([call.args for call in wait.call_args_list], [(False,), (), (False,), ()])

    def test_namespace_guard_excludes_existing_system_and_business_namespaces(self):
        self.assertEqual(k8s.namespace_name('metering-wasm-verify'), 'metering-wasm-verify')
        for name in ('default', 'istio-system', 'ai-gateway-ns', 'metering-wasm-',
                     'metering-wasm-../default', 'metering-wasm-' + 'a' * 60):
            with self.assertRaises(argparse.ArgumentTypeError):
                k8s.namespace_name(name)

    def harness(self, directory):
        harness = k8s.Harness(argparse.Namespace(context='kind-kind', kind_cluster='kind',
                                                 namespace='metering-wasm-verify'))
        harness.base = harness.output = Path(directory)
        harness.state_file = harness.base / 'state.json'
        return harness

    def test_cleanup_refuses_namespace_without_owner_before_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = self.harness(directory)
            namespace = {'metadata': {'name': harness.namespace, 'labels': {}}}
            with patch.object(harness, 'kubectl', return_value=json.dumps(namespace)) as kubectl, \
                    patch.object(harness, 'kafka') as kafka:
                with self.assertRaisesRegex(RuntimeError, '拒绝删除'):
                    harness.cleanup()
            self.assertEqual(kubectl.call_count, 1)
            kafka.assert_not_called()

    def test_cleanup_rejects_invalid_run_id_before_deleting_namespace(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = self.harness(directory)
            namespace = {'metadata': {'labels': {k8s.OWNER_KEY: k8s.OWNER},
                                       'annotations': {k8s.RUN_KEY: 'llm-metering-events'}}}
            with patch.object(harness, 'kubectl', return_value=json.dumps(namespace)) as kubectl:
                with self.assertRaisesRegex(RuntimeError, 'run-id 无效'):
                    harness.cleanup()
            self.assertEqual(kubectl.call_count, 1)

    def test_cleanup_resumes_after_namespace_was_already_deleted_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = self.harness(directory)
            run_id = '0123456789ab'
            topic, group = k8s.isolated_names(run_id)
            k8s.write_json(harness.state_file, {'context': 'kind-kind', 'namespace': harness.namespace,
                                               'run_id': run_id})
            with patch.object(harness, 'kubectl', return_value=''), patch.object(harness, 'kafka') as kafka:
                harness.cleanup()
                harness.cleanup()
            kafka.assert_not_called()
            self.assertTrue(json.loads(harness.state_file.read_text())['cleaned'])

    def test_kubernetes_consumer_and_observer_share_isolated_topic(self):
        topic, group = k8s.isolated_names('0123456789ab')
        observer, consumer, credentials = k8s.cluster_configs(topic, group)
        source = json.loads((k8s.ROOT / 'configs/observer.production.example.json').read_text())
        for kind in ('service_lookup', 'customer_lookup'):
            self.assertEqual(observer['observer'][kind]['cache'], source['observer'][kind]['cache'])
        for settings in (observer['observer'], consumer['consumer']):
            self.assertNotIn('accepted_service_ids', settings)
            self.assertNotIn('accepted_request_hosts', settings)
        self.assertEqual(observer['kafka']['topic'], consumer['kafka']['topic'])
        self.assertNotEqual(topic, 'llm-metering-events')
        self.assertNotEqual(consumer['consumer']['group_id'], 'llm-metering-doris-v1')
        self.assertIn('llm-token-stats-doris:9030', credentials['DORIS_DSN'])
        self.assertEqual(observer['kafka']['bootstrap_servers'], ['llm-token-stats-kafka:9092'])

    def test_acceptance_rejects_compose_producer_wrong_topic_and_unknown_zero(self):
        results = [{'scenario': 'unknown', 'x_request_id': 'request'}]
        rows = [['request', 'request_started', 'logical', 'unknown', 'NULL', 'NULL',
                 'complete', '0', 'gateway-test/gateway-pod', 'observer-pod-uid', 'test-topic', 'synthetic-model-route'],
                ['request', 'usage_observed', 'logical', 'unknown', 'NULL', 'NULL',
                 'complete', '0', 'gateway-test/gateway-pod', 'observer-pod-uid', 'test-topic', 'synthetic-model-route']]
        for row in rows:
            row.extend(['NULL'] * 4 + ['lookup_not_implemented'] * 2 + ['kind','stg','wasm-verify.local','NULL','NULL'])
        k8s.validate_events(copy.deepcopy(results), rows, 'gateway-test/gateway-pod', {'observer-pod-uid'}, 'test-topic')
        missing = copy.deepcopy(rows)
        for row in missing:
            row[11] = 'unknown'
        k8s.validate_events(copy.deepcopy(results), missing, 'gateway-test/gateway-pod', {'observer-pod-uid'}, 'test-topic')
        known = copy.deepcopy(rows)
        known[1][3:6] = ['known', '10', '4']
        known[1][7] = '1'
        k8s.validate_events([{'scenario': 'json', 'x_request_id': 'request'}], known,
                            'gateway-test/gateway-pod', {'observer-pod-uid'}, 'test-topic')
        for column, value in ((9, 'compose-observer'), (10, 'llm-metering-events'), (4, '0'), (11, '*'), (11, 'llm-chat')):
            invalid = copy.deepcopy(rows)
            invalid[1][column] = value
            with self.assertRaises(RuntimeError):
                k8s.validate_events(copy.deepcopy(results), invalid, 'gateway-test/gateway-pod', {'observer-pod-uid'}, 'test-topic')

    def test_entire_e2e_runs_in_namespace_without_host_dependencies(self):
        integration = k8s.k8s_integration
        infra = integration.infrastructure('metering-wasm-unit')
        names = {item['metadata']['name'] for item in infra['items']}
        self.assertEqual(names, {'llm-token-stats-kafka', 'llm-token-stats-doris'})
        job = integration.job_resources('metering-wasm-unit', 'llm-token-stats-integration:test')
        for item in infra['items'] + job['items']:
            self.assertTrue(item['metadata']['name'].startswith('llm-token-stats-'))
        encoded = json.dumps(job)
        for forbidden in ('hostPath', 'hostNetwork', 'docker.sock', 'cluster-admin'):
            self.assertNotIn(forbidden, encoded)
        roles = [item for item in job['items'] if item['kind'] == 'Role']
        self.assertEqual(len(roles), 1)
        self.assertTrue(all(rule['apiGroups'] == [''] for rule in roles[0]['rules']))
        workload = next(item for item in job['items'] if item['kind'] == 'Job')
        self.assertEqual(workload['spec']['backoffLimit'], 0)
        self.assertEqual(workload['spec']['template']['metadata']['annotations']['sidecar.istio.io/inject'], 'false')

    def test_suite_uses_prefixed_resources_and_dns_without_changing_local_configs(self):
        integration = k8s.k8s_integration
        local_paths = [k8s.ROOT / 'deploy/local' / (c + '.json') for c in ('observer', 'consumer')]
        originals = [p.read_bytes() for p in local_paths]
        images = {c: 'llm-token-stats-' + c + ':test' for c in ('observer', 'consumer', 'fixture')}
        with patch.object(integration, 'bootstrap', return_value={}):
            items = integration.suite_resources(k8s.ROOT, 'metering-wasm-unit', images)['items']
        objects = {(r['kind'], r['metadata']['name']) for r in items}
        self.assertTrue(all(name.startswith('llm-token-stats-') for _, name in objects))
        for item in items:
            if item['kind'] != 'Deployment':
                continue
            spec = item['spec']['template']['spec']
            for volume in spec.get('volumes', []):
                self.assertIn(('ConfigMap', volume['configMap']['name']), objects)
            for env in spec['containers'][0].get('envFrom', []):
                self.assertIn(('Secret', env['secretRef']['name']), objects)
        configs = [json.loads(r['data'][c + '.json']) for c in ('observer', 'consumer')
                   for r in items if r['kind'] == 'ConfigMap' and r['metadata']['name'] == 'llm-token-stats-e2e-' + c]
        self.assertEqual(len(configs), 2)
        self.assertTrue(all(c['kafka']['bootstrap_servers'] == ['llm-token-stats-kafka:9092'] for c in configs))
        self.assertEqual(configs[1]['doris']['allowed_load_hosts'], ['llm-token-stats-doris:8040'])
        self.assertEqual(originals, [p.read_bytes() for p in local_paths])

    def test_successful_job_must_not_hide_missing_or_skipped_scenarios(self):
        validate = k8s.k8s_integration.validate_test_log
        self.assertEqual(validate('--- PASS: TestA (1s)\n--- PASS: TestB (2s)\nPASS\n', {'TestA', 'TestB'}),
                         {'TestA', 'TestB'})
        for logs in ('PASS\n', '--- PASS: TestA (1s)\nPASS\n',
                     '--- PASS: TestA (1s)\n--- PASS: TestB (1s)\n    --- SKIP: TestB/edge (0s)\n'):
            with self.assertRaises(RuntimeError):
                validate(logs, {'TestA', 'TestB'})

    def test_normal_gateway_waits_for_download_service_before_start(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = self.harness(directory)
            harness.images = {'integration': 'llm-token-stats-integration:test'}
            gateway = {'kind': 'Pod', 'metadata': {'name': 'llm-token-stats-e2e-gateway'}}
            manifests = {'apiVersion': 'v1', 'kind': 'List', 'items': [gateway, {'kind': 'Service'}]}
            events = []
            with patch.object(harness, 'sql'), patch.object(harness, 'kafka'), \
                    patch.object(k8s.k8s_integration, 'suite_resources', return_value=manifests), \
                    patch.object(harness, 'apply', side_effect=lambda v: events.append(('apply', v))), \
                    patch.object(harness, 'kubectl', side_effect=lambda *a, **k: events.append(('wait', a))), \
                    patch.object(harness, 'get', side_effect=RuntimeError('stop after setup')):
                with self.assertRaisesRegex(RuntimeError, 'stop after setup'):
                    k8s.k8s_integration.run_suite(harness, k8s.ROOT, (), k8s.render_sql)
            deployment_waits = [i for i, e in enumerate(events) if e[0] == 'wait' and 'rollout' in e[1]]
            gateway_start = next(i for i, e in enumerate(events) if e == ('apply', gateway))
            self.assertEqual(len(deployment_waits), 3)
            self.assertGreater(gateway_start, max(deployment_waits))
            self.assertNotIn(gateway, events[0][1]['items'])

    def test_infrastructure_restart_cannot_be_reported_as_e2e_success(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = self.harness(directory)
            def pod(name, restarts):
                return {'metadata': {'name': name}, 'status': {'containerStatuses': [
                    {'ready': True, 'restartCount': restarts}]}}
            with patch.object(harness, 'pods', side_effect=[[pod('kafka', 0)], [pod('doris', 1)]]):
                with self.assertRaisesRegex(RuntimeError, '重启'):
                    harness.verify_infrastructure()

    def test_warming_configuration_is_not_accepted_as_active_wasm(self):
        wasm = {'name': 'envoy.filters.http.wasm', 'typed_config': {'config': {'name': 'gateway-observer'}}}
        dump = {'dynamic_listeners': [{'warming_state': {'listener': {'http_filters': [wasm]}}}]}
        self.assertEqual(k8s.wasm_configs(dump), [])
        dump['dynamic_listeners'][0]['active_state'] = {'listener': {'http_filters': [wasm]}}
        self.assertEqual(len(k8s.wasm_configs(dump)), 1)

    def test_offsets_accept_empty_partition_and_reject_uncommitted_records(self):
        raw = ('group topic 0 4 4 0 member /172.19.0.1 kgo\n'
               'group topic 1 2 2 0 member /172.19.0.1 kgo\n'
               'group topic 2 - 0 - member /172.19.0.1 kgo\n')
        self.assertTrue(k8s.offsets_caught_up(raw, 'topic'))
        self.assertFalse(k8s.offsets_caught_up(raw, 'topic', expected_events=18))
        scaled = raw.replace('0 4 4 0', '0 12 12 0').replace('1 2 2 0', '1 6 6 0')
        self.assertTrue(k8s.offsets_caught_up(scaled, 'topic', expected_events=18))
        self.assertFalse(k8s.offsets_caught_up(scaled, 'topic'))
        self.assertFalse(k8s.offsets_caught_up(raw.replace('1 2 2 0', '1 1 2 1'), 'topic'))
        self.assertFalse(k8s.offsets_caught_up(raw.replace('2 - 0 -', '2 - 1 -'), 'topic'))
        self.assertFalse(k8s.offsets_caught_up(raw.replace('member', '-'), 'topic'))


if __name__ == '__main__':
    unittest.main()
