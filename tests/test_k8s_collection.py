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
                        patch.object(k8s, 'run') as run:
                    harness.build_images()
                expected = ['metering-' + name + ':' + version for name in ('observer', 'consumer', 'fixture')]
                builds = [call.args[0] for call in run.call_args_list if call.args[0][:2] == ['docker', 'build']]
                self.assertEqual([cmd[cmd.index('-t') + 1] for cmd in builds], expected)
                self.assertEqual(run.call_args_list[-1].args[0], ['kind', 'load', 'docker-image',
                                                                 *expected, '--name', 'kind'])
                saved = json.loads((Path(directory) / 'images.json').read_text())
                self.assertEqual(saved['image_tag'], version)
                self.assertEqual(list(saved['images'].values()), expected)

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
            with patch.object(harness, 'kubectl', return_value=''), \
                    patch.object(harness, 'kafka', side_effect=[group, '', topic, '', '']) as kafka:
                harness.cleanup()
            self.assertEqual(kafka.call_args_list[1].args, ('consumer-groups', '--delete', '--group', group))
            self.assertEqual(kafka.call_args_list[3].args, ('topics', '--delete', '--topic', topic))
            with patch.object(harness, 'kubectl', return_value=''), patch.object(harness, 'kafka') as kafka:
                harness.cleanup()
            kafka.assert_not_called()

    def test_kubernetes_consumer_and_observer_share_isolated_topic(self):
        topic, group = k8s.isolated_names('0123456789ab')
        observer, consumer, credentials = k8s.local_configs('192.0.2.10', topic, group)
        self.assertEqual(observer['kafka']['topic'], consumer['kafka']['topic'])
        self.assertNotEqual(topic, 'llm-metering-events')
        self.assertNotEqual(consumer['consumer']['group_id'], 'llm-metering-doris-v1')
        self.assertIn('192.0.2.10:19030', credentials['DORIS_DSN'])
        for host in ('127.0.0.1', 'localhost', '0.0.0.0'):
            with self.assertRaisesRegex(RuntimeError, '不能是回环地址'):
                k8s.local_configs(host, topic, group)

    def test_acceptance_rejects_compose_producer_wrong_topic_and_unknown_zero(self):
        results = [{'scenario': 'unknown', 'x_request_id': 'request'}]
        rows = [['request', 'request_started', 'logical', 'unknown', 'NULL', 'NULL',
                 'complete', '0', 'gateway-uid', 'observer-pod-uid', 'test-topic', 'synthetic-model-route'],
                ['request', 'usage_observed', 'logical', 'unknown', 'NULL', 'NULL',
                 'complete', '0', 'gateway-uid', 'observer-pod-uid', 'test-topic', 'synthetic-model-route']]
        for row in rows:
            row.extend(['NULL'] * 4 + ['lookup_not_implemented'] * 2 + ['kind','test','wasm-verify.local','NULL','NULL'])
        k8s.validate_events(copy.deepcopy(results), rows, 'gateway-uid', {'observer-pod-uid'}, 'test-topic')
        missing = copy.deepcopy(rows)
        for row in missing:
            row[11] = 'unknown'
        k8s.validate_events(copy.deepcopy(results), missing, 'gateway-uid', {'observer-pod-uid'}, 'test-topic')
        for column, value in ((9, 'compose-observer'), (10, 'llm-metering-events'), (4, '0'), (11, '*'), (11, 'llm-chat')):
            invalid = copy.deepcopy(rows)
            invalid[1][column] = value
            with self.assertRaises(RuntimeError):
                k8s.validate_events(copy.deepcopy(results), invalid, 'gateway-uid', {'observer-pod-uid'}, 'test-topic')

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
        self.assertFalse(k8s.offsets_caught_up(raw.replace('1 2 2 0', '1 1 2 1'), 'topic'))
        self.assertFalse(k8s.offsets_caught_up(raw.replace('2 - 0 -', '2 - 1 -'), 'topic'))
        self.assertFalse(k8s.offsets_caught_up(raw.replace('member', '-'), 'topic'))


if __name__ == '__main__':
    unittest.main()
