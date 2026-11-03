import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from reload_gateway_filter import Reloader, extension, parse_stats, reload_counts, WASM


class ReloadTests(unittest.TestCase):
    def resource(self):
        return {'metadata': {'name': 'llm-token-stats-test', 'namespace': 'test', 'resourceVersion': '1'},
                'spec': {'workloadSelector': {'labels': {'app': 'gateway'}}, 'configPatches': [{
                    'applyTo': 'EXTENSION_CONFIG', 'match': {'proxy': {'metadata': {'NAMESPACE': 'test'}}},
                    'patch': {'value': {'name': 'llm-token-stats-test', 'typed_config': {'@type': WASM,
                        'config': {'name': 'gateway-observer', 'fail_open': True, 'vm_config': {
                            'nack_on_code_cache_miss': True, 'code': {'remote': {}},
                            'configuration': {'value': 'initial'}}}}}}},
                    {'applyTo': 'HTTP_FILTER', 'match': {'proxy': {'metadata': {'NAMESPACE': 'test'}}},
                     'patch': {'value': {'name': 'llm-token-stats-test', 'config_discovery': {}}}}]}}

    def test_rejects_disabled_foreign_and_unscoped_configs(self):
        original = self.resource()
        for mutate in (lambda r: r['spec'].update(configPatches=[]),
                       lambda r: r['spec']['configPatches'][0]['patch']['value']['typed_config']['config'].update(fail_open=False),
                       lambda r: r['spec']['configPatches'][0]['match']['proxy']['metadata'].update(NAMESPACE='other')):
            resource = copy.deepcopy(original)
            mutate(resource)
            with self.assertRaises(ValueError):
                extension(resource)

    def test_stats_must_belong_to_requested_extension(self):
        samples = parse_stats('http.extension_config_discovery.llm-token-stats-test.config_reload: 2\n'
                              'extension_config_discovery.http_filter.llm-token-stats-test.config_reload: 3\n'
                              'http.extension_config_discovery.other.config_reload: 5\n'
                              'server.uptime: 10\n')
        self.assertEqual(list(reload_counts(samples, 'llm-token-stats-test').values()), [2, 3])

    def test_repush_rotates_subscription_and_checks_resource_version(self):
        state = self.resource()
        original = copy.deepcopy(state)
        reloader = Reloader('kind-kind', 'test', 'llm-token-stats-test')
        pods = [{'metadata': {'name': 'gateway', 'uid': 'uid'}}]
        def samples(_):
            discovery = state['spec']['configPatches'][0]['patch']['value']['name']
            return {'http.extension_config_discovery.' + discovery + '.config_reload': int(state['metadata']['resourceVersion']) - 1}
        def command(*args):
            self.assertEqual(args[:3], ('patch', 'envoyfilter', 'llm-token-stats-test'))
            ops = json.loads(args[args.index('-p') + 1])
            self.assertEqual(ops[0], {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '1'})
            state['spec']['configPatches'][0]['patch']['value']['name'] = ops[1]['value']
            state['spec']['configPatches'][1]['patch']['value']['name'] = ops[2]['value']
            state['metadata']['resourceVersion'] = '2'
            return json.dumps(state)
        with patch.object(reloader, 'resource', side_effect=lambda: copy.deepcopy(state)), \
                patch.object(reloader, 'pods', return_value=pods), \
                patch.object(reloader, 'stats', side_effect=samples), \
                patch.object(reloader, 'command', side_effect=command):
            result = reloader.reload()
        self.assertTrue(result['ecds_loaded'])
        self.assertEqual(state['spec']['workloadSelector'], original['spec']['workloadSelector'])
        for i in (0, 1):
            original['spec']['configPatches'][i]['patch']['value']['name'] = state['spec']['configPatches'][i]['patch']['value']['name']
        self.assertEqual(state['spec'], original['spec'])

    def test_requires_agent_conversion_disabled_before_patching(self):
        reloader = Reloader('kind-kind', 'test', 'llm-token-stats-test')
        pod = {'metadata': {'name': 'gateway'}, 'spec': {'containers': [{'name': 'istio-proxy'}]},
               'status': {'containerStatuses': [{'name': 'istio-proxy', 'ready': True}]}}
        with patch.object(reloader, 'command', return_value=json.dumps({'items': [pod]})):
            with self.assertRaisesRegex(ValueError, '关闭 Agent'):
                reloader.pods(self.resource())

    def test_concurrent_spec_change_stops_without_overwriting(self):
        resource = self.resource()
        changed = copy.deepcopy(resource)
        changed['spec']['workloadSelector']['labels']['app'] = 'someone-else'
        reloader = Reloader('kind-kind', 'test', 'llm-token-stats-test')
        pods = [{'metadata': {'name': 'gateway', 'uid': 'uid'}}]
        with patch.object(reloader, 'resource', side_effect=[resource, changed]), \
                patch.object(reloader, 'pods', return_value=pods), \
                patch.object(reloader, 'stats', return_value={'http.extension_config_discovery.llm-token-stats-test.config_reload': 0}), \
                patch.object(reloader, 'command', return_value=json.dumps(resource)) as command:
            with self.assertRaisesRegex(RuntimeError, '并发修改'):
                reloader.reload()
        self.assertEqual(command.call_count, 1)

    def test_timeout_does_not_claim_passthrough_is_plugin_activation(self):
        resource = self.resource()
        reloader = Reloader('kind-kind', 'test', 'llm-token-stats-test', timeout=1)
        pods = [{'metadata': {'name': 'gateway', 'uid': 'uid'}}]
        with patch.object(reloader, 'resource', return_value=resource), \
                patch.object(reloader, 'pods', return_value=pods), \
                patch.object(reloader, 'stats', return_value={'http.extension_config_discovery.llm-token-stats-test.config_reload': 0}), \
                patch.object(reloader, 'command', return_value=json.dumps(resource)), \
                patch('reload_gateway_filter.time.monotonic', side_effect=[0, 0, 0, 2, 2]):
            with self.assertRaisesRegex(RuntimeError, '加载超时'):
                reloader.reload()

    def test_missing_metrics_fail_before_mutating_resource(self):
        reloader = Reloader('kind-kind', 'test', 'llm-token-stats-test')
        with patch.object(reloader, 'resource', return_value=self.resource()), \
                patch.object(reloader, 'pods', return_value=[{'metadata': {'name': 'gateway', 'uid': 'uid'}}]), \
                patch.object(reloader, 'stats', return_value={}), \
                patch.object(reloader, 'command') as command:
            with self.assertRaisesRegex(ValueError, 'proxyStatsMatcher'):
                reloader.reload()
        command.assert_not_called()
