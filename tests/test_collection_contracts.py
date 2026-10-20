import unittest
import copy
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("contracts", ROOT / "scripts/validate_collection_contracts.py")
contracts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contracts)

class CollectionContractsTest(unittest.TestCase):
    def setUp(self):
        self.event = contracts.load(ROOT / "testdata/collection/usage-known.json")
        self.config = contracts.load(ROOT / "configs/gateway-observer.example.json")

    def assert_event_invalid(self, change):
        event = copy.deepcopy(self.event)
        change(event)
        with self.assertRaises(contracts.ContractError):
            contracts.validate_event(event)

    def assert_config_invalid(self, change):
        config = copy.deepcopy(self.config)
        change(config)
        with self.assertRaises(contracts.ContractError):
            contracts.validate_config(config)

    def test_fixtures_and_repository_parity(self):
        contracts.validate_repository()

    def test_zero_is_reported_not_unknown(self):
        zero = contracts.load(ROOT / "testdata/collection/usage-zero.json")
        contracts.validate_event(zero)
        self.assertEqual(zero["usage"]["input_tokens"], {"value": 0, "source": "reported"})

    def test_utf8_model_boundary_and_invalid_surrogate(self):
        for model in ("a" * 1024, "你" * 341 + "a", "<" * 1024):
            event = copy.deepcopy(self.event)
            event["request_model"] = event["response_model"] = model
            contracts.validate_event(event)
        for model in ("a" * 1025, "你" * 342, "\ud800"):
            with self.subTest(size=len(model)):
                self.assert_event_invalid(lambda e: e.update(request_model=model))

    def test_missing_and_unknown_fields(self):
        self.assert_event_invalid(lambda e: e.pop("response_model"))
        self.assert_event_invalid(lambda e: e.update(messages=[]))
        self.assert_event_invalid(lambda e: e.update(schema_version=True))
        self.assert_event_invalid(lambda e: e.update(schema_version=2))

    def test_duplicate_json_keys_and_nonfinite_numbers(self):
        for text in ('{"schema_version":1,"schema_version":1}', '{"value":NaN}', '{"value":Infinity}'):
            with self.subTest():
                with self.assertRaises(contracts.ContractError):
                    contracts.strict_json(text)

    def test_token_numeric_types(self):
        for value in (False, "0", -1, 0.5, 1000.0, 9223372036854775808):
            with self.subTest(value_type=type(value).__name__):
                self.assert_event_invalid(lambda e: e["usage"]["input_tokens"].update(value=value))

    def test_unknown_source_cannot_carry_zero(self):
        self.assert_event_invalid(lambda e: e["usage"]["input_tokens"].update(value=0, source="unknown"))
        self.assert_event_invalid(lambda e: e["usage"]["input_tokens"].update(value=None, source="reported"))

    def test_derived_total_overflow_and_cache_overlap(self):
        self.assert_event_invalid(lambda e: e["usage"]["cached_read_tokens"].update(value=800))
        self.assert_event_invalid(lambda e: e["usage"]["reported_total_tokens"].update(value=1499))
        def overflow(e):
            e["usage"]["input_tokens"]["value"] = contracts.INT64_MAX
            e["usage"]["output_tokens"]["value"] = 1
            e["usage"]["reported_total_tokens"] = {"value": None, "source": "unknown"}
        self.assert_event_invalid(overflow)
        event = copy.deepcopy(self.event)
        event["usage"]["input_tokens"]["value"] = contracts.INT64_MAX
        event["usage"]["output_tokens"]["value"] = 0
        event["usage"]["reasoning_tokens"]["value"] = 0
        event["usage"]["reported_total_tokens"]["value"] = contracts.INT64_MAX
        contracts.validate_event(event)

    def test_conflict_retains_numeric_evidence_but_is_ineligible(self):
        event = copy.deepcopy(self.event)
        event["usage"]["reported_total_tokens"]["value"] = 1499
        event.update(usage_status="conflict", usage_eligible=False, anomaly_codes=["usage_conflict"])
        contracts.validate_event(event)
        event["usage_eligible"] = True
        with self.assertRaises(contracts.ContractError):
            contracts.validate_event(event)

    def test_partial_does_not_estimate_missing_input(self):
        event = contracts.load(ROOT / "testdata/collection/usage-partial.json")
        contracts.validate_event(event)
        self.assertIsNone(event["usage"]["input_tokens"]["value"])
        self.assertEqual(event["usage_status"], "partial")

    def test_start_and_end_invariants(self):
        self.assert_event_invalid(lambda e: e.update(event_type="request_started", event_id=e["logical_request_id"] + ":start"))
        self.assert_event_invalid(lambda e: e.update(terminal_observed=False))
        self.assert_event_invalid(lambda e: e.update(terminal_observed=False, request_finished_at_ms=None, anomaly_codes=["missing_end"]))
        self.assert_event_invalid(lambda e: e.update(observation_status="partial"))
        self.assert_event_invalid(lambda e: e.update(anomaly_codes=["protocol_incomplete"]))
        self.assert_event_invalid(lambda e: e.update(request_finished_at_ms=e["request_started_at_ms"] - 1))
        event = copy.deepcopy(self.event)
        event.update(request_finished_at_ms=event["request_started_at_ms"] - 1, usage_eligible=False, anomaly_codes=["clock_anomaly"])
        contracts.validate_event(event)

    def test_identity_id_and_kafka_key(self):
        self.assert_event_invalid(lambda e: e.update(customer_id=None))
        self.assert_event_invalid(lambda e: e.update(identity_status="invalid"))
        self.assert_event_invalid(lambda e: e.update(observation_id="9f6c2a90-4b13-4b68-9d21-67283e0c5f44"))
        self.assert_event_invalid(lambda e: e.update(logical_request_id=e["logical_request_id"].upper()))
        self.assert_event_invalid(lambda e: e.update(event_id=e["logical_request_id"] + ":start"))
        self.assert_event_invalid(lambda e: e.update(x_request_id="bad request"))
        self.assert_event_invalid(lambda e: e.update(http_status=100))
        self.assert_event_invalid(lambda e: e.update(http_status=600))
        with self.assertRaises(contracts.ContractError):
            contracts.validate_event(self.event, b"another-id")
        event = copy.deepcopy(self.event)
        event.update(api_key_id=None, customer_id=None, customer_name=None, customer_mapping_version=None, customer_mapping_status="lookup_not_implemented", identity_status="invalid", usage_eligible=False, anomaly_codes=["identity_invalid"])
        contracts.validate_event(event)

    def test_raw_usage_does_not_allow_body_or_strings(self):
        for raw in ('{"messages":[]}', '{"prompt_tokens":"secret"}', '{"prompt_tokens_details":{"text":"secret"}}', '{"prompt_tokens":-1}'):
            self.assert_event_invalid(lambda e: e.update(raw_usage_json=raw))
        self.assert_event_invalid(lambda e: e.update(raw_usage_json=" " * 4097))

    def test_anomalies_are_fixed_sorted_unique(self):
        for codes in (["free_text"], ["missing_end", "missing_end"], ["transport_lost", "missing_end"]):
            self.assert_event_invalid(lambda e: e.update(anomaly_codes=codes))

    def test_event_size_is_checked_before_and_after_canonicalization(self):
        contracts.validate_event(self.event, input_size=32768)
        with self.assertRaises(contracts.ContractError):
            contracts.validate_event(self.event, input_size=32769)

    def test_canonical_hashes_ignore_input_key_order(self):
        reversed_event = dict(reversed(list(self.event.items())))
        reversed_event["usage"] = {n: dict(reversed(list(v.items()))) for n, v in reversed(list(self.event["usage"].items()))}
        self.assertEqual(contracts.event_hashes(self.event), contracts.event_hashes(reversed_event))
        modified = copy.deepcopy(self.event)
        modified["x_request_id"] = "different"
        self.assertNotEqual(contracts.event_hashes(self.event), contracts.event_hashes(modified))
        self.assertEqual(contracts.go_json(["你好<&>\u2028\u2029"]), b'["\xe4\xbd\xa0\xe5\xa5\xbd\\u003c\\u0026\\u003e\\u2028\\u2029"]')
        contracts.validate_event(json.loads(contracts.canonical_event(self.event)))

    def test_utc_retention_and_future_clock_boundaries(self):
        now = int(datetime(2026, 10, 13, tzinfo=timezone.utc).timestamp() * 1000)
        for offset, allowed in ((-364 * 86400000, True), (-365 * 86400000, False), (300000, True), (300001, False)):
            event = copy.deepcopy(self.event)
            event.update(request_started_at_ms=now + offset, request_finished_at_ms=now + offset + 1)
            with self.subTest(offset=offset):
                if allowed:
                    contracts.validate_event(event, now_ms=now)
                else:
                    with self.assertRaises(contracts.ContractError):
                        contracts.validate_event(event, now_ms=now)

    def test_config_path_and_route_boundaries(self):
        for paths in ([], ["/x?query"], ["/x#fragment"], ["/x*"], ["/x", "/x"], ["x"], ["/x y"], ["/" + "你" * 342]):
            self.assert_config_invalid(lambda c: c["routes"][0].update(paths=paths))
        self.assert_config_invalid(lambda c: c["routes"].append(copy.deepcopy(c["routes"][0])))
        self.assert_config_invalid(lambda c: c["routes"][0].update(methods=["GET"]))
        self.assert_config_invalid(lambda c: c["routes"][0].update(adapter_id="other"))
        config = copy.deepcopy(self.config)
        config["routes"][0].pop("enabled")
        contracts.validate_config(config)  # Missing enabled is interpreted as false.
        config["routes"] = []
        contracts.validate_config(config)

    def test_method_path_only_wildcard_and_downstream_registration(self):
        config = copy.deepcopy(self.config)
        self.assertEqual(config['routes'][0]['route_name'], '*')
        self.assertEqual(config['routes'][0]['methods'], ['POST'])
        self.assertEqual(config['routes'][0]['paths'], ['/v1/chat/completions'])
        contracts.validate_config(config)
        for filename, component in [('configs/observer.local.json', 'observer'),
                                    ('deploy/local/observer.json', 'observer'),
                                    ('configs/observer.production.example.json', 'observer'),
                                    ('configs/consumer.local.json', 'consumer'),
                                    ('deploy/local/consumer.json', 'consumer'),
                                    ('configs/consumer.production.example.json', 'consumer')]:
            settings = contracts.load(ROOT / filename)[component]
            self.assertNotIn('accepted_request_hosts', settings)
            self.assertNotIn('accepted_service_ids', settings)
            self.assertNotIn('accepted_routes', settings)

    def test_diagnostic_route_only_changes_payload_hash(self):
        payload, identity = contracts.event_hashes(self.event)
        for route in ('unknown', 'other.actual-route', '*'):
            changed = copy.deepcopy(self.event)
            changed['route_name'] = route
            contracts.validate_event(changed)
            other_payload, other_identity = contracts.event_hashes(changed)
            self.assertNotEqual(payload, other_payload)
            self.assertEqual(identity, other_identity)

    def test_wasm_host_allowlist_patterns(self):
        for patterns in ([], ['*'], ['MODEL.Example.invalid.', '*.Example.invalid.']):
            config = copy.deepcopy(self.config)
            config['accepted_request_hosts'] = patterns
            contracts.validate_config(config)
        config = copy.deepcopy(self.config)
        config.pop('accepted_request_hosts')
        contracts.validate_config(config)
        config['accepted_request_hosts'] = ['h' + str(i) + '.example.com' for i in range(1024)]
        contracts.validate_config(config)
        for patterns in (None, 'example.com', [''], ['example.com:443'], ['https://example.com'],
                         ['127.0.0.1'], ['*.127.0.0.1'], ['model-*.example.com'],
                         ['**.example.com'], ['?.example.com'], ['*.example.com..'],
                         ['K.example.com'], ['a' * 64 + '.com'],
                         ['example.com', 'EXAMPLE.COM.'], ['*.example.com', '*.EXAMPLE.COM.'],
                         ['*'] * 1025):
            with self.subTest(patterns=str(patterns)[:80]):
                self.assert_config_invalid(lambda c: c.update(accepted_request_hosts=patterns))
        patterns = ['h' + str(i) + '.' + 'a' * 63 + '.' + 'b' * 63 + '.example.com' for i in range(512)]
        self.assert_config_invalid(lambda c: c.update(accepted_request_hosts=patterns))

    def test_config_resource_relationships(self):
        changes = [
            ("stream_inflight_bytes", 131072),
            ("stream_inflight_messages", 19),
            ("callback_max_chunks", 3),
            ("root_inflight_bytes", 1),
            ("root_rewrite_budget_bytes", 1048576),
            ("rewrite_reservation_bytes", 262144),
            ("request_body_max_bytes", 262145),
            ("rewrite_timeout_ms", 2001),
            ("tick_ms", 2001),
            ("chunk_bytes", 0),
        ]
        for name, value in changes:
            with self.subTest(field=name):
                self.assert_config_invalid(lambda c: c.update({name: value}))

    def test_release_rejects_placeholder(self):
        with self.assertRaises(contracts.ContractError):
            contracts.validate_config(self.config, release=True)
        config = copy.deepcopy(self.config)
        config["gateway_instance_id"] = "58c2a12d-5467-4eb5-bc56-29bd7e271dd0"
        contracts.validate_config(config, release=True)

    def test_mapping_null_pairs_and_secret_rejection(self):
        for field in ('api_key','application_id'):
            self.assert_event_invalid(lambda e: e.update({field:'sentinel'}))
        self.assert_event_invalid(lambda e: e.update(provider_id=None))
        self.assert_event_invalid(lambda e: e.update(mapping_status='lookup_not_implemented'))
        self.assert_event_invalid(lambda e: e.update(customer_mapping_status='lookup_not_implemented'))
        self.assert_event_invalid(lambda e: e.update(request_host='UPPER.example.invalid'))
        self.assert_event_invalid(lambda e: e.update(request_host='127.0.0.1'))
        event=copy.deepcopy(self.event)
        event.update(service_id=None,provider_id=None,service_name=None,provider_name=None,mapping_version=None,mapping_status='lookup_not_implemented')
        contracts.validate_event(event)
        event.update(api_key_id=None,customer_id=None,customer_name=None,customer_mapping_version=None,customer_mapping_status='lookup_not_implemented',identity_status='invalid',usage_eligible=False,anomaly_codes=['identity_invalid'])
        contracts.validate_event(event)

    def test_removed_gateway_identity_configuration_is_rejected(self):
        self.assert_config_invalid(lambda c: c.update(identity_namespace='envoy.filters.http.ext_authz'))
        self.assert_config_invalid(lambda c: c.update(identity_fields={}))
        self.assert_config_invalid(lambda c: c['routes'][0].update(service_id='old'))

if __name__ == "__main__":
    unittest.main()

