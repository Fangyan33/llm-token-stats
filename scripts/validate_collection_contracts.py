"""Offline current contract checks; no gateway, Kafka or database I/O.

The small schema evaluator supports only the keywords used by the two checked-in
schemas. It is not a general JSON Schema implementation or a runtime Consumer.
Errors contain field locations/reason codes, never input values or payloads.
"""
import argparse
import hashlib
import ipaddress
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAX_EVENT_BYTES = 32768
INT64_MAX = 9223372036854775807
IDENTITY_FIELDS = (
    "service_id", "provider_id", "api_key_id", "customer_id", "identity_status",
    "config_version", "gateway_instance_id", "adapter_id", "x_request_id", "logical_request_id",
    "cluster_name", "deployment_environment", "mapping_status", "mapping_version",
    "customer_mapping_status", "customer_mapping_version",
)
TOKEN_NAMES = (
    "input_tokens", "output_tokens", "cached_read_tokens", "cached_write_tokens",
    "reasoning_tokens", "reported_total_tokens",
)

class ContractError(ValueError):
    pass

def require(condition, reason):
    if not condition:
        raise ContractError(reason)

def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json_key")
        result[key] = value
    return result

def strict_json(text):
    def invalid_constant(_):
        raise ContractError("non_finite_json_number")
    try:
        return json.loads(text, object_pairs_hook=unique_object, parse_constant=invalid_constant)
    except (ValueError, RecursionError) as exc:
        if isinstance(exc, ContractError):
            raise
        raise ContractError("invalid_json") from None

def load(path):
    try:
        return strict_json(Path(path).read_text(encoding="utf-8"))
    except (UnicodeError, OSError):
        raise ContractError("unreadable_utf8_file") from None

def same_json(left, right):
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(same_json(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(same_json(a, b) for a, b in zip(left, right))
    return left == right

def validate_schema(value, schema, document=None, location="$"):
    document = schema if document is None else document
    supported = {
        "$schema", "$defs", "$ref", "title", "description", "default", "type",
        "const", "enum", "allOf", "anyOf", "if", "then", "else", "properties",
        "required", "additionalProperties", "items", "minItems", "maxItems",
        "uniqueItems", "minLength", "maxLength", "pattern", "minimum", "maximum",
    }
    require(set(schema) <= supported, "unsupported_schema_keyword")
    if "$ref" in schema:
        ref = schema["$ref"]
        require(ref.startswith("#/"), "unsupported_schema_reference")
        target = document
        for part in ref[2:].split("/"):
            target = target[part.replace("~1", "/").replace("~0", "~")]
        validate_schema(value, target, document, location)
    if "type" in schema:
        types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        checks = {
            "null": value is None, "object": type(value) is dict,
            "array": type(value) is list, "string": type(value) is str,
            "integer": type(value) is int, "boolean": type(value) is bool,
        }
        require(any(checks.get(t, False) for t in types), location + ":type")
    if "const" in schema:
        require(same_json(value, schema["const"]), location + ":const")
    if "enum" in schema:
        require(any(same_json(value, x) for x in schema["enum"]), location + ":enum")
    for child in schema.get("allOf", []):
        validate_schema(value, child, document, location)
    if "anyOf" in schema:
        successes = 0
        for child in schema["anyOf"]:
            try:
                validate_schema(value, child, document, location)
                successes += 1
            except ContractError:
                pass
        require(successes > 0, location + ":anyOf")
    if "if" in schema:
        try:
            validate_schema(value, schema["if"], document, location)
            branch = "then"
        except ContractError:
            branch = "else"
        validate_schema(value, schema.get(branch, {}), document, location)
    if type(value) is dict:
        props = schema.get("properties", {})
        require(set(schema.get("required", [])) <= value.keys(), location + ":required")
        if schema.get("additionalProperties") is False:
            require(value.keys() <= props.keys(), location + ":unknown_field")
        for name, field in props.items():
            if name in value:
                validate_schema(value[name], field, document, location + "." + name)
    elif type(value) is list:
        require(len(value) >= schema.get("minItems", 0), location + ":minItems")
        require(len(value) <= schema.get("maxItems", sys.maxsize), location + ":maxItems")
        if schema.get("uniqueItems"):
            require(len({json.dumps(x, sort_keys=True) for x in value}) == len(value), location + ":uniqueItems")
        for item in value:
            validate_schema(item, schema.get("items", {}), document, location + "[]")
    elif type(value) is str:
        require(len(value) >= schema.get("minLength", 0), location + ":minLength")
        require(len(value) <= schema.get("maxLength", sys.maxsize), location + ":maxLength")
        if "pattern" in schema:
            require(re.fullmatch(schema["pattern"], value) is not None, location + ":pattern")
    elif type(value) is int:
        require(value >= schema.get("minimum", value), location + ":minimum")
        require(value <= schema.get("maximum", value), location + ":maximum")

def utf8_size(value):
    try:
        return len(value.encode("utf-8", errors="strict"))
    except UnicodeError:
        raise ContractError("invalid_utf8_string") from None

def check_strings(value):
    if isinstance(value, str):
        utf8_size(value)
    elif isinstance(value, dict):
        for key, child in value.items():
            check_strings(key)
            check_strings(child)
    elif isinstance(value, list):
        for child in value:
            check_strings(child)

def go_json(value):
    # All contract numbers are integers. Match Go encoding/json string escaping.
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    for char, escape in (("&", r"\u0026"), ("<", r"\u003c"), (">", r"\u003e"),
                         ("\u2028", r"\u2028"), ("\u2029", r"\u2029")):
        text = text.replace(char, escape)
    return text.encode("utf-8")

def canonical_event(event):
    schema = load(ROOT / "api/events/v1/metering-event.schema.json")
    ordered = {name: event[name] for name in schema["properties"]}
    if event["usage"] is not None:
        ordered["usage"] = {name: {part: event["usage"][name][part] for part in ("value", "source")} for name in TOKEN_NAMES}
    return go_json(ordered)

def event_hashes(event):
    return (
        hashlib.sha256(canonical_event(event)).hexdigest(),
        hashlib.sha256(go_json([event[n] for n in IDENTITY_FIELDS])).hexdigest(),
    )

def validate_raw_usage(text):
    require(utf8_size(text) <= 4096, "raw_usage_bytes")
    raw = strict_json(text)
    allowed = {
        "prompt_tokens": None, "completion_tokens": None, "total_tokens": None,
        "prompt_tokens_details": {"cached_tokens", "cache_write_tokens"},
        "completion_tokens_details": {"reasoning_tokens"},
    }
    require(type(raw) is dict and raw.keys() <= allowed.keys(), "raw_usage_fields")
    for name, value in raw.items():
        if allowed[name] is None:
            values = [value]
        else:
            require(type(value) is dict and value.keys() <= allowed[name], "raw_usage_details")
            values = list(value.values())
        require(all(type(v) is int and 0 <= v <= INT64_MAX for v in values), "raw_usage_number")

def validate_event(event, kafka_key=None, now_ms=None, input_size=None):
    schema = load(ROOT / "api/events/v1/metering-event.schema.json")
    validate_schema(event, schema)
    check_strings(event)
    require(input_size is None or input_size <= MAX_EVENT_BYTES, "event_input_bytes")
    require(len(canonical_event(event)) <= MAX_EVENT_BYTES, "event_canonical_bytes")
    for name, field in schema["properties"].items():
        if isinstance(event[name], str) and "maxLength" in field:
            require(utf8_size(event[name]) <= field["maxLength"], name + ":utf8_bytes")
    request_id = event["observation_id"]
    suffix = ":start" if event["event_type"] == "request_started" else ":usage"
    require(event["event_id"] == request_id + suffix, "event_id_mismatch")
    if kafka_key is not None:
        require(kafka_key == request_id.encode("utf-8"), "kafka_key_mismatch")
    host = event['request_host']
    if host is not None:
        import ipaddress
        require(host == host.lower() and len(host) <= 253 and not host.endswith('.'), 'request_host_canonical')
        require(all(re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', part) for part in host.split('.')), 'request_host_dns')
        try: ipaddress.ip_address(host)
        except ValueError: pass
        else: raise ContractError('request_host_ip')
    service_mapped = event['mapping_status'] == 'mapped'
    require(service_mapped == all(event[n] is not None for n in ('service_id','provider_id','mapping_version','service_name','provider_name')), 'service_mapping')
    if not service_mapped:
        require(all(event[n] is None for n in ('service_id','provider_id','mapping_version','service_name','provider_name')), 'partial_service_mapping')
    require((event['mapping_status'] == 'invalid_host') == (host is None), 'host_mapping')
    customer_mapped = event['customer_mapping_status'] == 'mapped'
    require(customer_mapped == all(event[n] is not None for n in ('api_key_id','customer_id','customer_mapping_version','customer_name')), 'customer_mapping')
    if not customer_mapped:
        require(all(event[n] is None for n in ('api_key_id','customer_id','customer_mapping_version','customer_name')), 'partial_customer_mapping')
    identity_known = all(event[n] is not None for n in ("api_key_id", "customer_id"))
    require((event["identity_status"] == "known") == identity_known, "identity_status_mismatch")
    if not identity_known:
        require("identity_invalid" in event["anomaly_codes"], "identity_invalid_code")
    require(event["anomaly_codes"] == sorted(event["anomaly_codes"]), "anomaly_order")
    validate_raw_usage(event["raw_usage_json"])
    started = event["request_started_at_ms"]
    if now_ms is not None:
        require(started <= now_ms + 300000, "future_request_time")
        start_date = datetime.fromtimestamp(started // 1000, timezone.utc).date()
        today = datetime.fromtimestamp(now_ms // 1000, timezone.utc).date()
        require(start_date >= today.replace(year=today.year - 1, day=1), "expired_request_date")
    if event["request_finished_at_ms"] is not None and event["request_finished_at_ms"] < started:
        require("clock_anomaly" in event["anomaly_codes"], "negative_duration_without_clock_anomaly")
    if event["event_type"] == "request_started":
        return
    if not event["terminal_observed"]:
        require("missing_end" in event["anomaly_codes"], "missing_end_code")
        require(event["observation_status"] != "complete", "complete_without_end")
        require(event["business_outcome"] != "response_complete", "complete_business_without_end")
    values = {name: event["usage"][name]["value"] for name in TOKEN_NAMES}
    incoming, outgoing = values["input_tokens"], values["output_tokens"]
    any_known = any(v is not None for v in values.values())
    expected = "known" if incoming is not None and outgoing is not None else "partial" if any_known else "unknown"
    conflict = False
    for child, parent in (("cached_read_tokens", "input_tokens"), ("cached_write_tokens", "input_tokens"), ("reasoning_tokens", "output_tokens")):
        if values[child] is not None and values[parent] is not None:
            conflict |= values[child] > values[parent]
    read, write = values["cached_read_tokens"], values["cached_write_tokens"]
    if incoming is not None and read is not None and write is not None:
        conflict |= read + write > incoming
    if incoming is not None and outgoing is not None:
        conflict |= incoming + outgoing > INT64_MAX
        if values["reported_total_tokens"] is not None:
            conflict |= incoming + outgoing != values["reported_total_tokens"]
    if event["usage_status"] == "conflict":
        require("usage_conflict" in event["anomaly_codes"], "usage_conflict_code")
        require(not event["usage_eligible"], "eligible_conflict")
    else:
        require(not conflict and "usage_conflict" not in event["anomaly_codes"], "unmarked_usage_conflict")
        require(event["usage_status"] == expected, "usage_status_mismatch")
    if event["usage_status"] == "unknown":
        require("missing_usage" in event["anomaly_codes"], "missing_usage_code")
    if event["usage_eligible"]:
        excluded = {"invalid_usage", "usage_conflict", "missing_usage", "missing_end",
                    "protocol_error", "protocol_incomplete", "unsupported_encoding",
                    "unsupported_content_type", "parse_limit", "transport_lost",
                    "observation_timeout", "clock_anomaly"}
        require(not excluded.intersection(event["anomaly_codes"]), "eligible_anomaly")
    if event["observation_status"] == "complete":
        require(event["terminal_observed"], "complete_without_terminal")
        incomplete = {"missing_end", "protocol_error", "protocol_incomplete", "parse_limit", "transport_lost", "observation_timeout"}
        require(not incomplete.intersection(event["anomaly_codes"]), "complete_observation_anomaly")

def normalize_request_host_pattern(pattern):
    require(type(pattern) is str and pattern.isascii(), 'request_host_pattern_ascii')
    if pattern == '*':
        return pattern
    wildcard = pattern.startswith('*.')
    host = pattern[2:] if wildcard else pattern
    host = host.removesuffix('.').lower()
    require(0 < len(host) <= 253 and host.isascii(), 'request_host_pattern_length')
    require(all(re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', part)
                for part in host.split('.')), 'request_host_pattern_dns')
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ContractError('request_host_pattern_ip')
    return '*.' + host if wildcard else host

def validate_config(config, release=False):
    schema = load(ROOT / "configs/gateway-observer.schema.json")
    validate_schema(config, schema)
    check_strings(config)
    require(len(go_json(config)) <= 65536, 'config_bytes')
    for name, field in schema["properties"].items():
        if isinstance(config.get(name), str) and "maxLength" in field:
            require(utf8_size(config[name]) <= field["maxLength"], name + ":utf8_bytes")
    headers = [name.lower() for name in config.get('logical_request_id_headers', [])]
    require(len(headers) == len(set(headers)), 'duplicate_request_id_header')
    patterns = [normalize_request_host_pattern(p) for p in config.get('accepted_request_hosts', [])]
    require(len(patterns) == len(set(patterns)), 'duplicate_request_host_pattern')
    names = []
    for rule in config["routes"]:
        names.append(rule["route_name"])
        for name in ("route_name",):
            require(utf8_size(rule[name]) <= 128, "route_id_bytes")
        for path in rule["paths"]:
            require(utf8_size(path) <= 1024 and not any(c.isspace() for c in path), "path_bytes_or_whitespace")
    require(len(names) == len(set(names)), "duplicate_route")
    for dimension in ("bytes", "messages"):
        require(config["stream_inflight_" + dimension] <= config["root_inflight_" + dimension], "stream_exceeds_root")
    require(config["callback_max_bytes"] <= config["chunk_bytes"] * config["callback_max_chunks"], "callback_chunk_capacity")
    require(config["stream_inflight_messages"] > 4 and config["stream_inflight_bytes"] > 8192, "control_reservation")
    require(config["chunk_bytes"] + 256 <= 32768, "protobuf_frame_capacity")
    chunks = math.ceil(config["request_body_max_bytes"] / config["chunk_bytes"])
    require(chunks + 4 <= config["stream_inflight_messages"], "rewrite_message_capacity")
    require(config["request_body_max_bytes"] + chunks * 256 + 8192 <= config["stream_inflight_bytes"], "rewrite_byte_capacity")
    require(config["rewrite_reservation_bytes"] >= 4 * config["request_body_max_bytes"], "rewrite_copy_capacity")
    require(config["rewrite_reservation_bytes"] * config["root_max_rewrites"] <= config["root_rewrite_budget_bytes"], "rewrite_root_capacity")
    require(config["tick_ms"] <= min(config["rewrite_timeout_ms"], config["ack_timeout_ms"], config["end_ack_timeout_ms"]), "tick_deadline_capacity")
    if release:
        require(".example." not in config["observer_cluster"], "unresolved_observer_cluster")
        if config["enabled"]:
            require(any(r.get("enabled", False) for r in config["routes"]), "enabled_without_routes")

def validate_repository():
    validate_config(load(ROOT / "configs/gateway-observer.example.json"))
    fixtures = sorted((ROOT / "testdata/collection").glob("*.json"))
    require(len(fixtures) >= 5, "missing_event_fixtures")
    for path in fixtures:
        data = path.read_bytes()
        event = strict_json(data.decode("utf-8"))
        validate_event(event, event["observation_id"].encode("utf-8"), input_size=len(data))
    doc = (ROOT / "docs/gateway-request-collection-implementation.md").read_text(encoding="utf-8")
    fence = chr(96) * 3
    proto = (ROOT / "api/observation/v1/observation.proto").read_text(encoding="utf-8")
    require(re.search(fence + r"proto\n(.*?)\n" + fence, doc, re.S).group(1).strip() == proto.strip(), "proto_document_drift")
    for message in re.findall(r"message\s+\w+\s*\{(.*?)^\}", proto, re.S | re.M):
        numbers = re.findall(r"^\s*(?:optional\s+)?\w+\s+\w+\s*=\s*(\d+)", message, re.M)
        require(len(numbers) == len(set(numbers)), "duplicate_proto_field_number")
        reserved = [number for clause in re.findall(r"reserved\s+([\d,\s]+);", message)
                    for number in re.findall(r"\d+", clause)]
        require(not set(numbers).intersection(reserved), "reserved_proto_field_reused")
    require("reserved 2;" in proto and "rpc Observe(stream ObservationFrame) returns (stream ObservationAck);" in proto, "proto_service_contract")
    config = load(ROOT / "configs/gateway-observer.example.json")
    json_blocks = [strict_json(x) for x in re.findall(fence + r"json\n(.*?)\n" + fence, doc, re.S)]
    require(next(x for x in json_blocks if "observer_cluster" in x) == config, "config_document_drift")
    for name, value in config.items():
        if type(value) is int and name != "schema_version":
            row = re.search(r"^\| " + re.escape(name) + r" \|.*?\| (\d+) \|", doc, re.M)
            require(row is not None and int(row.group(1)) == value, "config_table_drift:" + name)
    ddl = (ROOT / "migrations/doris/metering/001_init.sql").read_text(encoding="utf-8")
    columns = set(re.findall(r"^\s+(\w+)\s+(?:DATE|VARCHAR|CHAR|BIGINT|SMALLINT|INT|STRING|TINYINT|ARRAY|DATETIME)", ddl, re.M))
    fields = load(ROOT / "api/events/v1/metering-event.schema.json")["properties"]
    require(set(fields) - {"usage"} <= columns, "event_doris_column_drift")
    require(all(n in columns and n + "_source" in columns for n in TOKEN_NAMES), "token_doris_column_drift")
    # Identity comparison must agree across hashes, evidence reads and both
    # database projections; route diagnostics must never re-enter this key.
    identity_sql = "CAST(JSON_ARRAY(" + ", ".join(IDENTITY_FIELDS) + ") AS STRING)"
    store = (ROOT / "internal/doris/store.go").read_text(encoding="utf-8")
    migration = ddl + (ROOT / "migrations/doris/metering/003_usage_summaries.sql").read_text(encoding="utf-8")
    require(identity_sql in store and migration.count("COUNT(DISTINCT " + identity_sql + ")") == 2,
            "identity_comparison_field_drift")
    require("COUNT(DISTINCT identity_hash)" not in migration, "legacy_identity_hash_comparison")
    print(f"PASS: protocol/document parity, config/table parity, Doris fields, {len(fixtures)} event fixtures.")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--release", action="store_true", help="Reject unresolved deployment placeholders; does not prove rollout safety.")
    parser.add_argument("--event", type=Path)
    parser.add_argument("--key", help="Kafka key text, required with --event.")
    parser.add_argument("--now-ms", type=int, help="Consumer UTC retention/future-time validation clock.")
    args = parser.parse_args()
    try:
        if args.config:
            validate_config(load(args.config), args.release)
            print("PASS: plugin config.")
        elif args.release:
            raise ContractError("release_requires_config")
        if args.event:
            require(args.key is not None, "event_requires_kafka_key")
            require(args.event.stat().st_size <= MAX_EVENT_BYTES, "event_input_bytes")
            raw = args.event.read_bytes()
            event = strict_json(raw.decode("utf-8"))
            validate_event(event, args.key.encode("utf-8"), args.now_ms, len(raw))
            print("PASS: Kafka event.")
        elif args.key is not None or args.now_ms is not None:
            raise ContractError("key_or_clock_requires_event")
        if not args.config and not args.event:
            validate_repository()
    except (ContractError, OSError, UnicodeError) as exc:
        reason = str(exc) if isinstance(exc, ContractError) else "unreadable_file"
        print("FAIL: " + reason, file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())

