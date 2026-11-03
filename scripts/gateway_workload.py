"""Validate explicit workload labels without consulting a Kubernetes cluster."""
import argparse
import re

DEFAULT_FILTER_NAME = 'llm-token-stats-envoyfilter-wasm'


def label(value):
    key, separator, val = value.partition('=')
    prefix, slash, name = key.rpartition('/')
    if not slash:
        name = key
    token = r'[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?'
    valid_prefix = (not slash or (len(prefix) <= 253 and all(
        len(part) <= 63 and re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', part)
        for part in prefix.split('.'))))
    if (not separator or not valid_prefix or len(name) > 63 or not re.fullmatch(token, name)
            or len(val) > 63 or (val and not re.fullmatch(token, val))):
        raise argparse.ArgumentTypeError('网关标签必须为合法的 Kubernetes KEY=VALUE')
    return key, val


def selector(labels):
    if labels is None:
        return {'istio': 'ingressgateway'}
    result = dict(labels)
    if not result or len(result) != len(labels):
        raise ValueError('网关标签不能为空或包含重复键')
    return result


def filter_name(value):
    """Validate an explicit resource name; never derive it from workload labels."""
    if (len(value) > 63 or not value.startswith('llm-token-stats-')
            or not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', value)):
        raise argparse.ArgumentTypeError('EnvoyFilter 名称必须是 llm-token-stats- 开头且不超过 63 字符的 DNS 标签')
    return value
