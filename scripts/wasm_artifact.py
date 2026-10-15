"""Shared release inputs for native Envoy remote Wasm loading."""
import hashlib
from urllib.parse import urlsplit


def checksum(path):
    try:
        data = path.read_bytes()
    except OSError:
        raise ValueError('Wasm artifact is unreadable; run make wasm first') from None
    if data[:8] != b'\x00asm\x01\x00\x00\x00':
        raise ValueError('Invalid Wasm artifact header')
    return hashlib.sha256(data).hexdigest()


def remote_source(path, url, cluster):
    try:
        uri = urlsplit(url)
        valid = (uri.scheme in ('http', 'https') and uri.hostname and uri.port != 0
                 and not uri.username and not uri.password and not uri.fragment
                 and not any(c.isspace() or ord(c) < 32 for c in url))
    except ValueError:
        valid = False
    if not valid:
        raise ValueError('Wasm URL must be an http(s) URL without credentials or fragments')
    if not cluster or any(c.isspace() or ord(c) < 32 for c in cluster):
        raise ValueError('Wasm HTTP cluster is required and must not contain whitespace')
    return {
        'http_uri': {'uri': url, 'cluster': cluster, 'timeout': '5s'},
        'sha256': checksum(path),
        'retry_policy': {'num_retries': 0},
    }
