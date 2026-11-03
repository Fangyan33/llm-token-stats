"""Test-only YAML parsing using the same Go YAML dependency as the plugin."""
import atexit
import json
import os
from pathlib import Path
import subprocess
import tempfile

_directory = None


def decode_yaml(text):
    global _directory
    if _directory is None:
        _directory = tempfile.TemporaryDirectory(prefix='llm-token-stats-yaml-test-')
        atexit.register(_directory.cleanup)
        env = dict(os.environ, GOCACHE='/tmp/metering-go-cache', GOMODCACHE='/tmp/metering-mod')
        subprocess.run(['go', 'build', '-o', _directory.name + '/yamlcheck', './tests/yamlcheck'],
                       cwd=Path(__file__).resolve().parents[1], env=env, check=True, capture_output=True)
    return json.loads(subprocess.run([_directory.name + '/yamlcheck'], input=text, text=True,
                                     check=True, capture_output=True).stdout)


def plugin_config(resource):
    patches = resource['spec']['configPatches']
    extension = next((p['patch']['value'] for p in patches if p['applyTo'] == 'EXTENSION_CONFIG'), None)
    return extension['typed_config']['config']['configuration']['value'] if extension else {'enabled': False}
