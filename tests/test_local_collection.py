import importlib.util
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))


spec = importlib.util.spec_from_file_location(
    "local_collection", Path(__file__).resolve().parents[1] / "scripts/local_collection.py"
)
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)
import doris_schema as schema


def result(stdout="", returncode=0):
    return subprocess.CompletedProcess([], returncode, stdout=stdout)


def env_path(temp):
    env = Path(temp) / "deploy/local/.env"
    env.parent.mkdir(parents=True, exist_ok=True)
    return env


class LocalCollectionTests(unittest.TestCase):
    def test_infra_up_starts_only_shared_kafka_and_doris(self):
        readiness = result('BackendId\tAlive\n1\ttrue\n')
        with patch.object(local, 'external_host'), patch.object(local, 'migrate_storage'), \
                patch.object(local, 'run', return_value=result()) as compose, \
                patch.object(local.subprocess, 'run', return_value=readiness), \
                patch.object(schema, 'live_columns', return_value={}) as metadata:
            local.infra_up()
        starts = [call.args for call in compose.call_args_list if call.args[0] == 'up']
        self.assertEqual(starts, [('up', '-d', 'kafka', 'doris')])
        metadata.assert_called_once_with('metering_development')
        statements = [call.kwargs['input'] for call in compose.call_args_list if call.kwargs.get('input')]
        self.assertEqual(len(statements), 3)

    def test_incompatible_database_is_rejected_before_ddl(self):
        readiness = result('BackendId\tAlive\n1\ttrue\n')
        with patch.object(local, 'external_host'), patch.object(local, 'migrate_storage'), \
                patch.object(local, 'run', return_value=result('application_id\n')) as compose, \
                patch.object(local.subprocess, 'run', return_value=readiness), \
                patch.object(schema, 'live_columns', return_value={'old_table': []}):
            with self.assertRaisesRegex(RuntimeError, 'Existing Doris schema differs'):
                local.infra_up()
        self.assertFalse(any(call.kwargs.get('input') for call in compose.call_args_list))

    def test_existing_final_schema_uses_idempotent_initialization_without_upgrade(self):
        existing = {name: relation['columns'] for name, relation in schema.schema().items()}
        with patch.object(local, 'external_host'), patch.object(local, 'migrate_storage'), \
                patch.object(local, 'run', return_value=result()) as commands, \
                patch.object(local.subprocess, 'run', return_value=result('BackendId\tAlive\n1\ttrue\n')), \
                patch.object(schema, 'live_columns', return_value=existing):
            local.infra_up()
        statements = [c.kwargs['input'] for c in commands.call_args_list if c.kwargs.get('input')]
        self.assertEqual(len(statements), 3)
        self.assertTrue(all('DROP ' not in s and 'ALTER ' not in s and 'CREATE OR REPLACE' not in s for s in statements))

    def test_gateway_render_replaces_local_source_and_refreshes_checksum(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(local, 'ROOT', Path(temp)):
            source = Path(temp) / 'deploy/local/envoy.json'
            source.parent.mkdir(parents=True)
            source.write_text((Path(__file__).resolve().parents[1] / 'deploy/local/envoy.json').read_text())
            artifact = Path(temp) / 'dist/gateway-observer-v1.wasm'
            artifact.parent.mkdir()
            for suffix in [b'first', b'second']:
                data = b'\x00asm\x01\x00\x00\x00' + suffix
                artifact.write_bytes(data)
                self.assertEqual(local.render_gateway(), hashlib.sha256(data).hexdigest())
                text = (Path(temp) / 'dist/envoy.remote.json').read_text()
                self.assertIn(hashlib.sha256(data).hexdigest(), text)
                self.assertNotIn('"local":', text)
                self.assertNotIn('filename', text)

    def test_external_host_preserves_other_env_settings(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(local, "ROOT", Path(temp)), \
                patch.dict(os.environ, {"KAFKA_EXTERNAL_HOST": "192.168.1.10"}, clear=True):
            env = env_path(temp)
            env.write_text("OTHER_SETTING=keep\n")
            local.external_host()
            self.assertIn("OTHER_SETTING=keep\n", env.read_text())
            self.assertIn("KAFKA_EXTERNAL_HOST=192.168.1.10\n", env.read_text())

    def test_unspecified_address_cannot_be_advertised(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(local, "ROOT", Path(temp)), \
                patch.dict(os.environ, {"KAFKA_EXTERNAL_HOST": "0.0.0.0"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "not 0.0.0.0"):
                local.external_host()
            self.assertFalse((Path(temp) / "deploy/local/.env").exists())

    def test_existing_env_uses_compose_interpolation(self):
        cfg = {"services": {"kafka": {"environment": {
            "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092,EXTERNAL://dev.example.test:29092"
        }}}}
        with tempfile.TemporaryDirectory() as temp, patch.object(local, "ROOT", Path(temp)), \
                patch.dict(os.environ, {}, clear=True), \
                patch.object(local, "run", return_value=result(json.dumps(cfg))):
            env = env_path(temp)
            original = "KAFKA_EXTERNAL_HOST='dev.example.test'\nOTHER_SETTING=keep\n"
            env.write_text(original)
            local.external_host()
            self.assertEqual(os.environ["KAFKA_EXTERNAL_HOST"], "dev.example.test")
            self.assertEqual(env.read_text(), original)

    def storage_config(self):
        return {"volumes": {key: {"name": "local-" + key} for key in
                            ["kafka-data", "doris-fe-meta", "doris-be-storage"]}}

    def compose(self, *args, **kwargs):
        if args[0] == "config":
            return result(json.dumps(self.storage_config()))
        if args[0] == "ps":
            return result(args[-1] + "-id\n")
        return result()

    def test_existing_target_volume_aborts_before_stopping(self):
        def docker(args, **kwargs):
            if args[1] == "inspect":
                return result(json.dumps([{"Mounts": []}]))
            if args[1:3] == ["volume", "inspect"]:
                return result(returncode=0)
            self.fail("unexpected Docker mutation: " + repr(args))

        with tempfile.TemporaryDirectory() as temp, patch.object(local, "ROOT", Path(temp)), \
                patch.object(local, "run", side_effect=self.compose) as compose, \
                patch.object(local.subprocess, "run", side_effect=docker):
            with self.assertRaisesRegex(RuntimeError, "Refusing to overwrite"):
                local.migrate_storage()
            self.assertFalse((Path(temp) / ".tmp").exists())
            self.assertFalse(any(call.args[0] == "stop" for call in compose.call_args_list))

    def test_backup_failure_restarts_original_containers(self):
        def docker(args, **kwargs):
            if args[1] == "inspect":
                return result(json.dumps([{"Mounts": []}]))
            if args[1:3] == ["volume", "inspect"]:
                return result(returncode=1)
            if args[1] == "cp":
                raise subprocess.CalledProcessError(1, args)
            if args[1] == "start":
                return result()
            self.fail("unexpected Docker mutation: " + repr(args))

        with tempfile.TemporaryDirectory() as temp, patch.object(local, "ROOT", Path(temp)), \
                patch.object(local, "run", side_effect=self.compose), \
                patch.object(local.subprocess, "run", side_effect=docker) as commands:
            with self.assertRaises(subprocess.CalledProcessError):
                local.migrate_storage()
            starts = [call.args[0] for call in commands.call_args_list if call.args[0][1] == "start"]
            self.assertEqual(starts, [["docker", "start", "doris-id", "kafka-id", "fixture-id",
                                      "observer-id", "consumer-id", "gateway-id"]])


if __name__ == "__main__":
    unittest.main()
