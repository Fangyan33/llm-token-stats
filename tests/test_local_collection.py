import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "local_collection", Path(__file__).resolve().parents[1] / "scripts/local_collection.py"
)
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)


def result(stdout="", returncode=0):
    return subprocess.CompletedProcess([], returncode, stdout=stdout)


def env_path(temp):
    env = Path(temp) / "deploy/local/.env"
    env.parent.mkdir(parents=True, exist_ok=True)
    return env


class LocalCollectionTests(unittest.TestCase):
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
