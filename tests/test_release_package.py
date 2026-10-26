import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import release_package as release


class ReleasePackageTests(unittest.TestCase):
    def environment(self):
        return {
            'RELEASE_ACCEPTED_REQUEST_HOSTS': '["API.Company.com.", "*.company.com"]',
            'RELEASE_GATEWAY_LABELS': '["app=ai-gateway"]',
        }

    def test_inputs_use_existing_contract_without_cluster_access(self):
        with patch.object(release.bundle, 'run') as run:
            options = release.package_options(self.environment())
        run.assert_not_called()
        args = release.bundle.parser().parse_args(options)
        self.assertEqual(args.accepted_request_host, ['api.company.com', '*.company.com'])
        self.assertEqual(release.bundle.selector(args.gateway_label), {'app': 'ai-gateway'})
        self.assertEqual(args.platform, 'linux/amd64')
        self.assertFalse(args.include_dependency_images)

    def test_release_needs_no_environment_inputs(self):
        with patch.object(release.bundle, "run") as run:
            args = release.bundle.parser().parse_args(release.package_options({}))
        run.assert_not_called()
        self.assertEqual(release.bundle.selector(args.gateway_label), {"istio": "ingressgateway"})

    def test_missing_or_blank_host_input_defaults_to_all_hosts(self):
        for host_input in (None, '', '   '):
            with self.subTest(host_input=host_input):
                environment = self.environment()
                if host_input is None:
                    del environment['RELEASE_ACCEPTED_REQUEST_HOSTS']
                else:
                    environment['RELEASE_ACCEPTED_REQUEST_HOSTS'] = host_input
                with patch.object(release.bundle, 'run') as run:
                    options = release.package_options(environment)
                run.assert_not_called()
                args = release.bundle.parser().parse_args(options)
                self.assertEqual(release.bundle.gateway_configuration(args)['accepted_request_hosts'], ['*'])

    def test_invalid_and_shell_make_injection_inputs_are_rejected(self):
        cases = [
            ('RELEASE_ACCEPTED_REQUEST_HOSTS', 'not-json'),
            ('RELEASE_ACCEPTED_REQUEST_HOSTS', '[]'),
            ('RELEASE_ACCEPTED_REQUEST_HOSTS', '{}'),
            ('RELEASE_ACCEPTED_REQUEST_HOSTS', '[1]'),
            ('RELEASE_ACCEPTED_REQUEST_HOSTS', '["api.company.com", "API.COMPANY.COM"]'),
            ('RELEASE_ACCEPTED_REQUEST_HOSTS', '["$(touch /tmp/injected)"]'),
            ('RELEASE_GATEWAY_LABELS', '["bad/key/name=x"]'),
            ('RELEASE_GATEWAY_LABELS', '["gateway`id`=value"]'),
            ('RELEASE_NAMESPACE', 'metering;id'),
            ('RELEASE_STORAGE_CLASS', '$(shell id)'),
        ]
        for name, value in cases:
            with self.subTest(name=name, value=value):
                environment = self.environment()
                environment[name] = value
                with self.assertRaises((ValueError, argparse.ArgumentTypeError)):
                    release.package_options(environment)

    def test_tag_must_exist_and_match_checked_out_commit(self):
        with patch.object(release, 'subprocess') as process:
            with self.assertRaises(ValueError):
                release.validate_tag('../other')
            process.run.assert_not_called()
        with patch.object(release.subprocess, 'run', side_effect=[
                subprocess.CompletedProcess([], 0, 'tag-commit\n'),
                subprocess.CompletedProcess([], 0, 'other-commit\n')]):
            with self.assertRaisesRegex(ValueError, '检出'):
                release.validate_tag('v1.0.0')

    def package(self, directory):
        package = directory / 'package'
        package.mkdir()
        (package / 'images').mkdir()
        (package / 'images/images.tar').write_bytes(os.urandom(4096))
        (package / 'envoyfilter.yaml').write_text(
            'apiVersion: networking.istio.io/v1alpha3\nkind: EnvoyFilter\nmetadata:\n  name: synthetic-test\n')
        images = {'observer': 'metering-observer:test', 'consumer': 'metering-consumer:test'}
        manifest = {'platform': 'linux/amd64', 'images': {**images, **release.bundle.RUNTIME_IMAGES},
                    'archived_images': images, 'external_images': release.bundle.RUNTIME_IMAGES}
        (package / 'manifest.json').write_text(json.dumps(manifest) + '\n')
        files = sorted(path for path in package.rglob('*') if path.is_file())
        (package / 'SHA256SUMS').write_text(''.join(
            release.bundle.file_sha256(path) + '  ' + str(path.relative_to(package)) + '\n'
            for path in files))
        return package

    def test_single_tgz_and_checksums_can_be_downloaded_and_extracted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = self.package(root)
            output = root / 'release'
            parts = release.archive_package('v1.0.0', package, output)
            self.assertEqual(len(parts), 1)
            with tarfile.open(parts[0], 'r:gz') as archive:
                self.assertIn('package/images/images.tar', archive.getnames())
                self.assertEqual(archive.extractfile('package/images/images.tar').read(),
                                 (package / 'images/images.tar').read_bytes())
            subprocess.run(['sha256sum', '-c', 'SHA256SUMS'], cwd=output,
                           check=True, capture_output=True)
            self.assertNotIn('cat ', (output / 'DOWNLOAD.md').read_text())
            self.assertIn('归档镜像：Observer、Consumer。', (output / 'DOWNLOAD.md').read_text())
            self.assertIn('部署清单仍保留', (output / 'DOWNLOAD.md').read_text())
            self.assertTrue((root / 'release-notes.md').exists())
            with self.assertRaisesRegex(ValueError, '非空'):
                release.archive_package('v1.0.0', package, output)

    def test_large_archive_parts_restore_to_valid_tgz_with_full_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = self.package(root)
            output = root / 'release'
            parts = release.archive_package('v1.0.0', package, output, max_bytes=1024)
            self.assertGreater(len(parts), 1)
            self.assertTrue(all(part.stat().st_size <= 1024 for part in parts))
            archive_name = 'llm-token-stats-v1.0.0-linux-amd64.tgz'
            self.assertFalse((output / archive_name).exists())
            content = b''.join(part.read_bytes() for part in parts)
            self.assertTrue(gzip.decompress(content))
            self.assertIn(hashlib.sha256(content).hexdigest(), (output / 'DOWNLOAD.md').read_text())
            subprocess.run(['sha256sum', '-c', 'SHA256SUMS'], cwd=output,
                           check=True, capture_output=True)

    def test_corrupt_package_fails_before_creating_release_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = self.package(root)
            (package / 'images/images.tar').write_bytes(b'corrupted')
            with self.assertRaises(subprocess.CalledProcessError):
                release.archive_package('v1.0.0', package, root / 'release')
            self.assertFalse((root / 'release').exists())

    def test_missing_envoyfilter_rejects_legacy_package_before_archiving(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = self.package(root)
            (package / 'envoyfilter.yaml').unlink()
            checksums = package / 'SHA256SUMS'
            checksums.write_text(''.join(line for line in checksums.read_text().splitlines(keepends=True)
                                         if not line.endswith('  envoyfilter.yaml\n')))
            with self.assertRaisesRegex(ValueError, '缺少 envoyfilter.yaml'):
                release.archive_package('v1.0.0', package, root / 'release')
            self.assertFalse((root / 'release').exists())

    def test_split_boundary_does_not_create_empty_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / 'archive.tgz'
            archive.write_bytes(b'a' * 8)
            self.assertEqual(release.split_archive(archive, 8), [archive])
            archive.write_bytes(b'a' * 16)
            parts = release.split_archive(archive, 8)
            self.assertEqual([part.stat().st_size for part in parts], [8, 8])


if __name__ == '__main__':
    unittest.main()
