from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from image_tag import resolve_image_tag


class ImageTagTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.repo = Path(temporary.name)
        self.git('init', '--quiet')
        self.commit()

    def git(self, *args):
        return subprocess.run(['git', '-c', 'user.name=Image Tag Test', '-c',
                               'user.email=image-tag@example.invalid', '-c', 'commit.gpgsign=false',
                               '-c', 'tag.gpgsign=false', '-c', 'core.hooksPath=/dev/null', *args],
                              cwd=self.repo, text=True, check=True, capture_output=True).stdout.strip()

    def commit(self):
        self.git('commit', '--quiet', '--allow-empty', '-m', 'synthetic image version fixture')

    def test_untagged_head_uses_short_commit(self):
        self.assertEqual(resolve_image_tag(self.repo), self.git('rev-parse', '--short=12', 'HEAD'))

    def test_lightweight_tag_takes_precedence_over_commit(self):
        self.git('tag', 'v1.2.3')
        self.assertEqual(resolve_image_tag(self.repo), 'v1.2.3')

    def test_annotated_tag_is_supported(self):
        self.git('tag', '-a', 'v1.2.3', '-m', 'synthetic release')
        self.assertEqual(resolve_image_tag(self.repo), 'v1.2.3')

    def test_multiple_tags_use_highest_git_version_order(self):
        self.git('tag', 'v1.9.0')
        self.git('tag', '-a', 'v1.10.0', '-m', 'synthetic release')
        self.assertEqual(resolve_image_tag(self.repo), 'v1.10.0')

    def test_historical_tag_is_not_reused_for_new_commit(self):
        self.git('tag', 'v1.2.3')
        self.commit()
        self.assertEqual(resolve_image_tag(self.repo), self.git('rev-parse', '--short=12', 'HEAD'))

    def test_incompatible_git_tags_fail_before_docker_build(self):
        for tag in ('release/v1.2.3', 'v1.2.3+build.1', 'v' + '1' * 128):
            with self.subTest(tag=tag):
                self.git('tag', tag)
                try:
                    with self.assertRaisesRegex(ValueError, '不能直接用作 Docker tag'):
                        resolve_image_tag(self.repo)
                finally:
                    self.git('tag', '-d', tag)


if __name__ == '__main__':
    unittest.main()
