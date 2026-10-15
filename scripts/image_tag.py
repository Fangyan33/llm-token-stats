"""镜像版本：优先取 HEAD 上的 Git tag，否则取至少 12 位的 commit 短 hash。"""
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def resolve_image_tag(repo=ROOT):
    def git(*args):
        return subprocess.run(['git', *args], cwd=repo, text=True, check=True,
                              capture_output=True, timeout=15).stdout.strip()

    # 同时支持轻量和附注 tag；只选当前提交，不沿用历史提交的版本号。
    tags = git('tag', '--points-at', 'HEAD', '--sort=-version:refname').splitlines()
    version = tags[0] if tags else git('rev-parse', '--short=12', 'HEAD')
    if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}', version):
        raise ValueError('Git tag 不能直接用作 Docker tag：' + version
                         + '；须为 1–128 位字母、数字、下划线、点或短横线，且不能以点或短横线开头')
    return version


if __name__ == '__main__':
    try:
        print(resolve_image_tag())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
