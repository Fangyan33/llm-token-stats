"""校验 GitHub 发布参数，调用 make package，并生成可上传 Release 的 tgz 附件。"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tarfile

import package_distribution as bundle

ROOT = Path(__file__).resolve().parents[1]
# GitHub Release 单附件必须小于 2 GiB，留出余量。
ASSET_BYTES = 1900 * 1024 * 1024


def validate_tag(tag):
    if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}', tag):
        raise ValueError('发布 tag 必须符合 Docker tag 的字符与长度限制')
    def git(ref):
        return subprocess.run(['git', 'rev-parse', '--verify', ref], cwd=ROOT,
                              check=True, text=True, capture_output=True, timeout=15).stdout.strip()
    if git('refs/tags/' + tag + '^{commit}') != git('HEAD'):
        raise ValueError('必须检出发布 tag 对应的提交，不能用其他提交的源码打包')


def string_array(environment, name):
    try:
        items = json.loads(environment.get(name, ''))
    except ValueError:
        raise ValueError(name + ' 必须配置为非空 JSON 字符串数组') from None
    if not isinstance(items, list) or not items or any(not isinstance(item, str) for item in items):
        raise ValueError(name + ' 必须配置为非空 JSON 字符串数组')
    return items


def package_options(environment):
    # 先按现有契约校验所有输入，再将受限字符集的值交给 Make，避免二次展开注入。
    hosts = (string_array(environment, 'RELEASE_ACCEPTED_REQUEST_HOSTS')
             if environment.get('RELEASE_ACCEPTED_REQUEST_HOSTS', '').strip() else ['*'])
    namespace = bundle.dns_name(environment.get('RELEASE_NAMESPACE') or 'metering')
    gateway_namespace = bundle.dns_name(environment.get('RELEASE_GATEWAY_NAMESPACE') or 'istio-system')
    options = ['--namespace', namespace, '--gateway-namespace', gateway_namespace]
    for host in hosts:
        options += ['--accepted-request-host', bundle.normalize_request_host_pattern(host)]
    if environment.get('RELEASE_GATEWAY_LABELS', '').strip():
        for item in string_array(environment, 'RELEASE_GATEWAY_LABELS'):
            key, value = bundle.label(item)
            options += ['--gateway-label', key + '=' + value]
    storage_class = environment.get('RELEASE_STORAGE_CLASS', '')
    if storage_class:
        # Kubernetes StorageClass 名称可包含点，使用 DNS 子域名规则。
        if len(storage_class) > 253:
            raise ValueError('StorageClass 名称过长')
        for label in storage_class.split('.'):
            bundle.dns_name(label)
        options += ['--storage-class', storage_class]
    args = bundle.parser().parse_args(options)
    bundle.gateway_configuration(args)
    bundle.selector(args.gateway_label)
    return options


def split_archive(archive, max_bytes=ASSET_BYTES):
    if max_bytes <= 0:
        raise ValueError('分卷大小必须为正整数')
    if archive.stat().st_size <= max_bytes:
        return [archive]
    parts = []
    with archive.open('rb') as source:
        while block := source.read(min(max_bytes, 1024 * 1024)):
            part = archive.with_name(archive.name + f'.part{len(parts):03d}')
            with part.open('xb') as target:
                target.write(block)
                remaining = max_bytes - len(block)
                while remaining:
                    block = source.read(min(remaining, 1024 * 1024))
                    if not block:
                        break
                    target.write(block)
                    remaining -= len(block)
            parts.append(part)
    archive.unlink()
    return parts


def archive_package(tag, package_dir, output, max_bytes=ASSET_BYTES):
    if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}', tag):
        raise ValueError('无效发布 tag')
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError('发布附件目录已存在且非空')
    if not (package_dir / 'envoyfilter.yaml').is_file():
        raise ValueError('分发包缺少 envoyfilter.yaml，请使用当前版本 make package 重新打包')
    subprocess.run(['sha256sum', '-c', 'SHA256SUMS'], cwd=package_dir, check=True)
    manifest = json.loads((package_dir / 'manifest.json').read_text(encoding='utf-8'))
    if manifest['platform'] != 'linux/amd64':
        raise ValueError('当前发布工作流仅支持 linux/amd64')
    output.mkdir(parents=True, exist_ok=True)
    archive = output / f'llm-token-stats-{tag}-linux-amd64.tgz'
    with tarfile.open(archive, 'w:gz') as target:
        target.add(package_dir, arcname='package')
    archive_sha = bundle.file_sha256(archive)
    parts = split_archive(archive, max_bytes)
    archived_images = manifest.get('archived_images', manifest.get('images', {}))
    labels = {'observer': 'Observer', 'consumer': 'Consumer', 'kafka': 'Kafka',
              'doris': 'Doris', 'gateway': 'Istio proxyv2'}
    image_note = ('归档镜像：' + '、'.join(labels.get(name, name) for name in archived_images) + '。'
                  if archived_images else '归档镜像见 manifest.json。')
    if manifest.get('external_images'):
        image_note += ('\nKafka、Doris、Istio proxyv2 镜像不随包提供；Kafka / Doris 的部署清单仍保留。'
                       '\n部署前需保证节点能够拉取依赖镜像，或已预先导入；继续复用已有 Istio 网关。')
    restore = ''
    if len(parts) > 1:
        restore = f'cat {archive.name}.part* > {archive.name}\n'
    instructions = f'''# {tag} 采集分发包

请下载本 Release 的全部附件到同一空目录。运行平台为 linux/amd64。
包含已渲染的 Kubernetes 清单。{image_note}
Wasm 已嵌入 Observer 镜像；目标网关 Pod 和业务域名见 manifest.json。
Host 参数省略时默认匹配全部合法 DNS Host；可通过显式名单限制域名。
包根目录 package/envoyfilter.yaml 汇总关闭态 EnvoyFilter，启用态位于 package/gateway/enabled/。
本包绑定打包时的实际网关 Pod，Pod 重建、扩容或更换集群后须重新打包。
单节点基础设施采用开发配置；生产部署仍需完成 G0–G5 验收。

先校验附件，再解压；分卷时以下命令会先合并为完整 tgz：

```sh
sha256sum -c SHA256SUMS
{restore}echo '{archive_sha}  {archive.name}' | sha256sum -c -
tar -xzf {archive.name}
cd package
sha256sum -c SHA256SUMS
```

然后按 package/README.md 导入镜像并部署。
GitHub 自动生成的 Source code 下载项仅包含源码，请下载上述发布附件。
'''
    (output / 'DOWNLOAD.md').write_text(instructions, encoding='utf-8')
    shutil.copyfile(package_dir / 'manifest.json', output / 'manifest.json')
    files = sorted(path for path in output.iterdir() if path.is_file())
    (output / 'SHA256SUMS').write_text(''.join(
        bundle.file_sha256(path) + '  ' + path.name + '\n' for path in files), encoding='utf-8')
    output.parent.joinpath('release-notes.md').write_text(instructions, encoding='utf-8')
    print('发布附件：' + ', '.join(part.name for part in parts), flush=True)
    return parts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['validate', 'build', 'archive'])
    parser.add_argument('--tag', required=True)
    args = parser.parse_args()
    try:
        validate_tag(args.tag)
        if args.command == 'archive':
            archive_package(args.tag, ROOT / 'dist/package', ROOT / 'dist/release')
        else:
            options = package_options(os.environ)
            if args.command == 'build':
                subprocess.run(['make', 'package', 'PACKAGE_DIR=dist/package',
                                'PACKAGE_ARGS=' + shlex.join(options)], cwd=ROOT, check=True)
    except (ValueError, OSError, argparse.ArgumentTypeError, subprocess.SubprocessError) as error:
        parser.exit(1, '发布打包失败：' + str(error) + '\n')


if __name__ == '__main__':
    main()
