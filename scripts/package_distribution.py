"""构建单节点分发包：应用镜像、完整 Kubernetes YAML 与配置，可选携带依赖镜像。"""
import argparse
import base64
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

from image_tag import resolve_image_tag
from gateway_workload import filter_name, label, selector
from render_doris_sql import MIGRATIONS, database_name, render_sql
from wasm_artifact import checksum
from validate_collection_contracts import load, normalize_request_host_pattern, validate_config

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_IMAGES = {'kafka': 'apache/kafka:3.9.2', 'doris': 'apache/doris:all-in-one-4.1.3',
                  'gateway': 'istio/proxyv2:1.13.5'}


def run(command, capture=True):
    return subprocess.run(command, cwd=ROOT, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None)


def dns_name(value):
    if len(value) > 63 or not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', value):
        raise argparse.ArgumentTypeError('名称必须为不超过 63 字符的 Kubernetes DNS 标签')
    return value


def yaml_text(value, indent=0):
    """只输出 JSON 数据类型的 YAML，不依赖目标环境安装 PyYAML。"""
    def scalar(item):
        return json.dumps(item, ensure_ascii=False)

    def field(prefix, item):
        if isinstance(item, (dict, list)) and item:
            return prefix + '\n' + yaml_text(item, indent + 2)
        if isinstance(item, str) and '\n' in item:
            marker = '|' if item.endswith('\n') else '|-'
            return prefix + ' ' + marker + '\n' + ''.join(
                ' ' * (indent + 2) + line + '\n' for line in item.splitlines())
        return prefix + ' ' + scalar(item) + '\n'

    space = ' ' * indent
    if isinstance(value, dict):
        return ''.join(field(space + scalar(key) + ':', item) for key, item in value.items())
    if isinstance(value, list):
        return ''.join(field(space + '-', item) for item in value)
    raise ValueError('YAML 根节点必须为对象或数组')


def write_yaml(path, resources):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('---\n'.join(yaml_text(resource) for resource in resources), encoding='utf-8')


def resource(kind, name, namespace, **fields):
    api = 'apps/v1' if kind in ('Deployment', 'StatefulSet') else 'batch/v1' if kind == 'Job' else 'v1'
    return {'apiVersion': api, 'kind': kind,
            'metadata': {'name': name, 'namespace': namespace}, **fields}


def pod_spec(containers, **fields):
    return {'metadata': {'annotations': {'sidecar.istio.io/inject': 'false'}},
            'spec': {'containers': containers, **fields}}


def volume_claim(name, size, storage_class):
    spec = {'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': size}}}
    if storage_class is not None:
        spec['storageClassName'] = storage_class
    return {'metadata': {'name': name}, 'spec': spec}


def service(name, namespace, ports, headless=False):
    spec = {'selector': {'app': name}, 'ports': [
        {'name': key, 'port': port, 'targetPort': key} for key, port in ports.items()]}
    if headless:
        spec.update(clusterIP='None', publishNotReadyAddresses=True)
    return resource('Service', name, namespace, spec=spec)


def infrastructure(args, images):
    ns = args.namespace
    kafka_dns = f'kafka-0.kafka.{ns}.svc.cluster.local'
    environment = {
        'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk', 'KAFKA_NODE_ID': '1',
        'KAFKA_PROCESS_ROLES': 'broker,controller',
        'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@' + kafka_dns + ':9093',
        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        'KAFKA_LISTENERS': 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093',
        'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://' + kafka_dns + ':9092',
        'KAFKA_LOG_DIRS': '/var/lib/kafka/data', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
        'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1', 'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false',
    }
    kafka = {'name': 'kafka', 'image': images['kafka'], 'imagePullPolicy': 'IfNotPresent',
             'env': [{'name': key, 'value': value} for key, value in environment.items()],
             'ports': [{'name': 'kafka', 'containerPort': 9092}, {'name': 'controller', 'containerPort': 9093}],
             'readinessProbe': {'tcpSocket': {'port': 'kafka'}, 'periodSeconds': 5},
             'resources': {'requests': {'cpu': '250m', 'memory': '1Gi'},
                           'limits': {'cpu': '2', 'memory': '2Gi'}},
             'volumeMounts': [{'name': 'data', 'mountPath': '/var/lib/kafka/data'}]}
    doris_ports = {'mysql': 9030, 'fe-http': 8030, 'be-http': 8040}
    doris = {'name': 'doris', 'image': images['doris'], 'imagePullPolicy': 'IfNotPresent',
             'ports': [{'name': key, 'containerPort': port} for key, port in doris_ports.items()],
             'startupProbe': {'httpGet': {'path': '/api/health', 'port': 'fe-http'},
                              'periodSeconds': 5, 'failureThreshold': 120},
             'readinessProbe': {'httpGet': {'path': '/api/health', 'port': 'fe-http'}, 'periodSeconds': 5},
             'volumeMounts': [{'name': 'fe-meta', 'mountPath': '/opt/apache-doris/fe/doris-meta'},
                              {'name': 'be-storage', 'mountPath': '/opt/apache-doris/be/storage'}]}
    resources = [service('kafka', ns, {'kafka': 9092, 'controller': 9093}, headless=True),
                 service('doris', ns, doris_ports, headless=True)]
    for name, container, claims in (
            ('kafka', kafka, [volume_claim('data', args.kafka_storage, args.storage_class)]),
            ('doris', doris, [volume_claim('fe-meta', args.doris_fe_storage, args.storage_class),
                              volume_claim('be-storage', args.doris_be_storage, args.storage_class)])):
        template = pod_spec([container], terminationGracePeriodSeconds=120)
        template['metadata']['labels'] = {'app': name}
        if name == 'kafka':
            template['spec']['securityContext'] = {'fsGroup': 1000}
        resources.append(resource('StatefulSet', name, ns, spec={
            'serviceName': name, 'replicas': 1, 'selector': {'matchLabels': {'app': name}},
            'template': template, 'volumeClaimTemplates': claims}))
    return resources


def configurations(args):
    ns = args.namespace
    kafka = {'bootstrap_servers': [f'kafka.{ns}.svc.cluster.local:9092'], 'topic': args.topic}
    observer = json.loads((ROOT / 'configs/observer.production.example.json').read_text())
    observer['kafka'] = kafka
    observer['observer'].update(cluster_name=args.cluster_name, deployment_environment='dev')
    consumer = json.loads((ROOT / 'configs/consumer.production.example.json').read_text())
    consumer['kafka'] = copy.deepcopy(kafka)
    consumer['consumer']['group_id'] = args.group
    consumer['doris'].update(database=args.database, fe_url=f'http://doris.{ns}.svc.cluster.local:8040',
                             allowed_load_hosts=[f'doris.{ns}.svc.cluster.local:8040'])
    # all-in-one 的 FE 会重定向到容器回环地址，因此直接访问 BE 的 Stream Load 端口。
    credentials = {
        'DORIS_DSN': (f'root@tcp(doris.{ns}.svc.cluster.local:9030)/{args.database}'
                      '?timeout=5s&readTimeout=30s&writeTimeout=10s&interpolateParams=true'
                      '&time_zone=%27%2B00%3A00%27'),
        'DORIS_USER': 'root', 'DORIS_PASSWORD': '',
    }
    resources = [resource('ConfigMap', 'metering-' + name, ns,
                          data={name + '.json': json.dumps(config, ensure_ascii=False, indent=2) + '\n'})
                 for name, config in (('observer', observer), ('consumer', consumer))]
    resources.append(resource('Secret', 'metering-consumer-credentials', ns, type='Opaque',
                              data={key: base64.b64encode(value.encode()).decode()
                                    for key, value in credentials.items()}))
    # Empty until real HTTP/HTTPS endpoints are configured; never package a lookup token.
    resources.append(resource('Secret', 'metering-observer-lookups', ns, type='Opaque', data={}))
    sql = {name: render_sql((ROOT / 'migrations/doris/metering' / name).read_text(), args.database)
           for name in MIGRATIONS}
    resources.append(resource('ConfigMap', 'metering-doris-migrations', ns, data=sql))
    return resources


def initialization(args, images):
    ns = args.namespace
    kafka_command = f'''until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \\
  --create --if-not-exists --topic {args.topic} --partitions 24 --replication-factor 1 \\
  --config min.insync.replicas=1 --config retention.ms=259200000 --config cleanup.policy=delete; do
  sleep 5
done
'''
    doris_command = '''sql() { mysql --default-character-set=utf8mb4 -uroot -hdoris -P9030 "$@"; }
until sql -e 'SHOW BACKENDS;' | awk -F '\\t' '
  NR==1 { for(i=1;i<=NF;i++) if($i=="Alive") alive=i }
  NR>1 && alive && $alive=="true" { ready=1 }
  END { exit !ready }'; do sleep 5; done
'''
    # Only an empty database or the final object/column layout can be initialized.
    from doris_schema import schema
    relations = schema()
    expected = '\n'.join(name + '.' + column['name'] for name in sorted(relations)
                         for column in relations[name]['columns'])
    doris_command += f'''columns=$(sql -N -e "SELECT CONCAT(table_name, '.', column_name) FROM information_schema.columns WHERE table_schema='{args.database}' ORDER BY table_name, ordinal_position;")
if [ -n "$columns" ] && [ "$columns" != '{expected}' ]; then
  echo 'Existing Doris schema differs from the final model; initialize an isolated empty database' >&2
  exit 1
fi
sql -e 'ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");'
'''
    doris_command += ''.join('sql --init-command="SET time_zone = \'+00:00\'" < /migrations/' + name + '\n'
                             for name in MIGRATIONS)
    resources = []
    for name, command in (('kafka', kafka_command), ('doris', doris_command)):
        container = {'name': 'init', 'image': images[name], 'imagePullPolicy': 'IfNotPresent',
                     'command': ['/bin/bash', '-ec', command]}
        template = pod_spec([container], restartPolicy='OnFailure')
        if name == 'doris':
            container['volumeMounts'] = [{'name': 'migrations', 'mountPath': '/migrations', 'readOnly': True}]
            template['spec']['volumes'] = [{'name': 'migrations',
                                            'configMap': {'name': 'metering-doris-migrations'}}]
        resources.append(resource('Job', 'metering-' + name + '-init', ns, spec={
            'backoffLimit': 6, 'activeDeadlineSeconds': 1200, 'template': template}))
    return resources


def applications(args, images):
    resources = []
    for name, replicas, ports, cpu, memory in (
            ('observer', 2, {'grpc': 9090, 'health': 9091, 'http-wasm': 9093}, '500m', '256Mi'),
            ('consumer', 1, {'health': 9092}, '250m', '128Mi')):
        app = 'metering-' + name
        container = {'name': name, 'image': images[name], 'imagePullPolicy': 'IfNotPresent',
                     'args': ['--config', '/config/' + name + '.json'],
                     'ports': [{'name': key, 'containerPort': port} for key, port in ports.items()],
                     'resources': {'requests': {'cpu': cpu, 'memory': memory},
                                   'limits': {'cpu': '2' if name == 'observer' else '1',
                                              'memory': '1Gi' if name == 'observer' else '512Mi'}},
                     'readinessProbe': {'httpGet': {'path': '/readyz', 'port': 'health'}},
                     'livenessProbe': {'httpGet': {'path': '/livez', 'port': 'health'}},
                     'volumeMounts': [{'name': 'config', 'mountPath': '/config', 'readOnly': True}]}
        if name == 'observer':
            container['env'] = [{'name': 'POD_UID', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.uid'}}}]
            container['envFrom'] = [{'secretRef': {'name': 'metering-observer-lookups', 'optional': True}}]
        else:
            container['envFrom'] = [{'secretRef': {'name': 'metering-consumer-credentials'}}]
        template = pod_spec([container], volumes=[{'name': 'config', 'configMap': {'name': app}}])
        template['metadata']['labels'] = {'app': app}
        resources.append(resource('Deployment', app, args.namespace, spec={
            'replicas': replicas, 'selector': {'matchLabels': {'app': app}}, 'template': template}))
        if name == 'observer':
            resources.append(service(app, args.namespace, ports))
    return resources


def gateway_configuration(args):
    hosts = ['*'] if args.accepted_request_host is None else args.accepted_request_host
    if not hosts:
        raise ValueError('Host 白名单不能为空')
    config = load(ROOT / 'configs/gateway-observer.example.json')
    config['accepted_request_hosts'] = [normalize_request_host_pattern(host)
                                        for host in hosts]
    config['observer_cluster'] = 'outbound|9090||metering-observer.' + args.namespace + '.svc.cluster.local'
    validate_config(config)
    return config



def summary_resources(args, images):
    # The controller schedule is UTC on the supported Kubernetes 1.22 setup.
    from datetime import date
    start = date.fromisoformat(args.collection_start_date)
    if start.isoformat() != args.collection_start_date or start > datetime.now(timezone.utc).date():
        raise ValueError('collection-start-date 必须为不晚于今日的 ISO UTC 日期')
    ns = args.namespace
    settings = {'doris': {'database': args.database, 'dsn_env': 'DORIS_DSN'},
                'summary_worker': {'collection_start_date': args.collection_start_date,
                                   'refresh_timeout_seconds': 1800, 'poll_interval_seconds': 2,
                                   'max_run_seconds': 21600, 'lock_mode': 'kubernetes',
                                   'lease_name': 'metering-usage-worker'}}
    container = {'name': 'usage-worker', 'image': images['usage-worker'], 'imagePullPolicy': 'IfNotPresent',
                 'args': ['--config', '/config/usage-worker.json', '--once'],
                 'envFrom': [{'secretRef': {'name': 'metering-consumer-credentials'}}],
                 'env': [{'name': key, 'valueFrom': {'fieldRef': {'fieldPath': field}}}
                         for key, field in [('POD_NAME', 'metadata.name'), ('POD_UID', 'metadata.uid'),
                                            ('POD_NAMESPACE', 'metadata.namespace')]],
                 'volumeMounts': [{'name': 'config', 'mountPath': '/config', 'readOnly': True}],
                 'resources': {'requests': {'cpu': '100m', 'memory': '64Mi'},
                               'limits': {'cpu': '1', 'memory': '512Mi'}}}
    template = pod_spec([container], restartPolicy='Never')
    template['spec'].update(serviceAccountName='metering-usage-worker',
                            volumes=[{'name': 'config', 'configMap': {'name': 'metering-usage-worker'}}])
    return [resource('ConfigMap', 'metering-usage-worker', ns,
                     data={'usage-worker.json': json.dumps(settings, ensure_ascii=False, indent=2)+'\n'}),
            resource('ServiceAccount', 'metering-usage-worker', ns),
            resource('Lease', 'metering-usage-worker', ns, apiVersion='coordination.k8s.io/v1', spec={}),
            resource('Role', 'metering-usage-worker', ns, apiVersion='rbac.authorization.k8s.io/v1', rules=[
                {'apiGroups': ['coordination.k8s.io'], 'resources': ['leases'],
                 'resourceNames': ['metering-usage-worker'], 'verbs': ['get', 'update']},
                {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get']}]),
            resource('RoleBinding', 'metering-usage-worker', ns, apiVersion='rbac.authorization.k8s.io/v1',
                     subjects=[{'kind': 'ServiceAccount', 'name': 'metering-usage-worker', 'namespace': ns}],
                     roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'metering-usage-worker'}),
            resource('CronJob', 'metering-usage-worker', ns, apiVersion='batch/v1', spec={
                'schedule': '0 1 * * *', 'concurrencyPolicy': 'Forbid',
                'successfulJobsHistoryLimit': 2, 'failedJobsHistoryLimit': 3,
                'jobTemplate': {'spec': {'backoffLimit': 2, 'activeDeadlineSeconds': 21600,
                                         'template': template}}})]


def render_manifests(args, images, output, wasm_file=None):
    gateway_config = gateway_configuration(args)
    ns = args.namespace
    write_yaml(output / 'k8s/00-namespace.yaml', [{
        'apiVersion': 'v1', 'kind': 'Namespace',
        'metadata': {'name': ns, 'labels': {'istio-injection': 'disabled'}}}])
    for filename, resources in (
            ('10-infrastructure', infrastructure(args, images)),
            ('20-config', configurations(args)), ('30-init', initialization(args, images)),
            ('40-applications', applications(args, images)),
            ('50-usage-worker', summary_resources(args, images))):
        write_yaml(output / 'k8s' / (filename + '.yaml'), resources)
    dns = f'metering-observer.{ns}.svc.cluster.local'
    config_path = output / 'gateway-observer.json'
    config_path.write_text(json.dumps(gateway_config, indent=2) + '\n')
    for mode in ('disabled', 'enabled'):
        target = output / 'gateway' / mode / 'envoyfilter.yaml'
        command = [sys.executable, str(ROOT / 'scripts/render_gateway_filter.py'),
                   '--gateway-namespace', args.gateway_namespace, '--config', str(config_path),
                   '--wasm-file', str(wasm_file or ROOT / 'dist/gateway-observer-v1.wasm'),
                   '--wasm-url', 'http://' + dns + ':9093/wasm/gateway-observer-v1.wasm',
                   '--wasm-cluster', 'outbound|9093||' + dns, '--output', str(target)]
        for key, value in selector(args.gateway_label).items():
            command += ['--gateway-label', key + '=' + value]
        if mode == 'enabled':
            command.append('--enable')
        run(command)
        text = target.read_text().replace(
            '# Template only: the JSON is default-disabled and must be rendered/validated.',
            '# 已渲染，可直接 kubectl apply；实例身份在 Wasm 运行时读取。')
        target.write_text(text)
        if mode == 'disabled':
            (output / 'envoyfilter.yaml').write_text(text, encoding='utf-8')
    config_path.unlink()


def prepare_images(args, images):
    for name in ('observer', 'consumer', 'usage-worker'):
        print('构建镜像：' + images[name], flush=True)
        run(['docker', 'build', '--platform', args.platform, '--network', args.build_network,
             '--build-arg', 'HTTP_PROXY', '--build-arg', 'HTTPS_PROXY', '--build-arg', 'NO_PROXY',
             '-f', f'deploy/{name}/Dockerfile',
             '-t', images[name], '.'], capture=False)
    records = {}
    for image in dict.fromkeys(images.values()):
        try:
            raw = run(['docker', 'image', 'inspect', image]).stdout
        except subprocess.CalledProcessError:
            run(['docker', 'pull', '--platform', args.platform, image], capture=False)
            raw = run(['docker', 'image', 'inspect', image]).stdout
        info = json.loads(raw)[0]
        if info['Os'] + '/' + info['Architecture'] != args.platform:
            raise ValueError('镜像平台与分发平台不一致：' + image)
        records[image] = {'id': info['Id'], 'repo_digests': info.get('RepoDigests') or []}
    return records


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def package(args):
    target = args.output.resolve()
    if target.exists() and (not target.is_dir() or any(target.iterdir())):
        raise ValueError('输出目录已存在且非空，请通过 PACKAGE_DIR 指定新目录：' + str(target))
    gateway_config = gateway_configuration(args)
    wasm_sha = checksum(ROOT / 'dist/gateway-observer-v1.wasm')
    labels = selector(args.gateway_label)
    tag = resolve_image_tag()
    images = {'observer': 'metering-observer:' + tag, 'consumer': 'metering-consumer:' + tag,
              'usage-worker': 'metering-usage-worker:' + tag,
              **RUNTIME_IMAGES}
    archived_images = images if args.include_dependency_images else {
        name: images[name] for name in ('observer', 'consumer', 'usage-worker')}
    external_images = {name: image for name, image in images.items() if name not in archived_images}
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.metering-package-', dir=target.parent) as directory:
        output = Path(directory) / 'package'
        output.mkdir(mode=0o700)
        render_manifests(args, images, output)
        # 配置、工作负载标签、Wasm 校验全部通过后，才构建镜像并导出。
        records = prepare_images(args, archived_images)
        (output / 'images').mkdir()
        print('导出镜像：' + ', '.join(archived_images.values()), flush=True)
        run(['docker', 'save', '--output', str(output / 'images/images.tar'),
             *dict.fromkeys(archived_images.values())], capture=False)
        manifest = {'image_tag': tag, 'platform': args.platform, 'images': images,
                    'archived_images': archived_images, 'external_images': external_images,
                    'image_details': records, 'wasm_sha256': wasm_sha,
                    'doris_migrations': list(MIGRATIONS),
                    'namespace': args.namespace, 'gateway_namespace': args.gateway_namespace,
                    'accepted_request_hosts': gateway_config['accepted_request_hosts'],
                    'gateway_selector': labels, 'gateway_filter_name': filter_name(labels),
                    'gateway_identity_source': 'node.metadata.NAMESPACE/NAME'}
        (output / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
        (output / 'README.md').write_text(package_readme(args), encoding='utf-8')
        files = sorted(path for path in output.rglob('*') if path.is_file())
        (output / 'SHA256SUMS').write_text(''.join(
            file_sha256(path) + '  ' + str(path.relative_to(output)) + '\n' for path in files))
        os.replace(output, target)
    print('分发包已生成：' + str(target), flush=True)


def package_readme(args):
    hosts = gateway_configuration(args)['accepted_request_hosts']
    image_note = ('镜像归档包含 Observer、Consumer、usage-worker、Kafka、Doris、Istio proxyv2。'
                  if args.include_dependency_images else
                  '镜像归档包含 Observer、Consumer、usage-worker；Kafka、Doris、Istio proxyv2 镜像不随包提供。\n'
                  'Kafka / Doris 部署清单和初始化 Job 仍保留；部署前需保证节点能拉取这些依赖镜像，\n'
                  '或在所有可能调度相关 Pod 的节点预先导入对应镜像。已有 Istio 网关继续复用。')
    return f'''# 单节点采集分发包

运行平台：{args.platform}；采集命名空间：{args.namespace}。
Kafka 3.9.2（24 分区、单副本、保留 72 小时），Doris 4.1.3（单 FE / BE）。
本包使用明文 Kafka 和 Doris root 空密码，仅用于受控开发环境；不代表生产 G0–G5 验收。
目标集群需已有 Istio 1.13.5、网关业务路由和鉴权，以及可用的 StorageClass / PV。
本包不安装 Istio、不修改已有网关的 Deployment、路由和鉴权。
{image_note}
Wasm 已嵌入 Observer 镜像。manifest.json 的 images 记录全部镜像引用，
archived_images 记录归档内镜像，external_images 记录需由目标环境准备的镜像。
Wasm Host 白名单：{json.dumps(hosts, ensure_ascii=False)}，同时记录于 manifest.json 和每个 EnvoyFilter。
省略 --accepted-request-host 时默认 ["*"]，匹配全部合法 DNS Host；显式传参完整覆盖默认值。
后缀通配匹配子域名，不含根域名。
包根目录 envoyfilter.yaml 提供关闭态 EnvoyFilter；
gateway/disabled/ 和 gateway/enabled/ 分别提供同一工作负载的关闭态与启用态配置。
打包不查询 Kubernetes，不记录 Pod 名称或 UID。实例标识在运行时读取为 namespace/pod-name。

归属 API 默认 URI 为空，使用占位实现。启用服务查询时，在 `k8s/20-config.yaml` 的
`observer.json` 中保留实际 `service_lookup.uri`，将 `service_lookup.credential_env` 设为
`METERING_SERVICE_LOOKUP_TOKEN`。`credential_env` 只能填写环境变量名，不能填写密钥；
密钥通过 `metering-observer-lookups` Secret 的同名键注入，现有 Deployment 已配置 `envFrom`。
客户查询 URI 为空时无需配置客户查询密钥；启用后同理使用 `METERING_CUSTOMER_LOOKUP_TOKEN`。

已部署环境可先用 `kubectl -n {args.namespace} edit configmap metering-observer` 修正变量名，
再从无额外换行的密钥文件更新 Secret，并重启 Observer：

```sh
kubectl -n {args.namespace} create secret generic metering-observer-lookups --from-file=METERING_SERVICE_LOOKUP_TOKEN=/安全目录/service-lookup-token --dry-run=client -o yaml | kubectl apply -f -
kubectl -n {args.namespace} rollout restart deployment/metering-observer
kubectl -n {args.namespace} rollout status deployment/metering-observer --timeout=5m
```

`config_lookup` 表示归属查询参数或凭据变量名校验失败；变量名合法但对应密钥缺失时，报 `lookup_credential`。
将同一修正同步到留存的 YAML；包内空 Secret 用于初次部署，提供真实密钥后不要用空内容覆盖它。

在本目录校验：

```sh
sha256sum -c SHA256SUMS
```

在每个可能调度这些 Pod 的 Kubernetes 节点导入镜像。
Docker 运行时使用 `docker load -i images/images.tar`；containerd 使用
`sudo ctr -n k8s.io images import images/images.tar`。
kind 环境可用 `kind load image-archive images/images.tar --name 集群名`。
镜像使用原始 tag 和 IfNotPresent，名称与 manifest.json、YAML 完全一致。

先部署采集组件，等待初始化完成；以下 YAML 已包含全部配置、Secret、迁移与 Job，无需再次渲染：

```sh
kubectl apply -f k8s/
kubectl -n {args.namespace} wait --for=condition=complete job/metering-kafka-init job/metering-doris-init --timeout=20m
kubectl -n {args.namespace} rollout status deployment/metering-consumer --timeout=5m
kubectl -n {args.namespace} rollout status deployment/metering-observer --timeout=5m
kubectl -n {args.namespace} get pods,svc,pvc
```

从旧包升级时先删除旧包 gateway/disabled/ 中按 UID 命名或固定名 gateway-observer 的 EnvoyFilter，避免重复安装；
不要同时保留新旧过滤器。旧配置中的 gateway_instance_id 已移除，必须使用与新 Wasm 配套的配置。

确认 Wasm 下载内容与 manifest.json 的 wasm_sha256 一致、过滤器顺序为 ext_authz → Wasm → router 后，
对已存在的网关应用插件。先应用包根目录汇总的关闭态配置，再按验证结果启用采集：

```sh
kubectl apply -f envoyfilter.yaml
# 完成加载与顺序检查后启用采集
kubectl apply -f gateway/enabled/
# 需要关闭采集时
kubectl apply -f envoyfilter.yaml
```

网关命名空间：{args.gateway_namespace}；工作负载标签：{json.dumps(selector(args.gateway_label), ensure_ascii=False)}。
EnvoyFilter 名称：{filter_name(selector(args.gateway_label))}，同样记录于 manifest.json 的 gateway_filter_name。
代理 NAMESPACE 匹配将作用范围固定在上述命名空间，即使该命名空间是 Istio 根配置命名空间。
名称由排序后的标签生成；不同工作负载使用独立名称，同一标签的启用态、关闭态和版本更新保持名称一致。
变更选择器会产生新资源，需先移除旧清单对应的过滤器；正式与灰度网关应使用互斥的标签。
网关 Pod 扩容、重建后自动应用同一过滤器，不需要重新打包；无需修改网关 Pod 模板。
同名 Pod 重建后实例标识相同，不能用于区分 Pod 生命周期。跨集群查询需结合 cluster_name。
若运行时身份字段缺失、非法或 namespace/pod-name 超过 128 字节，插件停止采集并计数
metering_gateway_identity_unavailable_total，业务继续放行，不回退到固定值或截断名称。
envoyfilter.yaml 与 gateway/disabled/ 内容相同，选择其中一个入口即可。
gateway 目录中全部为最终 YAML，不含待替换字段；不要递归同时 apply disabled 和 enabled。
usage-worker 由 k8s/50-usage-worker.yaml 每天 01:00 UTC 调度，部署前核对控制面时区。
已有数据环境须通过 --collection-start-date 指定真实采集起点，不能把包生成日期当作历史覆盖起点。
初始化 Job 只处理本包的单节点 Kafka / Doris；已有旧事件表会拒绝初始化，请使用隔离命名空间 / 数据卷。
数据库只执行 001_init.sql、002_consumer_coordination.sql、003_usage_summaries.sql，
直接创建最终明细、消费协调及日/月物化视图结构；不提供旧开发库兼容升级。
PVC 数据在删除 StatefulSet 后仍保留；本包不自动删除 PVC、既有数据或网关资源。
重新 apply 同一包会保留已完成的 Job；升级迁移时需审查 SQL 并单独安排初始化 Job。
'''


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, default=ROOT / 'dist/package')
    p.add_argument('--namespace', type=dns_name, default='metering')
    p.add_argument('--gateway-namespace', type=dns_name, default='istio-system')
    p.add_argument('--accepted-request-host', action='append', metavar='HOST',
                   help='目标业务 Host 白名单，可重复；省略时默认 *，显式传参完整覆盖默认值')
    p.add_argument('--gateway-label', type=label, action='append', metavar='KEY=VALUE',
                   help='目标网关标签，可重复；省略时默认 istio=ingressgateway')
    p.add_argument('--cluster-name', default='metering-single-node')
    p.add_argument('--database', type=database_name, default='metering')
    p.add_argument('--topic', default='llm-metering-events')
    p.add_argument('--group', default='llm-metering-doris-v1')
    p.add_argument('--collection-start-date', default=datetime.now(timezone.utc).date().isoformat(),
                   help='实际开始采集的 UTC 日期；已有环境必须显式指定，不能从空分区推断')
    p.add_argument('--platform', choices=['linux/amd64', 'linux/arm64'], default='linux/amd64')
    p.add_argument('--include-dependency-images', action='store_true',
                   help='同时归档 Kafka、Doris、Istio proxyv2 镜像；默认归档 Observer、Consumer、usage-worker')
    p.add_argument('--build-network', default='host', help='Docker 构建网络，默认 host；可指定 default')
    p.add_argument('--storage-class', help='省略时使用目标集群默认 StorageClass；空串表示手工绑定 PV')
    p.add_argument('--kafka-storage', default='10Gi')
    p.add_argument('--doris-fe-storage', default='10Gi')
    p.add_argument('--doris-be-storage', default='20Gi')
    return p


def main():
    p = parser()
    args = p.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9._-]{1,249}', args.topic) or args.topic in ('.', '..'):
        p.error('无效 Kafka topic')
    if not re.fullmatch(r'[A-Za-z0-9._-]{1,128}', args.group):
        p.error('无效 Consumer group')
    if not args.cluster_name.strip() or len(args.cluster_name) > 128:
        p.error('cluster-name 必须为 1–128 字符')
    for value in (args.kafka_storage, args.doris_fe_storage, args.doris_be_storage):
        if not re.fullmatch(r'[1-9][0-9]*(?:Mi|Gi|Ti)', value):
            p.error('存储容量必须为正整数并以 Mi、Gi 或 Ti 结尾')
    os.umask(0o077)
    try:
        package(args)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        p.exit(1, '打包失败：' + str(error) + '\n')


if __name__ == '__main__':
    main()
