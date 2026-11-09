"""Kubernetes-only infrastructure and the migrated Go integration Job."""
import hashlib
import json
import re
import shutil
import time

IMAGES = ('apache/kafka:3.9.2', 'apache/doris:all-in-one-4.1.3', 'istio/proxyv2:1.14.6')


def resource(kind, name, namespace, **fields):
    return {'apiVersion': 'v1', 'kind': kind,
            'metadata': {'name': name, 'namespace': namespace}, **fields}


def workload(name, namespace, image, ports, **container_fields):
    labels = {'app': name}
    container = {'name': name, 'image': image, 'imagePullPolicy': 'IfNotPresent',
                 'ports': [{'containerPort': port} for port in ports], **container_fields}
    deployment = {'apiVersion': 'apps/v1', 'kind': 'Deployment',
                  'metadata': {'name': name, 'namespace': namespace},
                  'spec': {'replicas': 1, 'selector': {'matchLabels': labels},
                           'template': {'metadata': {'labels': labels,
                                                    'annotations': {'sidecar.istio.io/inject': 'false'}},
                                        'spec': {'automountServiceAccountToken': False,
                                                 'containers': [container]}}}}
    service = resource('Service', name, namespace,
                       spec={'selector': labels, 'ports': [{'name': 'tcp-' + str(p), 'port': p} for p in ports]})
    return [deployment, service]


def infrastructure(namespace):
    env = {'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
           'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@127.0.0.1:9093',
           'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
           'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
           'KAFKA_LISTENERS': 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093',
           'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://llm-token-stats-kafka:9092',
           'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
           'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
           'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
           'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false', 'KAFKA_HEAP_OPTS': '-Xms128m -Xmx256m'}
    items = workload('llm-token-stats-kafka', namespace, IMAGES[0], [9092],
                     env=[{'name': k, 'value': v} for k, v in env.items()],
                     resources={'requests': {'cpu': '100m', 'memory': '384Mi'},
                                'limits': {'cpu': '1', 'memory': '768Mi'}},
                     readinessProbe={'tcpSocket': {'port': 9092}, 'periodSeconds': 2})
    items += workload('llm-token-stats-doris', namespace, IMAGES[1], [9030, 8040],
                      env=[{'name': 'FE_HEAP', 'value': '512m'},
                           {'name': 'BE_HEAP', 'value': '256m'},
                           {'name': 'BE_CONFIG_EXTRA',
                            'value': 'mem_limit = 50%\nmax_sys_mem_available_low_water_mark_bytes = 268435456'}],
                      resources={'requests': {'cpu': '250m', 'memory': '1Gi'},
                                 'limits': {'cpu': '2', 'memory': '3Gi'}},
                      readinessProbe={'tcpSocket': {'port': 9030}, 'periodSeconds': 2})
    return {'apiVersion': 'v1', 'kind': 'List', 'items': items}


def bootstrap(root, namespace):
    cfg = json.loads((root / 'deploy/local/envoy.json').read_text())
    cfg['node']['metadata'] = {'NAMESPACE': namespace, 'NAME': 'llm-token-stats-e2e-gateway'}
    cfg['node']['id'] = namespace + '/llm-token-stats-e2e-gateway'
    res = cfg['static_resources']
    for cluster in res['clusters']:
        address = cluster['load_assignment']['endpoints'][0]['lb_endpoints'][0]['endpoint']['address']['socket_address']
        address['address'] = 'llm-token-stats-e2e-fixture' if cluster['name'] in ('fixture', 'auth') else 'llm-token-stats-e2e-observer'
    plugin = res['listeners'][0]['filter_chains'][0]['filters'][0]['typed_config']['http_filters'][1]['typed_config']['config']
    remote = plugin['vm_config']['code']['remote']
    remote['http_uri']['uri'] = 'http://llm-token-stats-e2e-observer:9093/wasm/gateway-observer-v1.wasm'
    remote['sha256'] = hashlib.sha256((root / 'dist/gateway-observer-v1.wasm').read_bytes()).hexdigest()
    return cfg


def prepare_suite(root, namespace):
    suite = root / 'dist/k8s-suite'
    for directory in ('dist', 'tests/integration', 'deploy/local', 'testdata/collection'):
        (suite / directory).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(root / 'dist/integration.test', suite / 'integration.test')
    (suite / 'integration.test').chmod(0o555)
    shutil.copyfile(root / 'dist/gateway-observer-v1.wasm', suite / 'dist/gateway-observer-v1.wasm')
    shutil.copyfile(root / 'deploy/local/envoy.json', suite / 'deploy/local/envoy.json')
    for path in (root / 'testdata/collection').glob('*.json'):
        shutil.copyfile(path, suite / 'testdata/collection' / path.name)
    cfg = bootstrap(root, namespace)
    (suite / 'dist/envoy.remote.json').write_text(json.dumps(cfg))
    return cfg


def suite_resources(root, namespace, images, proxy_image=IMAGES[2]):
    items = []
    for component in ('observer', 'consumer', 'fixture'):
        name = 'llm-token-stats-e2e-' + component
        config = None
        if component != 'fixture':
            config = json.loads((root / ('deploy/local/' + component + '.json')).read_text())
            config['kafka']['bootstrap_servers'] = ['llm-token-stats-kafka:9092']
            if component == 'observer':
                config['observer']['wasm_download']['file'] = '/usr/share/metering/wasm/gateway-observer-v1.wasm'
            else:
                config['doris']['fe_url'] = 'http://llm-token-stats-doris:8040'
                config['doris']['allowed_load_hosts'] = ['llm-token-stats-doris:8040']
                # Dedicated batching tests set their own row/byte/time limits.
                config['consumer']['flush_interval_ms'] = 1000
            args = ['--config', '/config/' + component + '.json']
        else:
            args = []
        ports = {'observer': [9090, 9091, 9093], 'consumer': [9092], 'fixture': [8081, 9002]}[component]
        group = workload(name, namespace, images[component], ports, args=args,
                         resources={'requests': {'cpu': '50m', 'memory': '64Mi'},
                                    'limits': {'cpu': '1', 'memory': '256Mi'}})
        deployment = group[0]
        template = deployment['spec']['template']
        template['metadata']['labels']['e2e-component'] = 'true'
        spec = template['spec']
        container = spec['containers'][0]
        if component != 'fixture':
            container.setdefault('env', []).append({'name': 'LOG_LEVEL', 'value': 'info'})
        health, port = ('/healthz', 8081) if component == 'fixture' else ('/readyz', ports[-1] if component == 'consumer' else 9091)
        container['readinessProbe'] = {'httpGet': {'path': health, 'port': port}, 'periodSeconds': 2}
        if config:
            items.append(resource('ConfigMap', name, namespace, data={component + '.json': json.dumps(config)}))
            container['volumeMounts'] = [{'name': 'config', 'mountPath': '/config', 'readOnly': True}]
            spec['volumes'] = [{'name': 'config', 'configMap': {'name': name}}]
        if component == 'consumer':
            container['envFrom'] = [{'secretRef': {'name': 'llm-token-stats-e2e-credentials'}}]
        items += group
    items.append(resource('Secret', 'llm-token-stats-e2e-credentials', namespace, stringData={
        'DORIS_DSN': 'root@tcp(llm-token-stats-doris:9030)/metering_development?timeout=5s&readTimeout=30s&writeTimeout=10s&interpolateParams=true&time_zone=%27%2B00%3A00%27'}))
    items.append(resource('ConfigMap', 'llm-token-stats-e2e-gateway', namespace, data={'envoy.json': json.dumps(bootstrap(root, namespace))}))
    gateway = resource('Pod', 'llm-token-stats-e2e-gateway', namespace, spec={
        'automountServiceAccountToken': False, 'containers': [{
            'name': 'envoy', 'image': proxy_image, 'imagePullPolicy': 'IfNotPresent',
            'command': ['/usr/local/bin/envoy'], 'args': ['-c', '/config/envoy.json', '--concurrency', '1', '--log-level', 'warning', '--component-log-level', 'wasm:info'],
            'env': [{'name': 'LOG_LEVEL', 'value': 'info'}],
            'readinessProbe': {'httpGet': {'path': '/ready', 'port': 9901}, 'periodSeconds': 2},
            'resources': {'requests': {'cpu': '50m', 'memory': '64Mi'}, 'limits': {'cpu': '1', 'memory': '512Mi'}},
            'volumeMounts': [{'name': 'config', 'mountPath': '/config', 'readOnly': True}]}],
        'volumes': [{'name': 'config', 'configMap': {'name': 'llm-token-stats-e2e-gateway'}}]})
    gateway['metadata'].update(labels={'app': 'llm-token-stats-e2e-gateway', 'e2e-component': 'true'},
                               annotations={'sidecar.istio.io/inject': 'false'})
    items += [gateway, resource('Service', 'llm-token-stats-e2e-gateway', namespace,
                               spec={'selector': {'app': 'llm-token-stats-e2e-gateway'}, 'ports': [{'name': 'http', 'port': 8080}, {'name': 'admin', 'port': 9901}]})]
    return {'apiVersion': 'v1', 'kind': 'List', 'items': items}


def job_resources(namespace, image, consumer_image, proxy_image=IMAGES[2]):
    name = 'llm-token-stats-collection-integration'
    account = resource('ServiceAccount', name, namespace)
    role = resource('Role', name, namespace, rules=[
        {'apiGroups': [''], 'resources': ['pods', 'configmaps'], 'verbs': ['create', 'get', 'list', 'delete']},
        {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']}])
    role['apiVersion'] = 'rbac.authorization.k8s.io/v1'
    binding = resource('RoleBinding', name, namespace,
                       subjects=[{'kind': 'ServiceAccount', 'name': name, 'namespace': namespace}],
                       roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': name})
    binding['apiVersion'] = 'rbac.authorization.k8s.io/v1'
    job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name, 'namespace': namespace},
           'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 1920,
                    'template': {'metadata': {'annotations': {'sidecar.istio.io/inject': 'false'}},
                                 'spec': {'serviceAccountName': name, 'restartPolicy': 'Never',
                                          'containers': [{'name': 'tests', 'image': image, 'imagePullPolicy': 'IfNotPresent',
                                                          'env': [{'name': 'METERING_TEST_NAMESPACE', 'value': namespace},
                                                                  {'name': 'METERING_TEST_IMAGE', 'value': image},
                                                                  {'name': 'METERING_CONSUMER_IMAGE', 'value': consumer_image},
                                                                  {'name': 'METERING_ENVOY_IMAGE', 'value': proxy_image},
                                                                  {'name': 'POD_IP', 'valueFrom': {'fieldRef': {'fieldPath': 'status.podIP'}}}],
                                                          'resources': {'requests': {'cpu': '100m', 'memory': '128Mi'},
                                                                        'limits': {'cpu': '2', 'memory': '512Mi'}}}]}}}}
    return {'apiVersion': 'v1', 'kind': 'List', 'items': [account, role, binding, job]}


def run_suite(harness, root, migrations, render_sql):
    # Separate database/topic from the mesh deployment assertions, but in the
    # same disposable namespace and the same Kubernetes Kafka/Doris instances.
    for name in migrations:
        harness.sql(render_sql((root / 'migrations/doris/metering' / name).read_text(), 'metering_development'))
    harness.kafka('topics', '--create', '--topic', 'llm-metering-development', '--partitions', '24', '--replication-factor', '1')
    manifest = suite_resources(root, harness.namespace, harness.images, harness.proxy_image)
    (harness.output / 'integration-resources.json').write_text(json.dumps(manifest, indent=2))
    # Native remote Wasm loading is intentionally bounded and does not retry.
    # Start the normal gateway only after its download service is ready; the
    # explicit fault probes test an unavailable download service separately.
    gateway = next(item for item in manifest['items'] if item['kind'] == 'Pod')
    harness.apply({**manifest, 'items': [item for item in manifest['items'] if item is not gateway]})
    for name in ('llm-token-stats-e2e-observer', 'llm-token-stats-e2e-consumer', 'llm-token-stats-e2e-fixture'):
        harness.kubectl('-n', harness.namespace, 'rollout', 'status', 'deployment/' + name, '--timeout=180s', timeout=195)
    harness.apply(gateway)
    harness.kubectl('-n', harness.namespace, 'wait', '--for=condition=Ready', 'pod/llm-token-stats-e2e-gateway', '--timeout=120s', timeout=135)
    manifest = job_resources(harness.namespace, harness.images['integration'], harness.images['consumer'], harness.proxy_image)
    (harness.output / 'integration-job.json').write_text(json.dumps(manifest, indent=2))
    harness.apply(manifest)
    deadline = time.monotonic() + 1950
    while time.monotonic() < deadline:
        job = harness.get('-n', harness.namespace, 'get', 'job', 'llm-token-stats-collection-integration')
        status = job.get('status', {})
        if status.get('succeeded') or status.get('failed'):
            logs = harness.kubectl('-n', harness.namespace, 'logs', 'job/llm-token-stats-collection-integration', '--all-containers=true')
            (harness.output / 'integration.log').write_text(logs)
            print(logs, flush=True)
            if not status.get('succeeded'):
                raise RuntimeError('Kubernetes 集成测试失败，见 integration.log')
            expected = set()
            for path in (root / 'tests/integration').glob('*_test.go'):
                expected.update(re.findall(r'^func (Test\w+)\(t \*testing.T\)', path.read_text(), re.M))
            passed = validate_test_log(logs, expected)
            return {'status': 'passed', 'job': 'llm-token-stats-collection-integration', 'log': 'integration.log',
                    'passed_tests': len(passed), 'test_names': sorted(passed)}
        time.sleep(2)
    raise RuntimeError('Kubernetes 集成测试 Job 超时')


def validate_test_log(logs, expected):
    passed = set(re.findall(r'^--- PASS: (Test\w+) ', logs, re.M))
    if not expected or passed != expected or re.search(r'--- (?:SKIP|FAIL):', logs):
        raise RuntimeError('Kubernetes 集成测试未完整执行或包含跳过/失败场景')
    return passed
