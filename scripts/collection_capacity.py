"""隔离 Docker 容量对照：真实 Envoy/Observer/Kafka，排空后 Consumer/Doris 对账。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import statistics
import subprocess
import sys
import time
import urllib.request
import uuid

from render_doris_sql import MIGRATIONS, render_sql

ROOT = Path(__file__).resolve().parents[1]
IMAGES = {'envoy': 'istio/proxyv2:1.14.6', 'kafka': 'apache/kafka:3.9.2',
          'doris': 'apache/doris:all-in-one-4.1.3'}
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
MIXED_CONTEXTS = (1000000, 200000, 200000, 500000, 200000,
                  1000000, 200000, 500000, 200000, 100000)


def expected_profile_counts(count):
    whole, remainder = divmod(count, len(MIXED_CONTEXTS))
    return {str(tokens): whole * MIXED_CONTEXTS.count(tokens) + MIXED_CONTEXTS[:remainder].count(tokens)
            for tokens in set(MIXED_CONTEXTS)}


def run(args, **kw):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True,
                          timeout=kw.pop('timeout', 60), check=kw.pop('check', True), **kw)


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def get(url):
    with HTTP.open(url, timeout=2) as response:
        return response.read().decode()


def healthy(url):
    get(url)  # Readiness endpoints may legitimately return an empty 200 body.
    return True


def wait(probe, timeout=120):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            value = probe()
            if value:
                return value
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
        time.sleep(1)
    raise RuntimeError('等待隔离测试组件就绪超时')


def address(n):
    return {'socket_address': {'address': '127.0.0.1', 'port_value': n}}


def envoy_config(ports, enabled, wasm):
    cfg = json.loads((ROOT / 'deploy/local/envoy.json').read_text())
    cfg['node'] = {'id': 'capacity', 'cluster': 'capacity',
                   'metadata': {'NAMESPACE': 'capacity', 'NAME': 'llm-token-stats-capacity-gateway'}}
    cfg['admin']['address'] = address(ports['admin'])
    resources = cfg['static_resources']
    listener = resources['listeners'][0]
    listener['address'] = address(ports['business'])
    hcm = listener['filter_chains'][0]['filters'][0]['typed_config']
    hcm['http2_protocol_options'] = {'max_concurrent_streams': 100000,
                                   'initial_stream_window_size': 65536,
                                   'initial_connection_window_size': 16 << 20}
    hcm['stream_idle_timeout'] = '180s'
    for host in hcm['route_config']['virtual_hosts']:
        for route in host['routes']:
            route['route']['timeout'] = '0s'
    filters = hcm['http_filters']
    # Uninstalled baseline; authentication fixture is excluded in both groups.
    hcm['http_filters'] = filters[1:] if enabled else filters[2:]
    if enabled:
        plugin = filters[1]['typed_config']['config']
        plugin['vm_config']['code'] = {'local': {'filename': str(wasm)}}
        pc = json.loads((ROOT / 'configs/gateway-observer.example.json').read_text())
        pc.update(enabled=True, observer_cluster='observer')
        plugin['configuration'] = {'@type': 'type.googleapis.com/google.protobuf.Struct', 'value': pc}
    clusters = []
    for c in resources['clusters']:
        if c['name'] not in ('fixture', 'observer'):
            continue
        c['type'] = 'STATIC'
        c['load_assignment']['endpoints'][0]['lb_endpoints'][0]['endpoint']['address'] = address(ports[c['name']])
        c['http2_protocol_options'] = {'max_concurrent_streams': 100000,
                                      'initial_stream_window_size': 65536,
                                      'initial_connection_window_size': 16 << 20}
        c['circuit_breakers'] = {'thresholds': [{'max_connections': 1000, 'max_pending_requests': 131072,
                                               'max_requests': 131072, 'max_retries': 0}]}
        clusters.append(c)
    resources['clusters'] = clusters
    return cfg


def capacity_load_valid(args, count, profile, aborted, exit_code, load, fixture, steady_qps, profiles):
    target = args.qps * (args.warmup + args.steady)
    return (not count and profile == 'mixed' and not aborted and exit_code == 0
            and load.get('finished', False) and not load.get('cancelled', False)
            and args.qps >= 200 and args.warmup >= 60 and args.steady >= 300
            and load.get('issued') == target and load.get('complete') == target
            and load.get('business_errors') == 0
            and profiles == expected_profile_counts(target)
            and steady_qps >= args.qps * .99 and fixture['peak'] >= args.qps * 60 * .9)


def rss_comparison(baseline, enabled, budget=2048 << 20):
    if not baseline or not enabled:
        raise ValueError('RSS 对照缺少基线或启用组样本')
    peak_delta = max(x['rss'] for x in enabled) - max(x['rss'] for x in baseline)
    aligned = max(x['rss'] - min(baseline, key=lambda b: abs(b['elapsed'] - x['elapsed']))['rss']
                  for x in enabled)
    return {'extra_rss_peak_difference': peak_delta, 'extra_rss_max_time_aligned': aligned,
            'rss_budget_bytes': budget, 'sampled_rss_comparison_within_budget': peak_delta <= budget and aligned <= budget}


class Harness:
    def __init__(self, args):
        self.args = args
        self.id = uuid.uuid4().hex[:12]
        self.output = (ROOT / args.output / self.id).resolve()
        self.output.mkdir(parents=True)
        self.names = []
        self.processes = []
        self.ports = {k: port() for k in ('business', 'admin', 'fixture', 'observer', 'health', 'consumer', 'kafka', 'controller', 'doris', 'be')}
        self.topic = 'llm-capacity-' + self.id
        self.database = 'metering_capacity_' + self.id
        self.load = str(ROOT / 'dist/collection-load')
        self.verifications = []
        self.phases = []
        self.pidstats = {}
        self.gateway_pids = {}

    def docker(self, role, extra, image):
        name = 'llm-token-stats-capacity-' + role + '-' + self.id
        result = run(['docker', 'run', '-d', '--name', name,
                      '--label', 'llm-token-stats-capacity=' + self.id, *extra, image])
        self.names.append(name)
        return name

    def stop_container(self, name):
        if name in self.names:
            logs = run(['docker', 'logs', name], check=False)
            (self.output / (name + '.log')).write_text(logs.stdout + logs.stderr)
            run(['docker', 'stop', '--time', '5', name], timeout=20, check=False)
            run(['docker', 'rm', name], timeout=20, check=False)
            self.names.remove(name)

    def process(self, name, args, env=None):
        log = (self.output / (name + '.log')).open('w')
        p = subprocess.Popen(args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        log.close()
        self.processes.append(p)
        self.pidstats[name] = p.pid
        return p

    def stop_process(self, p):
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        if p in self.processes:
            self.processes.remove(p)

    def infrastructure(self):
        p = self.ports
        env = {'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
               'KAFKA_CONTROLLER_QUORUM_VOTERS': f"1@127.0.0.1:{p['controller']}",
               'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
               'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
               'KAFKA_LISTENERS': f"PLAINTEXT://127.0.0.1:{p['kafka']},CONTROLLER://127.0.0.1:{p['controller']}",
               'KAFKA_ADVERTISED_LISTENERS': f"PLAINTEXT://127.0.0.1:{p['kafka']}",
               'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
               'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1', 'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false',
               'KAFKA_HEAP_OPTS': '-Xms128m -Xmx512m'}
        extra = ['--network', 'host', '--add-host', socket.gethostname() + ':127.0.0.1']
        for k, v in env.items():
            extra += ['-e', k + '=' + v]
        self.kafka = self.docker('kafka', extra, IMAGES['kafka'])
        wait(lambda: self.kafka_command('topics', '--list').returncode == 0)
        self.kafka_command('topics', '--create', '--topic', self.topic, '--partitions', '3', '--replication-factor', '1', check=True)
        cfg = json.loads((ROOT / 'configs/observer.production.example.json').read_text())
        cfg['kafka'] = {'bootstrap_servers': [f"127.0.0.1:{p['kafka']}"], 'topic': self.topic}
        cfg['observer'].pop('wasm_download', None)
        cfg['observer'].update(listen=f"127.0.0.1:{p['observer']}", health_listen=f"127.0.0.1:{p['health']}",
                               producer_id='capacity-' + self.id, cluster_name='capacity', deployment_environment='dev')
        write(self.output / 'observer.json', cfg)
        self.observer = self.process('observer', [str(ROOT / 'dist/observer'), '--config', str(self.output / 'observer.json')],
                                     env={**os.environ, 'LOG_LEVEL': 'warn'})
        wait(lambda: healthy(f"http://127.0.0.1:{p['health']}/readyz"))

    def kafka_command(self, tool, *args, check=False):
        return run(['docker', 'exec', self.kafka, '/opt/kafka/bin/kafka-' + tool + '.sh',
                    '--bootstrap-server', f"127.0.0.1:{self.ports['kafka']}", *args], check=check)

    def sample(self, gateway, at):
        gateway_pid, started = self.gateway_pids[gateway]
        status = Path(f'/proc/{gateway_pid}/status').read_text()
        stat = Path(f'/proc/{gateway_pid}/stat').read_text().split(') ', 1)[1].split()
        if stat[19] != started:
            raise RuntimeError('网关进程 PID 已被复用，拒绝采样其他进程')
        rss = next(int(line.split()[1]) * 1024 for line in status.splitlines() if line.startswith('VmRSS:'))
        cpu = (int(stat[11]) + int(stat[12])) / os.sysconf('SC_CLK_TCK')
        mem = {a.split(':')[0]: int(a.split()[1]) * 1024 for a in Path('/proc/meminfo').read_text().splitlines() if a.startswith(('MemTotal:', 'MemAvailable:'))}
        processes = {}
        for name, pid in self.pidstats.items():
            try:
                status = Path(f'/proc/{pid}/status').read_text()
                stat = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
                resident = next(int(line.split()[1]) * 1024 for line in status.splitlines() if line.startswith('VmRSS:'))
                processes[name] = {'rss': resident, 'cpu_seconds': (int(stat[11]) + int(stat[12])) / os.sysconf('SC_CLK_TCK')}
            except (FileNotFoundError, ProcessLookupError, StopIteration):
                pass
        return {'elapsed': round(time.monotonic() - at, 3), 'rss': rss, 'cpu_seconds': cpu,
                'rss_source': 'host_proc', 'gateway_pid': gateway_pid,
                'host_available': mem['MemAvailable'], 'host_memtotal': mem['MemTotal'],
                'host_loadavg': Path('/proc/loadavg').read_text().split()[:3],
                'processes': processes}


    def phase(self, name, enabled, *, count=0, concurrency=2, profile='mixed', duration='60s', omit=False, restart_at=None):
        print('开始真实 Envoy 测试：' + name, flush=True)
        directory = self.output / name
        directory.mkdir()
        p = self.ports
        fixture_args = [self.load, 'fixture', '--listen', f"127.0.0.1:{p['fixture']}", '--duration', duration,
                        '--read-buffer-kib', str(self.args.fixture_read_buffer_kib)]
        if omit:
            fixture_args.append('--omit-usage')
        helper_env = {**os.environ, 'GOMAXPROCS': str(self.args.helper_gomaxprocs)}
        fixture_env = {**os.environ, 'GOMAXPROCS': str(self.args.fixture_gomaxprocs)}
        fixture = self.process(name + '-fixture', fixture_args, fixture_env)
        wait(lambda: get(f"http://127.0.0.1:{p['fixture']}/healthz"))
        write(directory / 'envoy.json', envoy_config(p, enabled, Path('/capacity/gateway-observer-v1.wasm')))
        # Keep image before Envoy arguments, without a shell.
        gateway = 'llm-token-stats-capacity-' + name + '-' + self.id
        run(['docker', 'run', '-d', '--name', gateway, '--label', 'llm-token-stats-capacity=' + self.id,
             '--network', 'host', '-v', f'{self.output}:/capacity:ro', '-e', 'LOG_LEVEL=warn',
             '--entrypoint', '/usr/local/bin/envoy', IMAGES['envoy'], '-c', '/capacity/' + name + '/envoy.json',
             '--concurrency', str(self.args.workers), '--log-level', 'warning', '--component-log-level', 'wasm:warn'])
        self.names.append(gateway)
        wait(lambda: get(f"http://127.0.0.1:{p['admin']}/ready"), 30)
        gateway_pid = int(run(['docker', 'inspect', '--format', '{{.State.Pid}}', gateway]).stdout.strip())
        started = Path(f'/proc/{gateway_pid}/stat').read_text().split(') ', 1)[1].split()[19]
        self.gateway_pids[gateway] = (gateway_pid, started)
        (directory / 'server_info.json').write_text(get(f"http://127.0.0.1:{p['admin']}/server_info"))
        args = [self.load, 'load', '--url', f"http://127.0.0.1:{p['business']}/v1/chat/completions",
                '--output', str(directory / 'load.json'), '--results', str(directory / 'requests.jsonl'),
                '--prefix', self.id + '-' + name, '--profile', profile, '--qps', str(self.args.qps),
                '--seconds', str(self.args.warmup + self.args.steady), '--timeout', '180s',
                '--connections', str(self.args.connections or max(4, self.args.workers * 4))]
        if count:
            args += ['--count', str(count), '--concurrency', str(concurrency)]
        if omit:
            args += ['--omit-usage']
        loader = self.process(name + '-load', args, helper_env)
        at = time.monotonic()
        unix_start = time.time()
        samples, aborted, restarted = [], None, False
        last_progress = -30
        with (directory / 'rss.jsonl').open('w') as output:
            while loader.poll() is None:
                sample = self.sample(gateway, at)
                samples.append(sample)
                if sample['elapsed'] - last_progress >= 30:
                    try:
                        counters = json.loads(get(f"http://127.0.0.1:{p['fixture']}/metrics"))
                        sample['fixture'] = {k: v for k, v in counters.items() if k != 'by_unix_second'}
                    except (OSError, ValueError) as error:
                        sample['fixture'] = {'probe_error': type(error).__name__}
                    print(json.dumps({'phase': name, 'elapsed': sample['elapsed'], 'rss_mib': round(sample['rss'] / (1 << 20), 1),
                                      'available_mib': round(sample['host_available'] / (1 << 20), 1), 'fixture': sample['fixture']}), flush=True)
                    last_progress = sample['elapsed']
                output.write(json.dumps(sample) + '\n')
                output.flush()
                if sample['host_available'] < self.args.min_available_mib * (1 << 20):
                    aborted = 'host_memory_guard'
                    self.stop_process(loader)
                    break
                if restart_at is not None and not restarted and sample['elapsed'] >= restart_at:
                    self.stop_process(self.observer)
                    self.observer = self.process('observer-restarted', [str(ROOT / 'dist/observer'), '--config', str(self.output / 'observer.json')],
                                                 env={**os.environ, 'LOG_LEVEL': 'warn'})
                    wait(lambda: healthy(f"http://127.0.0.1:{p['health']}/readyz"))
                    restarted = True
                time.sleep(.25)
        loader.wait(timeout=10)
        if loader in self.processes:
            self.processes.remove(loader)
        # The drain is valid only for a finished loader and no active uploads.
        with (directory / 'rss.jsonl').open('a') as output:
            for _ in range(3):
                sample = self.sample(gateway, at)
                samples.append(sample)
                output.write(json.dumps(sample) + '\n')
                time.sleep(1)
        metrics = wait(lambda: json.loads(get(f"http://127.0.0.1:{p['fixture']}/metrics")), 30)
        write(directory / 'fixture.json', metrics)
        (directory / 'wasm-stats.txt').write_text(get(f"http://127.0.0.1:{p['admin']}/stats?filter=metering_"))
        (directory / 'observer-metrics.txt').write_text(get(f"http://127.0.0.1:{p['health']}/metrics"))
        load_file = directory / 'load.json'
        load = json.loads(load_file.read_text()) if load_file.exists() else {
            'finished': False, 'issued': None, 'summary_missing': True,
            'target_requests': count or self.args.qps * (self.args.warmup + self.args.steady)}
        if not load.get('finished', False) and aborted is None:
            aborted = 'load_process_exited_without_final_summary'
        latencies, profiles = [], {}
        with (directory / 'requests.jsonl').open() as requests:
            for line in requests:
                record = json.loads(line)
                latencies.append(record['elapsed_us'])
                key = str(record['input'])
                item = profiles.setdefault(key, {'count': 0, 'body_bytes': record['body_bytes']})
                item['count'] += 1
        latencies.sort()
        latency = {'p50_us': latencies[len(latencies) // 2], 'p99_us': latencies[min(len(latencies) - 1, (len(latencies) * 99 + 99) // 100 - 1)]} if latencies else {}
        steady = [v['rss'] for v in samples if self.args.warmup <= v['elapsed'] <= self.args.warmup + self.args.steady]
        buckets = metrics['by_unix_second']
        start = int(unix_start + self.args.warmup) + 1
        steady_count = sum(buckets.get(str(i), 0) for i in range(start, start + max(1, self.args.steady - 1)))
        steady_qps = steady_count / max(1, self.args.steady - 1)
        summary = {'name': name, 'enabled': enabled, 'load': load, 'aborted': aborted, 'fixture': metrics,
                   'steady_actual_qps': steady_qps, 'rss_peak': max(x['rss'] for x in samples),
                   'rss_steady_median': statistics.median(steady) if steady else None,
                   'rss_drained': samples[-1]['rss'] if load.get('finished', False) and metrics['active'] == metrics['uploading'] == 0 else None,
                   'rss_after_load_exit': samples[-1]['rss'],
                   'recorded_outcomes': len(latencies),
                   'cpu_seconds': samples[-1]['cpu_seconds'] - samples[0]['cpu_seconds'],
                   'load_exit_code': loader.returncode, 'latency': latency, 'input_profiles': profiles,
                   'observer_restarted': restarted,
                   'capacity_load_valid': capacity_load_valid(self.args, count, profile, aborted, loader.returncode, load, metrics, steady_qps,
                                                               {key: value['count'] for key, value in profiles.items()})}
        write(directory / 'result.json', summary)
        self.phases.append(summary)
        self.stop_container(gateway)
        self.stop_process(fixture)
        if enabled:
            self.verifications.append((directory, omit))
        print(json.dumps({k: v for k, v in summary.items() if k not in ('load', 'fixture')}, ensure_ascii=False), flush=True)
        return summary, samples

    def sql(self, sql, check=True):
        return run(['docker', 'exec', '-i', self.doris, 'mysql', '--default-character-set=utf8mb4',
                    '-uroot', '-h127.0.0.1', '-P9030', '-N'], input=sql, check=check).stdout

    def reconcile(self):
        # Real production Observer Producer is drained before starting Doris.
        self.stop_process(self.observer)
        p = self.ports
        self.doris = self.docker('doris', ['-p', f"127.0.0.1:{p['doris']}:9030", '-p', f"127.0.0.1:{p['be']}:8040",
                                          '-e', 'FE_HEAP=512m', '-e', 'BE_HEAP=256m',
                                          '-e', 'BE_CONFIG_EXTRA=mem_limit = 50%\nmax_sys_mem_available_low_water_mark_bytes = 268435456'], IMAGES['doris'])
        def storage_ready():
            rows = self.sql('SHOW BACKENDS;', check=False).splitlines()
            return any(len(r.split('\t')) > 15 and r.split('\t')[9] == 'true'
                       and float(r.split('\t')[14].split()[0]) > 0 for r in rows)
        wait(storage_ready, 180)
        for migration in MIGRATIONS:
            self.sql(render_sql((ROOT / 'migrations/doris/metering' / migration).read_text(), self.database))
        cfg = {'kafka': {'bootstrap_servers': [f"127.0.0.1:{p['kafka']}"], 'topic': self.topic},
               'consumer': {'health_listen': f"127.0.0.1:{p['consumer']}", 'group_id': self.topic,
                            'accepted_config_versions': ['collection-v1'], 'flush_interval_ms': 1000},
               'doris': {'fe_url': f"http://127.0.0.1:{p['be']}", 'allowed_load_hosts': [f"127.0.0.1:{p['be']}"],
                         'database': self.database, 'dsn_env': 'DORIS_DSN'}}
        write(self.output / 'consumer.json', cfg)
        env = {**os.environ, 'LOG_LEVEL': 'warn', 'DORIS_DSN': f"root@tcp(127.0.0.1:{p['doris']})/{self.database}?timeout=5s&readTimeout=30s&writeTimeout=10s&interpolateParams=true&time_zone=%27%2B00%3A00%27"}
        self.consumer = self.process('consumer', [str(ROOT / 'dist/consumer'), '--config', str(self.output / 'consumer.json')], env)
        wait(lambda: healthy(f"http://127.0.0.1:{p['consumer']}/readyz"))
        reports = []
        for directory, omit in self.verifications:
            prefix = self.id + '-' + directory.name
            results_file = directory / 'requests.recovery.jsonl'
            if not results_file.exists():
                results_file = directory / 'requests.jsonl'
            with results_file.open() as requests:
                expected = sum(1 for line in requests) * 2
            scope = f"logical_request_id LIKE '{prefix}-%'"
            if results_file.name == 'requests.recovery.jsonl':
                scope += f" AND logical_request_id NOT IN ('{prefix}-000000000','{prefix}-000000001')"
            # On an aborted run, rows for requests with no saved outcome may
            # exceed the expected count. Wait for committed Kafka offsets,
            # rather than allowing those unrelated start rows to end the wait.
            audit_started = time.monotonic()
            deadline = audit_started + self.args.audit_timeout
            drained = False
            last_progress = -30
            while time.monotonic() < deadline:
                n = int(self.sql(f"SELECT COUNT(*) FROM {self.database}.events_raw WHERE {scope};").strip())
                offsets = self.kafka_command('consumer-groups', '--describe', '--group', self.topic)
                (directory / 'kafka-offsets.txt').write_text(offsets.stdout + offsets.stderr)
                partitions = [line.split() for line in offsets.stdout.splitlines()
                              if len(line.split()) >= 6 and line.split()[1] == self.topic]
                if time.monotonic() - audit_started - last_progress >= 30:
                    last_progress = time.monotonic() - audit_started
                    lag = sum(int(row[5]) for row in partitions if row[5].isdigit()) if len(partitions) == 3 else None
                    print(json.dumps({'phase': 'doris-audit', 'scenario': directory.name,
                                      'elapsed': round(last_progress, 1), 'visible_rows': n, 'kafka_lag': lag}), flush=True)
                if len(partitions) == 3 and all(row[5] == '0' for row in partitions):
                    drained = True
                    break
                time.sleep(2)
            # Loading/commits may finish while the Kafka CLI is running.
            n = int(self.sql(f"SELECT COUNT(*) FROM {self.database}.events_raw WHERE {scope};").strip())
            args = [self.load, 'verify', '--results', str(results_file), '--database', self.database,
                    '--output', str(directory / 'verification.json')]
            if omit:
                args += ['--omit-usage']
            verify = run(args, env=env, check=False, timeout=300)
            report = json.loads((directory / 'verification.json').read_text())
            report['phase'] = directory.name
            report['exit_code'] = verify.returncode
            report['kafka_drained'] = drained
            report['prefix_rows'] = n
            report['expected_rows_for_saved_outcomes'] = expected
            report['audit_elapsed_seconds'] = round(time.monotonic() - audit_started, 3)
            report['passed'] = report['passed'] and drained
            reports.append(report)
        return reports

    def execute(self):
        wasm = ROOT / 'dist/gateway-observer-v1.wasm'
        shutil.copyfile(wasm, self.output / 'gateway-observer-v1.wasm')
        env = {'cpus': os.cpu_count(), 'meminfo': Path('/proc/meminfo').read_text(), 'images': IMAGES,
               'wasm_sha256': hashlib.sha256(wasm.read_bytes()).hexdigest(), 'parameters': vars(self.args),
               'load_binary_sha256': hashlib.sha256(Path(self.load).read_bytes()).hexdigest(),
               'input_profile_counts_per_10': expected_profile_counts(10),
               'scope': 'local Docker Envoy + real Observer Producer/Kafka; deferred real Consumer/Doris',
               'token_measurement': 'synthetic usage; prompt content separately verifiable with tests/capacity/verify_token_profiles.py',
               'production_worker_count': 'unknown; workers below are local test settings'}
        write(self.output / 'environment.json', env)
        self.infrastructure()
        if self.args.mode == 'context':
            phase, _ = self.phase('mixed-context', True, count=100, concurrency=100,
                                  profile='mixed', duration='60s')
            verification = self.reconcile()
            profiles = {key: value['count'] for key, value in phase['input_profiles'].items()}
            report = {'mode': 'context', 'phase': phase, 'verification': verification,
                      'scope': '100 mixed-context requests, concurrency 100, 60-second SSE; correctness only.',
                      'passed': phase['load_exit_code'] == 0 and not phase['aborted']
                          and phase['load'].get('finished', False) and phase['load']['complete'] == 100
                          and profiles == expected_profile_counts(100)
                          and all(v['passed'] for v in verification),
                      'capacity_load_valid': False, 'production_capacity_accepted': False}
            write(self.output / 'result.json', report)
            print('报告：' + str(self.output / 'result.json'), flush=True)
            return 0 if report['passed'] else 1
        if self.args.mode == 'faults':
            phase, _ = self.phase('observer-outage', True, count=10, profile='16k', duration='5s', restart_at=1)
            directory = self.output / 'observer-outage'
            records = [json.loads(line) for line in (directory / 'requests.jsonl').read_text().splitlines()]
            affected = [r for r in records if int(r['id'].rsplit('-', 1)[1]) < 2]
            recovery = [r for r in records if int(r['id'].rsplit('-', 1)[1]) >= 2]
            with (directory / 'requests.recovery.jsonl').open('w') as output:
                for r in recovery:
                    output.write(json.dumps(r) + '\n')
            verification = self.reconcile()
            ids = ','.join("'" + r['id'] + "'" for r in affected)
            evidence = self.sql(f"SELECT logical_request_id,event_type,usage_status,terminal_observed,observation_status,anomaly_codes FROM {self.database}.events_raw WHERE logical_request_id IN ({ids});")
            (directory / 'interrupted-captures.tsv').write_text(evidence)
            report = {'mode': 'faults', 'phase': phase, 'verification': verification,
                      'interrupted_capture_ids': [r['id'] for r in affected],
                      'scope': 'Business remains complete during Observer restart; only subsequent captures require exact known usage.',
                      'passed': phase['load_exit_code'] == 0 and phase['observer_restarted'] and len(recovery) == 8
                          and all(v['passed'] for v in verification), 'production_capacity_accepted': False}
            write(self.output / 'result.json', report)
            print('报告：' + str(self.output / 'result.json'), flush=True)
            return 0 if report['passed'] else 1
        baseline, bsample = self.phase('baseline', False, count=100 if self.args.mode == 'regression' else 0,
                                       profile='16k' if self.args.mode == 'regression' else 'mixed',
                                       duration='150ms' if self.args.mode == 'regression' else '60s')
        enabled, esample = self.phase('enabled', True, count=100 if self.args.mode == 'regression' else 0,
                                      profile='16k' if self.args.mode == 'regression' else 'mixed',
                                      duration='150ms' if self.args.mode == 'regression' else '60s')
        if self.args.mode == 'regression':
            self.phase('large-body', True, count=4, profile='32m', duration='150ms')
            self.phase('long-stream', True, count=2, profile='16k', duration='65s')
            self.phase('missing-usage', True, count=10, profile='16k', duration='150ms', omit=True)
        verification = self.reconcile()
        rss = rss_comparison(bsample, esample)
        rss_pass = rss['sampled_rss_comparison_within_budget']
        valid_load = (baseline['load_exit_code'] == enabled['load_exit_code'] == 0) if self.args.mode == 'regression' else baseline['capacity_load_valid'] and enabled['capacity_load_valid']
        report = {'mode': self.args.mode, 'baseline': baseline, 'enabled': enabled, 'verification': verification,
                  **rss,
                  'rss_evaluation_valid': valid_load,
                  'rss_within_budget': rss_pass if valid_load else None,
                  'passed': valid_load and rss_pass and all(v['passed'] for v in verification)
                      and all(v['load_exit_code'] == 0 and not v['aborted'] for v in self.phases),
                  'phases': self.phases,
                  'production_capacity_accepted': False}
        write(self.output / 'result.json', report)
        print('报告：' + str(self.output / 'result.json'), flush=True)
        return 0 if report['passed'] else 1

    def cleanup(self):
        for p in list(self.processes):
            self.stop_process(p)
        for name in list(reversed(self.names)):
            self.stop_container(name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('capacity', 'regression', 'faults', 'context'), default='capacity')
    parser.add_argument('--workers', type=int, default=2, help='本地 Envoy Worker 数，线上数量仍未知')
    parser.add_argument('--connections', type=int, default=0, help='独立客户端 HTTP/2 连接数；0 为 Worker 数的 4 倍')
    parser.add_argument('--helper-gomaxprocs', type=int, default=2, help='两组负载端的 Go 并行度；不修改网关或 Observer')
    parser.add_argument('--fixture-gomaxprocs', type=int, default=4, help='两组合成模型的 Go 并行度；与负载端独立配置')
    parser.add_argument('--fixture-read-buffer-kib', type=int, default=64, help='fixture 每个正在上传请求的增量读取缓冲，1–64 KiB')
    parser.add_argument('--qps', type=int, default=200)
    parser.add_argument('--warmup', type=int, default=60)
    parser.add_argument('--steady', type=int, default=300)
    parser.add_argument('--audit-timeout', type=int, default=900, help='停止网关后等待实际 Consumer/Doris 排空的秒数；独立于业务容量时段')
    parser.add_argument('--min-available-mib', type=int, default=512, help='主机余量不足时终止本轮并标记失败')
    parser.add_argument('--output', default='.tmp/collection-capacity')
    args = parser.parse_args()
    if min(args.workers, args.qps, args.warmup, args.steady, args.min_available_mib, args.audit_timeout,
           args.helper_gomaxprocs, args.fixture_gomaxprocs, args.fixture_read_buffer_kib) < 1 or args.connections < 0 or args.fixture_read_buffer_kib > 64:
        parser.error('数值参数必须为正')
    h = Harness(args)
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        return h.execute()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        write(h.output / 'failure.json', {'error': str(error), 'passed': False})
        print('FAIL：' + str(error) + '；证据保留在 ' + str(h.output), file=sys.stderr)
        return 1
    finally:
        h.cleanup()


if __name__ == '__main__':
    sys.exit(main())
