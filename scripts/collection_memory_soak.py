"""同一 Envoy/Worker VM 多轮内存诊断；显式 GC 副本不用于容量验收。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import urllib.request

import collection_capacity as capacity


DIAGNOSTIC = r'''package main
import (
    "runtime"
    "strconv"
    "github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
)
var diagnosticWave string
func diagnosticMemory(r *root, wave string) {
    if diagnosticWave == wave || len(r.states) != 0 || r.inflight != 0 || r.messages != 0 { return }
    diagnosticWave = wave
    var before, after runtime.MemStats
    runtime.ReadMemStats(&before)
    runtime.GC()
    runtime.ReadMemStats(&after)
    u := func(v uint64) string { return strconv.FormatUint(v,10) }
    batchCapacity, recordCapacity := 0, 0
    for _, c := range r.channels { if c != nil { batchCapacity += cap(c.batch); recordCapacity += cap(c.records) } }
    proxywasm.LogWarn("diagnostic_heap wave=" + wave + " before_sys=" + u(before.HeapSys) + " before_alloc=" + u(before.HeapAlloc) +
        " after_sys=" + u(after.HeapSys) + " after_alloc=" + u(after.HeapAlloc) + " total_alloc=" + u(after.TotalAlloc) +
        " mallocs=" + u(after.Mallocs) + " frees=" + u(after.Frees) + " selected=" + u(r.next) +
        " states=" + u(uint64(len(r.states))) + " deadlines=" + u(uint64(len(r.q))) +
        " batch_capacity=" + u(uint64(batchCapacity)) + " record_capacity=" + u(uint64(recordCapacity)))
}
'''


def diagnostic_wasm(output):
    source = output / 'plugin'
    source.mkdir()
    original = capacity.ROOT / 'plugins/gateway-observer'
    for path in original.iterdir():
        if path.name in ('go.mod', 'go.sum') or path.suffix == '.go' and not path.name.endswith('_test.go'):
            shutil.copyfile(path, source / path.name)
    mod = source / 'go.mod'
    mod.write_text(mod.read_text().replace('=> ../..\n', '=> ' + str(capacity.ROOT) + '\n')
                   .replace('=> ../../third_party/proxy-wasm-go-sdk', '=> ' + str(capacity.ROOT / 'third_party/proxy-wasm-go-sdk')))
    main = source / 'main.go'
    marker = 'func (h *httpContext) OnHttpRequestHeaders(_ int, eos bool) types.Action {'
    main.write_text(main.read_text().replace(marker, marker + '''
    if path := header(":path"); strings.HasPrefix(path, "/__llm_token_stats_heap/") {
        diagnosticMemory(h.root, path)
        return types.ActionContinue
    }
'''))
    (source / 'diagnostic.go').write_text(DIAGNOSTIC)
    target = output / 'gateway-observer-v1.wasm'
    built = subprocess.run(['tinygo', 'build', '-target=wasi', '-buildmode=wasi-legacy', '-scheduler=none',
                  '-o', str(target), '.'], cwd=source, timeout=180, capture_output=True, text=True)
    (output / 'diagnostic-build.log').write_text(built.stdout + built.stderr)
    if built.returncode:
        raise RuntimeError('诊断构建失败，见 ' + str(output / 'diagnostic-build.log'))
    return target


def execute(args):
    h = capacity.Harness(args)
    try:
        wasm = diagnostic_wasm(h.output)
        capacity.write(h.output / 'environment.json', {'scope': 'Diagnostic GC copy; not capacity acceptance',
                       'args': vars(args), 'wasm_sha256': hashlib.sha256(wasm.read_bytes()).hexdigest()})
        print('诊断目录：' + str(h.output), flush=True)
        h.infrastructure()
        p = h.ports
        helper_env = {**os.environ, 'GOMAXPROCS': str(args.helper_gomaxprocs)}
        h.process('fixture', [h.load, 'fixture', '--listen', f"127.0.0.1:{p['fixture']}", '--duration', args.response_duration,
                             '--read-buffer-kib', str(args.fixture_read_buffer_kib)],
                  {**os.environ, 'GOMAXPROCS': str(args.fixture_gomaxprocs)})
        capacity.wait(lambda: capacity.get(f"http://127.0.0.1:{p['fixture']}/healthz"))
        capacity.write(h.output / 'envoy.json', capacity.envoy_config(p, True, Path('/capacity/gateway-observer-v1.wasm')))
        gateway = 'llm-token-stats-capacity-memory-soak-' + h.id
        capacity.run(['docker', 'run', '-d', '--name', gateway, '--label', 'llm-token-stats-capacity=' + h.id,
            '--network', 'host', '-v', f'{h.output}:/capacity:ro', '-e', 'LOG_LEVEL=warn',
            '--entrypoint', '/usr/local/bin/envoy', capacity.IMAGES['envoy'], '-c', '/capacity/envoy.json',
            '--concurrency', str(args.workers), '--log-level', 'warning', '--component-log-level', 'wasm:warn'])
        h.names.append(gateway)
        capacity.wait(lambda: capacity.get(f"http://127.0.0.1:{p['admin']}/ready"), 30)
        pid = int(capacity.run(['docker', 'inspect', '--format', '{{.State.Pid}}', gateway]).stdout)
        started = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()[19]
        h.gateway_pids[gateway] = (pid, started)
        client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        at = time.monotonic()
        phases = []
        worker_threads = set()

        def diagnose(wave):
            before = h.sample(gateway, at)
            (h.output / f'wave-{wave}-before-smaps.txt').write_text(Path(f'/proc/{pid}/smaps_rollup').read_text())
            (h.output / f'wave-{wave}-before-native.json').write_text(capacity.get(f"http://127.0.0.1:{p['admin']}/memory"))
            for _ in range(96):
                req = urllib.request.Request(f"http://127.0.0.1:{p['business']}/__llm_token_stats_heap/{wave}",
                    data=b'', headers={'Host': 'model.example.invalid', 'Connection': 'close'}, method='POST')
                try:
                    with client.open(req, timeout=2) as r:
                        r.read()
                except OSError:
                    pass  # This diagnostic path normally returns the route's 404.
            after = h.sample(gateway, at)
            (h.output / f'wave-{wave}-after-smaps.txt').write_text(Path(f'/proc/{pid}/smaps_rollup').read_text())
            (h.output / f'wave-{wave}-after-native.json').write_text(capacity.get(f"http://127.0.0.1:{p['admin']}/memory"))
            logs = capacity.run(['docker', 'logs', gateway], check=False)
            (h.output / 'worker-heap.log').write_text(logs.stdout + logs.stderr)
            heaps = []
            for line in (logs.stdout + logs.stderr).splitlines():
                if f'diagnostic_heap wave=/__llm_token_stats_heap/{wave} ' not in line:
                    continue
                match = re.search(r'\]\[(\d+)\]\[warning\]', line)
                if match:
                    heaps.append({'thread': int(match[1]), **{k: int(v) for k, v in re.findall(r'(\w+)=(\d+)', line)}})
            totals = {k: sum(row[k] for row in heaps) for k in ('before_sys', 'after_sys', 'before_alloc', 'after_alloc', 'total_alloc', 'selected')}
            capacity.write(h.output / f'wave-{wave}-memory.json', {'before_gc': before, 'after_gc': after,
                           'workers': heaps, 'totals': totals})
            threads = {row['thread'] for row in heaps}
            if len(heaps) != args.workers or len(threads) != args.workers:
                raise RuntimeError('未取得所有 Worker 的堆统计，不能判定多轮趋势')
            if worker_threads and threads != worker_threads:
                raise RuntimeError('Worker 集合改变，不能作为同一 VM 多轮对照')
            if totals['selected'] != wave * args.qps * args.seconds:
                raise RuntimeError('累计观察数量不符，不能排除 VM 重建或请求未被采集')
            worker_threads.update(threads)

        diagnose(0)
        for wave in range(1, args.waves + 1):
            directory = h.output / f'wave-{wave}'
            directory.mkdir()
            loader = h.process(f'load-{wave}', [h.load, 'load', '--url', f"http://127.0.0.1:{p['business']}/v1/chat/completions",
                '--output', str(directory / 'load.json'), '--results', str(directory / 'requests.jsonl'),
                '--prefix', h.id + '-wave-' + str(wave), '--profile', 'mixed', '--qps', str(args.qps),
                '--seconds', str(args.seconds), '--timeout', '180s', '--connections', str(args.connections)], helper_env)
            progress = time.monotonic()
            with (directory / 'rss.jsonl').open('w') as out:
                while loader.poll() is None:
                    sample = h.sample(gateway, at)
                    out.write(json.dumps(sample) + '\n'); out.flush()
                    if sample['host_available'] < args.min_available_mib * (1 << 20):
                        raise RuntimeError('host_memory_guard')
                    if time.monotonic() - progress >= 30:
                        print(json.dumps({'wave': wave, 'rss_mib': round(sample['rss'] / (1 << 20), 1)}), flush=True)
                        progress = time.monotonic()
                    time.sleep(.25)
            loader.wait(timeout=10)
            capacity.wait(lambda: json.loads(capacity.get(f"http://127.0.0.1:{p['fixture']}/metrics"))['active'] == 0, 180)
            time.sleep(3)
            result = json.loads((directory / 'load.json').read_text())
            phases.append(result)
            expected = args.qps * args.seconds
            if (loader.returncode or not result.get('finished') or result.get('cancelled')
                    or result.get('issued') != expected or result.get('complete') != expected
                    or result.get('business_errors') != 0):
                capacity.write(h.output / 'failure.json', {'reason': '业务负载未完整成功', 'wave': wave,
                               'load_exit_code': loader.returncode, 'result': result})
                raise RuntimeError('内存诊断业务负载未完整成功，见 failure.json')
            diagnose(wave)
            print('完成内存诊断轮次 ' + str(wave), flush=True)
        capacity.write(h.output / 'result.json', {'scope': 'Same VM diagnostic waves; explicit GC, not RSS acceptance',
                       'gateway_pid': pid, 'phases': phases, 'production_capacity_accepted': False})
        return h.output
    finally:
        h.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='.tmp/collection-memory-soak')
    parser.add_argument('--waves', type=int, default=3)
    parser.add_argument('--qps', type=int, default=100)
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--response-duration', default='60s', help='诊断 SSE 时长；正式容量固定 60 秒')
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--connections', type=int, default=64)
    parser.add_argument('--helper-gomaxprocs', type=int, default=2)
    parser.add_argument('--fixture-gomaxprocs', type=int, default=4)
    parser.add_argument('--fixture-read-buffer-kib', type=int, default=64)
    parser.add_argument('--min-available-mib', type=int, default=512)
    args = parser.parse_args()
    if min(vars(args)[k] for k in vars(args) if k not in ('output', 'response_duration')) < 1 or args.fixture_read_buffer_kib > 64:
        parser.error('数值参数无效')
    execute(args)


if __name__ == '__main__':
    main()
