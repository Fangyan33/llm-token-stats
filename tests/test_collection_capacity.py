import copy
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from collection_capacity import Harness, capacity_load_valid, expected_profile_counts, rss_comparison


class CapacityAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(qps=200, warmup=60, steady=300)
        self.load = {'finished': True, 'cancelled': False, 'issued': 72000,
                     'complete': 72000, 'business_errors': 0}

    def valid(self, **changes):
        params = dict(args=self.args, count=0, profile='mixed', aborted=None,
                      exit_code=0, load=self.load, fixture={'peak': 12000}, steady_qps=200,
                      profiles={'1000000': 14400, '200000': 36000, '500000': 14400, '100000': 7200})
        params.update(changes)
        return capacity_load_valid(**params)

    def test_only_full_target_can_pass(self):
        self.assertTrue(self.valid())
        self.assertFalse(self.valid(count=100))
        self.assertFalse(self.valid(profile='16k'))
        self.assertFalse(self.valid(aborted='host_memory_guard'))
        self.assertFalse(self.valid(exit_code=-9))

    def test_partial_checkpoint_or_cancelled_run_cannot_pass(self):
        for change in ({'finished': False}, {'cancelled': True}, {'issued': 71999},
                       {'complete': 71999}, {'business_errors': 1}):
            with self.subTest(change=change):
                self.assertFalse(self.valid(load=self.load | change))

    def test_actual_model_throughput_and_active_streams_are_required(self):
        self.assertFalse(self.valid(steady_qps=197.9))
        self.assertFalse(self.valid(fixture={'peak': 10799}))

    def test_lower_target_or_duration_cannot_substitute(self):
        for field, value in (('qps', 199), ('warmup', 59), ('steady', 299)):
            with self.subTest(field=field):
                args = copy.copy(self.args)
                setattr(args, field, value)
                target = args.qps * (args.warmup + args.steady)
                self.assertFalse(self.valid(args=args, load=self.load | {'issued': target, 'complete': target}))

    def test_context_distribution_must_match_the_new_acceptance(self):
        self.assertEqual(expected_profile_counts(100), {'1000000': 20, '200000': 50, '500000': 20, '100000': 10})
        self.assertFalse(self.valid(profiles={'1000000': 14400, '160000': 28800, '180000': 14400, '200000': 14400}))


class RSSIncrementTests(unittest.TestCase):
    def test_absolute_rss_can_exceed_two_gib(self):
        baseline = [{'elapsed': 0, 'rss': 12 << 30}]
        enabled = [{'elapsed': 0, 'rss': (12 << 30) + (1536 << 20)}]
        report = rss_comparison(baseline, enabled)
        self.assertTrue(report['sampled_rss_comparison_within_budget'])
        self.assertEqual(report['extra_rss_max_time_aligned'], 1536 << 20)

    def test_two_gib_increment_boundary(self):
        baseline = [{'elapsed': 0, 'rss': 12 << 30}]
        self.assertTrue(rss_comparison(baseline, [{'elapsed': 0, 'rss': 14 << 30}])['sampled_rss_comparison_within_budget'])
        self.assertFalse(rss_comparison(baseline, [{'elapsed': 0, 'rss': (14 << 30) + 1}])['sampled_rss_comparison_within_budget'])

    def test_time_aligned_increment_is_checked(self):
        baseline = [{'elapsed': 0, 'rss': 12 << 30}, {'elapsed': 60, 'rss': 10 << 30}]
        enabled = [{'elapsed': .1, 'rss': 13 << 30}, {'elapsed': 60.1, 'rss': 13 << 30}]
        report = rss_comparison(baseline, enabled)
        self.assertEqual(report['extra_rss_peak_difference'], 1 << 30)
        self.assertEqual(report['extra_rss_max_time_aligned'], 3 << 30)
        self.assertFalse(report['sampled_rss_comparison_within_budget'])


class ProcessSamplerTests(unittest.TestCase):
    def test_gateway_pid_is_not_replaced_by_helper_pid(self):
        pid = os.getpid()
        started = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()[19]
        helper = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(10)'])
        try:
            harness = Harness.__new__(Harness)
            harness.gateway_pids = {'gateway': (pid, started)}
            harness.pidstats = {'helper': helper.pid}
            sample = harness.sample('gateway', time.monotonic())
            self.assertEqual(sample['gateway_pid'], pid)
            self.assertGreater(sample['rss'], 0)
            self.assertIn('helper', sample['processes'])
        finally:
            helper.terminate()
            helper.wait(timeout=5)
