import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from render_doris_sql import render_sql


class DorisSQLTests(unittest.TestCase):
    def test_database_render_preserves_literal_topic_and_escaped_quotes(self):
        sql = ("CREATE DATABASE metering; SELECT * FROM `metering`.events_raw "
               "WHERE kafka_topic='llm-metering-k8s-123' AND group_id='metering' "
               "AND reason='it''s metering' AND source_id='it\\'s metering';")
        result = render_sql(sql, 'metering_isolated')
        self.assertIn('CREATE DATABASE metering_isolated;', result)
        self.assertIn('`metering_isolated`.events_raw', result)
        self.assertEqual(result.split('WHERE', 1)[1], sql.split('WHERE', 1)[1])

    def test_database_identifier_and_every_initialization_reference(self):
        root = Path(__file__).resolve().parents[1]
        for path in (root / 'migrations/doris/metering').glob('*.sql'):
            rendered = render_sql(path.read_text(), 'metering_isolated_123')
            self.assertNotIn('metering.', rendered)
            self.assertNotIn('application_id', rendered)
        for name in ('', 'x;drop database metering', 'a.b', 'x`', 'x-y', '1name', 'a' * 65):
            with self.assertRaises(ValueError):
                render_sql('SELECT * FROM metering.events_raw', name)
