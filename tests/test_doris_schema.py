import copy
import contextlib
import html
import io
from pathlib import Path
import re
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import doris_schema as schema


class DorisSchemaTests(unittest.TestCase):
    def setUp(self):
        self.relations = schema.schema()

    def document_metadata(self):
        document = (schema.ROOT / 'docs/doris-database-schema.md').read_text()
        result = {}
        for name, relation in self.relations.items():
            section = document.split('## ' + name + '\n', 1)[1].split('\n## ', 1)[0]
            self.assertIn('**作用：**', section)
            for label in ('写入者', '读取者', '数据粒度', '唯一键', '分区、分桶及保留', '实现状态', '定义来源'):
                self.assertIn('| ' + label + ' |', section)
            rows = section.split('| 字段 | 类型 | 可空 | 默认值 | 键属性 | 说明 |\n', 1)[1].splitlines()[1:]
            result[name] = []
            for row in rows:
                if not row.startswith('| '):
                    continue
                cells = [html.unescape(value.strip()) for value in row.split('|')[1:-1]]
                self.assertEqual(len(cells), 6)
                column, typ, nullable, default, key, comment = cells
                result[name].append(dict(name=column, type=typ, nullable=nullable == '是', comment=comment))
                col = relation['columns'][len(result[name]) - 1]
                if relation['kind'] == 'TABLE':
                    self.assertEqual(default, col['default'] or '未声明')
                    self.assertEqual(key, '唯一键' if column in relation['keys'] else '—')
            self.assertEqual([c['name'] for c in result[name]], [c['name'] for c in relation['columns']])
        return document, result

    def test_all_current_objects_and_columns_have_annotations(self):
        tables = [r for r in self.relations.values() if r['kind'] == 'TABLE']
        views = [r for r in self.relations.values() if r['kind'] == 'VIEW']
        self.assertEqual((len(tables), sum(len(r['columns']) for r in tables)), (10, 122))
        self.assertEqual((len(views), sum(len(r['columns']) for r in views)), (5, 191))
        materialized = [r for r in self.relations.values() if r['kind'] == 'MATERIALIZED VIEW']
        self.assertEqual([r['name'] for r in materialized], ['usage_daily_mv', 'usage_monthly_mv'])
        for relation in self.relations.values():
            self.assertTrue(relation['purpose'].strip())
            self.assertTrue(all(col['comment'].strip() for col in relation['columns']))

    def test_only_final_objects_are_created_in_dependency_order(self):
        retired = {'usage_daily', 'usage_monthly', 'summary_refresh_inputs',
                   'summary_refresh_runs', 'summary_refresh_state',
                   'event_variants', 'events_valid', 'request_statistics_mv'}
        self.assertFalse(retired.intersection(self.relations))
        created = set()
        for name in schema.INITIALIZATIONS:
            for statement in schema.statements((schema.DDL / name).read_text()):
                self.assertFalse(statement.startswith(('ALTER ', 'DROP ', 'CREATE OR REPLACE ')))
                if not statement.startswith(('CREATE TABLE ', 'CREATE VIEW ', 'CREATE MATERIALIZED VIEW ')):
                    continue
                relation = schema.parse_relation(statement, name)
                if 'query' in relation:
                    dependencies = set(re.findall(r'(?:FROM|JOIN) metering\.(\w+)', relation['query']))
                    self.assertTrue(dependencies <= created, (relation['name'], dependencies - created))
                self.assertNotIn(relation['name'], created)
                created.add(relation['name'])
        self.assertEqual(created, set(self.relations))

    def test_document_inventory_meanings_and_table_types_match_ddl(self):
        document, live = self.document_metadata()
        schema.validate_live(self.relations, live)
        self.assertEqual(document, schema.document(self.relations, live))

    def test_missing_outdated_and_reordered_live_columns_are_rejected(self):
        _, original = self.document_metadata()
        cases = []
        missing_table = copy.deepcopy(original)
        missing_table.pop('events_raw')
        cases.append(missing_table)
        for change in ('missing', 'reorder', 'comment', 'type', 'nullable'):
            live = copy.deepcopy(original)
            columns = live['events_raw']
            if change == 'missing':
                columns.pop()
            elif change == 'reorder':
                columns[0], columns[1] = columns[1], columns[0]
            elif change == 'comment':
                columns[0]['comment'] = ''
            elif change == 'type':
                columns[0]['type'] = 'BIGINT'
            else:
                columns[0]['nullable'] = True
            cases.append(live)
        for live in cases:
            with self.assertRaises(ValueError):
                schema.validate_live(self.relations, live)

    def test_duplicate_or_unlisted_initialization_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in schema.INITIALIZATIONS:
                shutil.copyfile(schema.DDL / name, root / name)
            path = root / schema.INITIALIZATIONS[-1]
            duplicate = next(q for q in schema.statements((root / schema.INITIALIZATIONS[0]).read_text())
                             if q.startswith('CREATE VIEW '))
            path.write_text(path.read_text() + duplicate + ';\n')
            with self.assertRaisesRegex(ValueError, 'duplicate relation'):
                schema.schema(root)
            (root / '999_unlisted.sql').write_text('SELECT 1;')
            with self.assertRaisesRegex(ValueError, 'file inventory drift'):
                schema.schema(root)

    def test_sql_strings_decimal_arrays_and_empty_comments(self):
        source = """-- metadata
CREATE TABLE IF NOT EXISTS metering.probe (
    id BIGINT NOT NULL COMMENT '主键',
    amount DECIMAL(38, 0) NULL COMMENT '数量; 保留逗号',
    values_array ARRAY<VARCHAR(64)> NOT NULL COMMENT '值数组，含 '' 引号'
)
UNIQUE KEY(id)
COMMENT '测试表';
"""
        statements = list(schema.statements(source))
        self.assertEqual(len(statements), 1)
        relation = schema.parse_relation(statements[0], 'probe.sql')
        self.assertEqual(relation['columns'][1]['type'], 'DECIMAL(38, 0)')
        self.assertEqual(relation['columns'][2]['comment'], "值数组，含 ' 引号")
        for old in ("COMMENT '主键'", "COMMENT '测试表'"):
            with self.assertRaises(ValueError):
                schema.parse_relation(statements[0].replace(old, "COMMENT ''"), 'probe.sql')
        with self.assertRaises(ValueError):
            list(schema.statements("SELECT 'unterminated"))

    def test_invalid_database_is_rejected_before_any_subprocess(self):
        with patch.object(schema.subprocess, 'check_output') as command:
            with self.assertRaises(ValueError):
                schema.live_columns("metering'; DROP DATABASE metering")
        command.assert_not_called()

    def test_document_cli_reads_explicit_database(self):
        _, live = self.document_metadata()
        output = io.StringIO()
        with patch.object(sys, 'argv', ['doris_schema.py', 'document', '--database', 'metering_notes_test']), \
                patch.object(schema, 'live_columns', return_value=live) as metadata, \
                contextlib.redirect_stdout(output):
            schema.main()
        metadata.assert_called_once_with('metering_notes_test')
        self.assertEqual(output.getvalue().strip(), schema.document(self.relations, live).strip())

    def test_packaging_and_initialization_share_the_same_migrations(self):
        import package_distribution
        import local_collection
        import k8s_collection
        for module in (package_distribution, local_collection, k8s_collection):
            self.assertEqual(module.MIGRATIONS, schema.INITIALIZATIONS)


if __name__ == '__main__':
    unittest.main()
