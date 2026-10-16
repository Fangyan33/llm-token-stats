import copy
import contextlib
import html
import io
from pathlib import Path
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
        self.assertEqual((len(tables), sum(len(r['columns']) for r in tables)), (12, 211))
        self.assertEqual((len(views), sum(len(r['columns']) for r in views)), (7, 299))
        for relation in self.relations.values():
            self.assertTrue(relation['purpose'].strip())
            self.assertTrue(all(col['comment'].strip() for col in relation['columns']))

    def test_migration_is_reproducible_and_metadata_only(self):
        actual = (schema.DDL / schema.COMMENT_MIGRATION).read_text()
        self.assertEqual(actual, schema.migration(self.relations))
        changes = list(schema.statements(actual))
        self.assertEqual(len(changes), 12 + 211 + 7)
        for change in changes:
            self.assertTrue(change.startswith(('ALTER TABLE metering.', 'ALTER VIEW metering.')))
            if change.startswith('ALTER TABLE'):
                self.assertRegex(change, r'^ALTER TABLE metering\.\w+ MODIFY (?:COLUMN \w+ )?COMMENT ')
            else:
                name = change.split(' ', 3)[2].split('.', 1)[1]
                self.assertEqual(change.split(') AS\n', 1)[1], self.relations[name]['query'])

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

    def test_existing_projection_override_cannot_drop_annotations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in schema.INITIALIZATIONS:
                shutil.copyfile(schema.DDL / name, root / name)
            path = root / '006_optional_route_identity.sql'
            text = path.read_text().replace("request_date COMMENT '", "request_date COMMENT '漂移", 1)
            path.write_text(text)
            with self.assertRaisesRegex(ValueError, 'annotation drift'):
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

    def test_migration_cli_honors_explicit_database(self):
        output = io.StringIO()
        with patch.object(sys, 'argv', ['doris_schema.py', 'migration', '--database', 'metering_notes_test']), \
                contextlib.redirect_stdout(output):
            schema.main()
        for statement in schema.statements(output.getvalue()):
            self.assertTrue(statement.startswith(('ALTER TABLE metering_notes_test.',
                                                  'ALTER VIEW metering_notes_test.')))

    def test_initialization_harnesses_apply_comment_update(self):
        for name in ('scripts/local_collection.py', 'scripts/k8s_collection.py'):
            self.assertIn(schema.COMMENT_MIGRATION, (schema.ROOT / name).read_text())


if __name__ == '__main__':
    unittest.main()
