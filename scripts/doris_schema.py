"""Read the repository's annotated Doris DDL; emit a validated data dictionary.

The parser intentionally supports the checked-in format (one column per line),
not arbitrary user SQL. Document generation reads metadata only, never records.
"""
import argparse
from collections import OrderedDict
from pathlib import Path
import re
import subprocess
import sys

from render_doris_sql import MIGRATIONS, database_name

ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / 'migrations/doris/metering'
INITIALIZATIONS = MIGRATIONS
SQL_STRING = r"'((?:''|\\.|[^'\\])*)'"

# Purpose lives in each CREATE TABLE COMMENT. These notes describe ownership,
# dependencies and current implementation status without inventing DB FKs.
NOTES = {
    'events_raw': ('Consumer', '冲突核验、明细视图、统计投影', '一条规范事件内容变体',
                   'UTC 请求日期动态分区，保留前 12 个完整月加当月，由 Worker 按月首清理；按请求 ID 分 16 桶',
                   '已实现', '开始事件与结果事件是同一请求的两个事实，不能计为两次调用。Unique Key 使同内容重放幂等，payload_hash 使不同内容保留。'),
    'request_conflict_ids': ('Consumer 批次处理及补偿巡检', 'requests、request_statistics', '一个存在冲突的逻辑请求',
                             '按请求 ID 分 16 桶；长期保留，当前不自动清理', '已实现',
                             '原始冲突证据过期后仍隔离请求，防止剩余变体重新进入有效统计。'),
    'ingest_rejections': ('Consumer', '运维诊断及消费核验', '一个 Kafka 坐标和原始内容摘要',
                         'UTC Kafka 消息日期动态分区，保留前 12 个完整月加当月；按 topic/分区分 8 桶', '已实现',
                         '永久非法消息确认诊断写入后才能越过；未知 schema 会暂停分区，不按普通拒收跳过。原始消息不落库。'),
    'summary_dirty_notifications': ('Consumer；初始化任务', 'usage-worker', '某日期的一条幂等重算通知',
                                    '按日期/通知 ID 分 4 桶；独立于原始分区保留，当前不自动清理', '已实现',
                                    '新增事件、冲突、初始化或修复触发重算。通知不是用量增量，也不是已消费布尔位；Worker 仅确认成功任务捕获的通知集合。'),
    'consumer_batch_receipts': ('拥有对应分区的 Consumer', 'Consumer 恢复、水位巡检及汇总覆盖核验', '一个冻结 Kafka 批次及其通知集合凭据',
                               '按 topic/分区分 4 桶；当前不自动清理', '已实现',
                               '原始/拒收、冲突、日期通知和本凭据全部确认可见后才提交 Kafka offset。凭据可能领先于提交，MAX(next_offset) 不能证明连续覆盖。'),
    'consumer_partition_origins': ('Consumer 分区初始化或显式接管', 'Consumer 恢复及连续水位巡检', '消费组/分区的一份可信起点事实',
                                  '按 topic/分区分 4 桶；独立保留，当前不自动清理', '已实现',
                                  '新组登记最早可用位置，已有组缺可信起点需显式接管；保留多份不兼容起点并阻塞处理，不做最后写入覆盖。'),
    'consumer_partition_gaps': ('Consumer', 'Consumer 恢复、运维及覆盖核验', '消费组/分区的一条缺口事实',
                               '按 topic/分区分 4 桶；独立保留，当前不自动清理', '已实现',
                               '记录例如 Kafka 保留期导致的缺口。默认暂停，显式恢复位置必须符合恢复约定；恢复后仍保留历史缺口证据。'),
    'event_conflicts': ('逻辑视图，无独立写入', 'event_details、冲突诊断', '同一逻辑请求和事件 ID 的冲突组',
                        '跨全部在线日期分区查询，无物理存储', '已实现',
                        '按逻辑请求和事件 ID 分组，只返回内容变体行数大于 1 的事件，并列出摘要。'),
    'event_details': ('逻辑视图，无独立写入', '明细查询、usage_effective', '一个无同事件内容冲突的事件',
                      '无物理存储，继承基础表在线范围', '已实现',
                      '直接从 events_raw 排除同事件内容冲突并投影业务字段，省去内部 Kafka 坐标、入库时间等字段；请求级冲突还需通过 requests 判断。'),
    'requests': ('逻辑视图，无独立写入', '请求诊断、usage_effective', '一个逻辑请求，跨在线日期聚合',
                 '无物理存储，查询会涉及全部在线日期证据', '已实现',
                 '合并开始/结果事件并检查内容、开始时间、日期、归属及持久冲突标记。它保留冲突行，通过 has_conflict 标识；冲突行的聚合归属不能作为可信归属使用。'),
    'usage_effective': ('逻辑视图，无独立写入', '有效统计候选明细查询', '一个无请求冲突的统计候选结果事件',
                        '无物理存储，继承基础表在线范围', '已实现',
                        '连接 event_details 与 requests，按当前质量口径重算资格并排除 has_conflict=1 的事件，包含历史客户未知但用量有效的请求；统计候选不是收费资格。'),
    'request_statistics': ('逻辑投影', 'usage_daily_mv', '日期加逻辑请求 ID', '继承原始日分区范围', '已实现', '保留未知归属，按质量重算历史 statistics_usage_eligible，不修改原始事件。'),
    'usage_daily_mv': ('usage-worker 显式刷新', '日统计查询、usage_monthly_mv', 'UTC 日期加八个统计维度', '按日分区，保留前 12 个完整 UTC 月加当月；仅计算已结束日期', '已实现，生产验收待完成', '公共统计含未知客户；客户/API Key 报表过滤未知归属，服务及供应商按已知 ID 查询。'),
    'usage_monthly_mv': ('usage-worker 在日成功后刷新', '月统计查询', 'UTC 月首加八个统计维度', '按自然月分区，随日视图同步过期，当前月累计到昨日', '已实现，生产验收待完成', '直接累加日物化视图；全部未知仍为 NULL，不扫描原始事件。'),
    'summary_mv_refresh_runs': ('usage-worker', '任务恢复及通知确认', '统计期加刷新 UUID', '独立保存，不随原始分区删除', '已实现', '提交前持久化恢复信息，成功须匹配 Doris 任务 ID 及完成分区。'),
    'summary_mv_refresh_inputs': ('usage-worker', '通知确认', '日期加刷新 UUID 加通知 ID', '独立保存，不随原始分区删除', '已实现', '仅关联 SUCCEEDED 任务的精确输入得到确认；新通知留给下轮。'),
    'summary_mv_refresh_state': ('usage-worker', '查询覆盖和任务状态', '统计期', '保留最新成功版本及覆盖、月依赖向量', '已实现', 'GAP、未生成、过期与已确认零请求分别展示。')
}

VIEW_PURPOSES = {
    'event_conflicts': '同一逻辑请求及事件 ID 的内容冲突诊断',
    'event_details': '事件业务字段明细投影',
    'requests': '跨在线日期合并开始与结果事件的请求级诊断投影',
    'usage_effective': '无请求冲突的有效统计候选用量明细',
    'request_statistics': '按 UTC 请求日期组织的日汇总输入投影',
}


def statements(source):
    """Split trusted SQL while preserving quoted semicolons and query text."""
    parts, quote, i = [], None, 0
    while i < len(source):
        char = source[i]
        if quote:
            parts.append(char)
            if char == '\\' and i + 1 < len(source):
                i += 1
                parts.append(source[i])
            elif char == quote:
                if i + 1 < len(source) and source[i + 1] == quote:
                    i += 1
                    parts.append(source[i])
                else:
                    quote = None
        elif source.startswith('--', i):
            end = source.find('\n', i)
            i = len(source) if end < 0 else end
            parts.append('\n')
            continue
        elif char in "'\"`":
            quote = char
            parts.append(char)
        elif char == ';':
            if ''.join(parts).strip():
                yield ''.join(parts).strip()
            parts = []
        else:
            parts.append(char)
        i += 1
    if quote:
        raise ValueError('unterminated SQL string')
    if ''.join(parts).strip():
        yield ''.join(parts).strip()


def quoted_value(value):
    return value.replace("''", "'").replace('\\\\', '\\').replace("\\'", "'")


def parse_relation(statement, filename):
    match = re.match(r'CREATE (?:OR REPLACE )?(TABLE|MATERIALIZED VIEW|VIEW) (?:IF NOT EXISTS )?metering\.(\w+) \(\n(.*?)\n\)(.*)$', statement, re.S)
    if not match:
        raise ValueError('unannotated or unsupported relation in ' + filename)
    kind, name, body, tail = match.groups()
    columns = []
    for line in body.splitlines():
        line = line.strip().removesuffix(',')
        column = re.fullmatch(r'(\w+) (?:(.+?) (NOT NULL|NULL)(?: DEFAULT (.+?))? )?COMMENT ' + SQL_STRING, line)
        if not column:
            raise ValueError('missing column comment: ' + name + ': ' + line)
        col, typ, nullable, default, comment = column.groups()
        if not comment.strip() or (kind == 'TABLE') != (typ is not None):
            raise ValueError('invalid annotated column: ' + name + '.' + col)
        columns.append(dict(name=col, type=typ, nullable=nullable == 'NULL',
                            default=default, comment=quoted_value(comment)))
    if len({c['name'] for c in columns}) != len(columns):
        raise ValueError('duplicate column in ' + name)
    relation = dict(name=name, kind=kind, columns=columns, source=filename)
    if kind == 'TABLE':
        keys = re.search(r'UNIQUE KEY\(([^)]+)\)', tail)
        purpose = re.search(r'\bCOMMENT ' + SQL_STRING, tail)
        if not keys or not purpose or not purpose.group(1).strip():
            raise ValueError('missing table key or comment: ' + name)
        relation.update(keys=[v.strip() for v in keys.group(1).split(',')],
                        purpose=quoted_value(purpose.group(1)))
    elif kind == 'MATERIALIZED VIEW':
        purpose = re.search(r'\bCOMMENT ' + SQL_STRING, tail)
        if not purpose or ' AS\n' not in tail:
            raise ValueError('missing materialized view comment/query: ' + name)
        relation.update(keys=[], query=tail.rsplit(' AS\n', 1)[1], purpose=quoted_value(purpose.group(1)))
    else:
        if not tail.startswith(' AS\n'):
            raise ValueError('missing view query: ' + name)
        relation.update(keys=[], query=tail[len(' AS\n'):], purpose=VIEW_PURPOSES.get(name, NOTES[name][5]))
    return relation


def schema(directory=DDL):
    relations = OrderedDict()
    if {p.name for p in directory.glob('*.sql')} != set(INITIALIZATIONS):
        raise ValueError('initialization file inventory drift')
    for filename in INITIALIZATIONS:
        for statement in statements((directory / filename).read_text()):
            if re.match(r'CREATE (?:OR REPLACE )?(?:TABLE|MATERIALIZED VIEW|VIEW) ', statement):
                relation = parse_relation(statement, filename)
                previous = relations.get(relation['name'])
                if previous:
                    raise ValueError('duplicate relation definition: ' + relation['name'])
                relations[relation['name']] = relation
    if set(relations) != set(NOTES):
        raise ValueError('relation purpose inventory drift')
    return OrderedDict((name, relation) for kind in ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
                       for name, relation in relations.items() if relation['kind'] == kind)


def live_columns(database):
    database = database_name(database)
    query = ("SELECT table_name,column_name,column_type,is_nullable,COALESCE(column_comment,'') "
             "FROM information_schema.columns WHERE table_schema='" + database +
             "' ORDER BY table_name,ordinal_position;")
    command = ['docker', 'compose', '-f', str(ROOT / 'deploy/local/compose.yaml'), 'exec', '-T',
               'doris', 'mysql', '-uroot', '-h127.0.0.1', '-P9030', '--default-character-set=utf8mb4',
               '--batch', '--raw', '--skip-column-names', '-e', query]
    raw = subprocess.check_output(command, text=True, cwd=ROOT)
    result = {}
    for line in raw.splitlines():
        table, name, typ, nullable, comment = line.split('\t')
        result.setdefault(table, []).append(dict(name=name, type=typ, nullable=nullable == 'YES', comment=comment))
    return result


def normal_type(typ):
    typ = typ.upper().replace('DECIMALV3', 'DECIMAL')
    typ = re.sub(r'\bTINYINT\(1\)', 'BOOLEAN', typ)
    typ = re.sub(r'\b(BIGINT|SMALLINT|TINYINT|INT)\(\d+\)', r'\1', typ)
    return re.sub(r'\s+', '', typ)


def validate_live(relations, live):
    if set(relations) != set(live):
        raise ValueError('live relation inventory does not match current model')
    for name, relation in relations.items():
        expected, actual = relation['columns'], live[name]
        if [(c['name'], c['comment']) for c in expected] != [(c['name'], c['comment']) for c in actual]:
            raise ValueError('live column/comment drift: ' + name)
        if relation['kind'] == 'TABLE':
            if [(normal_type(c['type']), c['nullable']) for c in expected] != [(normal_type(c['type']), c['nullable']) for c in actual]:
                raise ValueError('live column type/nullability drift: ' + name)


def cell(value):
    return str(value).replace('|', '&#124;').replace('\n', '<br>')


def document(relations, live):
    validate_live(relations, live)
    table_count = sum(r['kind'] == 'TABLE' for r in relations.values())
    table_columns = sum(len(r['columns']) for r in relations.values() if r['kind'] == 'TABLE')
    view_count = sum(r['kind'] == 'VIEW' for r in relations.values())
    mv_count = sum(r['kind'] == 'MATERIALIZED VIEW' for r in relations.values())
    view_columns = sum(len(r['columns']) for r in relations.values() if r['kind'] == 'VIEW')
    out = ['# Doris 数据库表与字段说明', '',
           '本文描述当前唯一采集模型，面向开发、运维和数据使用者。实体表的类型、键、说明来自仓库 DDL；视图类型及可空性经 Doris 4.1.3 元数据核对。整数显示宽度和 DECIMALV3 名称归一化为 SQL 类型。', '',
           f'覆盖 {table_count} 张实体表、{table_columns} 个实体字段，以及 {view_count} 个逻辑视图、{view_columns} 个逻辑视图字段，以及 {mv_count} 个异步物化视图。当前本地 Consumer 写入 metering_development；迁移仅定义当前最终结构，不提供旧开发库的原地升级。', '',
           '## 使用口径', '',
           '- 一个逻辑请求最多包含 request_started、usage_observed 两类事件；查询调用量按 logical_request_id 统计，不能按事件行数统计。',
           '- 原始用量 NULL 表示未知，0 表示已知零。输入/输出及缓存、推理子项不能重复相加。usage_eligible 表示统计候选，不表示收费资格。',
           '- Observer 支持 HTTP/HTTPS 归属查询，环境仅接受 prd/stg/dev；ID 与服务、供应商和客户名称作为请求快照保存。默认缓存成功结果 30 分钟，瞬时故障最多再沿用 15 分钟并保持 mapped；明确错误作废旧值。名称可能来自缓存，历史记录不补写；空 URI 或无可用结果侧为 NULL。',
           '- 采集范围由 Wasm 的 accepted_request_hosts 统一筛选；无效或不匹配的 Host 不产生观察事件，Observer / Consumer 不做服务或 Host 白名单过滤，历史诊断仍按原契约保存。',
           '- 观察取消后保留已完成的单侧归属，未返回侧在原查询截止时间复核旧值回退资格；无法回退时标为 lookup_timeout，快照冻结后不接受迟到回写。',
           '- 全部时间按 UTC。Unix 毫秒字段带 _ms；DATETIME(3) 是 UTC 审计时间。offset 仅在同一 topic/分区比较，first/last 包含，next 不包含。',
           '- Unique Key 是 Doris 幂等键。下述关系是应用逻辑关联，没有声明数据库外键。协调表不是数据库锁，也不使 Kafka/Doris 成为分布式事务。',
           '- usage-worker 已实现 T+1 日/月物化视图刷新、恢复及自然月清理；统计 HTTP API 尚未实现。本地测试不代表生产验收。', '',
           '## 表与视图总览', '', '| 名称 | 类型 | 作用 | 实现状态 |', '| --- | --- | --- | --- |']
    for name, relation in relations.items():
        out.append('| [' + name + '](#' + name + ') | ' + ('实体表' if relation['kind'] == 'TABLE' else ('异步物化视图' if relation['kind'] == 'MATERIALIZED VIEW' else '逻辑视图')) + ' | ' + cell(relation['purpose']) + ' | ' + NOTES[name][4] + ' |')
    out.extend(['', '## 数据流与逻辑关联', '', '```mermaid', 'flowchart TD',
                '  Kafka --> Consumer', '  Consumer --> events_raw', '  Consumer --> ingest_rejections',
                '  Consumer --> request_conflict_ids', '  Consumer --> summary_dirty_notifications',
                '  Consumer --> consumer_batch_receipts', '  Consumer --> consumer_partition_origins',
                '  Consumer --> consumer_partition_gaps', '  events_raw --> event_conflicts',
                '  events_raw --> event_details', '  event_conflicts -. 排除同事件冲突 .-> event_details',
                '  events_raw --> requests', '  request_conflict_ids --> requests',
                '  event_details --> usage_effective', '  requests --> usage_effective',
                '  events_raw --> request_statistics', '  request_conflict_ids --> request_statistics',
                '  request_statistics --> usage_daily_mv', '  usage_daily_mv --> usage_monthly_mv',
                '  summary_dirty_notifications --> summary_mv_refresh_inputs',
                '  summary_mv_refresh_inputs --> summary_mv_refresh_runs',
                '  summary_mv_refresh_runs --> summary_mv_refresh_state', '```', '',
                '消费屏障顺序为：原始/拒收记录可见 → 冲突维护可见 → 日期通知可见 → 批次凭据可见 → Kafka offset 提交。汇总由 usage-worker 的 Doris 任务核验状态确认，Consumer 不等待汇总。', ''])
    for name, relation in relations.items():
        writer, reader, grain, retention, state, detail = NOTES[name]
        out.extend(['## ' + name, '', '**作用：** ' + relation['purpose'] + '。', '', detail, '',
                    '| 属性 | 说明 |', '| --- | --- |',
                    '| 类型 | ' + ('实体表，Unique Key Merge-on-Write' if relation['kind'] == 'TABLE' else ('异步物化视图，手动分区刷新' if relation['kind'] == 'MATERIALIZED VIEW' else '逻辑视图，无物理数据副本')) + ' |',
                    '| 写入者 | ' + writer + ' |', '| 读取者 | ' + reader + ' |',
                    '| 数据粒度 | ' + grain + ' |', '| 唯一键 | ' + (', '.join(relation['keys']) or '无独立唯一键') + ' |',
                    '| 分区、分桶及保留 | ' + retention + ' |', '| 实现状态 | ' + state + ' |',
                    '| 定义来源 | [' + relation['source'] + '](../migrations/doris/metering/' + relation['source'] + ') |', '',
                    '| 字段 | 类型 | 可空 | 默认值 | 键属性 | 说明 |', '| --- | --- | --- | --- | --- | --- |'])
        for col, compiled in zip(relation['columns'], live[name]):
            is_table = relation['kind'] == 'TABLE'
            typ = col['type'] if is_table else normal_type(compiled['type'])
            nullable = col['nullable'] if is_table else compiled['nullable']
            default = (col['default'] or '未声明') if is_table else '—（派生）'
            key = '唯一键' if col['name'] in relation['keys'] else '—'
            out.append('| ' + ' | '.join(cell(v) for v in (col['name'], typ, '是' if nullable else '否', default, key, col['comment'])) + ' |')
        out.append('')
    out.extend(['## 初始化与文档再生成', '',
                '新库按 001_init.sql → 002_consumer_coordination.sql → 003_usage_summaries.sql 初始化。全部字段、注释和视图直接采用最终定义，不执行历史补丁。旧开发库需选用新库重建，初始化工具不会自动删库或转换旧数据。', '',
                '修改字段或注释后，在隔离空库执行全部初始化脚本，再读取该库元数据生成文档：', '', '```sh',
                'python3 scripts/doris_schema.py document --database metering_schema_test > docs/doris-database-schema.md',
                'make test', '```', '',
                '文档生成要求 SQL 与目标库的全部关系、列顺序和注释一致，结构不一致的库会拒绝生成。该命令只读取元数据，不读取业务记录。表和列的业务说明以 SQL COMMENT 为准；每个对象的职责和保留策略在生成器中维护。', '',
                '相关说明：[数据库初始化](../deploy/doris/README.md)、[Kafka 事件字段](kafka-event-integration-guide.md)、[日/月汇总口径](daily-monthly-summary-design.md)、[协调与恢复协议](doris-summary-coordination.md)。', ''])
    return '\n'.join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['document'])
    parser.add_argument('--database', type=database_name, default='metering_development',
                        help='Validated database containing the current initialized model')
    args = parser.parse_args()
    relations = schema()
    print(document(relations, live_columns(args.database)), end='')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, subprocess.SubprocessError) as error:
        print('Doris schema generation failed: ' + str(error), file=sys.stderr)
        sys.exit(1)
