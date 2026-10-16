"""Render the checked-in metering SQL for a validated, isolated database."""
import argparse
from pathlib import Path
import re


def database_name(value):
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', value):
        raise ValueError('invalid_doris_database')
    return value


def render_sql(sql, database):
    database = database_name(database)
    # Checked-in SQL uses single-quoted literals. Preserve them, including
    # topic/group names such as llm-metering-k8s-..., when changing identifiers.
    return re.sub(r"'(?:''|\\.|[^'\\])*'|\bmetering\b",
                  lambda match: database if match.group() == 'metering' else match.group(), sql)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    parser.add_argument('--database', default='metering', type=database_name)
    args = parser.parse_args()
    print(render_sql(args.file.read_text(), args.database), end='')
