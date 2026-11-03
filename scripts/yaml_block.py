"""输出块式 YAML；仅依赖标准库，非空集合不使用 flow/JSON 写法。"""
import json
import re


def yaml_block(value, indent=0):
    def scalar(item):
        return json.dumps(item, ensure_ascii=False, allow_nan=False).replace('\u0085', r'\u0085').replace('\u2028', r'\u2028').replace('\u2029', r'\u2029')

    def field(prefix, item):
        if isinstance(item, (dict, list)) and item:
            return prefix + '\n' + yaml_block(item, indent + 2)
        return prefix + ' ' + scalar(item) + '\n'

    space = ' ' * indent
    if isinstance(value, dict):
        return ''.join(field(space + (key if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.-]*', key) and key.lower() not in {'null', 'true', 'false', 'on', 'off', 'yes', 'no', 'y', 'n'}
                                      else scalar(key)) + ':', item) for key, item in value.items())
    if isinstance(value, list):
        return ''.join(field(space + '-', item) for item in value)
    raise ValueError('YAML 根节点必须为映射或列表')
