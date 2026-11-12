"""Offline token-count proof for the synthetic prompt content used by collection-load."""
import argparse
import hashlib
import json
from pathlib import Path

import tiktoken


def verify():
    encoding = tiktoken.get_encoding('cl100k_base')
    profiles = []
    for count in (100000, 200000, 500000, 1000000):
        content = ' word' * count
        actual = len(encoding.encode_ordinary(content))
        if actual != count:
            raise RuntimeError(f'token_count_mismatch: expected={count}, actual={actual}')
        profiles.append({'content_units': count, 'tokens': actual, 'utf8_bytes': len(content.encode()),
                         'content_sha256': hashlib.sha256(content.encode()).hexdigest()})
    return {'encoding': encoding.name, 'tiktoken_version': tiktoken.__version__,
            'pattern': ' word repeated n times',
            'scope': 'Prompt content only; synthetic model usage; not a real model chat template or inference.',
            'profiles': profiles}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = verify()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
