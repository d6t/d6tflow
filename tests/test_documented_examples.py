from pathlib import Path
import os
import re
import subprocess
import sys

import pytest


@pytest.mark.parametrize('path', ['README.md', 'docs/example-ml.md', 'docs/example-minimal-cache.py'])
def test_documented_example(tmp_path, path):
    root = Path(__file__).resolve().parents[1]
    code = (root / path).read_text(encoding='utf-8')
    if path.endswith('.md'):
        code = re.findall(r'```python\s*\n(.*?)```', code, re.S)[0]
    code = "import d6tflow\nd6tflow.set_dir('data')\n" + code
    result = subprocess.run(
        [sys.executable, '-c', code], cwd=tmp_path,
        env={**os.environ, 'PYTHONPATH': str(root)},
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
