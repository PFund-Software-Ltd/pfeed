"""Guards `import pfeed` against becoming slow.

The import runs in a fresh interpreter, because once pfeed is in `sys.modules`
re-importing it costs nothing and would always pass.
"""

import subprocess
import sys

import pytest

# `import pfeed` takes ~60ms warm as of 2026-10; most of it is pfund's config
IMPORT_TIME_BUDGET = 0.3  # seconds
NUM_RUNS = 5


@pytest.mark.smoke
def test_import_time_within_budget():
    code = "import time; t = time.perf_counter(); import pfeed; print(time.perf_counter() - t)"
    # min() filters out noise, e.g. the first run compiling .pyc files or a busy machine
    import_time = min(
        float(subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout)
        for _ in range(NUM_RUNS)
    )
    assert import_time < IMPORT_TIME_BUDGET, (
        f"`import pfeed` took {import_time:.3f}s, budget is {IMPORT_TIME_BUDGET}s; "
        "run `python -X importtime -c 'import pfeed'` to find the slow import"
    )
