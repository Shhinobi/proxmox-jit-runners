"""Runs the browser-side unit tests that cannot be expressed in Python.

Most UI assertions in this suite read the served files as text, which is enough
for structural rules ("the mark has no <rect>"). The policy diff is different:
it is real logic with sentinels, unit conversions and set arithmetic, and a
text assertion would prove nothing about it. So it is tested as JavaScript, by
the same engine that runs it.

`node` is not a dependency of this project -- there is deliberately no build
step and none is being introduced -- so these skip when it is absent rather
than failing. CI has node; a minimal deploy host does not need it.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

JS_TESTS = sorted((Path(__file__).parent / "js").glob("*.test.mjs"))


@pytest.mark.parametrize("test_file", JS_TESTS, ids=lambda p: p.name)
def test_javascript_unit_tests_pass(test_file: Path) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed; skipping the browser-side unit tests")

    result = subprocess.run(
        [node, str(test_file)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, (
        f"{test_file.name} failed\n--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )


def test_the_javascript_suite_is_not_silently_empty() -> None:
    """A glob that matches nothing turns the parametrised test above into a
    no-op that still reports green."""
    assert JS_TESTS, "tests/js/*.test.mjs matched nothing"
