"""Tripwire: `once.__version__` must match the version in pyproject.toml.

It drifted once already (published 0.1.1 while the literal still said
0.1.0), which is invisible in normal use and wrong in every bug report
that quotes it. This test is the guard.
"""

import pathlib
import re

import once


def test_dunder_version_matches_pyproject() -> None:
    pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    assert match, "no version found in pyproject.toml"
    assert once.__version__ == match.group(1), (
        f"once.__version__ ({once.__version__}) is out of sync with "
        f"pyproject.toml ({match.group(1)})"
    )
