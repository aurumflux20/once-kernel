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


def test_server_json_version_matches_pyproject() -> None:
    """server.json carries its own copy of the version for the MCP Registry.

    Nothing else checks it, and it drifted once already — which publishes a
    registry listing pointing at a release that is not the current one.
    """
    import json

    root = pathlib.Path(__file__).resolve().parent.parent
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    expected = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.MULTILINE).group(1)

    doc = json.loads((root / "server.json").read_text(encoding="utf-8"))
    assert doc["version"] == expected, "server.json .version is out of sync"
    assert doc["packages"][0]["version"] == expected, (
        "server.json .packages[0].version is out of sync"
    )
