"""docs/CONFIGURATION.md is complete and current, or the build fails."""

from __future__ import annotations

import re
from pathlib import Path

from cloudscale.entrypoints.http.settings import HttpSettings

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "CONFIGURATION.md"
CODE_DIRS = ("cloudscale", "cqrs", "migrations", "scripts")
VAR = re.compile(r"\bCLOUDSCALE_[A-Z0-9_]+\b")


def _referenced_in_code() -> set[str]:
    found: set[str] = set()
    for directory in CODE_DIRS:
        for path in (ROOT / directory).rglob("*.py"):
            found |= set(VAR.findall(path.read_text()))
    prefix = HttpSettings.model_config["env_prefix"]
    found |= {f"{prefix}{name.upper()}" for name in HttpSettings.model_fields}
    return found


def _documented() -> set[str]:
    return set(VAR.findall(DOC.read_text()))


def test_every_environment_variable_is_documented() -> None:
    missing = _referenced_in_code() - _documented()
    assert not missing, f"add to docs/CONFIGURATION.md: {sorted(missing)}"


def test_documentation_names_only_real_variables() -> None:
    stale = _documented() - _referenced_in_code()
    assert not stale, f"documented but not in code: {sorted(stale)}"
