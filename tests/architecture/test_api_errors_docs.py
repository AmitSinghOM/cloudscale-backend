"""docs/API_ERRORS.md lists every error code and outcome the code can emit."""

from __future__ import annotations

import inspect
from pathlib import Path

from cloudscale.domain import errors
from cloudscale.domain.results import CommandOutcome

DOC = (Path(__file__).resolve().parents[2] / "docs" / "API_ERRORS.md").read_text()


def _domain_codes() -> set[str]:
    return {
        cls.code
        for _, cls in inspect.getmembers(errors, inspect.isclass)
        if issubclass(cls, errors.DomainError)
    }


def test_every_domain_error_code_is_documented() -> None:
    missing = {code for code in _domain_codes() if f"`{code}`" not in DOC}
    assert not missing, f"add to docs/API_ERRORS.md: {sorted(missing)}"


def test_every_command_outcome_is_documented() -> None:
    missing = {o.value for o in CommandOutcome if f"`{o.value}`" not in DOC}
    assert not missing, f"add to docs/API_ERRORS.md: {sorted(missing)}"
