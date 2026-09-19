"""Governance documents are part of the build: they may not drift silently.

- Every ADR file is numbered, has a Status line, and is indexed in
  docs/adr/README.md with the SAME status.
- Every indexed ADR exists.
- CHANGELOG.md keeps an [Unreleased] section (CONTRIBUTING §5).
- The documents the charter and AGENTS.md point at actually exist.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ADR_DIR = ROOT / "docs" / "adr"
ADR_FILE = re.compile(r"^(\d{4})-[a-z0-9-]+\.md$")
INDEX_ROW = re.compile(
    r"^\|\s*\[(\d{4})\]\(([^)]+)\)\s*\|\s*[^|]+\|\s*([A-Za-z ]+?)\s*\|$"
)
STATUS_LINE = re.compile(r"\*\*Status:\*\*\s*([A-Za-z]+)")


def _adr_files() -> dict[str, Path]:
    files = {}
    for path in ADR_DIR.iterdir():
        if path.name in ("README.md", "TEMPLATE.md"):
            continue
        match = ADR_FILE.match(path.name)
        assert match, f"ADR file name must be NNNN-kebab-title.md: {path.name}"
        files[match.group(1)] = path
    return files


def _index() -> dict[str, tuple[str, str]]:
    rows = {}
    for line in (ADR_DIR / "README.md").read_text().splitlines():
        match = INDEX_ROW.match(line.strip())
        if match:
            rows[match.group(1)] = (match.group(2), match.group(3).strip())
    assert rows, "ADR index table not found in docs/adr/README.md"
    return rows


def test_every_adr_is_indexed_with_matching_status() -> None:
    files, index = _adr_files(), _index()
    assert set(files) == set(index), (
        f"ADR files and index disagree: files={sorted(files)} index={sorted(index)}"
    )
    for number, path in files.items():
        link, indexed_status = index[number]
        assert link == path.name, (
            f"ADR-{number} index links {link}, file is {path.name}"
        )
        header = STATUS_LINE.search(path.read_text())
        assert header, f"ADR-{number} has no **Status:** line"
        file_status = header.group(1)
        assert indexed_status.startswith(file_status), (
            f"ADR-{number}: file says {file_status!r}, index says {indexed_status!r}"
        )


def test_adr_numbers_are_contiguous_from_0001() -> None:
    numbers = sorted(int(n) for n in _adr_files())
    assert numbers == list(range(1, len(numbers) + 1)), numbers


def test_changelog_keeps_an_unreleased_section() -> None:
    text = (ROOT / "CHANGELOG.md").read_text()
    assert "## [Unreleased]" in text


def test_governance_documents_exist() -> None:
    for relative in (
        "CONTRIBUTING.md",
        "AGENTS.md",
        "CODEOWNERS",
        "SECURITY.md",
        "CHANGELOG.md",
        "docs/LONGEVITY.md",
        "docs/CONFIGURATION.md",
        "docs/ARCHITECTURE.md",
        "docs/API_ERRORS.md",
        "docs/RUNBOOK.md",
        "docs/SLO.md",
        "docs/THREAT_MODEL.md",
        "docs/adr/TEMPLATE.md",
    ):
        assert (ROOT / relative).is_file(), relative


def test_upcasting_module_names_the_snapshot_state_version_rule() -> None:
    """ADR-0012: an upcaster that changes fold semantics must bump
    CURRENT_STATE_VERSION. The rule lives where the author of such a step
    will read it -- the upcasting module's docstring -- and this test keeps
    it there."""
    text = (ROOT / "cloudscale" / "domain" / "upcasting.py").read_text()
    assert "CURRENT_STATE_VERSION" in text and "ADR-0012" in text
