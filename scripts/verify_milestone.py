#!/usr/bin/env python3
"""Run deterministic milestone verification and retain revision-bound evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Sequence

REPOSITORY_ROOT: Final = Path(__file__).resolve().parents[1]
HYPOTHESIS_SEED: Final = 41609
LEGACY_TEST_COUNT: Final = 17

LEGACY_TEST_PATHS: Final = (
    "tests/test_cqrs.py",
    "tests/test_durable_cqrs.py",
)

MILESTONE_1_TEST_GROUPS: Final = {
    "architecture": ("tests/architecture/test_dependency_boundaries.py",),
    "unit_domain": ("tests/unit/domain",),
    "normalization_and_idempotency": ("tests/unit/application",),
    "properties": (
        "tests/properties/test_property_01_valid_commands.py",
        "tests/properties/test_property_02_invalid_commands.py",
        "tests/properties/test_property_03_optimistic_append.py",
        "tests/properties/test_property_04_concurrent_no_overdraft.py",
        "tests/properties/test_property_05_envelope_stability.py",
        "tests/properties/test_property_06_command_idempotency.py",
        "tests/properties/test_property_07_correlation_identity.py",
        "tests/properties/test_property_08_schema_versions.py",
        "tests/properties/test_property_21_sqlite_compatibility.py",
    ),
    "compatibility_adapters": ("tests/compat/test_legacy_api.py",),
    "concurrency_and_process_harness": ("tests/failure/test_harness_selftest.py",),
    "milestone_contract": ("tests/milestones/test_milestone_1.py",),
}

OUTSTANDING_REQUIRED_GATES: Final = (
    "acceptance_criteria_requirements_5_through_19",
    "sustained_1000_requests_per_second",
    "command_p99_at_most_300_ms",
    "query_p99_at_most_100_ms",
    "maximum_projection_lag_at_most_1_second",
    "availability_at_least_99_9_percent_over_30_days",
    "fresh_revision_bound_release_evidence",
)

EXCLUDED_SCOPE: Final = (
    "http_or_network_behavior",
    "postgresql_production_tier",
    "kafka_delivery",
    "authentication_and_authorization",
    "deployment_and_operability",
    "load_and_availability_gates",
)


@dataclass(frozen=True)
class SuiteResult:
    """Machine-readable summary of one pytest subprocess."""

    name: str
    status: str
    command: tuple[str, ...]
    report: str
    test_count: int
    failures: int
    errors: int
    skipped: int
    duration_seconds: float
    test_cases: tuple[str, ...]
    return_code: int


def milestone_1_test_paths() -> tuple[str, ...]:
    """Return every additive Milestone 1 test path exactly once."""
    paths = tuple(path for group in MILESTONE_1_TEST_GROUPS.values() for path in group)
    if len(paths) != len(set(paths)):
        raise ValueError("Milestone 1 test groups contain duplicate paths")
    return paths


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("milestone", choices=("1",))
    parser.add_argument(
        "--evidence-root",
        type=Path,
        help="Override the default revision-bound evidence directory.",
    )
    return parser.parse_args(arguments)


def _git_output(*arguments: str) -> str:
    completed = subprocess.run(
        ("git", *arguments),
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _revision() -> str:
    try:
        return _git_output("rev-parse", "HEAD") or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _working_tree_state() -> str:
    try:
        status = _git_output(
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            str(REPOSITORY_ROOT),
            # The gate's own evidence output must not mark the run dirty.
            f":(exclude){REPOSITORY_ROOT / 'evidence'}",
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return "dirty" if status else "clean"


def _source_fingerprint() -> str:
    """Hash milestone inputs so dirty-tree evidence remains identifiable."""
    digest = hashlib.sha256()
    roots = (
        REPOSITORY_ROOT / "cloudscale",
        REPOSITORY_ROOT / "cqrs",
        REPOSITORY_ROOT / "tests",
        REPOSITORY_ROOT / "scripts",
    )
    files = [
        path
        for root in roots
        if root.exists()
        for path in root.rglob("*.py")
        if path.is_file()
    ]
    files.extend(
        path
        for path in (
            REPOSITORY_ROOT / "pyproject.toml",
            REPOSITORY_ROOT / "requirements.lock",
            REPOSITORY_ROOT / "requirements-dev.lock",
        )
        if path.exists()
    )
    for path in sorted(files):
        relative_path = path.relative_to(REPOSITORY_ROOT).as_posix()
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _junit_summary(
    report_path: Path,
) -> tuple[int, int, int, int, float, tuple[str, ...]]:
    root = ET.parse(report_path).getroot()
    suites = (root,) if root.tag == "testsuite" else tuple(root.findall("testsuite"))
    test_count = sum(int(suite.attrib.get("tests", "0")) for suite in suites)
    failures = sum(int(suite.attrib.get("failures", "0")) for suite in suites)
    errors = sum(int(suite.attrib.get("errors", "0")) for suite in suites)
    skipped = sum(int(suite.attrib.get("skipped", "0")) for suite in suites)
    duration_seconds = sum(float(suite.attrib.get("time", "0")) for suite in suites)
    test_cases = tuple(
        sorted(
            f"{case.attrib.get('classname', '')}::{case.attrib.get('name', '')}"
            for suite in suites
            for case in suite.iter("testcase")
        )
    )
    return test_count, failures, errors, skipped, duration_seconds, test_cases


def _run_pytest(
    *,
    name: str,
    test_paths: Sequence[str],
    report_path: Path,
    include_hypothesis_seed: bool,
) -> SuiteResult:
    command = [
        sys.executable,
        "-m",
        "pytest",
        *test_paths,
        "-q",
        f"--junitxml={report_path}",
    ]
    if include_hypothesis_seed:
        command.append(f"--hypothesis-seed={HYPOTHESIS_SEED}")

    environment = dict(os.environ)
    environment["PYTHONHASHSEED"] = "0"
    completed = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)

    if not report_path.exists():
        return SuiteResult(
            name=name,
            status="failed",
            command=tuple(command),
            report=str(report_path.relative_to(REPOSITORY_ROOT)),
            test_count=0,
            failures=0,
            errors=1,
            skipped=0,
            duration_seconds=0.0,
            test_cases=(),
            return_code=completed.returncode,
        )

    test_count, failures, errors, skipped, duration_seconds, test_cases = (
        _junit_summary(report_path)
    )
    passed = completed.returncode == 0 and failures == 0 and errors == 0
    return SuiteResult(
        name=name,
        status="passed" if passed else "failed",
        command=tuple(command),
        report=str(report_path.relative_to(REPOSITORY_ROOT)),
        test_count=test_count,
        failures=failures,
        errors=errors,
        skipped=skipped,
        duration_seconds=duration_seconds,
        test_cases=test_cases,
        return_code=completed.returncode,
    )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def verify_milestone_1(evidence_root: Path | None = None) -> int:
    revision = _revision()
    evidence_directory = (
        evidence_root.resolve()
        if evidence_root is not None
        else REPOSITORY_ROOT / "evidence" / revision / "milestone-1"
    )
    reports_directory = evidence_directory / "test-reports"
    reports_directory.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(UTC)
    started_ns = time.monotonic_ns()
    legacy_result = _run_pytest(
        name="legacy_compatibility",
        test_paths=LEGACY_TEST_PATHS,
        report_path=reports_directory / "legacy-compatibility.junit.xml",
        include_hypothesis_seed=False,
    )
    additions_result = _run_pytest(
        name="milestone_1_additions",
        test_paths=milestone_1_test_paths(),
        report_path=reports_directory / "milestone-1-additions.junit.xml",
        include_hypothesis_seed=True,
    )
    ended_at = datetime.now(UTC)

    legacy_count_preserved = legacy_result.test_count == LEGACY_TEST_COUNT
    status = (
        "passed"
        if legacy_result.status == additions_result.status == "passed"
        and legacy_count_preserved
        and additions_result.test_count > 0
        else "failed"
    )
    manifest_path = evidence_directory / "manifest.json"
    manifest = {
        "schema_version": 1,
        "milestone": 1,
        "capability": "correctness_foundation",
        "status": status,
        "release_designation": "milestone_1_only",
        "completed_milestones": [1] if status == "passed" else [],
        "outstanding_required_gates": list(OUTSTANDING_REQUIRED_GATES),
        "excluded_scope": list(EXCLUDED_SCOPE),
        "known_limitations": [
            "Concurrency checks use the Milestone 1 in-memory model; real PostgreSQL concurrency is deferred.",
            "Process harness self-tests prove child-process control, not later persistence or broker crash windows.",
            "No HTTP, network, production persistence, load, or availability behavior is exercised.",
        ],
        "revision": revision,
        "working_tree_state": _working_tree_state(),
        "source_fingerprint_sha256": _source_fingerprint(),
        "hypothesis_seed": HYPOTHESIS_SEED,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": (time.monotonic_ns() - started_ns) / 1_000_000_000,
        "legacy_test_count_expected": LEGACY_TEST_COUNT,
        "legacy_test_count_preserved": legacy_count_preserved,
        "test_groups": {
            name: list(paths) for name, paths in MILESTONE_1_TEST_GROUPS.items()
        },
        "test_suites": [asdict(legacy_result), asdict(additions_result)],
    }
    _write_json(manifest_path, manifest)

    print(f"Milestone 1 verification: {status}")
    print(f"Legacy compatibility: {legacy_result.test_count}/{LEGACY_TEST_COUNT} tests")
    print(f"New checks: {additions_result.test_count} tests")
    print(f"Evidence: {manifest_path.relative_to(REPOSITORY_ROOT)}")
    if status != "passed":
        if not legacy_count_preserved:
            print(
                "Milestone 1 requires exactly 17 original compatibility tests; "
                f"collected {legacy_result.test_count}.",
                file=sys.stderr,
            )
        return 1
    return 0


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = _parse_args(arguments)
    if parsed.milestone == "1":
        return verify_milestone_1(parsed.evidence_root)
    raise AssertionError("argparse accepted an unsupported milestone")


if __name__ == "__main__":
    raise SystemExit(main())
