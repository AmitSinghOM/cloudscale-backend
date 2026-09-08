"""Revision-bound JSON and JUnit evidence capture for test harnesses."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
import traceback
import xml.etree.ElementTree as ET
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4


class SupportsEvidence(Protocol):
    """Structural interface accepted by ``record_process_exit``."""

    def to_dict(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class EvidenceTiming:
    name: str
    started_monotonic_ns: int
    ended_monotonic_ns: int
    duration_ms: float
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class EvidenceCase:
    name: str
    status: str
    started_monotonic_ns: int
    ended_monotonic_ns: int
    duration_ms: float
    message: str | None = None
    traceback: str | None = None


class EvidenceRecorder:
    """Collect deterministic execution metadata and emit portable artifacts."""

    schema_version = 1

    def __init__(
        self,
        *,
        revision: str,
        seed: int | str,
        run_id: str | None = None,
    ) -> None:
        if not revision:
            raise ValueError("revision must be non-empty")
        self.revision = revision
        self.seed = seed
        self.run_id = run_id or uuid4().hex
        self.started_at = datetime.now(UTC)
        self.started_monotonic_ns = time.monotonic_ns()
        self.ended_at: datetime | None = None
        self.ended_monotonic_ns: int | None = None
        self.status = "running"
        self.timings: list[EvidenceTiming] = []
        self.cases: list[EvidenceCase] = []
        self.process_exits: list[dict[str, Any]] = []

    @classmethod
    def for_repository(
        cls,
        repository: str | Path,
        *,
        seed: int | str,
        run_id: str | None = None,
    ) -> EvidenceRecorder:
        """Resolve the current Git revision without modifying repository state."""
        return cls(
            revision=resolve_revision(repository),
            seed=seed,
            run_id=run_id,
        )

    def record_process_exit(
        self, exit_record: SupportsEvidence | Mapping[str, Any]
    ) -> None:
        """Capture child exit status, signal, output, and timing."""
        if isinstance(exit_record, Mapping):
            payload = dict(exit_record)
        else:
            payload = exit_record.to_dict()
        required = {"pid", "returncode", "started_at", "exited_at", "duration_ms"}
        missing = sorted(required.difference(payload))
        if missing:
            raise ValueError(f"process exit evidence missing fields: {missing}")
        self.process_exits.append(payload)

    def record_timing(
        self,
        name: str,
        started_monotonic_ns: int,
        ended_monotonic_ns: int,
        metadata: Mapping[str, Any] | None = None,
    ) -> EvidenceTiming:
        if not name:
            raise ValueError("timing name must be non-empty")
        if ended_monotonic_ns < started_monotonic_ns:
            raise ValueError("timing end must not precede its start")
        timing = EvidenceTiming(
            name=name,
            started_monotonic_ns=started_monotonic_ns,
            ended_monotonic_ns=ended_monotonic_ns,
            duration_ms=(ended_monotonic_ns - started_monotonic_ns) / 1_000_000,
            metadata=dict(metadata or {}),
        )
        self.timings.append(timing)
        return timing

    @contextmanager
    def timed(
        self, name: str, metadata: Mapping[str, Any] | None = None
    ) -> Iterator[None]:
        started_ns = time.monotonic_ns()
        try:
            yield
        finally:
            self.record_timing(name, started_ns, time.monotonic_ns(), metadata)

    @contextmanager
    def case(self, name: str) -> Iterator[None]:
        """Record a JUnit-style passed or failed test case."""
        started_ns = time.monotonic_ns()
        try:
            yield
        except BaseException as error:
            ended_ns = time.monotonic_ns()
            self.cases.append(
                EvidenceCase(
                    name=name,
                    status="failed",
                    started_monotonic_ns=started_ns,
                    ended_monotonic_ns=ended_ns,
                    duration_ms=(ended_ns - started_ns) / 1_000_000,
                    message=str(error),
                    traceback="".join(traceback.format_exception(error)),
                )
            )
            raise
        else:
            ended_ns = time.monotonic_ns()
            self.cases.append(
                EvidenceCase(
                    name=name,
                    status="passed",
                    started_monotonic_ns=started_ns,
                    ended_monotonic_ns=ended_ns,
                    duration_ms=(ended_ns - started_ns) / 1_000_000,
                )
            )

    def finish(self, status: str | None = None) -> None:
        """Seal top-level timing and derive status from recorded cases."""
        if self.ended_at is not None:
            if status is not None and status != self.status:
                raise RuntimeError("evidence has already been finished")
            return
        self.ended_at = datetime.now(UTC)
        self.ended_monotonic_ns = time.monotonic_ns()
        self.status = status or (
            "failed"
            if any(case.status != "passed" for case in self.cases)
            else "passed"
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the complete JSON evidence document, finishing if needed."""
        self.finish()
        assert self.ended_at is not None
        assert self.ended_monotonic_ns is not None
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "revision": self.revision,
            "seed": self.seed,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "started_monotonic_ns": self.started_monotonic_ns,
            "ended_monotonic_ns": self.ended_monotonic_ns,
            "duration_ms": (self.ended_monotonic_ns - self.started_monotonic_ns)
            / 1_000_000,
            "cases": [asdict(case) for case in self.cases],
            "process_exits": self.process_exits,
            "timings": [asdict(timing) for timing in self.timings],
        }

    def write_json(self, path: str | Path) -> Path:
        destination = Path(path)
        content = json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"
        _atomic_write(destination, content)
        return destination

    def write_junit(self, path: str | Path, *, suite_name: str) -> Path:
        """Write JUnit XML with revision/seed properties and raw harness evidence."""
        if not suite_name:
            raise ValueError("suite_name must be non-empty")
        document = self.to_dict()
        failures = sum(case.status != "passed" for case in self.cases)
        suite = ET.Element(
            "testsuite",
            {
                "name": suite_name,
                "tests": str(len(self.cases)),
                "failures": str(failures),
                "errors": "0",
                "time": f"{document['duration_ms'] / 1000:.9f}",
                "timestamp": str(document["started_at"]),
            },
        )
        properties = ET.SubElement(suite, "properties")
        for name, value in (
            ("revision", self.revision),
            ("seed", self.seed),
            ("run_id", self.run_id),
        ):
            ET.SubElement(
                properties,
                "property",
                {"name": name, "value": str(value)},
            )
        for case in self.cases:
            testcase = ET.SubElement(
                suite,
                "testcase",
                {
                    "classname": suite_name,
                    "name": case.name,
                    "time": f"{case.duration_ms / 1000:.9f}",
                },
            )
            if case.status != "passed":
                failure = ET.SubElement(
                    testcase,
                    "failure",
                    {"message": case.message or "failure"},
                )
                failure.text = case.traceback or case.message
        system_out = ET.SubElement(suite, "system-out")
        system_out.text = json.dumps(
            {
                "process_exits": self.process_exits,
                "timings": [asdict(timing) for timing in self.timings],
            },
            sort_keys=True,
        )
        xml = ET.tostring(suite, encoding="unicode", xml_declaration=True)
        destination = Path(path)
        _atomic_write(destination, xml + "\n")
        return destination

    def __enter__(self) -> EvidenceRecorder:
        return self

    def __exit__(self, error_type: object, *_: object) -> None:
        self.finish("failed" if error_type is not None else None)


def resolve_revision(repository: str | Path) -> str:
    """Return the exact Git commit for ``repository`` or ``unknown``."""
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(repository),
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    revision = completed.stdout.strip()
    return revision or "unknown"


def _atomic_write(destination: Path, content: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            delete=False,
        ) as temporary:
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_name = temporary.name
        os.replace(temporary_name, destination)
    finally:
        if temporary_name is not None and os.path.exists(temporary_name):
            os.unlink(temporary_name)
