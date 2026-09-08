"""Focused self-tests for reusable concurrency and failure-injection support.

Validates: Requirements 4.1, 4.9.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tests.support.concurrency import DeterministicBarrier, run_concurrently
from tests.support.evidence import EvidenceRecorder
from tests.support.failpoints import DependencyOutageController, FailpointCoordinator
from tests.support.process_controller import ProcessController, ProcessTimeout

REPOSITORY_ROOT = Path(__file__).parents[2]


def test_named_barrier_releases_only_after_every_declared_participant() -> None:
    barrier = DeterministicBarrier(
        ("writer-2", "writer-1", "writer-3"),
        name="same-version-writers",
        timeout=2,
    )
    allow_last_arrival = threading.Event()
    passed: list[str] = []
    passed_lock = threading.Lock()

    def arrive(name: str) -> object:
        if name == "writer-3":
            assert allow_last_arrival.wait(1)
        result = barrier.wait(name)
        with passed_lock:
            passed.append(name)
        return result

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {name: executor.submit(arrive, name) for name in barrier.participants}
        time.sleep(0.05)
        assert passed == []
        allow_last_arrival.set()
        results = {name: future.result(timeout=1) for name, future in futures.items()}

    assert {result.generation for result in results.values()} == {0}
    assert {result.released_monotonic_ns for result in results.values()} == {
        barrier.history[0].released_monotonic_ns
    }
    assert [results[name].ordinal for name in barrier.participants] == [0, 1, 2]
    assert [name for name, _ in barrier.history[0].arrivals] == list(
        barrier.participants
    )


def test_concurrent_runner_returns_declared_order_with_worker_timing() -> None:
    workers = {
        "later-finish": lambda: (time.sleep(0.02), "later")[1],
        "earlier-finish": lambda: "earlier",
    }

    results = run_concurrently(workers, timeout=1)

    assert [result.name for result in results] == list(workers)
    assert [result.value for result in results] == ["later", "earlier"]
    assert all(result.duration_ms >= 0 for result in results)
    assert len({result.thread_id for result in results}) == 2


def test_real_child_kill_restart_failpoint_and_evidence_capture(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "after-failpoint.txt"
    child_source = """
from pathlib import Path
import sys
from tests.support.failpoints import FailpointClient

client = FailpointClient.from_environment()
client.hit("before-side-effect", {"phase": "commit-window"})
Path(sys.argv[1]).write_text("completed", encoding="utf-8")
"""
    python_path = os.pathsep.join(
        filter(None, (str(REPOSITORY_ROOT), os.environ.get("PYTHONPATH")))
    )
    recorder = EvidenceRecorder.for_repository(
        REPOSITORY_ROOT,
        seed=41609,
        run_id="harness-selftest",
    )

    with FailpointCoordinator() as failpoints:
        controller = ProcessController(
            (sys.executable, "-c", child_source, str(marker)),
            name="failpoint-child",
            cwd=REPOSITORY_ROOT,
            env={**failpoints.environment, "PYTHONPATH": python_path},
            shutdown_timeout=1,
        )
        try:
            with recorder.case("real-child-kill-and-restart"):
                first_pid = controller.start()
                first_hit = failpoints.wait_for("before-side-effect", timeout=2)
                assert first_hit.pid == first_pid
                assert first_hit.metadata == {"phase": "commit-window"}

                first_exit = controller.kill()
                recorder.record_process_exit(first_exit)
                assert first_exit.returncode != 0
                assert first_exit.termination == "kill"
                assert not marker.exists()

                second_pid = controller.restart()
                second_hit = failpoints.wait_for("before-side-effect", timeout=2)
                assert second_hit.pid == second_pid
                failpoints.release(second_hit)
                second_exit = controller.wait(timeout=2)
                recorder.record_process_exit(second_exit)

                assert second_exit.generation == 2
                assert second_exit.returncode == 0
                assert marker.read_text(encoding="utf-8") == "completed"
                recorder.record_timing(
                    "kill-to-restart",
                    first_exit.exited_monotonic_ns,
                    second_exit.started_monotonic_ns,
                    {"first_pid": first_pid, "second_pid": second_pid},
                )
        finally:
            controller.close()

    json_path = recorder.write_json(tmp_path / "evidence.json")
    junit_path = recorder.write_junit(
        tmp_path / "evidence.junit.xml", suite_name="failure-harness"
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["revision"] != "unknown"
    assert payload["seed"] == 41609
    assert payload["status"] == "passed"
    assert payload["duration_ms"] >= 0
    assert [item["generation"] for item in payload["process_exits"]] == [1, 2]
    assert payload["process_exits"][0]["returncode"] != 0
    assert payload["process_exits"][1]["returncode"] == 0
    assert payload["timings"][0]["name"] == "kill-to-restart"

    suite = ET.parse(junit_path).getroot()
    properties_element = suite.find("properties")
    assert properties_element is not None
    properties = {
        item.attrib["name"]: item.attrib["value"] for item in properties_element
    }
    assert suite.attrib["tests"] == "1"
    assert suite.attrib["failures"] == "0"
    assert properties["revision"] == payload["revision"]
    assert properties["seed"] == "41609"
    system_out = json.loads(suite.findtext("system-out", default="{}"))
    assert len(system_out["process_exits"]) == 2


def test_process_wait_timeout_forcibly_cleans_up_child() -> None:
    controller = ProcessController(
        (sys.executable, "-c", "import time; time.sleep(60)"),
        name="hung-child",
        cwd=REPOSITORY_ROOT,
        shutdown_timeout=1,
    )
    controller.start()

    with pytest.raises(ProcessTimeout) as raised:
        controller.wait(timeout=0.05)

    assert raised.value.exit_record is not None
    assert raised.value.exit_record.termination == "timeout"
    assert raised.value.exit_record.returncode != 0
    assert not controller.running
    assert controller.close() is None


def test_dependency_outage_context_confirms_transition_and_restores() -> None:
    state = {"available": True}
    transitions: list[str] = []

    def take_offline() -> None:
        transitions.append("offline")
        state["available"] = False

    def restore() -> None:
        transitions.append("online")
        state["available"] = True

    controller = DependencyOutageController(
        "test-dependency",
        take_offline=take_offline,
        restore=restore,
        is_available=lambda: state["available"],
    )

    with pytest.raises(RuntimeError, match="exercise cleanup"):
        with controller.outage(timeout=1) as outage:
            assert not state["available"]
            assert outage.recovered_at is None
            raise RuntimeError("exercise cleanup")

    assert state["available"]
    assert transitions == ["offline", "online"]
    assert controller.history[0].recovered_at is not None
    assert controller.history[0].duration_ms is not None
