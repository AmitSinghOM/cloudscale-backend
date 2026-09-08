"""Operating-system child process lifecycle control for failure-injection tests."""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any


@dataclass(frozen=True)
class ProcessExit:
    """Evidence describing one completed child-process generation."""

    name: str
    generation: int
    pid: int
    returncode: int
    signal: int | None
    termination: str
    started_at: str
    exited_at: str
    started_monotonic_ns: int
    exited_monotonic_ns: int
    duration_ms: float
    stdout: str
    stderr: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return asdict(self)


class ProcessTimeout(TimeoutError):
    """Raised after a timed-out child has been forcibly cleaned up."""

    def __init__(self, message: str, exit_record: ProcessExit | None = None) -> None:
        super().__init__(message)
        self.exit_record = exit_record


class ProcessController:
    """Start, stop, kill, and restart a real OS child process.

    A new process session is used on POSIX so cleanup can terminate the child's
    descendants as well as the immediate process. Output is written to temporary
    files to avoid pipe-buffer deadlocks during long-running integration tests.
    """

    def __init__(
        self,
        command: Sequence[str],
        *,
        name: str = "child",
        cwd: str | Path | None = None,
        env: Mapping[str, str] | None = None,
        shutdown_timeout: float = 2.0,
    ) -> None:
        if not command or any(
            not isinstance(part, str) or not part for part in command
        ):
            raise ValueError("command must contain non-empty string arguments")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be positive")
        self.command = tuple(command)
        self.name = name
        self.cwd = Path(cwd) if cwd is not None else None
        self.env = dict(env or {})
        self.shutdown_timeout = shutdown_timeout
        self.generation = 0
        self.exits: list[ProcessExit] = []
        self._process: subprocess.Popen[str] | None = None
        self._stdout: IO[str] | None = None
        self._stderr: IO[str] | None = None
        self._started_at: datetime | None = None
        self._started_ns: int | None = None
        self._termination = "natural"

    @property
    def pid(self) -> int | None:
        """Return the current child PID, if started and not yet collected."""
        return self._process.pid if self._process is not None else None

    @property
    def running(self) -> bool:
        """Return whether the current child is alive."""
        return self._process is not None and self._process.poll() is None

    def start(self) -> int:
        """Start a new child generation and return its operating-system PID."""
        if self._process is not None:
            if self._process.poll() is None:
                raise RuntimeError(f"{self.name} is already running")
            self._collect_exit()

        stdout = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
        stderr = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
        child_env = os.environ.copy()
        child_env.update(self.env)
        kwargs: dict[str, Any] = {}
        if os.name == "posix":
            kwargs["start_new_session"] = True
        elif os.name == "nt":  # pragma: no cover - exercised by Windows CI
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

        started_at = datetime.now(UTC)
        started_ns = time.monotonic_ns()
        try:
            process = subprocess.Popen(
                self.command,
                cwd=self.cwd,
                env=child_env,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                text=True,
                **kwargs,
            )
        except BaseException:
            stdout.close()
            stderr.close()
            raise

        self.generation += 1
        self._process = process
        self._stdout = stdout
        self._stderr = stderr
        self._started_at = started_at
        self._started_ns = started_ns
        self._termination = "natural"
        return process.pid

    def wait(
        self,
        timeout: float | None = None,
        *,
        cleanup_on_timeout: bool = True,
    ) -> ProcessExit:
        """Wait for exit, forcibly cleaning up by default if the deadline expires."""
        process = self._require_process()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            exit_record = self.kill(reason="timeout") if cleanup_on_timeout else None
            raise ProcessTimeout(
                f"{self.name} did not exit within {timeout} seconds", exit_record
            ) from error
        return self._collect_exit()

    def terminate(self, timeout: float | None = None) -> ProcessExit:
        """Request graceful OS termination, escalating to a kill on timeout."""
        process = self._require_process()
        if process.poll() is not None:
            return self._collect_exit()
        self._termination = "terminate"
        self._send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=timeout or self.shutdown_timeout)
        except subprocess.TimeoutExpired:
            return self.kill(reason="terminate-timeout")
        return self._collect_exit()

    def kill(self, *, reason: str = "kill") -> ProcessExit:
        """Forcibly terminate the OS process (and its POSIX process group)."""
        process = self._require_process()
        if process.poll() is None:
            self._termination = reason
            self._send_signal(signal.SIGKILL)
            try:
                process.wait(timeout=self.shutdown_timeout)
            except subprocess.TimeoutExpired as error:  # pragma: no cover - OS fault
                raise ProcessTimeout(
                    f"{self.name} remained alive after SIGKILL"
                ) from error
        return self._collect_exit()

    def restart(self) -> int:
        """Stop an existing generation and start a fresh OS child."""
        if self._process is not None:
            if self._process.poll() is None:
                self.terminate()
            else:
                self._collect_exit()
        return self.start()

    def close(self) -> ProcessExit | None:
        """Ensure no managed child remains alive and return its final exit evidence."""
        if self._process is None:
            return None
        if self._process.poll() is None:
            return self.kill(reason="cleanup")
        return self._collect_exit()

    def __enter__(self) -> ProcessController:
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _require_process(self) -> subprocess.Popen[str]:
        if self._process is None:
            raise RuntimeError(f"{self.name} has not been started")
        return self._process

    def _send_signal(self, requested_signal: signal.Signals) -> None:
        process = self._require_process()
        if os.name == "posix":
            try:
                os.killpg(process.pid, requested_signal)
                return
            except ProcessLookupError:
                return
        if requested_signal == signal.SIGKILL:
            process.kill()
        else:
            process.terminate()

    def _collect_exit(self) -> ProcessExit:
        process = self._require_process()
        returncode = process.poll()
        if returncode is None:
            raise RuntimeError(f"{self.name} is still running")
        if self._started_at is None or self._started_ns is None:
            raise RuntimeError("process start timing was not recorded")

        exited_ns = time.monotonic_ns()
        exited_at = datetime.now(UTC)
        stdout = self._read_and_close(self._stdout)
        stderr = self._read_and_close(self._stderr)
        exit_record = ProcessExit(
            name=self.name,
            generation=self.generation,
            pid=process.pid,
            returncode=returncode,
            signal=-returncode if returncode < 0 else None,
            termination=self._termination,
            started_at=self._started_at.isoformat(),
            exited_at=exited_at.isoformat(),
            started_monotonic_ns=self._started_ns,
            exited_monotonic_ns=exited_ns,
            duration_ms=(exited_ns - self._started_ns) / 1_000_000,
            stdout=stdout,
            stderr=stderr,
        )
        self.exits.append(exit_record)
        self._process = None
        self._stdout = None
        self._stderr = None
        self._started_at = None
        self._started_ns = None
        return exit_record

    @staticmethod
    def _read_and_close(stream: IO[str] | None) -> str:
        if stream is None:
            return ""
        try:
            stream.flush()
            stream.seek(0)
            return stream.read()
        finally:
            stream.close()
