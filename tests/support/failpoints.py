"""Explicit cross-process failpoints and dependency-outage controls."""

from __future__ import annotations

import json
import os
import secrets
import socket
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

FAILPOINT_HOST_ENV = "CLOUDSCALE_FAILPOINT_HOST"
FAILPOINT_PORT_ENV = "CLOUDSCALE_FAILPOINT_PORT"
FAILPOINT_TOKEN_ENV = "CLOUDSCALE_FAILPOINT_TOKEN"
_MAX_MESSAGE_BYTES = 1_048_576


class FailpointTimeout(TimeoutError):
    """Raised when a failpoint hit or dependency transition misses its deadline."""


class InjectedFailure(RuntimeError):
    """Raised in a child when the controller releases a failpoint as a failure."""


@dataclass(frozen=True)
class FailpointHit:
    """Controller-side evidence for one child failpoint arrival."""

    hit_id: str
    name: str
    pid: int
    occurred_at: str
    monotonic_ns: int
    metadata: Mapping[str, Any]


@dataclass
class _PendingHit:
    event: threading.Event
    action: Literal["continue", "raise"] = "continue"


class FailpointCoordinator:
    """Authenticated loopback IPC server that blocks children at named points."""

    def __init__(self, *, host: str = "127.0.0.1") -> None:
        self._token = secrets.token_urlsafe(32)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((host, 0))
        self._socket.listen()
        self._socket.settimeout(0.1)
        bound_host, bound_port = self._socket.getsockname()
        self.host = str(bound_host)
        self.port = int(bound_port)
        self._condition = threading.Condition()
        self._hits: dict[str, deque[FailpointHit]] = defaultdict(deque)
        self._pending: dict[str, _PendingHit] = {}
        self._closed = threading.Event()
        self._handlers: list[threading.Thread] = []
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name="failpoint-coordinator",
            daemon=True,
        )
        self._accept_thread.start()

    @property
    def environment(self) -> dict[str, str]:
        """Environment variables required by ``FailpointClient.from_environment``."""
        return {
            FAILPOINT_HOST_ENV: self.host,
            FAILPOINT_PORT_ENV: str(self.port),
            FAILPOINT_TOKEN_ENV: self._token,
        }

    def wait_for(self, name: str, timeout: float = 5.0) -> FailpointHit:
        """Wait for and consume the next child arrival at ``name``."""
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        deadline = time.monotonic() + timeout
        with self._condition:
            while not self._hits[name]:
                if self._closed.is_set():
                    raise RuntimeError("failpoint coordinator is closed")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise FailpointTimeout(f"failpoint {name!r} was not reached")
                self._condition.wait(remaining)
            return self._hits[name].popleft()

    def release(
        self,
        hit: FailpointHit,
        *,
        action: Literal["continue", "raise"] = "continue",
    ) -> None:
        """Release a blocked child, optionally making its client raise."""
        with self._condition:
            pending = self._pending.get(hit.hit_id)
            if pending is None:
                raise KeyError(f"unknown or already released hit {hit.hit_id}")
            pending.action = action
            pending.event.set()

    def release_all(self) -> None:
        """Release every blocked child so coordinator shutdown cannot strand one."""
        with self._condition:
            for pending in self._pending.values():
                pending.event.set()

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self.release_all()
        self._socket.close()
        self._accept_thread.join(timeout=1.0)
        for handler in tuple(self._handlers):
            handler.join(timeout=1.0)

    def __enter__(self) -> FailpointCoordinator:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _accept_loop(self) -> None:
        while not self._closed.is_set():
            try:
                connection, _ = self._socket.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            handler = threading.Thread(
                target=self._handle_connection,
                args=(connection,),
                name="failpoint-handler",
                daemon=True,
            )
            self._handlers.append(handler)
            handler.start()

    def _handle_connection(self, connection: socket.socket) -> None:
        hit_id: str | None = None
        try:
            message = _receive_message(connection)
            if not secrets.compare_digest(str(message.get("token", "")), self._token):
                _send_message(connection, {"status": "error", "error": "unauthorized"})
                return
            name = message.get("name")
            pid = message.get("pid")
            metadata = message.get("metadata", {})
            if not isinstance(name, str) or not name:
                raise ValueError("failpoint name must be a non-empty string")
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                raise ValueError("failpoint pid must be a positive integer")
            if not isinstance(metadata, dict):
                raise ValueError("failpoint metadata must be an object")

            hit_id = secrets.token_hex(16)
            hit = FailpointHit(
                hit_id=hit_id,
                name=name,
                pid=pid,
                occurred_at=datetime.now(UTC).isoformat(),
                monotonic_ns=time.monotonic_ns(),
                metadata=metadata,
            )
            pending = _PendingHit(threading.Event())
            with self._condition:
                self._pending[hit_id] = pending
                self._hits[name].append(hit)
                self._condition.notify_all()
            pending.event.wait()
            _send_message(connection, {"status": pending.action, "hit_id": hit_id})
        except (ConnectionError, OSError):
            pass
        except (ValueError, json.JSONDecodeError) as error:
            try:
                _send_message(connection, {"status": "error", "error": str(error)})
            except OSError:
                pass
        finally:
            if hit_id is not None:
                with self._condition:
                    self._pending.pop(hit_id, None)
            connection.close()


class FailpointClient:
    """Child-side client for blocking at controller-owned failpoints."""

    def __init__(self, host: str, port: int, token: str) -> None:
        self.host = host
        self.port = port
        self.token = token

    @classmethod
    def from_environment(cls, env: Mapping[str, str] | None = None) -> FailpointClient:
        source = os.environ if env is None else env
        try:
            host = source[FAILPOINT_HOST_ENV]
            port = int(source[FAILPOINT_PORT_ENV])
            token = source[FAILPOINT_TOKEN_ENV]
        except (KeyError, ValueError) as error:
            raise RuntimeError("failpoint IPC environment is incomplete") from error
        return cls(host, port, token)

    def hit(
        self,
        name: str,
        metadata: Mapping[str, Any] | None = None,
        *,
        timeout: float = 30.0,
    ) -> None:
        """Notify the controller and block until it releases this exact hit."""
        if not name:
            raise ValueError("failpoint name must be non-empty")
        with socket.create_connection(
            (self.host, self.port), timeout=timeout
        ) as connection:
            connection.settimeout(timeout)
            _send_message(
                connection,
                {
                    "token": self.token,
                    "name": name,
                    "pid": os.getpid(),
                    "metadata": dict(metadata or {}),
                },
            )
            response = _receive_message(connection)
        status = response.get("status")
        if status == "continue":
            return
        if status == "raise":
            raise InjectedFailure(f"injected failure at {name}")
        raise RuntimeError(f"failpoint controller rejected {name}: {response}")


@dataclass
class DependencyOutage:
    """Timing and status for one controlled dependency outage."""

    dependency: str
    started_at: str
    started_monotonic_ns: int
    recovered_at: str | None = None
    recovered_monotonic_ns: int | None = None

    @property
    def duration_ms(self) -> float | None:
        if self.recovered_monotonic_ns is None:
            return None
        return (self.recovered_monotonic_ns - self.started_monotonic_ns) / 1_000_000


class DependencyOutageController:
    """Generic outage control for containers, proxies, or service processes."""

    def __init__(
        self,
        dependency: str,
        *,
        take_offline: Callable[[], None],
        restore: Callable[[], None],
        is_available: Callable[[], bool] | None = None,
        poll_interval: float = 0.01,
    ) -> None:
        if not dependency:
            raise ValueError("dependency name must be non-empty")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self.dependency = dependency
        self._take_offline = take_offline
        self._restore = restore
        self._is_available = is_available
        self.poll_interval = poll_interval
        self.history: list[DependencyOutage] = []
        self._active: DependencyOutage | None = None

    def begin(self, timeout: float = 5.0) -> DependencyOutage:
        """Take the dependency offline and wait until a probe confirms it."""
        if self._active is not None:
            raise RuntimeError(f"{self.dependency} already has an active outage")
        outage = DependencyOutage(
            dependency=self.dependency,
            started_at=datetime.now(UTC).isoformat(),
            started_monotonic_ns=time.monotonic_ns(),
        )
        self._take_offline()
        try:
            self._wait_for_availability(False, timeout)
        except BaseException:
            self._restore()
            self._wait_for_availability(True, timeout)
            raise
        self._active = outage
        self.history.append(outage)
        return outage

    def recover(self, timeout: float = 5.0) -> DependencyOutage:
        """Restore an active dependency and wait for availability."""
        if self._active is None:
            raise RuntimeError(f"{self.dependency} has no active outage")
        outage = self._active
        try:
            self._restore()
            self._wait_for_availability(True, timeout)
        finally:
            self._active = None
        outage.recovered_at = datetime.now(UTC).isoformat()
        outage.recovered_monotonic_ns = time.monotonic_ns()
        return outage

    @contextmanager
    def outage(self, timeout: float = 5.0) -> Iterator[DependencyOutage]:
        """Guarantee dependency restoration when the injected outage scope ends."""
        outage = self.begin(timeout)
        try:
            yield outage
        finally:
            if self._active is outage:
                self.recover(timeout)

    def _wait_for_availability(self, expected: bool, timeout: float) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if self._is_available is None:
            return
        deadline = time.monotonic() + timeout
        while self._is_available() is not expected:
            if time.monotonic() >= deadline:
                state = "available" if expected else "unavailable"
                raise FailpointTimeout(
                    f"{self.dependency} did not become {state} within {timeout} seconds"
                )
            time.sleep(self.poll_interval)


def _send_message(connection: socket.socket, message: Mapping[str, Any]) -> None:
    encoded = json.dumps(message, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > _MAX_MESSAGE_BYTES:
        raise ValueError("failpoint message is too large")
    connection.sendall(encoded + b"\n")


def _receive_message(connection: socket.socket) -> dict[str, Any]:
    chunks = bytearray()
    while True:
        chunk = connection.recv(4096)
        if not chunk:
            raise ConnectionError("failpoint IPC connection closed")
        chunks.extend(chunk)
        if len(chunks) > _MAX_MESSAGE_BYTES:
            raise ValueError("failpoint message is too large")
        newline = chunks.find(b"\n")
        if newline >= 0:
            payload = json.loads(chunks[:newline].decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("failpoint message must be an object")
            return payload
