"""Minimal, correct client for the CloudScale HTTP API (stdlib + httpx).

Copy this file into your project. It shows the four things integrators get
wrong with an event-sourced API:

1. **Retry with the SAME ``command_id``.** The server is idempotent per
   command id: a retry after a timeout or 503 returns the original result or
   performs the command exactly once. Never mint a new id for a retry.
2. **Handle 409 by re-reading the version.** ``expected_version`` is an
   optimistic-concurrency guard. On 409 the body carries ``current_version``;
   re-run your business decision against it and resubmit.
3. **Reads are eventual.** The balance is a projection of the log. After a
   write, poll until the read model reaches the committed version instead of
   asserting on the first read.
4. **A transfer writes two streams.** ``POST .../{source}/transfers`` debits
   the source and credits the target in one transaction (ADR-0011). The
   response lists one posting per stream; ``expected_version`` guards the
   source only, and the target posting's ``committed_version`` is ``None``
   when you may not read that account - wait on the source's version, and on
   the target's only when it was returned.

Run against the dev stack::

    make dev DEV_PORT=8123
    CLOUDSCALE_TOKEN=$(make -s token DEV_PORT=8123) \\
        python examples/python_client.py http://127.0.0.1:8123
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from dataclasses import dataclass

import httpx


class VersionConflict(Exception):
    """409 version_conflict: another writer got there first. Re-read and resubmit."""

    def __init__(self, current_version: int) -> None:
        super().__init__(f"stream is at version {current_version}")
        self.current_version = current_version


class CommandIdConflict(Exception):
    """409 command_id_conflict: the same command_id was reused with a different
    request. A command_id identifies ONE intent; mint a new id for a new intent.
    This is a caller bug, not a race - never retry it."""


class DomainRejected(Exception):
    """400/422 with an ``error_code`` (``insufficient_funds``, ``same_account``,
    ``amount_out_of_range``): the server understood the command and refused it
    on business grounds. The rejection is persisted under the command_id, so a
    retry with the same request returns the same rejection - fix the request."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True)
class Posting:
    """One stream a command wrote. ``committed_version`` is None when the caller
    may not read that account (the server does not reveal another account's
    activity count through a transfer)."""

    account_id: str
    event_id: uuid.UUID
    committed_version: int | None


@dataclass(frozen=True)
class CommandResult:
    command_id: uuid.UUID
    outcome: str
    committed_version: int | None
    postings: tuple[Posting, ...] = ()


class CloudScaleClient:
    def __init__(self, base_url: str, token: str, *, max_attempts: int = 5) -> None:
        self._http = httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        self._max_attempts = max_attempts

    # -- commands -----------------------------------------------------------------

    def deposit(
        self, account_id: str, amount: int, expected_version: int
    ) -> CommandResult:
        return self._command(account_id, "deposit", amount, expected_version)

    def withdraw(
        self, account_id: str, amount: int, expected_version: int
    ) -> CommandResult:
        return self._command(account_id, "withdraw", amount, expected_version)

    def transfer(
        self,
        source_account_id: str,
        target_account_id: str,
        amount: int,
        expected_version: int,
    ) -> CommandResult:
        """Debit ``source`` and credit ``target`` atomically (ADR-0011).

        ``expected_version`` is the SOURCE stream's version - the account whose
        money is at risk. The target needs no version from you; a concurrent
        writer on the target is resolved server-side.
        """
        return self._post(
            f"/v1/accounts/{source_account_id}/transfers",
            {
                "target_account_id": target_account_id,
                "amount": amount,
                "expected_version": expected_version,
            },
        )

    def _command(
        self, account_id: str, kind: str, amount: int, expected_version: int
    ) -> CommandResult:
        return self._post(
            f"/v1/accounts/{account_id}/commands",
            {"type": kind, "amount": amount, "expected_version": expected_version},
        )

    def _post(self, path: str, fields: dict) -> CommandResult:
        command_id = uuid.uuid4()  # minted ONCE; reused across every retry below
        body = {"command_id": str(command_id), **fields}
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http.post(path, json=body)
            except httpx.TransportError:
                if attempt == self._max_attempts:
                    raise
                time.sleep(min(2**attempt * 0.1, 2.0))
                continue  # same command_id: safe

            if response.status_code in (200, 201):
                data = response.json()
                return CommandResult(
                    command_id,
                    data["outcome"],
                    data["committed_version"],
                    tuple(
                        Posting(
                            p["account_id"],
                            uuid.UUID(p["event_id"]),
                            p["committed_version"],
                        )
                        for p in data.get("postings", ())
                    ),
                )
            if response.status_code == 409:
                data = response.json()
                if data.get("error_code") == "version_conflict":
                    raise VersionConflict(int(data["current_version"]))
                # command_id_conflict: this id was already used for a DIFFERENT
                # request. That is a bug in the caller, never a race; do not retry.
                raise CommandIdConflict(str(command_id))
            if response.status_code in (400, 422):
                data = response.json()
                code = data.get("error_code") or data.get("detail")
                if isinstance(code, str):
                    raise DomainRejected(code)
            if response.status_code in (429, 503) and attempt < self._max_attempts:
                # Both carry Retry-After; both are safe to retry with the SAME id.
                time.sleep(float(response.headers.get("Retry-After", "1")))
                continue
            response.raise_for_status()
        raise RuntimeError("unreachable")

    # -- queries ------------------------------------------------------------------

    def balance(self, account_id: str) -> dict:
        for attempt in range(1, self._max_attempts + 1):
            response = self._http.get(f"/v1/accounts/{account_id}/balance")
            if response.status_code == 429 and attempt < self._max_attempts:
                time.sleep(float(response.headers.get("Retry-After", "1")))
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError("unreachable")

    def wait_for_version(
        self, account_id: str, version: int, timeout: float = 5.0
    ) -> dict:
        """Read-your-write: poll until the projection has applied ``version``."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                state = self.balance(account_id)
                if state["version"] >= version:
                    return state
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 404:  # 404 = not projected yet
                    raise
            if time.monotonic() >= deadline:
                raise TimeoutError(f"projection did not reach version {version}")
            time.sleep(0.05)

    def deposit_with_conflict_retry(
        self, account_id: str, amount: int
    ) -> CommandResult:
        """Typical pattern: read version, attempt, on 409 re-read and retry."""
        version = self._current_version(account_id)
        while True:
            try:
                return self.deposit(account_id, amount, version)
            except VersionConflict as conflict:
                version = conflict.current_version

    def _current_version(self, account_id: str) -> int:
        try:
            return int(self.balance(account_id)["version"])
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 404:
                return 0
            raise

    def close(self) -> None:
        self._http.close()


def main(argv: list[str]) -> int:
    base_url = argv[1] if len(argv) > 1 else "http://127.0.0.1:8000"
    token = os.environ.get("CLOUDSCALE_TOKEN")
    if not token:
        print("set CLOUDSCALE_TOKEN (e.g. $(make -s token))", file=sys.stderr)
        return 2
    account = f"example-{uuid.uuid4().hex[:6]}"
    client = CloudScaleClient(base_url, token)
    try:
        first = client.deposit(account, 100, expected_version=0)
        print("deposit:", first.outcome, "→ version", first.committed_version)
        stale = client.deposit  # simulate a concurrent writer using a stale version
        try:
            stale(account, 1, expected_version=0)
        except VersionConflict as conflict:
            print("stale write rejected: stream is at", conflict.current_version)
        second = client.deposit_with_conflict_retry(account, 50)
        print(
            "deposit (auto-retried on 409):",
            second.outcome,
            "→ version",
            second.committed_version,
        )
        state = client.wait_for_version(account, second.committed_version or 0)
        print("balance:", state["balance"], "at version", state["version"])
        if state["balance"] != 150:
            print("unexpected balance", state["balance"], file=sys.stderr)
            return 1

        # -- transfer: two streams, one transaction --------------------------------
        target = f"example-{uuid.uuid4().hex[:6]}"
        try:
            client.transfer(account, target, 500, expected_version=state["version"])
        except DomainRejected as rejected:
            # Persisted under its command_id: the same request would be
            # rejected the same way. Nothing was written to either stream.
            print("transfer of 500 rejected:", rejected.error_code)
        moved = client.transfer(account, target, 40, expected_version=state["version"])
        by_account = {p.account_id: p for p in moved.postings}
        print(
            "transfer:",
            moved.outcome,
            "→ source version",
            by_account[account].committed_version,
            "/ target version",
            by_account[target].committed_version,
        )
        source_state = client.wait_for_version(
            account, by_account[account].committed_version or 0
        )
        target_version = by_account[target].committed_version
        if target_version is None:
            # Redacted: this token may not read the target. You can still
            # observe your own side; the credit lands in the target's own reads.
            print("target posting redacted (no read access); source only")
            target_state = {"balance": None, "version": None}
        else:
            target_state = client.wait_for_version(target, target_version)
        print(
            "balances after transfer:",
            source_state["balance"],
            "+",
            target_state["balance"],
            "=",
            (source_state["balance"] or 0) + (target_state["balance"] or 0),
        )
        if source_state["balance"] != 110 or target_state["balance"] not in (40, None):
            print("unexpected balances after transfer", file=sys.stderr)
            return 1
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
