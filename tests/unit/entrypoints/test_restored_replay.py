"""Restored retries and authorization over HTTP (P1-C, recovery draft section 2).

Each test writes through the real HTTP app into a fresh source store, keeps
the exact request body, issuer, subject, claims and original response, then
dumps and restores with the restore drill's own tier and replays against an
app over the restored copy only. Requests are never rebuilt from events: a
rejected command has no event, and caller identity is part of the request
hash. "Unchanged" means equal ``authoritative_snapshot`` fingerprints of every
system-of-record table plus equal folded account states.

PostgreSQL variants need ``CLOUDSCALE_TEST_PG`` and a version-matched
``CLOUDSCALE_PG_BIN``; CI asserts nothing skipped.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from cloudscale.entrypoints.http.context import CommandExecutor
from tests.support.concurrency import run_concurrently
from tests.support.restored_replay import (
    Identity,
    Sent,
    Tier,
    command,
    postgres_skip_reason,
    postings,
)

DENIED = {"detail": "principal is not authorized for this account"}
ALICE = Identity("alice")
BOB = Identity("bob")

_PG_SKIP = postgres_skip_reason()
_POSTGRES = pytest.param(
    "postgresql",
    marks=pytest.mark.skipif(_PG_SKIP is not None, reason=_PG_SKIP or ""),
)


@pytest.fixture(params=["sqlite", _POSTGRES])
def tier(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Tier]:
    with Tier(request.param, tmp_path).active() as active:
        yield active


@pytest.fixture(params=[_POSTGRES])
def postgres_tier(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Tier]:
    with Tier(request.param, tmp_path).active() as active:
        yield active


def _assert_original(response: Any, sent: Sent) -> None:
    assert response.status_code == sent.status, response.text
    assert response.json() == sent.response


def test_restored_accepted_command_replays_without_append(tier: Tier) -> None:
    accounts = ("acct-a", "acct-b", "acct-payee")
    source = tier.fresh_source()
    with tier.store(source) as store:
        store.registry.register("acct-a", "alice")
        store.registry.register("acct-b", "alice")
        deposit = store.send("/v1/accounts/acct-a/commands", command(100, 0), ALICE)
        store.send("/v1/accounts/acct-b/commands", command(50, 0), ALICE)
        posting = store.send(
            "/v1/accounts/acct-a/postings",
            postings(
                [
                    ("acct-a", 30, "debit"),
                    ("acct-b", 20, "debit"),
                    ("acct-payee", 50, "credit"),
                ],
                version=1,
            ),
            ALICE,
        )
        assert (deposit.status, posting.status) == (201, 201), posting.response
        assert posting.response["postings"][2]["committed_version"] is None
    baseline = tier.snapshot(source)

    restored = tier.dump_and_restore(source)
    assert tier.snapshot(restored) == baseline
    with tier.store(restored) as store:
        before = store.states(accounts)
        assert {a: s.balance for a, s in before.items()} == {
            "acct-a": 70,
            "acct-b": 30,
            "acct-payee": 50,
        }
        for sent in (deposit, posting, deposit):
            _assert_original(store.replay(sent), sent)
        assert store.states(accounts) == before
    assert tier.snapshot(restored) == baseline


def test_restored_rejected_command_remains_rejected_after_funding(tier: Tier) -> None:
    source = tier.fresh_source()
    with tier.store(source) as store:
        store.registry.register("acct-a", "alice")
        store.send("/v1/accounts/acct-a/commands", command(100, 0), ALICE)
        rejected = store.send(
            "/v1/accounts/acct-a/commands", command(500, 1, "withdraw"), ALICE
        )
        assert rejected.status == 422
        assert rejected.response["error_code"] == "insufficient_funds"
        funded = store.send("/v1/accounts/acct-a/commands", command(1000, 1), ALICE)
        assert funded.status == 201
    baseline = tier.snapshot(source)

    restored = tier.dump_and_restore(source)
    with tier.store(restored) as store:
        before = store.uow.fold_stream("acct-a")
        assert (before.balance, before.version) == (1100, 2)
        for _ in range(2):
            _assert_original(store.replay(rejected), rejected)
        assert store.uow.fold_stream("acct-a") == before
        assert tier.snapshot(restored) == baseline

        # Contrast: the same body under a new id is decided afresh against the
        # funded stream and gets version_conflict, so the 422 above can only
        # have come from the restored stored result.
        fresh = store.send(
            rejected.path, {**rejected.body, "command_id": str(uuid4())}, ALICE
        )
        assert fresh.status == 409
        assert fresh.response["error_code"] == "version_conflict"
        assert store.uow.fold_stream("acct-a") == before


@pytest.mark.parametrize("original_outcome", ["accepted", "rejected"])
def test_restored_command_id_conflict_keeps_original_result(
    tier: Tier, original_outcome: str
) -> None:
    source = tier.fresh_source()
    with tier.store(source) as store:
        store.registry.register("acct-a", "alice")
        original = store.send("/v1/accounts/acct-a/commands", command(100, 0), ALICE)
        if original_outcome == "rejected":
            original = store.send(
                "/v1/accounts/acct-a/commands", command(900, 1, "withdraw"), ALICE
            )
        assert original.status == (201 if original_outcome == "accepted" else 422)
    baseline = tier.snapshot(source)

    restored = tier.dump_and_restore(source)
    with tier.store(restored) as store:
        before = store.uow.fold_stream("acct-a")
        stale = {**original.body, "amount": original.body["amount"] + 1}
        # Same id, an otherwise valid deposit at the current version.
        fresh = {**stale, "type": "deposit", "expected_version": before.version}
        for changed in (stale, fresh):
            response = store.client.post(
                original.path, json=changed, headers=ALICE.headers()
            )
            assert response.status_code == 409, response.text
            body = response.json()
            assert (body["outcome"], body["error_code"]) == (
                "command_id_conflict",
                "command_id_conflict",
            )
            assert body["command_id"] == original.body["command_id"]
        _assert_original(store.replay(original), original)
        assert store.uow.fold_stream("acct-a") == before
    assert tier.snapshot(restored) == baseline


@pytest.mark.parametrize("account", ["acct-empty", "acct-funded", "acct-owned"])
def test_self_registration_cannot_claim_existing_financial_stream(
    tier: Tier, account: str
) -> None:
    source = tier.fresh_source()
    with tier.store(source) as store:
        store.registry.register("acct-owned", "alice")
        funded = store.send(
            "/v1/accounts/acct-funded/commands",
            command(100, 0),
            Identity("issuer", accounts=("acct-funded",)),
        )
        assert funded.status == 201
        before = tier.snapshot(source)
        for identity in (BOB, Identity("ops", accounts=(account,))):
            response = store.client.post(
                "/v1/accounts",
                json={"account_id": account},
                headers=identity.headers(),
            )
            assert response.status_code == 403
            assert response.json() == DENIED
            assert tier.snapshot(source) == before
        assert store.registry.owner_of(account) == (
            "alice" if account == "acct-owned" else None
        )
        assert (
            store.client.post("/v1/accounts", json={"account_id": account}).status_code
            == 401
        )


def test_restored_registry_owner_is_authorized_and_outsider_denied(
    tier: Tier,
) -> None:
    source = tier.fresh_source()
    with tier.store(source) as store:
        store.registry.register("acct-owned", "alice")
        deposit = store.send("/v1/accounts/acct-owned/commands", command(100, 0), ALICE)
        assert deposit.status == 201
    tier.drill.rebuild(source)  # the dump carries a current read model
    baseline = tier.snapshot(source)

    restored = tier.dump_and_restore(source)
    with tier.store(restored) as store:
        # Ownership came back with the restore; nothing re-registers it.
        assert store.registry.owner_of("acct-owned") == "alice"
        before = store.uow.fold_stream("acct-owned")
        outsider = [
            store.client.get("/v1/accounts/acct-owned/balance", headers=BOB.headers()),
            store.client.post(
                "/v1/accounts/acct-owned/commands",
                json=command(1, 1, "withdraw"),
                headers=BOB.headers(),
            ),
            store.replay(deposit, BOB),  # nor read alice's stored result
        ]
        for response in outsider:
            assert response.status_code == 403
            assert response.json() == DENIED
        assert store.uow.fold_stream("acct-owned") == before
        assert tier.snapshot(restored) == baseline

        balance = store.client.get(
            "/v1/accounts/acct-owned/balance", headers=ALICE.headers()
        )
        assert balance.status_code == 200, balance.text
        assert balance.json()["balance"] == 100
        _assert_original(store.replay(deposit), deposit)
        follow_up = store.send(
            "/v1/accounts/acct-owned/commands", command(1, 1, "withdraw"), ALICE
        )
        assert follow_up.status == 201, follow_up.response
        assert store.uow.fold_stream("acct-owned").balance == 99


@pytest.mark.parametrize("anchor_grant", ["registry", "claims"])
def test_restored_posting_replay_requires_every_debit(
    tier: Tier, anchor_grant: str
) -> None:
    accounts = ("acct-anchor", "acct-secondary", "acct-payee")
    if anchor_grant == "registry":
        # alice owns the anchor by registration and is named on the secondary.
        payer = Identity("alice", accounts=("acct-secondary",))
        anchor_only = Identity("alice")
    else:
        payer = Identity("treasury", accounts=("acct-anchor", "acct-secondary"))
        anchor_only = Identity("treasury", accounts=("acct-anchor",))
    source = tier.fresh_source()
    with tier.store(source) as store:
        store.registry.register("acct-anchor", "alice")
        for account, amount in (("acct-anchor", 1), ("acct-secondary", 100)):
            seeded = store.send(
                f"/v1/accounts/{account}/commands", command(amount, 0), payer
            )
            assert seeded.status == 201, seeded.response
        posting = store.send(
            "/v1/accounts/acct-anchor/postings",
            postings(
                [
                    ("acct-anchor", 1, "debit"),
                    ("acct-secondary", 100, "debit"),
                    ("acct-payee", 101, "credit"),
                ],
                version=1,
            ),
            payer,
        )
        assert posting.status == 201, posting.response
    baseline = tier.snapshot(source)

    restored = tier.dump_and_restore(source)
    with tier.store(restored) as store:
        before = store.states(accounts)

        def unexpected(*_args: object, **_kwargs: object) -> None:
            pytest.fail("anchor-only replay reached the executor or stored result")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(CommandExecutor, "execute", unexpected)
            patch.setattr(tier.uow_class, "execute", unexpected)
            patch.setattr(tier.decision_module, "execute_command_decision", unexpected)
            denied = store.replay(posting, anchor_only)
        assert denied.status_code == 403
        assert denied.json() == DENIED
        assert store.states(accounts) == before
        assert tier.snapshot(restored) == baseline

        _assert_original(store.replay(posting), posting)
        assert store.states(accounts) == before
    assert tier.snapshot(restored) == baseline


def test_concurrent_restored_retries_apply_no_additional_posting(
    postgres_tier: Tier,
) -> None:
    tier, workers = postgres_tier, 8
    accounts = ("acct-a", "acct-b", "acct-payee")
    source = tier.fresh_source()
    with tier.store(source) as store:
        for account in ("acct-a", "acct-b"):
            store.registry.register(account, "alice")
            store.send(f"/v1/accounts/{account}/commands", command(100, 0), ALICE)
        posting = store.send(
            "/v1/accounts/acct-a/postings",
            postings(
                [
                    ("acct-a", 60, "debit"),
                    ("acct-b", 40, "debit"),
                    ("acct-payee", 100, "credit"),
                ],
                version=1,
            ),
            ALICE,
        )
        assert posting.status == 201, posting.response
    baseline = tier.snapshot(source)

    restored = tier.dump_and_restore(source)
    with tier.store(restored, pool_max=workers) as store:
        before = store.states(accounts)
        results = run_concurrently(
            {f"retry-{i}": (lambda: store.replay(posting)) for i in range(workers)},
            timeout=30.0,
        )
        assert len(results) == workers
        for result in results:
            _assert_original(result.value, posting)
        assert len(store.uow.legs_of(posting.command_id)) == 3
        assert store.states(accounts) == before
    assert tier.snapshot(restored) == baseline
