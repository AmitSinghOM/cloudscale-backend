"""Feature: cloudscale-production-readiness, Property 3.

Property 3: Optimistic append is contiguous and single-winner.
For arbitrary appends competing at expected stream versions, an append succeeds
only at the current version, at most one competitor wins that version, rejected
appends leave persistence unchanged, and committed versions remain exactly
``1..n``.

Production persistence is intentionally deferred to Milestone 3. This property
therefore exercises the current ``CommandService``/``CommandUnitOfWork`` seam
with an explicit atomic in-memory model; it does not claim database concurrency
coverage.

Validates: Requirements 1.6, 2.2, 2.3, 2.4.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.application.command_service import CommandService
from cloudscale.application.ports import NormalizedCommand
from cloudscale.domain.account import AccountState, apply, decide
from cloudscale.domain.commands import Deposit
from cloudscale.domain.events import AccountEvent
from cloudscale.domain.results import CommandOutcome, CommandResult

_CREATED_AT = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
_ISSUER = "https://issuer.example"
_SUBJECT = "property-03"


@dataclass(frozen=True, slots=True)
class _PersistedAppend:
    """One event persisted by the optimistic-append model."""

    stream_version: int
    event: AccountEvent


@dataclass(frozen=True, slots=True)
class _PersistenceSnapshot:
    """Complete observable state used to prove conflicts do not mutate storage."""

    state: AccountState
    appends: tuple[_PersistedAppend, ...]


class _OptimisticAppendUnitOfWork:
    """Atomic model of the future persistence adapter's version-check boundary."""

    def __init__(self) -> None:
        self._states: dict[str, AccountState] = {}
        self._streams: dict[str, list[_PersistedAppend]] = {}
        self._next_event_id = 10_000

    def execute(self, request: NormalizedCommand) -> CommandResult:
        """Compare and append as one modeled atomic operation."""

        account_id = request.command.account_id
        state = self._states.get(account_id, AccountState())
        if request.command.expected_version != state.version:
            return CommandResult(
                command_id=request.command_id,
                request_hash=request.request_hash,
                outcome=CommandOutcome.VERSION_CONFLICT,
                account_id=account_id,
                expected_version=request.command.expected_version,
                current_version=state.version,
                committed_version=None,
                event_id=None,
                correlation_id=request.correlation_id,
                error_code="version_conflict",
                http_status=409,
                created_at=_CREATED_AT,
            )

        event = decide(state, request.command)
        next_state = apply(state, event)
        event_id = UUID(int=self._next_event_id)
        self._next_event_id += 1
        self._streams.setdefault(account_id, []).append(
            _PersistedAppend(stream_version=next_state.version, event=event)
        )
        self._states[account_id] = next_state
        return CommandResult(
            command_id=request.command_id,
            request_hash=request.request_hash,
            outcome=CommandOutcome.ACCEPTED,
            account_id=account_id,
            expected_version=request.command.expected_version,
            current_version=state.version,
            committed_version=next_state.version,
            event_id=event_id,
            correlation_id=request.correlation_id,
            error_code=None,
            http_status=201,
            created_at=_CREATED_AT,
        )

    def snapshot(self, account_id: str) -> _PersistenceSnapshot:
        """Return an immutable view of all modeled persistence for one stream."""

        return _PersistenceSnapshot(
            state=self._states.get(account_id, AccountState()),
            appends=tuple(self._streams.get(account_id, ())),
        )


_APPEND_ATTEMPTS = st.lists(
    st.tuples(
        st.integers(min_value=0, max_value=30),
        st.integers(min_value=1, max_value=10_000),
    ),
    min_size=1,
    max_size=60,
)


@settings(max_examples=200)
@given(
    account_id=st.text(min_size=1, max_size=32),
    attempts=_APPEND_ATTEMPTS,
)
def test_optimistic_appends_are_contiguous_and_single_winner(
    account_id: str,
    attempts: list[tuple[int, int]],
) -> None:
    """Property 3: only the current version can win and mutate its stream."""

    unit_of_work = _OptimisticAppendUnitOfWork()
    service = CommandService(unit_of_work)
    model_current_version = 0
    winner_count_by_current_version: dict[int, int] = {}

    for attempt_number, (expected_version, amount) in enumerate(attempts, start=1):
        before = unit_of_work.snapshot(account_id)
        should_succeed = expected_version == model_current_version

        result = service.execute(
            Deposit(
                account_id=account_id,
                amount=amount,
                expected_version=expected_version,
            ),
            command_id=UUID(int=attempt_number),
            correlation_id=UUID(int=1_000 + attempt_number),
            issuer=_ISSUER,
            subject=_SUBJECT,
        )
        after = unit_of_work.snapshot(account_id)

        if should_succeed:
            winner_count_by_current_version[model_current_version] = (
                winner_count_by_current_version.get(model_current_version, 0) + 1
            )
            assert winner_count_by_current_version[model_current_version] == 1
            model_current_version += 1
            assert result.outcome is CommandOutcome.ACCEPTED
            assert result.current_version == model_current_version - 1
            assert result.committed_version == model_current_version
            assert len(after.appends) == len(before.appends) + 1
        else:
            assert result.outcome is CommandOutcome.VERSION_CONFLICT
            assert result.current_version == model_current_version
            assert result.committed_version is None
            assert result.event_id is None
            assert after == before

        persisted_versions = [append.stream_version for append in after.appends]
        assert persisted_versions == list(range(1, len(persisted_versions) + 1))
        assert after.state.version == model_current_version

    final_snapshot = unit_of_work.snapshot(account_id)
    assert [append.stream_version for append in final_snapshot.appends] == list(
        range(1, model_current_version + 1)
    )
    assert all(
        winner_count <= 1 for winner_count in winner_count_by_current_version.values()
    )
