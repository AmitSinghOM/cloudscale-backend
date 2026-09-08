"""Unit tests for projection-only account queries.

Validates: Requirement 5.4.
"""

from cloudscale.application.query_service import QueryService
from cloudscale.domain.results import BalanceView


class FakeProjectionReader:
    def __init__(self, values: dict[str, BalanceView]) -> None:
        self.values = values
        self.calls: list[str] = []

    def get_balance(self, account_id: str) -> BalanceView | None:
        self.calls.append(account_id)
        return self.values.get(account_id)


def test_unknown_projection_falls_back_to_exact_account_at_zero_zero() -> None:
    reader = FakeProjectionReader({})

    result = QueryService(reader).get_balance(" Unknown Account ")

    assert result == BalanceView(" Unknown Account ", balance=0, version=0)
    assert reader.calls == [" Unknown Account "]


def test_existing_projection_is_returned_unchanged() -> None:
    existing = BalanceView("account-1", balance=45, version=3)
    reader = FakeProjectionReader({"account-1": existing})

    result = QueryService(reader).get_balance("account-1")

    assert result is existing
