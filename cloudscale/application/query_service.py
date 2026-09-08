"""Projection-only account query orchestration."""

from cloudscale.domain.commands import _validate_account_id
from cloudscale.domain.results import BalanceView

from .ports import ProjectionReader


class QueryService:
    """Read account balances without consulting the command event stream."""

    def __init__(self, projection_reader: ProjectionReader) -> None:
        self._projection_reader = projection_reader

    def get_balance(self, account_id: str) -> BalanceView:
        """Return the projection or the specified unknown account at version zero."""

        _validate_account_id(account_id)
        projection = self._projection_reader.get_balance(account_id)
        if projection is None:
            return BalanceView(account_id=account_id, balance=0, version=0)
        if projection.account_id != account_id:
            raise ValueError("projection account_id must match the requested account")
        return projection


__all__ = ["QueryService"]
