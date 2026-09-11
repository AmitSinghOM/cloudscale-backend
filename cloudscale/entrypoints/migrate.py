"""Apply PostgreSQL schema migrations.

    CLOUDSCALE_PG_DSN=postgresql://... python -m cloudscale.entrypoints.migrate [upgrade|current|downgrade]

Production sequence: run this, then start the server and consumer with
``CLOUDSCALE_PG_SCHEMA=migrations`` so they verify the revision and never
create tables themselves. Exit code 0 on success.
"""

from __future__ import annotations

import sys
from pathlib import Path

from alembic import command
from alembic.config import Config

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def alembic_config() -> Config:
    config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(REPOSITORY_ROOT / "migrations"))
    return config


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    action = args[0] if args else "upgrade"
    config = alembic_config()
    if action == "upgrade":
        command.upgrade(config, args[1] if len(args) > 1 else "head")
    elif action == "downgrade":
        command.downgrade(config, args[1] if len(args) > 1 else "-1")
    elif action == "current":
        command.current(config, verbose=True)
    else:
        print(
            f"unknown action {action!r}; use upgrade|downgrade|current", file=sys.stderr
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
