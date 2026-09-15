#!/usr/bin/env python3
"""Inspect and redrive the dead-letter queue of a projection database.

Usage:
    python scripts/dlq.py DB_PATH list [--json]
    python scripts/dlq.py DB_PATH show EVENT_ID
    python scripts/dlq.py DB_PATH redrive (EVENT_ID ... | --all)

Exit codes: 0 success; 1 at least one redrive failed again; 2 event not found.
Redrive is exactly-once: re-apply and letter removal commit together, and a
letter that fails again stays parked with an incremented attempt count.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from cloudscale.adapters.sqlite_compat.dead_letter_store import (  # noqa: E402
    DeadLetteringProjectionStore,
    RedriveOutcome,
)

EXIT_OK = 0
EXIT_FAILED_AGAIN = 1
EXIT_NOT_FOUND = 2


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db_path", help="Path to the projection SQLite database")
    parser.add_argument(
        "--consumer",
        default="balances",
        help="Consumer name the projection was created with (default: balances)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List parked events")
    list_parser.add_argument(
        "--json", action="store_true", help="Emit the full entries as JSON"
    )

    show_parser = subparsers.add_parser("show", help="Show one parked event")
    show_parser.add_argument("event_id")

    redrive_parser = subparsers.add_parser(
        "redrive", help="Re-apply parked events and remove their letters"
    )
    redrive_parser.add_argument("event_ids", nargs="*", metavar="EVENT_ID")
    redrive_parser.add_argument(
        "--all", action="store_true", help="Redrive every parked event, oldest first"
    )
    return parser.parse_args(arguments)


def _cmd_list(store: DeadLetteringProjectionStore, as_json: bool) -> int:
    entries = store.dead_letters()
    if as_json:
        print(json.dumps(entries, indent=2, sort_keys=True))
        return EXIT_OK
    if not entries:
        print("dead-letter queue is empty")
        return EXIT_OK
    print(f"{'EVENT_ID':<40} {'LOG_ID':>6} {'ATTEMPTS':>8}  ERROR")
    for entry in entries:
        error = f"{entry['error_type']}: {entry['error_message']}"
        print(
            f"{entry['event_id']:<40} {entry['log_id']:>6} "
            f"{entry['attempts']:>8}  {error}"
        )
    print(f"total: {len(entries)}")
    return EXIT_OK


def _cmd_show(store: DeadLetteringProjectionStore, event_id: str) -> int:
    for entry in store.dead_letters():
        if entry["event_id"] == event_id:
            print(json.dumps(entry, indent=2, sort_keys=True))
            return EXIT_OK
    print(f"not found: {event_id}", file=sys.stderr)
    return EXIT_NOT_FOUND


def _cmd_redrive(
    store: DeadLetteringProjectionStore,
    event_ids: list[str],
    redrive_all: bool,
) -> int:
    if redrive_all == bool(event_ids):
        print("redrive requires either EVENT_ID arguments or --all", file=sys.stderr)
        return EXIT_NOT_FOUND

    outcomes = (
        store.redrive_all()
        if redrive_all
        else {event_id: store.redrive(event_id) for event_id in event_ids}
    )
    exit_code = EXIT_OK
    for event_id, outcome in outcomes.items():
        print(f"{outcome.value:<12} {event_id}")
        if outcome is RedriveOutcome.FAILED_AGAIN:
            exit_code = max(exit_code, EXIT_FAILED_AGAIN)
        elif outcome is RedriveOutcome.NOT_FOUND:
            exit_code = max(exit_code, EXIT_NOT_FOUND)
    remaining = store.dead_letter_count()
    print(
        f"redriven: {sum(o is RedriveOutcome.APPLIED for o in outcomes.values())}, "
        f"still parked: {remaining}"
    )
    return exit_code


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(arguments)
    if not Path(args.db_path).exists():
        print(f"database not found: {args.db_path}", file=sys.stderr)
        return EXIT_NOT_FOUND
    store = DeadLetteringProjectionStore(path=args.db_path, consumer=args.consumer)
    try:
        if args.command == "list":
            return _cmd_list(store, args.json)
        if args.command == "show":
            return _cmd_show(store, args.event_id)
        return _cmd_redrive(store, args.event_ids, args.all)
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
