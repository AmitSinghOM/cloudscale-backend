"""docs/openapi.json is the integration contract; it may not drift from the app."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from export_openapi import OUTPUT, generate, render  # noqa: E402


def test_committed_openapi_matches_the_application() -> None:
    assert OUTPUT.exists(), "run: python scripts/export_openapi.py"
    assert OUTPUT.read_text() == render(generate()), (
        "docs/openapi.json is stale; run: python scripts/export_openapi.py "
        "and review the diff as a contract change (CONTRIBUTING §3)"
    )


def test_openapi_covers_the_public_surface() -> None:
    document = json.loads(OUTPUT.read_text())
    paths = set(document["paths"])
    for route in (
        "/v1/health",
        "/v1/ready",
        "/v1/accounts",
        "/v1/accounts/{account_id}/commands",
        "/v1/accounts/{account_id}/balance",
    ):
        assert route in paths, route
    assert document["info"]["title"] == "cloudscale-backend"
