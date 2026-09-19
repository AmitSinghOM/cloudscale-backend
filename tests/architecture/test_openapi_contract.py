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
    paths = document["paths"]
    for route in (
        "/v1/health",
        "/v1/ready",
        "/v1/accounts",
        "/v1/accounts/{account_id}/commands",
        "/v1/accounts/{account_id}/balance",
    ):
        assert route in paths, route
    assert document["info"]["title"] == "cloudscale-backend"


def test_openapi_declares_bearer_auth_on_every_account_route() -> None:
    """A generated client must send Authorization; probes must stay open."""
    document = json.loads(OUTPUT.read_text())
    schemes = document["components"]["securitySchemes"]
    assert schemes["bearerAuth"] == {"type": "http", "scheme": "bearer"}
    for path, ops in document["paths"].items():
        for method, op in ops.items():
            if path in ("/v1/health", "/v1/ready"):
                assert "security" not in op, f"{method} {path} must be unauthenticated"
            else:
                assert op.get("security") == [{"bearerAuth": []}], f"{method} {path}"


def test_openapi_documents_the_real_status_codes() -> None:
    """The codes docs/API_ERRORS.md promises must be in the contract.

    A replayed accepted command returns the stored 201 byte-for-byte, so no
    route promises a 200 for commands (the previous contract did, wrongly).
    """
    document = json.loads(OUTPUT.read_text())
    expected = {"201", "400", "409", "422", "401", "403", "429", "503"}
    for route in ("commands", "transfers"):
        op = document["paths"][f"/v1/accounts/{{account_id}}/{route}"]["post"]
        assert expected <= set(op["responses"]), route
        assert "200" not in op["responses"], f"{route}: replay is 201, not 200"
    balance = document["paths"]["/v1/accounts/{account_id}/balance"]["get"]
    assert {"200", "404", "401", "403"} <= set(balance["responses"])
