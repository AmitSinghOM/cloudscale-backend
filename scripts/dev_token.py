"""Mint a LOCAL development bearer token for the SQLite quickstart.

Usage::

    python scripts/dev_token.py            # prints the token
    python scripts/dev_token.py --curl     # prints a ready-to-run curl deposit

Reads the same ``CLOUDSCALE_JWT_SECRET`` the dev server uses (``make dev``
sets one). The token has the admin scope so the quickstart can touch any
account. This is for a laptop only: production uses OIDC/JWKS or a secret
that never appears in a shell history (RUNBOOK D4).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt

DEV_SECRET_ENV = "CLOUDSCALE_JWT_SECRET"
MAX_MINUTES = (
    60  # the server caps lifetime at CLOUDSCALE_JWT_MAX_LIFETIME_SECONDS (3600)
)


def mint(
    secret: str, *, subject: str, minutes: int, issuer: str, audience: str | None
) -> str:
    now = datetime.now(UTC)
    claims: dict[str, object] = {
        "iss": issuer,
        "sub": subject,
        "scope": "accounts:admin",
        "jti": f"dev-{uuid4().hex}",
        "iat": now,
        "exp": now + timedelta(minutes=minutes),
    }
    if audience:
        claims["aud"] = audience
    return jwt.encode(claims, secret, algorithm="HS256")


def _minutes(value: str) -> int:
    minutes = int(value)
    if not 1 <= minutes <= MAX_MINUTES:
        raise argparse.ArgumentTypeError(
            f"must be 1..{MAX_MINUTES}: the server rejects tokens whose lifetime "
            "exceeds CLOUDSCALE_JWT_MAX_LIFETIME_SECONDS (default 3600) with a fixed 401"
        )
    return minutes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--subject", default="dev-user")
    parser.add_argument(
        "--minutes",
        type=_minutes,
        default=55,
        help=f"token lifetime, 1..{MAX_MINUTES} (server caps at 1 h)",
    )
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--curl",
        action="store_true",
        help="print a curl deposit instead of the bare token",
    )
    args = parser.parse_args(argv)

    secret = os.environ.get(DEV_SECRET_ENV)
    if not secret:
        print(
            f"{DEV_SECRET_ENV} is not set. Start the dev stack with `make dev` "
            "(it exports one) or export the same value the server uses.",
            file=sys.stderr,
        )
        return 2
    token = mint(
        secret,
        subject=args.subject,
        minutes=args.minutes,
        issuer=os.environ.get("CLOUDSCALE_JWT_ISSUER", "cloudscale"),
        audience=os.environ.get("CLOUDSCALE_JWT_AUDIENCE") or None,
    )
    if not args.curl:
        print(token)
        return 0
    print(
        f"curl -s -X POST http://127.0.0.1:{args.port}/v1/accounts/demo/commands \\\n"
        f"  -H 'Authorization: Bearer {token}' \\\n"
        "  -H 'Content-Type: application/json' \\\n"
        f'  -d \'{{"command_id":"{uuid4()}","type":"deposit","amount":100,"expected_version":0}}\''
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
