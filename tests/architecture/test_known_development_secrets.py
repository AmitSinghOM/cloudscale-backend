"""Every fixed JWT secret shipped in this public repo must be a *known* one.

The production-mode startup warning (``HttpSettings.production_warnings``) can
only fire for secrets it knows about. Review 4: the harness secret in
``scripts/http_gate_run.py`` was committed but unknown, so a process started
with it in migrations mode would have been silent.
"""

from __future__ import annotations

import re
from pathlib import Path

from cloudscale.entrypoints.http.settings import KNOWN_DEVELOPMENT_SECRETS

ROOT = Path(__file__).resolve().parents[2]
SHIPPED = ("Makefile", "compose.yaml", "scripts", ".devcontainer", "cloudscale")
SECRET_LITERAL = re.compile(r"[A-Za-z][A-Za-z0-9-]*-secret-[A-Za-z0-9-]{16,}")


def _shipped_files() -> list[Path]:
    out: list[Path] = []
    for entry in SHIPPED:
        path = ROOT / entry
        if path.is_file():
            out.append(path)
        elif path.is_dir():
            out.extend(
                p
                for p in path.rglob("*")
                if p.is_file() and p.suffix != ".pyc" and "__pycache__" not in p.parts
            )
    return out


def test_every_shipped_secret_literal_is_a_known_development_secret() -> None:
    found: dict[str, list[str]] = {}
    for path in _shipped_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in SECRET_LITERAL.findall(text):
            found.setdefault(match, []).append(str(path.relative_to(ROOT)))
    assert found, "expected at least the Makefile/compose development secret"
    unknown = {
        secret: where
        for secret, where in found.items()
        if secret not in KNOWN_DEVELOPMENT_SECRETS
    }
    assert not unknown, (
        "fixed secret literals shipped but not in KNOWN_DEVELOPMENT_SECRETS "
        f"(the production warning cannot fire for them): {unknown}"
    )


def test_known_development_secrets_are_all_still_shipped() -> None:
    """The set must not accumulate stale entries either."""
    texts = "\n".join(
        p.read_text(encoding="utf-8", errors="ignore")
        for p in _shipped_files()
        if p.name != "settings.py"
    )
    stale = {s for s in KNOWN_DEVELOPMENT_SECRETS if s not in texts}
    assert not stale, f"listed as development secrets but no longer shipped: {stale}"
