"""Export the HTTP API's OpenAPI document to docs/openapi.json.

The committed document is the integration contract: clients are generated
from it, and ``tests/architecture/test_openapi_contract.py`` fails the build
when the running app's schema differs from the committed file — so a route
or field change is a visible, reviewed diff rather than a surprise.

    python scripts/export_openapi.py            # rewrite docs/openapi.json
    python scripts/export_openapi.py --check    # exit 1 if it would change

Builds the app over a throwaway SQLite stack with a fixed secret; the schema
does not depend on storage tier or secret value.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

OUTPUT = REPOSITORY_ROOT / "docs" / "openapi.json"


def generate() -> dict:
    with tempfile.TemporaryDirectory(prefix="cloudscale-openapi-") as workdir:
        env = {
            "CLOUDSCALE_STORAGE": "sqlite",
            "CLOUDSCALE_LOG_DB": os.path.join(workdir, "log.db"),
            "CLOUDSCALE_PROJECTION_DB": os.path.join(workdir, "projection.db"),
            "CLOUDSCALE_JWT_SECRET": "openapi-export-only-0123456789abcdef-0123456789abcdef",
        }
        previous = {key: os.environ.get(key) for key in env}
        os.environ.update(env)
        try:
            from cloudscale.entrypoints.http.main import build_app

            app = build_app()
            try:
                return app.openapi()
            finally:
                # create_app registers closeables on lifespan shutdown; there is
                # no lifespan here, so close the SQLite handles explicitly.
                for closeable in getattr(app.state, "closeables", ()):
                    closeable.close()
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def render(document: dict) -> str:
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--check", action="store_true", help="fail if the file would change"
    )
    args = parser.parse_args(argv)
    text = render(generate())
    if args.check:
        current = OUTPUT.read_text() if OUTPUT.exists() else ""
        if current != text:
            print(
                f"{OUTPUT.relative_to(REPOSITORY_ROOT)} is out of date; "
                "run: python scripts/export_openapi.py",
                file=sys.stderr,
            )
            return 1
        print("openapi.json is current")
        return 0
    OUTPUT.write_text(text)
    print(f"wrote {OUTPUT.relative_to(REPOSITORY_ROOT)} ({len(text)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
