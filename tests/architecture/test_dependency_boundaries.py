"""Executable checks for CloudScale's inward-only dependency direction."""

import ast
import importlib
from collections.abc import Iterator
from importlib.util import resolve_name
from pathlib import Path
from typing import AbstractSet

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLOUDSCALE_ROOT = PROJECT_ROOT / "cloudscale"

FRAMEWORK_IMPORTS = frozenset(
    {
        "alembic",
        "confluent_kafka",
        "cryptography",
        "fastapi",
        "httpx",
        "jwt",
        "opentelemetry",
        "prometheus_client",
        "psycopg",
        "pydantic",
        "pydantic_settings",
        "sqlalchemy",
        "starlette",
        "uvicorn",
    }
)
DOMAIN_FORBIDDEN_IMPORTS = FRAMEWORK_IMPORTS | {
    "cloudscale.adapters",
    "cloudscale.application",
    "cloudscale.config",
    "cloudscale.processes",
    "cqrs",
}
APPLICATION_FORBIDDEN_IMPORTS = {
    "cloudscale.adapters",
    "cloudscale.config",
    "cloudscale.processes",
    "cqrs",
}
EXPECTED_CQRS_EXPORTS = (
    "EventStore",
    "SqliteEventStore",
    "ConcurrencyError",
    "CommandHandler",
    "CommandError",
    "BalanceProjection",
    "IdempotentProjectionStore",
    "run_consumer",
)


def _module_and_package(source_file: Path) -> tuple[str, str]:
    module_parts = list(source_file.relative_to(PROJECT_ROOT).with_suffix("").parts)
    if module_parts[-1] == "__init__":
        module_parts.pop()
        module = ".".join(module_parts)
        return module, module

    module = ".".join(module_parts)
    return module, module.rpartition(".")[0]


def _imported_modules(source_file: Path) -> Iterator[tuple[int, str]]:
    """Yield line numbers and absolute module candidates imported by a file."""

    tree = ast.parse(source_file.read_text(encoding="utf-8"), filename=str(source_file))
    _, package = _module_and_package(source_file)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield node.lineno, alias.name
            continue

        if not isinstance(node, ast.ImportFrom):
            continue

        if node.level:
            relative_name = "." * node.level + (node.module or "")
            imported_base = resolve_name(relative_name, package)
        else:
            imported_base = node.module or ""

        if imported_base:
            yield node.lineno, imported_base
        for alias in node.names:
            if alias.name != "*":
                imported_name = ".".join(
                    part for part in (imported_base, alias.name) if part
                )
                yield node.lineno, imported_name


def _is_forbidden(imported_module: str, forbidden_prefixes: AbstractSet[str]) -> bool:
    return any(
        imported_module == prefix or imported_module.startswith(f"{prefix}.")
        for prefix in forbidden_prefixes
    )


def _find_violations(
    package_directory: Path, forbidden_prefixes: AbstractSet[str]
) -> list[str]:
    violations: set[tuple[str, int, str]] = set()
    for source_file in package_directory.rglob("*.py"):
        for line_number, imported_module in _imported_modules(source_file):
            if _is_forbidden(imported_module, forbidden_prefixes):
                violations.add(
                    (
                        str(source_file.relative_to(PROJECT_ROOT)),
                        line_number,
                        imported_module,
                    )
                )

    return [
        f"{path}:{line_number} imports {imported_module}"
        for path, line_number, imported_module in sorted(violations)
    ]


@pytest.mark.parametrize(
    "package_name",
    [
        "cloudscale",
        "cloudscale.domain",
        "cloudscale.application",
        "cloudscale.application.ports",
        "cloudscale.adapters",
        "cloudscale.config",
        "cloudscale.processes",
    ],
)
def test_production_packages_are_importable(package_name: str) -> None:
    assert importlib.import_module(package_name).__name__ == package_name


def test_domain_has_no_outward_or_framework_imports() -> None:
    violations = _find_violations(CLOUDSCALE_ROOT / "domain", DOMAIN_FORBIDDEN_IMPORTS)
    assert not violations, "Domain dependency violations:\n" + "\n".join(violations)


def test_application_has_no_concrete_adapter_imports() -> None:
    violations = _find_violations(
        CLOUDSCALE_ROOT / "application", APPLICATION_FORBIDDEN_IMPORTS
    )
    assert not violations, "Application dependency violations:\n" + "\n".join(
        violations
    )


def test_cqrs_compatibility_exports_remain_stable() -> None:
    cqrs = importlib.import_module("cqrs")

    assert tuple(cqrs.__all__) == EXPECTED_CQRS_EXPORTS
    assert all(hasattr(cqrs, symbol) for symbol in EXPECTED_CQRS_EXPORTS)
