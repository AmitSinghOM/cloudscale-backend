"""Contract checks for the Milestone 1 regression/evidence gate.

Validates: Requirements 1-4, 17.8, 19.8-19.10.
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.verify_milestone import (
    EXCLUDED_SCOPE,
    HYPOTHESIS_SEED,
    LEGACY_TEST_COUNT,
    LEGACY_TEST_PATHS,
    MILESTONE_1_TEST_GROUPS,
    OUTSTANDING_REQUIRED_GATES,
    milestone_1_test_paths,
)

REPOSITORY_ROOT = Path(__file__).parents[2]


def test_milestone_1_preserves_the_exact_legacy_suite() -> None:
    assert LEGACY_TEST_COUNT == 17
    assert LEGACY_TEST_PATHS == (
        "tests/test_cqrs.py",
        "tests/test_durable_cqrs.py",
    )
    assert all((REPOSITORY_ROOT / path).is_file() for path in LEGACY_TEST_PATHS)


def test_milestone_1_composes_every_required_local_check_category() -> None:
    assert set(MILESTONE_1_TEST_GROUPS) == {
        "architecture",
        "unit_domain",
        "normalization_and_idempotency",
        "properties",
        "compatibility_adapters",
        "concurrency_and_process_harness",
        "milestone_contract",
    }
    paths = milestone_1_test_paths()
    assert len(paths) == len(set(paths))
    assert all((REPOSITORY_ROOT / path).exists() for path in paths)
    assert not set(paths).intersection(LEGACY_TEST_PATHS)


def test_milestone_1_includes_all_approved_property_tests() -> None:
    expected_properties = {
        f"tests/properties/test_property_{property_number}.py"
        for property_number in (
            "01_valid_commands",
            "02_invalid_commands",
            "03_optimistic_append",
            "04_concurrent_no_overdraft",
            "05_envelope_stability",
            "06_command_idempotency",
            "07_correlation_identity",
            "08_schema_versions",
            "21_sqlite_compatibility",
        )
    }

    assert set(MILESTONE_1_TEST_GROUPS["properties"]) == expected_properties
    assert HYPOTHESIS_SEED == 41609


def test_milestone_1_scope_is_local_and_claim_safe() -> None:
    serialized_scope = json.dumps(
        {
            "release_designation": "milestone_1_only",
            "completed_milestones": [1],
            "outstanding_required_gates": OUTSTANDING_REQUIRED_GATES,
            "excluded_scope": EXCLUDED_SCOPE,
        },
        sort_keys=True,
    )

    assert "Production_Ready" not in serialized_scope
    assert "http_or_network_behavior" in EXCLUDED_SCOPE
    assert OUTSTANDING_REQUIRED_GATES
