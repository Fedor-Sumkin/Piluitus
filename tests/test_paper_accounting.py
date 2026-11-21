from __future__ import annotations

import math

import pytest

from portfolio.paper_accounting import calculate_open_pnl, parse_finite_float


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (12, 12.0),
        (12.5, 12.5),
        ("-3.25", -3.25),
        (True, None),
        (None, None),
        ({}, None),
        ("not-a-number", None),
        (math.inf, None),
        (math.nan, None),
    ],
)
def test_parse_finite_float_rejects_invalid_values(value, expected):
    assert parse_finite_float(value) == expected


def test_calculate_open_pnl_sums_only_open_positions():
    positions = [
        {"status": "OPEN", "pnl": 125.5},
        {"status": "open", "pnl": "-25.25"},
        {"status": "CLOSED", "pnl": 900.0},
        {"status": "OPEN", "pnl": "invalid"},
        {"status": "OPEN", "pnl": True},
        None,
    ]

    assert calculate_open_pnl(positions) == pytest.approx(100.25)


def test_calculate_open_pnl_defaults_missing_status_to_open():
    assert calculate_open_pnl([{"pnl": 17.0}]) == 17.0


def test_calculate_open_pnl_is_disabled_outside_paper_mode():
    assert calculate_open_pnl([{"status": "OPEN", "pnl": 100.0}], paper_mode=False) == 0.0
