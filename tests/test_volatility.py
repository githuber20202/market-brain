from datetime import UTC, datetime

import pytest

from market_brain.domain.models import LiquidityProfile, MarketSnapshot
from market_brain.engines.volatility import (
    apply_volatility_context,
    target_atr_budget_reason,
    volatility_gate_reason,
    wilder_atr,
)


def test_wilder_atr_uses_true_range_and_wilder_smoothing():
    rows = [(101.0, 99.0, 100.0) for _ in range(20)]
    assert wilder_atr(rows, period=14) == pytest.approx(2.0)


def test_volatility_context_tracks_remaining_long_atr():
    profile = LiquidityProfile(
        symbol="TEST",
        adv20=10_000_000,
        close=100.0,
        as_of=datetime.now(UTC),
        atr14=2.0,
        atr14_pct=2.0,
    )
    snapshot = MarketSnapshot(
        symbol="TEST",
        last=101.0,
        prior_close=100.0,
    )

    apply_volatility_context(snapshot, profile)

    assert snapshot.atr14 == pytest.approx(2.0)
    assert snapshot.atr14_pct == pytest.approx(2.0 / 101.0 * 100.0)
    assert snapshot.remaining_atr == pytest.approx(1.0)
    assert snapshot.remaining_atr_pct == pytest.approx(100.0 / 101.0)


def test_remaining_atr_consumes_intraday_true_range():
    profile = LiquidityProfile(
        symbol="TEST",
        adv20=10_000_000,
        close=100.0,
        as_of=__import__("datetime").datetime.now(__import__("datetime").UTC),
        atr14=3.0,
        atr14_pct=3.0,
    )
    snapshot = MarketSnapshot(
        symbol="TEST",
        last=100.5,
        prior_close=100.0,
        high=101.0,
        low=98.0,
    )

    apply_volatility_context(snapshot, profile)

    assert snapshot.remaining_atr == pytest.approx(0.0)
    assert volatility_gate_reason(snapshot, min_atr_pct=1.0) == "ATR_EXHAUSTED"


def test_atr_gate_and_target_budget_fail_closed():
    snapshot = MarketSnapshot(
        symbol="TEST",
        last=101.0,
        prior_close=100.0,
        atr14=2.0,
        atr14_pct=2.0,
        remaining_atr=1.0,
        remaining_atr_pct=100.0 / 101.0,
    )

    assert volatility_gate_reason(snapshot, min_atr_pct=1.0) is None
    assert (
        target_atr_budget_reason(snapshot, target=102.0, multiplier=1.0)
        is None
    )
    assert (
        target_atr_budget_reason(snapshot, target=102.01, multiplier=1.0)
        == "TARGET_EXCEEDS_ATR_BUDGET"
    )

    snapshot.atr14_pct = 0.9
    assert volatility_gate_reason(snapshot, min_atr_pct=1.0) == "ATR_TOO_LOW"

    snapshot.atr14 = 2.0
    snapshot.atr14_pct = 2.0
    snapshot.remaining_atr = 0.0
    assert volatility_gate_reason(snapshot, min_atr_pct=1.0) == "ATR_EXHAUSTED"

    snapshot.remaining_atr = None
    assert (
        volatility_gate_reason(snapshot, min_atr_pct=1.0)
        == "ATR_REMAINING_MISSING"
    )

    snapshot.atr14 = None
    snapshot.atr14_pct = None
    assert volatility_gate_reason(snapshot, min_atr_pct=1.0) == "ATR_MISSING"


def test_remaining_atr_reasons_fail_closed():
    snapshot = MarketSnapshot(
        symbol="TEST",
        last=101.0,
        prior_close=100.0,
        atr14=2.0,
        atr14_pct=2.0,
        remaining_atr=None,
        remaining_atr_pct=None,
    )
    assert (
        volatility_gate_reason(snapshot, min_atr_pct=1.0)
        == "ATR_REMAINING_MISSING"
    )

    snapshot.remaining_atr = 0.0
    snapshot.remaining_atr_pct = 0.0
    assert volatility_gate_reason(snapshot, min_atr_pct=1.0) == "ATR_EXHAUSTED"
