from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import MarketSnapshot, StrategyLane, WalletState
from market_brain.engines.activation import activate_plan
from market_brain.engines.features import compute_features
from market_brain.engines.plan import PlanBuildError, build_trade_plan
from market_brain.engines.position import evaluate_position, validate_stop_change
from market_brain.engines.quality import classify_quality, lane_risk_multiplier
from market_brain.engines.ranking import score_features
from market_brain.engines.wallet import size_from_wallet


def snapshot(**overrides):
    base = {
        "symbol": "TEST",
        "last": 101.0,
        "prior_close": 98.0,
        "bid": 100.99,
        "ask": 101.01,
        "volume": 2_000_000,
        "avg_volume": 1_000_000,
        "vwap": 100.0,
        "open_price": 99.0,
        "opening_range_high": 100.8,
        "retest_low": 100.0,
        "benchmark_return_pct": 0.5,
        "catalyst_verified": True,
        "catalyst_strength": 0.9,
        "data_age_seconds": 1.0,
        "source_id": "AUTH",
        "authoritative": True,
    }
    base.update(overrides)
    return MarketSnapshot(**base)


def make_plan(s=None, quality_score=85, lane=StrategyLane.CORE_MOMENTUM):
    s = s or snapshot()
    features = compute_features(s)
    score = score_features(features, structure_score=15, rr_score=10)
    quality = classify_quality(s.symbol, quality_score, datetime.now(UTC))
    return build_trade_plan(snapshot=s, score=score, quality=quality, lane=lane)


def test_scoring_weights_sum_to_100():
    score = score_features(compute_features(snapshot()), structure_score=15, rr_score=10)
    assert score.total <= 100
    assert score.discovery_total <= 100


def test_quality_modifies_risk_not_market_score():
    a = classify_quality("A", 90, datetime.now(UTC))
    c = classify_quality("C", 55, datetime.now(UTC))
    assert a.risk_multiplier == 1.0
    assert c.risk_multiplier == 0.5


def test_event_lane_allows_medium_quality_at_reduced_risk():
    profile = classify_quality("E", 45, datetime.now(UTC))
    assert lane_risk_multiplier(StrategyLane.EVENT_MOMENTUM, profile, 0.9) == 0.25


def test_low_quality_core_is_blocked():
    with pytest.raises(PlanBuildError, match="QUALITY_OR_LANE_BLOCKED"):
        make_plan(quality_score=40)


def test_plan_is_deterministic_and_has_15_20_targets():
    plan = make_plan()
    risk = plan.entry_trigger - plan.stop
    assert plan.tp1 == pytest.approx(plan.entry_trigger + risk * 1.5)
    assert plan.tp2 == pytest.approx(plan.entry_trigger + risk * 2.0)


def test_authoritative_market_data_required_for_buy_now():
    plan = make_plan()
    wallet = WalletState(10_000, 10_000)
    decision = activate_plan(plan, snapshot(authoritative=False), wallet, retest_valid=True, above_vwap=True)
    assert decision.state == "ARMED"
    assert "AUTHORITATIVE_MARKET_FEED_REQUIRED" in decision.reasons


def test_no_chase_blocks_entry():
    plan = make_plan()
    wallet = WalletState(10_000, 10_000)
    decision = activate_plan(
        plan,
        snapshot(last=plan.entry_zone_high + 0.1),
        wallet,
        retest_valid=True,
        above_vwap=True,
    )
    assert decision.state == "NO_TRADE"


def test_risk_wallet_sizes_position_without_account_read():
    plan = make_plan()
    result = size_from_wallet(WalletState(10_000, 10_000), plan)
    assert result.allowed
    assert result.quantity > 0


def test_daily_risk_limit_blocks_new_trade():
    plan = make_plan()
    wallet = WalletState(10_000, 10_000, daily_realized_loss=100)
    result = size_from_wallet(wallet, plan)
    assert not result.allowed
    assert "DAILY_RISK_LIMIT_REACHED" in result.reasons


def test_buy_now_requires_wallet_and_market_gates_only():
    plan = make_plan()
    wallet = WalletState(10_000, 10_000)
    decision = activate_plan(plan, snapshot(last=plan.entry_trigger), wallet, retest_valid=True, above_vwap=True)
    assert decision.state == "BUY_NOW"
    assert decision.quantity > 0


def test_expired_plan_cannot_activate():
    plan = make_plan()
    plan.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    decision = activate_plan(plan, snapshot(last=plan.entry_trigger), WalletState(10_000, 10_000), retest_valid=True, above_vwap=True)
    assert decision.state == "EXPIRED"


def test_stop_widening_is_forbidden():
    from market_brain.domain.models import PositionState
    position = PositionState("p","plan","TEST",10,10,101,100,102.5,103,datetime.now(UTC),datetime.now(UTC)+timedelta(minutes=30))
    with pytest.raises(ValueError, match="STOP_WIDENING_FORBIDDEN"):
        validate_stop_change(position, 99.5)


def test_unknown_position_fails_closed():
    assert evaluate_position(None, last=100) == "UNKNOWN_POSITION"

