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
        "opening_range_low": 99.6,
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


@pytest.mark.parametrize(
    ("symbol", "entry", "stop", "opening_low"),
    [
        ("LHX", 263.2468, 263.14, 262.0),
        ("ONDS", 8.1718, 8.15, 8.10),
        ("MS", 215.3546, 215.21, 214.0),
        ("NOW", 140.6593, 140.0, 139.0),
    ],
)
def test_live_replay_micro_risk_examples_are_rejected(
    symbol: str,
    entry: float,
    stop: float,
    opening_low: float,
):
    with pytest.raises(PlanBuildError, match="RISK_TOO_SMALL"):
        make_plan(
            snapshot(
                symbol=symbol,
                opening_range_high=entry,
                opening_range_low=opening_low,
                retest_low=stop,
            )
        )


def test_synthetic_eight_tenths_percent_risk_passes():
    plan = make_plan(
        snapshot(
            symbol="SYNTHETIC",
            opening_range_high=100.0,
            opening_range_low=99.0,
            retest_low=99.2,
        )
    )
    assert plan.entry_trigger == 100.0
    assert plan.stop == 99.2


def test_narrow_opening_range_is_rejected():
    with pytest.raises(PlanBuildError, match="OPENING_RANGE_TOO_NARROW"):
        make_plan(
            snapshot(
                opening_range_high=100.0,
                opening_range_low=99.8,
                retest_low=99.2,
            )
        )


def test_rounded_target_must_remain_above_entry_zone():
    with pytest.raises(PlanBuildError, match="TARGET_BELOW_ENTRY"):
        build_trade_plan(
            snapshot=snapshot(
                opening_range_high=100.0,
                opening_range_low=99.0,
                retest_low=99.99999,
            ),
            score=score_features(
                compute_features(snapshot()), structure_score=15, rr_score=10
            ),
            quality=classify_quality("TEST", 90, datetime.now(UTC)),
            lane=StrategyLane.CORE_MOMENTUM,
            min_risk_pct=0.0,
        )


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


def test_keyless_activation_does_not_require_bbo_after_liquidity_gate_passes():
    plan = make_plan()
    market = snapshot(
        last=plan.entry_trigger,
        bid=None,
        ask=None,
        source_id="YAHOO_DELAYED",
        authoritative=True,
    )
    decision = activate_plan(
        plan,
        market,
        WalletState(10_000, 10_000),
        retest_valid=True,
        above_vwap=True,
    )
    assert decision.state == "BUY_NOW"
    assert "BBO_MISSING" not in decision.reasons


def test_keyless_activation_rejects_wide_bbo_when_present():
    plan = make_plan()
    market = snapshot(
        last=plan.entry_trigger,
        bid=plan.entry_trigger - 1.0,
        ask=plan.entry_trigger + 1.0,
        source_id="CBOE_DELAYED",
        authoritative=True,
    )
    decision = activate_plan(
        plan,
        market,
        WalletState(10_000, 10_000),
        retest_valid=True,
        above_vwap=True,
    )
    assert decision.state == "ARMED"
    assert "SPREAD_TOO_WIDE" in decision.reasons


def test_iex_activation_still_requires_bbo():
    plan = make_plan()
    market = snapshot(
        last=plan.entry_trigger,
        bid=None,
        ask=None,
        source_id="ALPACA_IEX",
        authoritative=True,
    )
    decision = activate_plan(
        plan,
        market,
        WalletState(10_000, 10_000),
        retest_valid=True,
        above_vwap=True,
    )
    assert decision.state == "ARMED"
    assert "BBO_MISSING" in decision.reasons


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
