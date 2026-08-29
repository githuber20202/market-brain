from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from market_brain.domain.models import LiquidityProfile, StrategyLane, TradePlan
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import PostgresEventStore


async def seed(dsn: str, *, created_at: datetime) -> None:
    store = PostgresEventStore(dsn)
    timestamp = created_at.astimezone(UTC)
    plan = TradePlan(
        symbol="SPY",
        lane=StrategyLane.CORE_MOMENTUM,
        entry_trigger=100.0,
        entry_zone_high=100.75,
        stop=99.0,
        tp1=101.5,
        tp2=102.0,
        max_spread_pct=0.25,
        max_slippage_pct=0.30,
        created_at=timestamp,
        expires_at=timestamp + timedelta(minutes=30),
        quality_risk_multiplier=0.5,
        plan_id="00000000-0000-4000-8000-000000000024",
        source_ids=["YAHOO_DELAYED"],
    )
    profile = LiquidityProfile(
        symbol="SPY",
        adv20=10_000_000.0,
        close=98.0,
        as_of=timestamp - timedelta(days=1),
        refreshed_at=timestamp,
    )
    async with store.transaction():
        await store.save_plan(plan)
        await store.save_liquidity_profile(profile)
        await store.append(
            LedgerEvent(
                "PLAN_ISSUED",
                plan.plan_id,
                {"plan": asdict(plan), "fixture": "BATCH_PLAN_WATCH"},
                occurred_at=timestamp,
            )
        )
    await store.close()
    print(f"BATCH_SMOKE_PLAN={plan.plan_id}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--created-at", required=True)
    args = parser.parse_args()
    created_at = datetime.fromisoformat(args.created_at)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    asyncio.run(seed(args.dsn, created_at=created_at))


if __name__ == "__main__":
    main()
