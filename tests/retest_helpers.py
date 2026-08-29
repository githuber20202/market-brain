from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")


def _bar(day, minute, *, high, low, close, vwap):
    stamp = datetime.combine(day, datetime.min.time(), EASTERN).replace(hour=9, minute=30) + timedelta(minutes=minute)
    return {
        "t": stamp.astimezone(UTC).isoformat(),
        "o": close,
        "h": high,
        "l": low,
        "c": close,
        "v": 10_000,
        "vw": vwap,
    }


async def seed_server_retest(service, plan):
    now = datetime.now(UTC)
    day = now.astimezone(EASTERN).date()
    trigger = float(plan.entry_trigger)
    span = max(1.2, trigger * 0.01)
    rows = [
        _bar(day, 0, high=trigger - 0.60 * span, low=trigger - span, close=trigger - 0.70 * span, vwap=trigger - 0.70 * span),
        _bar(day, 1, high=trigger - 0.45 * span, low=trigger - 0.85 * span, close=trigger - 0.55 * span, vwap=trigger - 0.60 * span),
        _bar(day, 2, high=trigger - 0.30 * span, low=trigger - 0.70 * span, close=trigger - 0.40 * span, vwap=trigger - 0.50 * span),
        _bar(day, 3, high=trigger - 0.15 * span, low=trigger - 0.55 * span, close=trigger - 0.25 * span, vwap=trigger - 0.40 * span),
        _bar(day, 4, high=trigger, low=trigger - 0.40 * span, close=trigger - 0.10 * span, vwap=trigger - 0.30 * span),
        _bar(day, 5, high=trigger + 0.25 * span, low=trigger - 0.04 * span, close=trigger + 0.18 * span, vwap=trigger - 0.10 * span),
        _bar(day, 6, high=trigger + 0.18 * span, low=trigger - min(trigger * 0.001, 0.10 * span), close=trigger + 0.08 * span, vwap=trigger),
    ]
    for row in rows:
        await service.record_intraday_bar(plan.symbol, row, now=now)
    valid, reasons, state = await service.server_retest(plan, now=now)
    assert valid is True, (reasons, state)


async def activate_with_server_retest(service, plan_id):
    plan = await service.store.get_plan(plan_id)
    assert plan is not None
    await seed_server_retest(service, plan)
    return await service.activate(plan_id)

