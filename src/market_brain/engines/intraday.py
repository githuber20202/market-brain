from __future__ import annotations

import json
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

from market_brain.domain.models import IntradayStructure, IntradayStructureState
from market_brain.settings import Settings, settings

EASTERN = ZoneInfo("America/New_York")


def session_date_for(value: datetime) -> str:
    return _aware(value).astimezone(EASTERN).date().isoformat()


def structure_key(symbol: str, session_date: str) -> str:
    return f"intraday_structure:{session_date}:{symbol.upper()}"


def new_intraday_structure(symbol: str, now: datetime) -> IntradayStructure:
    return IntradayStructure(symbol=symbol.upper(), session_date=session_date_for(now))


def structure_from_dict(data: dict) -> IntradayStructure:
    return IntradayStructure(
        symbol=str(data["symbol"]).upper(),
        session_date=str(data["session_date"]),
        state=IntradayStructureState(data.get("state", "BUILDING_OR")),
        opening_range_high=_float(data.get("opening_range_high")),
        opening_range_low=_float(data.get("opening_range_low")),
        opening_bars_seen=int(data.get("opening_bars_seen", 0)),
        running_vwap=_float(data.get("running_vwap")),
        cumulative_volume=float(data.get("cumulative_volume", 0.0)),
        cumulative_vwap_notional=float(data.get("cumulative_vwap_notional", 0.0)),
        last_bar_at=_dt(data.get("last_bar_at")),
        last_close=_float(data.get("last_close")),
        breakout_at=_dt(data.get("breakout_at")),
        retest_at=_dt(data.get("retest_at")),
        reasons=list(data.get("reasons", [])),
        updated_at=_dt(data.get("updated_at")) or datetime.now(UTC),
    )


def compute_structure(
    symbol: str,
    session_date: str,
    bars: list[dict],
    cfg: Settings = settings,
    *,
    now: datetime | None = None,
    sip_confirmed_through: datetime | None = None,
) -> IntradayStructure:
    session_day = datetime.fromisoformat(session_date).date()
    session_start_local = datetime.combine(session_day, time(9, 30), EASTERN)
    session_start = session_start_local.astimezone(UTC)
    session_end = datetime.combine(session_day, time(16, 0), EASTERN).astimezone(UTC)
    merged = [
        bar
        for bar in _merged_bars(bars)
        if (stamp := _bar_stamp(bar)) is not None
        and session_date_for(stamp) == session_date
        and session_start <= stamp < session_end
    ]
    by_minute = {
        _bar_stamp(bar).replace(second=0, microsecond=0): bar
        for bar in merged
        if _bar_stamp(bar) is not None
    }
    structure = IntradayStructure(symbol=symbol.upper(), session_date=session_date)
    structure.updated_at = _aware(now or (max(by_minute) if by_minute else session_start))

    opening_stamps = [session_start + timedelta(minutes=index) for index in range(cfg.intraday_opening_range_minutes)]
    missing = [stamp for stamp in opening_stamps if stamp not in by_minute]
    opening = [by_minute[stamp] for stamp in opening_stamps if stamp in by_minute]
    structure.opening_bars_seen = len(opening)
    if opening:
        highs = [_required_float(bar, "h", "high") for bar in opening]
        lows = [_required_float(bar, "l", "low") for bar in opening]
        structure.opening_range_high = max(highs)
        structure.opening_range_low = min(lows)

    if missing:
        confirmed = _aware(sip_confirmed_through) if sip_confirmed_through is not None else None
        confirmed_missing = [stamp for stamp in missing if confirmed is not None and stamp + timedelta(minutes=1) <= confirmed]
        if confirmed_missing:
            structure.state = IntradayStructureState.INVALID
            structure.reasons = ["OPENING_RANGE_CONFIRMED_EMPTY"]
        else:
            structure.state = IntradayStructureState.BUILDING_OR
            structure.reasons = ["OPENING_RANGE_WAITING_FOR_SIP"]
        _apply_vwap(structure, [by_minute[stamp] for stamp in sorted(by_minute)])
        if by_minute:
            structure.last_bar_at = max(by_minute)
            structure.last_close = _required_float(by_minute[structure.last_bar_at], "c", "close")
        return structure

    structure.state = IntradayStructureState.ARMED
    structure.reasons = ["OPENING_RANGE_ESTABLISHED"]
    ordered = [by_minute[stamp] for stamp in sorted(by_minute)]
    previous_close: float | None = None
    orh = structure.opening_range_high
    orl = structure.opening_range_low
    assert orh is not None and orl is not None
    r_value = orh - orl
    if r_value <= 0:
        structure.state = IntradayStructureState.INVALID
        structure.reasons = ["OPENING_RANGE_INVALID"]
        return structure
    tolerance = cfg.retest_touch_tolerance_pct / 100.0

    for bar in ordered:
        stamp = _bar_stamp(bar)
        assert stamp is not None
        _required_float(bar, "h", "high")
        low = _required_float(bar, "l", "low")
        close = _required_float(bar, "c", "close")
        _apply_vwap(structure, [bar])
        structure.last_bar_at = stamp
        structure.last_close = close
        if stamp < session_start + timedelta(minutes=cfg.intraday_opening_range_minutes):
            previous_close = close
            continue
        invalidation_reason = _structure_invalidation_reason(
            structure.state,
            low=low,
            orh=orh,
            orl=orl,
            buffer_r=cfg.retest_invalidation_buffer_r,
        )
        if invalidation_reason is not None:
            structure.state = IntradayStructureState.INVALID
            structure.reasons = [invalidation_reason]
            return structure
        if structure.state == IntradayStructureState.RETEST_VALID:
            previous_close = close
            continue
        if structure.state == IntradayStructureState.BREAKOUT_SEEN:
            assert structure.breakout_at is not None
            if stamp - structure.breakout_at > timedelta(minutes=cfg.retest_window_minutes):
                structure.state = IntradayStructureState.ARMED
                structure.breakout_at = None
                structure.reasons = ["RETEST_WINDOW_EXPIRED"]
            elif close <= orh:
                structure.state = IntradayStructureState.ARMED
                structure.breakout_at = None
                structure.reasons = ["RETEST_CLOSE_BELOW_ORH"]
            else:
                near_orh = abs(low - orh) / orh <= tolerance
                vwap = structure.running_vwap
                near_vwap = vwap is not None and vwap > 0 and abs(low - vwap) / vwap <= tolerance
                if near_orh or near_vwap:
                    structure.state = IntradayStructureState.RETEST_VALID
                    structure.retest_at = stamp
                    structure.reasons = ["SERVER_RETEST_VALID"]
                else:
                    structure.reasons = ["RETEST_WINDOW_OPEN"]
            previous_close = close
            continue
        if previous_close is not None and previous_close <= orh and close > orh:
            structure.state = IntradayStructureState.BREAKOUT_SEEN
            structure.breakout_at = stamp
            structure.reasons = ["ORH_BREAKOUT_CONFIRMED"]
        previous_close = close
    return structure


def _apply_vwap(structure: IntradayStructure, bars: list[dict]) -> None:
    for bar in bars:
        volume = _float(bar.get("v", bar.get("volume")))
        bar_vwap = _float(bar.get("vw", bar.get("vwap")))
        if volume is not None and volume > 0 and bar_vwap is not None and bar_vwap > 0:
            structure.cumulative_volume += volume
            structure.cumulative_vwap_notional += bar_vwap * volume
            structure.running_vwap = structure.cumulative_vwap_notional / structure.cumulative_volume


def _required_float(bar: dict, short: str, long: str) -> float:
    value = _float(bar.get(short, bar.get(long)))
    if value is None:
        raise ValueError("INTRADAY_BAR_INCOMPLETE")
    return value


def update_intraday_structure(
    structure: IntradayStructure,
    bar: dict,
    cfg: Settings,
    *,
    now: datetime | None = None,
) -> IntradayStructure:
    raw_stamp = bar.get("t") or bar.get("timestamp")
    high = _float(bar.get("h", bar.get("high")))
    low = _float(bar.get("l", bar.get("low")))
    close = _float(bar.get("c", bar.get("close")))
    if raw_stamp is None or high is None or low is None or close is None:
        raise ValueError("INTRADAY_BAR_INCOMPLETE")
    stamp = _dt(raw_stamp)
    if stamp is None:
        raise ValueError("INTRADAY_BAR_TIMESTAMP_INVALID")
    if high < low or close <= 0:
        raise ValueError("INTRADAY_BAR_INVALID")
    if session_date_for(stamp) != structure.session_date:
        raise ValueError("INTRADAY_BAR_SESSION_MISMATCH")
    if structure.last_bar_at is not None and stamp <= structure.last_bar_at:
        return structure

    local = stamp.astimezone(EASTERN)
    session_start = datetime.combine(local.date(), time(9, 30), EASTERN)
    session_end = datetime.combine(local.date(), time(16, 0), EASTERN)
    if local < session_start or local >= session_end:
        return structure
    minute_index = int((local - session_start).total_seconds() // 60)

    volume = _float(bar.get("v", bar.get("volume")))
    bar_vwap = _float(bar.get("vw", bar.get("vwap")))
    if volume is not None and volume > 0 and bar_vwap is not None and bar_vwap > 0:
        structure.cumulative_volume += volume
        structure.cumulative_vwap_notional += bar_vwap * volume
        structure.running_vwap = structure.cumulative_vwap_notional / structure.cumulative_volume

    previous_close = structure.last_close
    structure.last_bar_at = stamp
    structure.last_close = close
    structure.updated_at = _aware(now or datetime.now(UTC))

    opening_minutes = cfg.intraday_opening_range_minutes
    if minute_index < opening_minutes:
        if minute_index != structure.opening_bars_seen:
            structure.state = IntradayStructureState.INVALID
            structure.reasons = ["OPENING_RANGE_INCOMPLETE"]
            return structure
        structure.opening_range_high = (
            high if structure.opening_range_high is None else max(structure.opening_range_high, high)
        )
        structure.opening_range_low = (
            low if structure.opening_range_low is None else min(structure.opening_range_low, low)
        )
        structure.opening_bars_seen += 1
        if structure.opening_bars_seen == opening_minutes:
            structure.state = IntradayStructureState.ARMED
            structure.reasons = ["OPENING_RANGE_ESTABLISHED"]
        return structure

    if structure.state == IntradayStructureState.INVALID:
        return structure
    if structure.opening_bars_seen < opening_minutes:
        structure.state = IntradayStructureState.INVALID
        structure.reasons = ["OPENING_RANGE_INCOMPLETE"]
        return structure
    if structure.opening_range_high is None or structure.opening_range_low is None:
        structure.state = IntradayStructureState.INVALID
        structure.reasons = ["OPENING_RANGE_MISSING"]
        return structure

    orh = structure.opening_range_high
    r_value = orh - structure.opening_range_low
    if r_value <= 0:
        structure.state = IntradayStructureState.INVALID
        structure.reasons = ["OPENING_RANGE_INVALID"]
        return structure
    invalidation_reason = _structure_invalidation_reason(
        structure.state,
        low=low,
        orh=orh,
        orl=structure.opening_range_low,
        buffer_r=cfg.retest_invalidation_buffer_r,
    )
    if invalidation_reason is not None:
        structure.state = IntradayStructureState.INVALID
        structure.reasons = [invalidation_reason]
        return structure
    if structure.state == IntradayStructureState.RETEST_VALID:
        return structure

    if structure.state == IntradayStructureState.BREAKOUT_SEEN:
        if structure.breakout_at is None:
            structure.state = IntradayStructureState.ARMED
            structure.reasons = ["RETEST_BREAKOUT_TIMESTAMP_MISSING"]
            return structure
        elapsed = stamp - structure.breakout_at
        if elapsed > timedelta(minutes=cfg.retest_window_minutes):
            structure.state = IntradayStructureState.ARMED
            structure.breakout_at = None
            structure.reasons = ["RETEST_WINDOW_EXPIRED"]
            return structure
        tolerance = cfg.retest_touch_tolerance_pct / 100.0
        near_orh = abs(low - orh) / orh <= tolerance
        vwap = structure.running_vwap
        near_vwap = vwap is not None and vwap > 0 and abs(low - vwap) / vwap <= tolerance
        if (near_orh or near_vwap) and close > orh:
            structure.state = IntradayStructureState.RETEST_VALID
            structure.retest_at = stamp
            structure.reasons = ["SERVER_RETEST_VALID"]
        elif close <= orh:
            structure.state = IntradayStructureState.ARMED
            structure.breakout_at = None
            structure.reasons = ["RETEST_CLOSE_BELOW_ORH"]
        else:
            structure.reasons = ["RETEST_WINDOW_OPEN"]
        return structure

    if (
        structure.state == IntradayStructureState.ARMED
        and previous_close is not None
        and previous_close <= orh
        and close > orh
    ):
        structure.state = IntradayStructureState.BREAKOUT_SEEN
        structure.breakout_at = stamp
        structure.reasons = ["ORH_BREAKOUT_CONFIRMED"]
    return structure


def _structure_invalidation_reason(
    state: IntradayStructureState,
    *,
    low: float,
    orh: float,
    orl: float,
    buffer_r: float,
) -> str | None:
    r_value = orh - orl
    if state == IntradayStructureState.ARMED:
        range_breakdown = orl - buffer_r * r_value
        if low < range_breakdown:
            return "RANGE_BREAKDOWN"
        return None
    if state in {
        IntradayStructureState.BREAKOUT_SEEN,
        IntradayStructureState.RETEST_VALID,
    }:
        failed_breakout = orh - buffer_r * r_value
        if low < failed_breakout:
            return "RETEST_INVALIDATED_BELOW_ORH_BUFFER"
    return None


def _merged_bars(bars: list[dict]) -> list[dict]:
    selected: dict[datetime, tuple[int, str, dict]] = {}
    for raw in bars:
        if not isinstance(raw, dict):
            continue
        stamp = _bar_stamp(raw)
        if stamp is None:
            continue
        minute = stamp.replace(second=0, microsecond=0)
        source = str(raw.get("source") or raw.get("_source") or "UNKNOWN").upper()
        source_rank = 2 if "SIP" in source else 1 if "IEX" in source else 0
        canonical = json.dumps(raw, sort_keys=True, default=str, separators=(",", ":"))
        candidate = (source_rank, canonical, dict(raw))
        existing = selected.get(minute)
        if existing is None or candidate[:2] > existing[:2]:
            selected[minute] = candidate
    return [selected[minute][2] for minute in sorted(selected)]


def _bar_stamp(bar: dict) -> datetime | None:
    return _dt(bar.get("t") or bar.get("timestamp"))


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _dt(value) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return _aware(value) if isinstance(value, datetime) else None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return _aware(parsed)


def _float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
