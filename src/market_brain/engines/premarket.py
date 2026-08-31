from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from market_brain.domain.models import MarketSnapshot

TRUSTED_PUBLISHERS = (
    "REUTERS",
    "BUSINESS WIRE",
    "PR NEWSWIRE",
    "GLOBENEWSWIRE",
    "ASSOCIATED PRESS",
    "BLOOMBERG",
    "THE WALL STREET JOURNAL",
    "CNBC",
    "BARRON",
    "MARKETWATCH",
)

NEGATIVE_TERMS = (
    "cuts guidance",
    "lowers guidance",
    "misses estimates",
    "misses expectations",
    "downgrade",
    "investigation",
    "lawsuit",
    "recall",
    "bankruptcy",
    "share offering",
    "secondary offering",
    "tariff",
    "warning",
)

POSITIVE_CATEGORIES = (
    (
        "EARNINGS_GUIDANCE",
        20.0,
        (
            "beats estimates",
            "beats expectations",
            "raises guidance",
            "raised guidance",
            "record revenue",
            "profit jumps",
            "earnings beat",
        ),
    ),
    (
        "REGULATORY_APPROVAL",
        19.0,
        ("fda approval", "wins approval", "approved by", "regulatory approval"),
    ),
    (
        "CONTRACT_DEAL",
        18.0,
        ("wins contract", "awarded contract", "major contract", "new contract"),
    ),
    (
        "M_AND_A",
        17.0,
        ("to acquire", "acquisition", "takeover", "buyout", "strategic investment"),
    ),
    (
        "ANALYST_ACTION",
        15.0,
        ("upgraded to", "price target raised", "raises price target", "initiated at buy"),
    ),
    (
        "PRODUCT_PARTNERSHIP",
        12.0,
        ("launches", "unveils", "partnership", "partners with", "collaboration"),
    ),
)


@dataclass(frozen=True, slots=True)
class CatalystAssessment:
    verified: bool
    negative: bool
    score: float
    category: str
    headline: str | None
    publisher: str | None
    published_at: str | None
    url: str | None
    source_id: str | None
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def assess_catalyst(news: list[dict], *, as_of: datetime) -> CatalystAssessment:
    timestamp = _aware(as_of)
    best: CatalystAssessment | None = None
    for item in news:
        if not isinstance(item, dict) or item.get("direct_symbol_match") is not True:
            continue
        title = str(item.get("title") or "").strip()
        publisher = str(item.get("publisher") or "UNKNOWN").strip()
        if not title:
            continue
        published_at = _parse_time(item.get("published_at"))
        if published_at is None or published_at > timestamp:
            continue
        trusted = any(value in publisher.upper() for value in TRUSTED_PUBLISHERS)
        lowered = title.casefold()
        if any(term in lowered for term in NEGATIVE_TERMS):
            current = CatalystAssessment(
                verified=trusted,
                negative=True,
                score=0.0,
                category="NEGATIVE_EVENT",
                headline=title,
                publisher=publisher,
                published_at=published_at.isoformat(),
                url=_optional_text(item.get("url")),
                source_id=_optional_text(item.get("source_id")),
                reason_codes=(
                    "DIRECT_NEWS_MATCH",
                    "TRUSTED_PUBLISHER" if trusted else "UNVERIFIED_PUBLISHER",
                    "NEGATIVE_CATALYST",
                ),
            )
            if trusted:
                return current
            best = best or current
            continue
        category = "OTHER"
        score = 0.0
        for candidate_category, candidate_score, terms in POSITIVE_CATEGORIES:
            if any(term in lowered for term in terms):
                category = candidate_category
                score = candidate_score
                break
        verified = bool(trusted and category != "OTHER")
        if category == "OTHER":
            score = 4.0 if trusted else 0.0
        elif not trusted:
            score = min(score, 8.0)
        current = CatalystAssessment(
            verified=verified,
            negative=False,
            score=score,
            category=category,
            headline=title,
            publisher=publisher,
            published_at=published_at.isoformat(),
            url=_optional_text(item.get("url")),
            source_id=_optional_text(item.get("source_id")),
            reason_codes=(
                "DIRECT_NEWS_MATCH",
                "TRUSTED_PUBLISHER" if trusted else "UNVERIFIED_PUBLISHER",
                "CATALYST_CLASSIFIED" if category != "OTHER" else "CATALYST_UNCLASSIFIED",
            ),
        )
        if best is None or (current.verified, current.score) > (best.verified, best.score):
            best = current
    return best or CatalystAssessment(
        verified=False,
        negative=False,
        score=0.0,
        category="NONE",
        headline=None,
        publisher=None,
        published_at=None,
        url=None,
        source_id=None,
        reason_codes=("NO_DIRECT_CATALYST",),
    )


def score_premarket_candidate(
    snapshot: MarketSnapshot,
    *,
    adv20: float | None,
    benchmark_return_pct: float | None,
    sector_return_pct: float | None,
    catalyst: CatalystAssessment,
    minimum_price: float,
    minimum_adv: float,
    finalist_score: float,
) -> dict[str, Any]:
    gap = _pct(snapshot.last, snapshot.prior_close)
    return_15m = _number(snapshot.metadata.get("premarket_return_15m_percent"))
    lower_highs = _integer(snapshot.metadata.get("premarket_lower_highs_count"))
    premarket_high = _number(snapshot.metadata.get("premarket_high")) or snapshot.high
    distance_from_high = None
    if premarket_high is not None and premarket_high > 0:
        distance_from_high = max(0.0, (premarket_high - snapshot.last) / premarket_high * 100.0)
    premarket_volume_fraction = None
    if snapshot.volume is not None and adv20 not in (None, 0):
        premarket_volume_fraction = snapshot.volume / adv20
    anchor = sector_return_pct if sector_return_pct is not None else benchmark_return_pct
    relative_strength = gap - anchor if gap is not None and anchor is not None else None
    distance_from_vwap = _pct(snapshot.last, snapshot.vwap)

    deterioration_signals: list[str] = []
    if distance_from_high is not None and distance_from_high >= 1.0:
        deterioration_signals.append("DISTANCE_FROM_PREMARKET_HIGH")
    if return_15m is not None and return_15m <= -0.5:
        deterioration_signals.append("NEGATIVE_15M_RETURN")
    if lower_highs is not None and lower_highs >= 2:
        deterioration_signals.append("LOWER_HIGHS")
    deterioration_confirmed = len(deterioration_signals) >= 2
    deterioration_severe = bool(
        deterioration_confirmed
        and distance_from_high is not None
        and distance_from_high >= 2.0
        and (
            (return_15m is not None and return_15m <= -1.0)
            or (lower_highs is not None and lower_highs >= 2)
        )
    )

    momentum = 0.0
    if gap is not None:
        momentum += _clamp(gap / 3.0, 0.0, 1.0) * 15.0
    if return_15m is not None:
        momentum += _clamp(return_15m / 1.0, 0.0, 1.0) * 5.0
    volume = (
        _clamp(premarket_volume_fraction / 0.10, 0.0, 1.0) * 20.0
        if premarket_volume_fraction is not None
        else 0.0
    )
    relative = (
        _clamp(relative_strength / 3.0, 0.0, 1.0) * 15.0
        if relative_strength is not None
        else 0.0
    )
    structure = 0.0
    if distance_from_high is not None:
        structure += 7.0 if distance_from_high <= 0.5 else 4.0 if distance_from_high <= 1.0 else 0.0
    if return_15m is not None:
        structure += 4.0 if return_15m > 0 else 2.0 if return_15m > -0.2 else 0.0
    if lower_highs is not None:
        structure += 4.0 if lower_highs == 0 else 2.0 if lower_highs == 1 else 0.0
    structure = _clamp(structure, 0.0, 15.0)
    risk_reward = 0.0

    raw_score = catalyst.score + momentum + volume + relative + structure + risk_reward
    score_caps: list[str] = ["PREOPEN_EXECUTION_FIELDS_MISSING_CAP_79"]
    score = min(raw_score, 79.0)
    if not catalyst.verified:
        score = min(score, 74.0)
        score_caps.append("NO_VERIFIED_CATALYST_CAP_74")

    reason_codes: list[str] = []
    if snapshot.last < minimum_price:
        reason_codes.append("PRICE_BELOW_MINIMUM")
    if gap is None:
        reason_codes.append("PRIOR_CLOSE_MISSING")
    elif gap < 0.25:
        reason_codes.append("PREMARKET_MOMENTUM_TOO_WEAK")
    if adv20 is None:
        reason_codes.append("LIQUIDITY_PROFILE_MISSING")
    elif adv20 < minimum_adv:
        reason_codes.append("ADV_BELOW_MINIMUM")
    if not snapshot.authoritative:
        reason_codes.append("PREMARKET_DATA_STALE")
    if catalyst.negative:
        reason_codes.append("NEGATIVE_CATALYST")
    if deterioration_confirmed:
        reason_codes.append("PREMARKET_DETERIORATION")

    ranking_allowed = not any(
        reason in reason_codes
        for reason in (
            "PRICE_BELOW_MINIMUM",
            "PRIOR_CLOSE_MISSING",
            "PREMARKET_MOMENTUM_TOO_WEAK",
            "LIQUIDITY_PROFILE_MISSING",
            "ADV_BELOW_MINIMUM",
            "PREMARKET_DATA_STALE",
            "NEGATIVE_CATALYST",
        )
    )
    finalist_eligible = bool(
        ranking_allowed
        and not deterioration_confirmed
        and score >= finalist_score
    )
    return {
        "score": round(score, 2),
        "raw_score": round(raw_score, 2),
        "score_caps": score_caps,
        "score_components": {
            "catalyst_or_continuation": {
                "score": round(catalyst.score, 2),
                "state": "VERIFIED" if catalyst.verified else "MISSING",
                "evidence": catalyst.headline,
            },
            "price_momentum": {
                "score": round(momentum, 2),
                "state": "DELAYED",
                "evidence": {"gap_percent": _rounded(gap), "return_15m_percent": _rounded(return_15m)},
            },
            "volume_liquidity": {
                "score": round(volume, 2),
                "state": "DELAYED" if premarket_volume_fraction is not None else "MISSING",
                "evidence": {"premarket_volume_fraction_adv20": _rounded(premarket_volume_fraction)},
            },
            "relative_strength_sector": {
                "score": round(relative, 2),
                "state": "DELAYED" if relative_strength is not None else "MISSING",
                "evidence": {"relative_strength_percent": _rounded(relative_strength)},
            },
            "entry_invalidation_structure": {
                "score": round(structure, 2),
                "state": "DELAYED",
                "evidence": {"distance_from_premarket_high_percent": _rounded(distance_from_high)},
            },
            "risk_reward": {
                "score": risk_reward,
                "state": "MISSING",
                "evidence": "PREOPEN_NO_EXECUTION_GEOMETRY",
            },
        },
        "metrics": {
            "price": snapshot.last,
            "prior_close": snapshot.prior_close,
            "gap_percent": _rounded(gap),
            "premarket_volume": snapshot.volume,
            "adv20": adv20,
            "premarket_volume_fraction_adv20": _rounded(premarket_volume_fraction),
            "premarket_vwap": snapshot.vwap,
            "distance_from_vwap_percent": _rounded(distance_from_vwap),
            "premarket_high": premarket_high,
            "distance_from_premarket_high_percent": _rounded(distance_from_high),
            "return_15m_percent": _rounded(return_15m),
            "lower_highs_count": lower_highs,
            "benchmark_return_percent": _rounded(benchmark_return_pct),
            "sector_return_percent": _rounded(sector_return_pct),
            "relative_strength_percent": _rounded(relative_strength),
        },
        "premarket_deterioration": {
            "signals": deterioration_signals,
            "confirmed": deterioration_confirmed,
            "severe": deterioration_severe,
        },
        "ranking_allowed": ranking_allowed,
        "finalist_eligible": finalist_eligible,
        "status": "PREDICTION/WATCH" if finalist_eligible else "WATCH",
        "reason_codes": list(dict.fromkeys(reason_codes)),
    }


def _pct(value: float | None, reference: float | None) -> float | None:
    if value is None or reference in (None, 0):
        return None
    return (value / reference - 1.0) * 100.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rounded(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware(datetime.fromisoformat(value))
    except ValueError:
        return None


def _optional_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
