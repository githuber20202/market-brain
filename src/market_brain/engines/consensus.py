from __future__ import annotations

from dataclasses import dataclass, replace
from statistics import median

from market_brain.domain.models import MarketSnapshot


@dataclass(slots=True)
class MarketAuthorityResult:
    accepted: bool
    snapshot: MarketSnapshot | None
    mode: str
    reasons: list[str]
    source_ids: list[str]
    divergence_bps: float | None = None


def _group(snapshot: MarketSnapshot) -> str:
    return str(snapshot.metadata.get("independence_group") or snapshot.source_id or "UNKNOWN")


def _valid(snapshot: MarketSnapshot, max_age_seconds: float) -> bool:
    return bool(
        snapshot.last > 0
        and snapshot.bid is not None
        and snapshot.ask is not None
        and snapshot.ask >= snapshot.bid
        and snapshot.data_age_seconds is not None
        and snapshot.data_age_seconds <= max_age_seconds
        and not snapshot.halted
    )


def resolve_market_authority(
    snapshots: list[MarketSnapshot],
    *,
    max_age_seconds: float = 15.0,
    max_divergence_bps: float = 20.0,
    minimum_independent_sources: int = 2,
) -> MarketAuthorityResult:
    valid = [row for row in snapshots if _valid(row, max_age_seconds)]
    if not valid:
        return MarketAuthorityResult(False, None, "NONE", ["NO_FRESH_MARKET_SAMPLE"], [])

    authoritative = [row for row in valid if row.authoritative]
    if authoritative:
        chosen = min(authoritative, key=lambda row: row.data_age_seconds or 10**9)
        return MarketAuthorityResult(
            True,
            chosen,
            "FULL_MARKET",
            [],
            [chosen.source_id or "AUTHORITATIVE"],
            0.0,
        )

    by_group: dict[str, MarketSnapshot] = {}
    for row in valid:
        group = _group(row)
        current = by_group.get(group)
        if current is None or (row.data_age_seconds or 10**9) < (current.data_age_seconds or 10**9):
            by_group[group] = row
    independent = list(by_group.values())
    if len(independent) < minimum_independent_sources:
        return MarketAuthorityResult(
            False,
            None,
            "QUORUM_INSUFFICIENT",
            ["INDEPENDENT_SOURCE_QUORUM_REQUIRED"],
            [row.source_id or "UNKNOWN" for row in independent],
        )

    prices = [row.last for row in independent]
    consensus_price = float(median(prices))
    divergence_bps = (max(prices) - min(prices)) / consensus_price * 10_000.0
    if divergence_bps > max_divergence_bps:
        return MarketAuthorityResult(
            False,
            None,
            "QUORUM_CONFLICT",
            ["MARKET_SOURCE_DIVERGENCE"],
            [row.source_id or "UNKNOWN" for row in independent],
            round(divergence_bps, 4),
        )

    freshest = min(independent, key=lambda row: row.data_age_seconds or 10**9)
    bid = float(median([row.bid for row in independent if row.bid is not None]))
    ask = float(median([row.ask for row in independent if row.ask is not None]))
    source_ids = [row.source_id or "UNKNOWN" for row in independent]
    synthetic = replace(
        freshest,
        last=consensus_price,
        bid=bid,
        ask=ask,
        source_id="QUORUM:" + ",".join(sorted(source_ids)),
        authoritative=True,
        metadata={**freshest.metadata, "quorum_sources": source_ids, "divergence_bps": divergence_bps},
    )
    return MarketAuthorityResult(
        True,
        synthetic,
        "INDEPENDENT_QUORUM",
        [],
        source_ids,
        round(divergence_bps, 4),
    )

