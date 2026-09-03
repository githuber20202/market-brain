from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from market_brain.domain.models import AlertRecord, MarketSnapshot
from market_brain.engines.premarket import assess_catalyst, score_premarket_candidate
from market_brain.ledger.events import LedgerEvent
from market_brain.orchestration.universe import (
    EASTERN,
    UniverseEntry,
    load_market_calendar,
    load_universe,
)
from market_brain.providers.base import DataUnavailable
from market_brain.settings import Settings

ISRAEL = ZoneInfo("Asia/Jerusalem")
PREMARKET_CHECKPOINTS = ("T-30", "T-12", "T-3")
PRIOR_CHECKPOINT = {"T-12": "T-30", "T-3": "T-12"}
MANDATORY_AUDIT_SYMBOLS = {"MRNA", "MRVL"}


class PremarketFunnel:
    def __init__(
        self,
        *,
        store,
        service,
        provider,
        universe_dir: Path,
        calendar_path: Path,
        cfg: Settings,
        state_dir: Path,
    ) -> None:
        self.store = store
        self.service = service
        self.provider = provider
        self.universe_dir = universe_dir
        self.calendar_path = calendar_path
        self.cfg = cfg
        self.state_dir = state_dir

    async def run(self, checkpoint: str, *, now: datetime) -> dict[str, Any]:
        stage = checkpoint.upper()
        if stage not in PREMARKET_CHECKPOINTS:
            raise ValueError("PREMARKET_CHECKPOINT_INVALID")
        timestamp = _aware(now)
        local = timestamp.astimezone(EASTERN)
        session_date = local.date().isoformat()
        run_id = f"premarket:{session_date}:{stage}"
        status_key = f"premarket_run:{run_id}"
        existing = await self.store.get_runtime_status_key(status_key)
        if isinstance(existing, dict) and existing.get("status") == "COMPLETED":
            return {
                "mode": "premarket",
                "status": "ALREADY_COMPLETED",
                "checkpoint": stage,
                "run_id": run_id,
                "artifact_dir": existing.get("artifact_dir"),
            }

        universe = load_universe(self.universe_dir)
        required = [entry for entry in universe if entry.audit_required]
        required_symbols = {entry.symbol for entry in required}
        missing_mandatory = sorted(MANDATORY_AUDIT_SYMBOLS - required_symbols)
        if missing_mandatory:
            raise RuntimeError(
                "PREMARKET_MANDATORY_AUDIT_MISSING=" + ",".join(missing_mandatory)
            )
        calendar = load_market_calendar(
            self.calendar_path,
            required_years={local.year, local.year + 1},
        )
        session = calendar.session_for(local.date())
        if session is None:
            artifact = self._empty_artifact(
                run_id,
                stage,
                timestamp,
                status="NO_SESSION",
                required=len(required),
                blockers=["MARKET_CLOSED"],
            )
            return await self._persist(artifact, status_key=status_key)
        if local >= session.opens_at:
            artifact = self._empty_artifact(
                run_id,
                stage,
                timestamp,
                status="MISSED_AFTER_OPEN",
                required=len(required),
                blockers=["PREMARKET_CHECKPOINT_LATE"],
            )
            return await self._persist(artifact, status_key=status_key)

        artifact = await self._collect(
            run_id=run_id,
            checkpoint=stage,
            timestamp=timestamp,
            universe=universe,
            required=required,
            market_open=session.opens_at,
        )
        return await self._persist(artifact, status_key=status_key)

    async def mark_missed(
        self,
        checkpoint: str,
        *,
        scheduled_for: datetime,
        now: datetime,
    ) -> dict[str, Any]:
        stage = checkpoint.upper()
        if stage not in PREMARKET_CHECKPOINTS:
            raise ValueError("PREMARKET_CHECKPOINT_INVALID")
        timestamp = _aware(now)
        session_id = _aware(scheduled_for).astimezone(EASTERN).date().isoformat()
        run_id = f"premarket:{session_id}:{stage}"
        status_key = f"premarket_run:{run_id}"
        existing = await self.store.get_runtime_status_key(status_key)
        if isinstance(existing, dict) and existing.get("status") in {
            "COMPLETED",
            "MISSED",
        }:
            return {
                "mode": "premarket",
                "status": "ALREADY_COMPLETED",
                "checkpoint": stage,
                "run_id": run_id,
                "artifact_dir": existing.get("artifact_dir"),
            }
        universe = load_universe(self.universe_dir)
        required = sum(entry.audit_required for entry in universe)
        artifact = self._empty_artifact(
            run_id,
            stage,
            timestamp,
            status="MISSED",
            required=required,
            blockers=["PREMARKET_CHECKPOINT_MISSED"],
        )
        artifact["session_id"] = session_id
        artifact["scheduled_for"] = _aware(scheduled_for).astimezone(EASTERN).isoformat()
        artifact["missed_at"] = timestamp.isoformat()
        artifact["text"] = format_premarket_report(artifact)
        return await self._persist(artifact, status_key=status_key)

    async def _collect(
        self,
        *,
        run_id: str,
        checkpoint: str,
        timestamp: datetime,
        universe: tuple[UniverseEntry, ...],
        required: list[UniverseEntry],
        market_open: datetime,
    ) -> dict[str, Any]:
        warnings: list[str] = [
            "YAHOO_PUBLIC_DATA_DELAYED",
            "CALENDAR_STATIC_NYSE_ONLY",
            "ACCOUNT_AND_EXECUTION_FIELDS_SUPPRESSED",
        ]
        universe_symbols = {entry.symbol for entry in universe}
        external_metadata: dict[str, dict] = {}
        if hasattr(self.provider, "external_movers"):
            try:
                external = await self.provider.external_movers()
                external_metadata = {
                    str(row["symbol"]).upper(): row
                    for row in external
                    if isinstance(row, dict)
                    and str(row.get("symbol") or "").upper() not in universe_symbols
                }
            except (DataUnavailable, OSError, RuntimeError, TypeError, ValueError) as exc:
                warnings.append(f"EXTERNAL_DISCOVERY_UNAVAILABLE:{_error_type(exc)}")

        ranking_entries = [
            entry
            for entry in universe
            if entry.ranking_eligible and entry.instrument_type != "UNRESOLVED"
        ]
        ranking_symbols = [entry.symbol for entry in ranking_entries]
        external_symbols = sorted(external_metadata)
        profile_symbols = [*ranking_symbols, *external_symbols]
        liquidity_refresh = await self.service.refresh_liquidity_profiles_for_symbols(
            profile_symbols,
            now=timestamp,
        )
        profiles = {
            profile.symbol.upper(): profile
            for profile in await self.store.list_liquidity_profiles()
        }

        context_symbols = {
            entry.sector_proxy
            for entry in universe
            if entry.sector_proxy and entry.sector_proxy not in universe_symbols
        }
        requested_symbols = list(dict.fromkeys([*profile_symbols, *sorted(context_symbols)]))
        snapshots = await self.provider.premarket_snapshots(requested_symbols)
        by_symbol = {snapshot.symbol.upper(): snapshot for snapshot in snapshots}
        skipped = [
            {"symbol": item.symbol, "error_type": item.error_type}
            for item in getattr(snapshots, "skipped_symbols", ())
        ]
        if skipped:
            warnings.append(f"PREMARKET_SYMBOLS_SKIPPED:{len(skipped)}")

        benchmark_return = _snapshot_return(by_symbol.get("SPY"))
        news_by_symbol: dict[str, list[dict]] = {}
        news_errors: dict[str, str] = {}
        news_symbols = [
            entry.symbol
            for entry in ranking_entries
            if entry.instrument_type == "EQUITY" and entry.symbol in by_symbol
        ]
        news_symbols.extend(
            symbol for symbol in external_symbols if symbol in by_symbol
        )
        if hasattr(self.provider, "news"):
            for symbol in dict.fromkeys(news_symbols):
                try:
                    news_by_symbol[symbol] = await self.provider.news(symbol)
                except (DataUnavailable, OSError, RuntimeError, TypeError, ValueError) as exc:
                    news_errors[symbol] = _error_type(exc)
        if news_errors:
            warnings.append(f"NEWS_SYMBOLS_UNAVAILABLE:{len(news_errors)}")

        audit: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        for entry in required:
            if entry.instrument_type == "UNRESOLVED" or not entry.ranking_eligible:
                audit.append(self._missing_audit_row(entry, "IDENTIFIER_UNRESOLVED"))
                continue
            snapshot = by_symbol.get(entry.symbol)
            if snapshot is None:
                error_type = next(
                    (
                        item["error_type"]
                        for item in skipped
                        if item["symbol"] == entry.symbol
                    ),
                    "PREMARKET_SNAPSHOT_MISSING",
                )
                audit.append(self._missing_audit_row(entry, error_type))
                continue
            row = self._candidate_row(
                snapshot,
                entry=entry,
                profile=profiles.get(entry.symbol),
                benchmark_return=benchmark_return,
                sector_return=_snapshot_return(by_symbol.get(entry.sector_proxy or "")),
                news=news_by_symbol.get(entry.symbol, []),
                news_error=news_errors.get(entry.symbol),
                checkpoint=checkpoint,
                external=False,
                external_metadata=None,
                timestamp=timestamp,
            )
            audit.append(row)
            if row["ranking_allowed"]:
                candidates.append(row)

        external_rows: list[dict[str, Any]] = []
        for symbol in external_symbols:
            snapshot = by_symbol.get(symbol)
            if snapshot is None:
                continue
            metadata = external_metadata[symbol]
            entry = UniverseEntry(
                symbol=symbol,
                ranking_eligible=True,
                source_file="YAHOO_PREDEFINED_SCREENER",
                instrument_type="EQUITY",
                audit_required=False,
                name=str(metadata.get("name") or symbol),
                exchange=str(metadata.get("exchange") or ""),
                sector_proxy="SPY",
            )
            row = self._candidate_row(
                snapshot,
                entry=entry,
                profile=profiles.get(symbol),
                benchmark_return=benchmark_return,
                sector_return=None,
                news=news_by_symbol.get(symbol, []),
                news_error=news_errors.get(symbol),
                checkpoint=checkpoint,
                external=True,
                external_metadata=metadata,
                timestamp=timestamp,
            )
            external_rows.append(row)
            if row["ranking_allowed"]:
                candidates.append(row)

        required_usable = sum(row["data_state"] == "DELAYED" for row in audit)
        ranking_denominator = max(1, len(ranking_entries))
        missing_ratio = (len(ranking_entries) - required_usable) / ranking_denominator
        fail_closed = "SPY" not in by_symbol or missing_ratio > self.cfg.keyless_max_failure_ratio
        blockers: list[str] = []
        if "SPY" not in by_symbol:
            blockers.append("MARKET_ANCHOR_UNAVAILABLE")
        if missing_ratio > self.cfg.keyless_max_failure_ratio:
            blockers.append("KEYLESS_FAILURE_RATIO_EXCEEDED")

        candidates.sort(
            key=lambda row: (
                -float(row["score"]),
                -float(row["metrics"].get("gap_percent") or -999.0),
                row["symbol"],
            )
        )
        top = [] if fail_closed else candidates[:10]
        prior = await self._prior_artifact(checkpoint, timestamp)
        delta_state = self._apply_delta(top, prior)
        finalists = [
            row["symbol"]
            for row in top
            if row["finalist_eligible"]
        ][:2]
        data_state = "MISSING" if any(row["data_state"] != "DELAYED" for row in audit) else "DELAYED"
        batch_id = (
            f"{timestamp.astimezone(EASTERN).date().isoformat()}-"
            f"{checkpoint.replace('+', 'p').replace('-', 'm')}-"
            f"{timestamp.strftime('%H%M%SZ')}"
        )
        artifact: dict[str, Any] = {
            "schema_version": "market-premarket-artifact.v1",
            "run_id": run_id,
            "batch_id": batch_id,
            "session_id": timestamp.astimezone(EASTERN).date().isoformat(),
            "checkpoint": checkpoint,
            "as_of": timestamp.isoformat(),
            "market_state": "PREOPEN",
            "market_open_et": market_open.isoformat(),
            "data_state": data_state,
            "confidence": "LOW",
            "market_context": {
                "benchmark": "SPY",
                "benchmark_return_percent": (
                    round(benchmark_return, 4)
                    if benchmark_return is not None
                    else None
                ),
                "source_id": (
                    by_symbol["SPY"].source_id if "SPY" in by_symbol else None
                ),
                "state": "DELAYED" if "SPY" in by_symbol else "MISSING",
            },
            "missing_execution_fields": [
                "authoritative_live_price",
                "authoritative_live_volume",
                "authoritative_vwap",
                "bid",
                "ask",
                "spread",
                "opening_range",
                "trigger",
                "entry",
                "stop",
                "targets",
                "quantity",
            ],
            "status": "DATA_UNAVAILABLE" if fail_closed else "COMPLETED",
            "coverage": {
                "required": len(required),
                "audit_rows": len(audit),
                "usable_prices": required_usable,
                "ready_inputs": 0,
                "external_discovered": len(external_rows),
                "ranking_eligible": len(candidates),
                "top_count": len(top),
            },
            "audit": audit,
            "external_audit": external_rows,
            "top10": [row["symbol"] for row in top],
            "top10_rows": top,
            "finalists": finalists,
            "delta_state": delta_state,
            "warnings": sorted(set(warnings)),
            "blockers": sorted(set(blockers)),
            "skipped_symbols": skipped,
            "liquidity_refresh": liquidity_refresh,
            "news_errors": news_errors,
            "numeric_execution_allowed": False,
            "ready_allowed": False,
            "broker_actions_allowed": False,
            "labels": ["SHADOW", "DELAYED", "PREDICTION", "WATCH"],
        }
        artifact["text"] = format_premarket_report(artifact)
        return artifact

    def _candidate_row(
        self,
        snapshot: MarketSnapshot,
        *,
        entry: UniverseEntry,
        profile,
        benchmark_return: float | None,
        sector_return: float | None,
        news: list[dict],
        news_error: str | None,
        checkpoint: str,
        external: bool,
        external_metadata: dict | None,
        timestamp: datetime,
    ) -> dict[str, Any]:
        catalyst = assess_catalyst(news, as_of=timestamp)
        scoring = score_premarket_candidate(
            snapshot,
            adv20=profile.adv20 if profile is not None else None,
            benchmark_return_pct=benchmark_return,
            sector_return_pct=sector_return,
            catalyst=catalyst,
            minimum_price=self.cfg.min_price,
            minimum_adv=self.cfg.min_adv_keyless,
            finalist_score=self.cfg.premarket_finalist_score,
        )
        quote_timestamp = snapshot.metadata.get("quote_timestamp")
        return {
            "symbol": entry.symbol,
            "name": entry.name or entry.symbol,
            "exchange": entry.exchange,
            "sector_proxy": entry.sector_proxy,
            "audit_required": entry.audit_required,
            "external": external,
            "external_discovery": external_metadata,
            "data_state": "DELAYED" if snapshot.authoritative else "STALE",
            "source_id": snapshot.source_id,
            "sample_id": f"premarket:{checkpoint}:{quote_timestamp}",
            "quote_timestamp": quote_timestamp,
            "fetched_at": snapshot.fetched_at.isoformat() if snapshot.fetched_at else None,
            "delay_minutes": snapshot.delay_minutes,
            "catalyst": catalyst.to_dict(),
            "news_error": news_error,
            "why_may_rise": _why_may_rise(catalyst.to_dict()),
            "why_rising_now": _why_rising_now(scoring["metrics"]),
            "direct_driver": catalyst.headline,
            **scoring,
        }

    @staticmethod
    def _missing_audit_row(entry: UniverseEntry, reason: str) -> dict[str, Any]:
        return {
            "symbol": entry.symbol,
            "name": entry.name or entry.symbol,
            "exchange": entry.exchange,
            "sector_proxy": entry.sector_proxy,
            "audit_required": entry.audit_required,
            "external": False,
            "data_state": "MISSING",
            "source_id": None,
            "score": 0.0,
            "score_components": {},
            "metrics": {},
            "catalyst": None,
            "ranking_allowed": False,
            "finalist_eligible": False,
            "status": "WATCH",
            "reason_codes": [reason],
        }

    async def _prior_artifact(
        self,
        checkpoint: str,
        timestamp: datetime,
    ) -> dict | None:
        prior_checkpoint = PRIOR_CHECKPOINT.get(checkpoint)
        if prior_checkpoint is None:
            return None
        session_date = timestamp.astimezone(EASTERN).date().isoformat()
        prior = await self.store.get_runtime_status_key(
            f"premarket_artifact:{session_date}:{prior_checkpoint}"
        )
        if isinstance(prior, dict):
            return prior
        if checkpoint == "T-3":
            fallback = await self.store.get_runtime_status_key(
                f"premarket_artifact:{session_date}:T-30"
            )
            return fallback if isinstance(fallback, dict) else None
        return None

    @staticmethod
    def _apply_delta(top: list[dict], prior: dict | None) -> str:
        if prior is None:
            return "DELTA_UNAVAILABLE"
        prior_rows = {
            row.get("symbol"): row
            for row in [
                *prior.get("audit", []),
                *prior.get("external_audit", []),
            ]
            if isinstance(row, dict) and row.get("symbol")
        }
        matched = 0
        for row in top:
            previous = prior_rows.get(row["symbol"])
            if previous is None:
                continue
            row["delta"] = {
                "score": _difference(row.get("score"), previous.get("score")),
                "price": _difference(
                    row.get("metrics", {}).get("price"),
                    previous.get("metrics", {}).get("price"),
                ),
                "premarket_volume": _difference(
                    row.get("metrics", {}).get("premarket_volume"),
                    previous.get("metrics", {}).get("premarket_volume"),
                ),
            }
            matched += 1
        return "AVAILABLE" if matched else "DELTA_UNAVAILABLE"

    async def _persist(self, artifact: dict[str, Any], *, status_key: str) -> dict[str, Any]:
        artifact_dir = write_premarket_artifacts(artifact, self.state_dir)
        artifact["artifact_dir"] = str(artifact_dir)
        alert = AlertRecord(
            kind="PREMARKET_PREDICTION",
            payload={
                "run_id": artifact["run_id"],
                "session_date": artifact["session_id"],
                "checkpoint": artifact["checkpoint"],
                "status": artifact["status"],
                "data_state": artifact["data_state"],
                "top10": artifact.get("top10", []),
                "finalists": artifact.get("finalists", []),
                "text": artifact["text"],
            },
            created_at=datetime.fromisoformat(artifact["as_of"]),
        )
        async with self.store.transaction():
            current = await self.store.get_runtime_status_key(status_key)
            if isinstance(current, dict) and current.get("status") == "COMPLETED":
                return {
                    "mode": "premarket",
                    "status": "ALREADY_COMPLETED",
                    "checkpoint": artifact["checkpoint"],
                    "run_id": artifact["run_id"],
                    "artifact_dir": current.get("artifact_dir"),
                }
            await self.store.append(
                LedgerEvent(
                    "PREMARKET_RUN",
                    artifact["run_id"],
                    {key: value for key, value in artifact.items() if key != "text"},
                    occurred_at=datetime.fromisoformat(artifact["as_of"]),
                )
            )
            await self.store.save_alert(alert)
            await self.store.set_runtime_status(
                status_key,
                {
                    "status": artifact["status"],
                    "checkpoint": artifact["checkpoint"],
                    "batch_id": artifact["batch_id"],
                    "artifact_dir": str(artifact_dir),
                    "alert_id": alert.alert_id,
                    "as_of": artifact["as_of"],
                },
            )
            await self.store.set_runtime_status(
                f"premarket_artifact:{artifact['session_id']}:{artifact['checkpoint']}",
                artifact,
            )
        return {
            "mode": "premarket",
            "status": artifact["status"],
            "checkpoint": artifact["checkpoint"],
            "run_id": artifact["run_id"],
            "batch_id": artifact["batch_id"],
            "coverage": artifact["coverage"],
            "top10": artifact.get("top10", []),
            "finalists": artifact.get("finalists", []),
            "delta_state": artifact.get("delta_state"),
            "artifact_dir": str(artifact_dir),
        }

    @staticmethod
    def _empty_artifact(
        run_id: str,
        checkpoint: str,
        timestamp: datetime,
        *,
        status: str,
        required: int,
        blockers: list[str],
    ) -> dict[str, Any]:
        session_id = timestamp.astimezone(EASTERN).date().isoformat()
        batch_id = (
            f"{session_id}-{checkpoint.replace('+', 'p').replace('-', 'm')}-"
            f"{timestamp.strftime('%H%M%SZ')}"
        )
        artifact = {
            "schema_version": "market-premarket-artifact.v1",
            "run_id": run_id,
            "batch_id": batch_id,
            "session_id": session_id,
            "checkpoint": checkpoint,
            "as_of": timestamp.isoformat(),
            "market_state": "CLOSED" if status == "NO_SESSION" else "OPEN",
            "market_open_et": None,
            "data_state": "MISSING",
            "confidence": "NONE",
            "market_context": {
                "benchmark": "SPY",
                "benchmark_return_percent": None,
                "source_id": None,
                "state": "MISSING",
            },
            "missing_execution_fields": [
                "authoritative_live_price",
                "authoritative_live_volume",
                "authoritative_vwap",
                "bid",
                "ask",
                "spread",
                "opening_range",
                "trigger",
                "entry",
                "stop",
                "targets",
                "quantity",
            ],
            "status": status,
            "coverage": {
                "required": required,
                "audit_rows": 0,
                "usable_prices": 0,
                "ready_inputs": 0,
                "external_discovered": 0,
                "ranking_eligible": 0,
                "top_count": 0,
            },
            "audit": [],
            "external_audit": [],
            "top10": [],
            "top10_rows": [],
            "finalists": [],
            "delta_state": "DELTA_UNAVAILABLE",
            "warnings": [],
            "blockers": blockers,
            "skipped_symbols": [],
            "liquidity_refresh": None,
            "news_errors": {},
            "numeric_execution_allowed": False,
            "ready_allowed": False,
            "broker_actions_allowed": False,
            "labels": ["SHADOW", "DELAYED", "PREDICTION", "WATCH"],
        }
        artifact["text"] = format_premarket_report(artifact)
        return artifact


def write_premarket_artifacts(artifact: dict[str, Any], state_dir: Path) -> Path:
    directory = (
        state_dir
        / "premarket"
        / str(artifact["session_id"])
        / str(artifact["checkpoint"])
        / str(artifact["batch_id"])
    )
    directory.mkdir(parents=True, exist_ok=True)
    report = {key: value for key, value in artifact.items() if key not in {"audit", "text"}}
    (directory / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    (directory / "audit.jsonl").write_text(
        "".join(
            json.dumps(row, sort_keys=True, default=str) + "\n"
            for row in artifact.get("audit", [])
        ),
        encoding="utf-8",
    )
    (directory / "funnel.json").write_text(
        json.dumps(artifact.get("top10_rows", []), indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    return directory


def format_premarket_report(artifact: dict[str, Any]) -> str:
    as_of = datetime.fromisoformat(artifact["as_of"])
    coverage = artifact["coverage"]
    market_open = artifact.get("market_open_et")
    minutes_to_open = None
    if isinstance(market_open, str):
        minutes_to_open = int((datetime.fromisoformat(market_open) - as_of.astimezone(EASTERN)).total_seconds() // 60)
    lines = [
        (
            f"DATA STATE: {artifact['data_state']} | "
            f"MARKET STATE: {artifact['market_state']} | "
            "[SHADOW][DELAYED][PREDICTION][WATCH]"
        ),
        f"Premarket Prediction {artifact['checkpoint']} — {artifact['session_id']}",
        (
            f"Updated: {as_of.astimezone(ISRAEL).strftime('%H:%M:%S %Z')} / "
            f"{as_of.astimezone(EASTERN).strftime('%H:%M:%S %Z')}"
        ),
        f"Minutes to open: {minutes_to_open if minutes_to_open is not None else 'N/A'}",
        (
            f"Coverage: audit={coverage['audit_rows']}/{coverage['required']} "
            f"usable_prices={coverage['usable_prices']}/{coverage['required']} "
            "READY_inputs=0"
        ),
        f"Status: {artifact['status']} | Delta: {artifact.get('delta_state', 'DELTA_UNAVAILABLE')}",
        (
            "Market context: SPY="
            f"{_fmt_pct(artifact.get('market_context', {}).get('benchmark_return_percent'))} "
            f"state={artifact.get('market_context', {}).get('state', 'MISSING')}"
        ),
        (
            f"Confidence: {artifact.get('confidence', 'NONE')} | "
            "Missing execution fields: "
            + ", ".join(artifact.get("missing_execution_fields", []))
        ),
        "",
        "Top candidates (all are PREDICTION/WATCH):",
    ]
    top = artifact.get("top10_rows", [])
    if not top:
        lines.append("- none")
    for index, row in enumerate(top, start=1):
        metrics = row.get("metrics", {})
        delta = row.get("delta", {})
        delta_score = delta.get("score")
        delta_text = f" delta={delta_score:+.2f}" if isinstance(delta_score, (int, float)) else ""
        catalyst = row.get("catalyst") or {}
        headline = catalyst.get("headline") or "no verified catalyst"
        lines.append(
            f"{index}. {row['symbol']} | score={row['score']:.2f}{delta_text} "
            f"gap={_fmt_pct(metrics.get('gap_percent'))} "
            f"PM-vol/ADV={_fmt_ratio(metrics.get('premarket_volume_fraction_adv20'))} "
            f"RS={_fmt_pct(metrics.get('relative_strength_percent'))} | {headline}"
        )
    finalists = artifact.get("finalists", [])
    lines.extend(
        [
            "",
            "Final predictions: " + (", ".join(finalists) if finalists else "none"),
            "Post-open requirement: Opening Range + VWAP + Retest + live authoritative gate.",
            "No Trigger/Stop/targets/quantity are published before the open.",
        ]
    )
    deteriorated = [
        row["symbol"]
        for row in top
        if row.get("premarket_deterioration", {}).get("confirmed")
    ]
    if deteriorated:
        lines.append("Blocked by Premarket Deterioration: " + ", ".join(deteriorated))
    if artifact.get("blockers"):
        lines.append("Blockers: " + ", ".join(artifact["blockers"]))
    lines.append("Market analysis for Shadow measurement only; not an investment instruction.")
    return "\n".join(lines)


def _why_may_rise(catalyst: dict[str, Any]) -> str:
    if catalyst.get("verified") and catalyst.get("headline"):
        return f"Verified headline catalyst: {catalyst['category']}"
    return "Momentum continuation hypothesis; catalyst not fully verified"


def _why_rising_now(metrics: dict[str, Any]) -> str:
    return (
        f"Premarket gap {_fmt_pct(metrics.get('gap_percent'))}; "
        f"relative strength {_fmt_pct(metrics.get('relative_strength_percent'))}; "
        f"PM volume/ADV {_fmt_ratio(metrics.get('premarket_volume_fraction_adv20'))}"
    )


def _snapshot_return(snapshot: MarketSnapshot | None) -> float | None:
    if snapshot is None or snapshot.prior_close in (None, 0):
        return None
    return (snapshot.last / snapshot.prior_close - 1.0) * 100.0


def _difference(current: Any, previous: Any) -> float | None:
    try:
        return round(float(current) - float(previous), 4)
    except (TypeError, ValueError):
        return None


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_ratio(value: Any) -> str:
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return "N/A"


def _error_type(exc: BaseException) -> str:
    return exc.error_type if isinstance(exc, DataUnavailable) else type(exc).__name__


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
