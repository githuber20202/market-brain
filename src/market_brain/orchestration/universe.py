from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from market_brain.domain.models import EvidenceCard, QualityProfile
from market_brain.engines.quality import classify_quality

EASTERN = ZoneInfo("America/New_York")
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")


@dataclass(frozen=True, slots=True)
class UniverseEntry:
    symbol: str
    ranking_eligible: bool
    source_file: str
    instrument_type: str = "EQUITY"


@dataclass(frozen=True, slots=True)
class ManualQuality:
    symbol: str
    quality_score: float
    as_of: datetime
    source: str = "MANUAL"
    partial: bool = False

    def profile(self) -> QualityProfile:
        profile = classify_quality(self.symbol, self.quality_score, self.as_of)
        profile.evidence.append(
            EvidenceCard(
                evidence_type="QUALITY_ASSESSMENT",
                summary=(
                    f"Documented quality score {self.quality_score:g}; "
                    f"partial={str(self.partial).lower()}"
                ),
                source=self.source,
                published_at=self.as_of,
                confidence=1.0,
                expires_at=datetime.max.replace(tzinfo=UTC),
            )
        )
        return profile


@dataclass(frozen=True, slots=True)
class MarketSession:
    session_date: date
    opens_at: datetime
    closes_at: datetime
    early_close: bool


class NyseMarketCalendar:
    def __init__(self, special_days: dict[date, tuple[str, time | None]], covered_years: set[int]):
        self.special_days = special_days
        self.covered_years = covered_years

    def validate_years(self, years: set[int]) -> None:
        missing = sorted(years - self.covered_years)
        if missing:
            raise RuntimeError(f"MARKET_CALENDAR_YEARS_MISSING={missing}")

    def session_for(self, session_date: date) -> MarketSession | None:
        if session_date.year not in self.covered_years:
            raise RuntimeError(f"MARKET_CALENDAR_YEAR_UNAVAILABLE={session_date.year}")
        if session_date.weekday() >= 5:
            return None
        status, close_time = self.special_days.get(session_date, ("REGULAR", time(16, 0)))
        if status == "CLOSED":
            return None
        opens_at = datetime.combine(session_date, time(9, 30), EASTERN)
        closes_at = datetime.combine(session_date, close_time or time(16, 0), EASTERN)
        return MarketSession(session_date, opens_at, closes_at, status == "EARLY_CLOSE")


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError(f"UNIVERSE_SYMBOL_INVALID={value!r}")
    return symbol


def load_universe(directory: Path) -> tuple[UniverseEntry, ...]:
    paths = sorted(directory.glob("*.csv"))
    if not paths:
        raise RuntimeError("UNIVERSE_FILES_MISSING")
    entries: list[UniverseEntry] = []
    seen: dict[str, str] = {}
    for path in paths:
        try:
            handle = path.open(newline="", encoding="utf-8-sig")
        except OSError as exc:
            raise RuntimeError(f"UNIVERSE_FILE_UNREADABLE={path.name}") from exc
        with handle:
            reader = csv.DictReader(handle)
            headers = set(reader.fieldnames or [])
            symbol_field = "symbol" if "symbol" in headers else "ticker" if "ticker" in headers else None
            if symbol_field is None:
                raise RuntimeError(f"UNIVERSE_SYMBOL_COLUMN_MISSING={path.name}")
            for line_number, row in enumerate(reader, start=2):
                raw_symbol = row.get(symbol_field)
                if raw_symbol is None or not raw_symbol.strip():
                    raise ValueError(f"UNIVERSE_SYMBOL_MISSING={path.name}:{line_number}")
                symbol = normalize_symbol(raw_symbol)
                if symbol in seen:
                    raise ValueError(
                        f"UNIVERSE_DUPLICATE_SYMBOL={symbol}:{seen[symbol]}:{path.name}:{line_number}"
                    )
                seen[symbol] = f"{path.name}:{line_number}"
                instrument_type = str(
                    row.get("instrument_type") or row.get("asset_type") or "EQUITY"
                ).strip().upper()
                if instrument_type not in {"EQUITY", "ETF", "UNRESOLVED"}:
                    raise ValueError(
                        f"UNIVERSE_INSTRUMENT_TYPE_INVALID={symbol}:{path.name}:{line_number}"
                    )
                entries.append(
                    UniverseEntry(
                        symbol=symbol,
                        ranking_eligible=_csv_bool(row.get("ranking_eligible"), default=True),
                        source_file=path.name,
                        instrument_type=instrument_type,
                    )
                )
    if not entries:
        raise RuntimeError("UNIVERSE_EMPTY")
    return tuple(entries)


def load_manual_quality(path: Path) -> dict[str, ManualQuality]:
    try:
        handle = path.open(newline="", encoding="utf-8-sig")
    except OSError as exc:
        raise RuntimeError("QUALITY_FILE_UNREADABLE") from exc
    records: dict[str, ManualQuality] = {}
    with handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "quality_score", "as_of"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise RuntimeError("QUALITY_COLUMNS_INVALID")
        for line_number, row in enumerate(reader, start=2):
            symbol = normalize_symbol(row.get("symbol", ""))
            if symbol in records:
                raise ValueError(f"QUALITY_DUPLICATE_SYMBOL={symbol}:{line_number}")
            try:
                score = float(row.get("quality_score", ""))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"QUALITY_SCORE_INVALID={symbol}:{line_number}") from exc
            if not 0.0 <= score <= 100.0:
                raise ValueError(f"QUALITY_SCORE_INVALID={symbol}:{line_number}")
            as_of = _parse_datetime(row.get("as_of", ""), f"QUALITY_AS_OF_INVALID={symbol}:{line_number}")
            source = str(row.get("source") or "MANUAL").strip().upper()
            if source not in {"MANUAL", "EDGAR_AUTO", "YAHOO_FUNDAMENTALS"}:
                raise ValueError(f"QUALITY_SOURCE_INVALID={symbol}:{line_number}")
            partial = _csv_bool(row.get("partial"), default=False)
            records[symbol] = ManualQuality(symbol, score, as_of, source, partial)
    return records


def load_market_calendar(
    path: Path,
    *,
    required_years: set[int] | None = None,
) -> NyseMarketCalendar:
    try:
        handle = path.open(newline="", encoding="utf-8-sig")
    except OSError as exc:
        raise RuntimeError("MARKET_CALENDAR_UNREADABLE") from exc
    special_days: dict[date, tuple[str, time | None]] = {}
    covered_years: set[int] = set()
    with handle:
        reader = csv.DictReader(handle)
        required = {"date", "status", "open_time", "close_time", "source"}
        if set(reader.fieldnames or []) != required:
            raise RuntimeError("MARKET_CALENDAR_COLUMNS_INVALID")
        for line_number, row in enumerate(reader, start=2):
            try:
                session_date = date.fromisoformat(row.get("date", ""))
            except ValueError as exc:
                raise ValueError(f"MARKET_CALENDAR_DATE_INVALID={line_number}") from exc
            if session_date in special_days:
                raise ValueError(f"MARKET_CALENDAR_DUPLICATE_DATE={session_date}")
            if row.get("source", "").strip().upper() != "NYSE":
                raise ValueError(f"MARKET_CALENDAR_SOURCE_INVALID={session_date}")
            status = row.get("status", "").strip().upper()
            if status not in {"CLOSED", "EARLY_CLOSE"}:
                raise ValueError(f"MARKET_CALENDAR_STATUS_INVALID={session_date}")
            if status == "CLOSED":
                if row.get("open_time", "").strip() or row.get("close_time", "").strip():
                    raise ValueError(f"MARKET_CALENDAR_CLOSED_TIMES_INVALID={session_date}")
                close_time = None
            else:
                open_time = _parse_time(row.get("open_time", ""), session_date)
                close_time = _parse_time(row.get("close_time", ""), session_date)
                if open_time != time(9, 30) or not open_time < close_time < time(16, 0):
                    raise ValueError(f"MARKET_CALENDAR_EARLY_CLOSE_INVALID={session_date}")
            special_days[session_date] = (status, close_time)
            covered_years.add(session_date.year)
    calendar = NyseMarketCalendar(special_days, covered_years)
    if required_years:
        calendar.validate_years(required_years)
    return calendar


def _csv_bool(value: str | None, *, default: bool) -> bool:
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"UNIVERSE_BOOLEAN_INVALID={value!r}")


def _parse_datetime(value: str, error: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(error) from exc
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _parse_time(value: str, session_date: date) -> time:
    try:
        return time.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(f"MARKET_CALENDAR_TIME_INVALID={session_date}") from exc
