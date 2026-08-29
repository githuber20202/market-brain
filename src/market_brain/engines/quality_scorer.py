from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from market_brain.providers.yahoo_fundamentals import YahooFundamentalsSnapshot

REVENUE_TAGS = (
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
)
OPERATING_INCOME_TAGS = ("OperatingIncomeLoss",)
LONG_TERM_DEBT_TAGS = ("LongTermDebt", "LongTermDebtNoncurrent")
CURRENT_DEBT_TAGS = ("DebtCurrent", "LongTermDebtCurrent")
CASH_TAGS = (
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
)
EQUITY_TAGS = (
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
)
CFO_TAGS = ("NetCashProvidedByUsedInOperatingActivities",)
CAPEX_TAGS = ("PaymentsToAcquirePropertyPlantAndEquipment",)
DILUTED_SHARES_TAGS = ("WeightedAverageNumberOfDilutedSharesOutstanding",)


@dataclass(frozen=True, slots=True)
class MetricScore:
    value: float | None
    points: int
    facts_as_of: str | None


@dataclass(frozen=True, slots=True)
class QualityScore:
    symbol: str
    quality_score: int
    as_of: datetime
    source: str
    partial: bool
    metrics: dict[str, MetricScore]
    dilution_penalty: int
    missing_metrics: tuple[str, ...]


EdgarQualityScore = QualityScore


def score_companyfacts(
    symbol: str,
    payload: dict,
    *,
    as_of: datetime,
) -> EdgarQualityScore:
    timestamp = _aware(as_of)
    facts = payload.get("facts", {}).get("us-gaap", {})
    if not isinstance(facts, dict):
        facts = {}

    revenue = _quarterly_series(facts, REVENUE_TAGS, "USD")
    operating_income = _quarterly_series(facts, OPERATING_INCOME_TAGS, "USD")
    cfo = _quarterly_series(facts, CFO_TAGS, "USD")
    capex = _quarterly_series(facts, CAPEX_TAGS, "USD")
    diluted_shares = _quarterly_series(facts, DILUTED_SHARES_TAGS, "shares")

    revenue_growth = _yoy_growth(revenue)
    operating_margin = _trailing_margin(operating_income, revenue)
    fcf_margin = _fcf_margin(cfo, capex, revenue)
    leverage = _leverage(facts, operating_income)
    dilution = _yoy_growth(diluted_shares)
    if dilution is None:
        dilution = _annual_yoy_growth(
            _annual_series(facts, DILUTED_SHARES_TAGS, "shares")
        )

    return _build_quality_score(
        symbol=symbol,
        as_of=timestamp,
        source="EDGAR_AUTO",
        revenue_growth=revenue_growth,
        operating_margin=operating_margin,
        leverage=leverage[0],
        fcf_margin=fcf_margin,
        dilution=dilution,
        facts_as_of={
            "revenue_growth_yoy": _latest_date(revenue),
            "operating_margin": _latest_common_date(operating_income, revenue),
            "leverage": leverage[1],
            "fcf_margin": _latest_common_date(cfo, capex, revenue),
        },
    )


def score_yahoo_fundamentals(
    snapshot: YahooFundamentalsSnapshot,
    *,
    as_of: datetime,
) -> QualityScore:
    annual_revenue = _yahoo_series(snapshot, "annualTotalRevenue")
    quarterly_revenue = _yahoo_series(snapshot, "quarterlyTotalRevenue")
    annual_operating = _yahoo_series(snapshot, "annualOperatingIncome")
    quarterly_operating = _yahoo_series(snapshot, "quarterlyOperatingIncome")
    annual_debt = _yahoo_series(snapshot, "annualTotalDebt")
    annual_cash = _yahoo_series(snapshot, "annualCashAndCashEquivalents")
    annual_fcf = _yahoo_series(snapshot, "annualFreeCashFlow")
    quarterly_fcf = _yahoo_series(snapshot, "quarterlyFreeCashFlow")
    annual_shares = _yahoo_series(snapshot, "annualDilutedAverageShares")
    quarterly_shares = _yahoo_series(snapshot, "quarterlyDilutedAverageShares")

    revenue_growth = _yoy_growth(quarterly_revenue)
    revenue_growth_series = quarterly_revenue
    if revenue_growth is None:
        revenue_growth = _annual_yoy_growth(annual_revenue)
        revenue_growth_series = annual_revenue

    operating_margin = _trailing_margin(quarterly_operating, quarterly_revenue)
    operating_date = _latest_common_date(quarterly_operating, quarterly_revenue)
    if operating_margin is None:
        operating_margin = _latest_ratio(annual_operating, annual_revenue)
        operating_date = _latest_common_date(annual_operating, annual_revenue)

    fcf_margin = _trailing_margin(quarterly_fcf, quarterly_revenue)
    fcf_date = _latest_common_date(quarterly_fcf, quarterly_revenue)
    if fcf_margin is None:
        fcf_margin = _latest_ratio(annual_fcf, annual_revenue)
        fcf_date = _latest_common_date(annual_fcf, annual_revenue)

    leverage, leverage_date = _yahoo_leverage(
        annual_debt,
        annual_cash,
        annual_operating,
    )
    dilution = _yoy_growth(quarterly_shares)
    if dilution is None:
        dilution = _annual_yoy_growth(annual_shares)

    return _build_quality_score(
        symbol=snapshot.symbol,
        as_of=_aware(as_of),
        source="YAHOO_FUNDAMENTALS",
        revenue_growth=revenue_growth,
        operating_margin=operating_margin,
        leverage=leverage,
        fcf_margin=fcf_margin,
        dilution=dilution,
        facts_as_of={
            "revenue_growth_yoy": _latest_date(revenue_growth_series),
            "operating_margin": operating_date,
            "leverage": leverage_date,
            "fcf_margin": fcf_date,
        },
    )


def _build_quality_score(
    *,
    symbol: str,
    as_of: datetime,
    source: str,
    revenue_growth: float | None,
    operating_margin: float | None,
    leverage: float | None,
    fcf_margin: float | None,
    dilution: float | None,
    facts_as_of: dict[str, str | None],
) -> QualityScore:
    metrics = {
        "revenue_growth_yoy": MetricScore(
            revenue_growth,
            _higher_is_better(revenue_growth, (0.20, 0.10, 0.05, 0.0, -0.10)),
            facts_as_of.get("revenue_growth_yoy"),
        ),
        "operating_margin": MetricScore(
            operating_margin,
            _higher_is_better(operating_margin, (0.25, 0.15, 0.10, 0.05, 0.0)),
            facts_as_of.get("operating_margin"),
        ),
        "leverage": MetricScore(
            leverage,
            _lower_is_better(leverage, (0.0, 1.0, 2.0, 3.0, 4.0)),
            facts_as_of.get("leverage"),
        ),
        "fcf_margin": MetricScore(
            fcf_margin,
            _higher_is_better(fcf_margin, (0.20, 0.15, 0.10, 0.05, 0.0)),
            facts_as_of.get("fcf_margin"),
        ),
    }
    missing = [name for name, metric in metrics.items() if metric.value is None]
    if dilution is None:
        missing.append("dilution_yoy")
    penalty = _dilution_penalty(dilution)
    total = max(0, min(100, sum(metric.points for metric in metrics.values()) - penalty))
    return QualityScore(
        symbol=symbol.upper(),
        quality_score=total,
        as_of=as_of,
        source=source,
        partial=bool(missing),
        metrics=metrics,
        dilution_penalty=penalty,
        missing_metrics=tuple(missing),
    )


def _higher_is_better(value: float | None, thresholds: tuple[float, ...]) -> int:
    if value is None:
        return 0
    for threshold, points in zip(thresholds, (25, 20, 15, 10, 5), strict=True):
        if value + 1e-12 >= threshold:
            return points
    return 0


def _lower_is_better(value: float | None, thresholds: tuple[float, ...]) -> int:
    if value is None:
        return 0
    for threshold, points in zip(thresholds, (25, 20, 15, 10, 5), strict=True):
        if value <= threshold + 1e-12:
            return points
    return 0


def _dilution_penalty(value: float | None) -> int:
    if value is None or value <= 0:
        return 0
    if value <= 0.02 + 1e-12:
        return 2
    if value <= 0.05 + 1e-12:
        return 5
    if value <= 0.10 + 1e-12:
        return 10
    return 15


def _yoy_growth(series: dict[date, float]) -> float | None:
    ordered = sorted(series.items())
    if len(ordered) < 8:
        return None
    latest = sum(value for _day, value in ordered[-4:])
    previous = sum(value for _day, value in ordered[-8:-4])
    return (latest / previous - 1.0) if previous > 0 else None


def _annual_yoy_growth(series: dict[date, float]) -> float | None:
    ordered = sorted(series.items())
    if len(ordered) < 2 or ordered[-2][1] <= 0:
        return None
    return ordered[-1][1] / ordered[-2][1] - 1.0


def _trailing_margin(numerator: dict[date, float], revenue: dict[date, float]) -> float | None:
    common = sorted(set(numerator) & set(revenue))
    if len(common) < 4:
        return None
    selected = common[-4:]
    denominator = sum(revenue[day] for day in selected)
    return sum(numerator[day] for day in selected) / denominator if denominator > 0 else None


def _fcf_margin(
    cfo: dict[date, float],
    capex: dict[date, float],
    revenue: dict[date, float],
) -> float | None:
    common = sorted(set(cfo) & set(capex) & set(revenue))
    if len(common) < 4:
        return None
    selected = common[-4:]
    denominator = sum(revenue[day] for day in selected)
    free_cash_flow = sum(cfo[day] - capex[day] for day in selected)
    return free_cash_flow / denominator if denominator > 0 else None


def _latest_ratio(
    numerator: dict[date, float],
    denominator: dict[date, float],
) -> float | None:
    common = sorted(set(numerator) & set(denominator))
    if not common:
        return None
    day = common[-1]
    return numerator[day] / denominator[day] if denominator[day] > 0 else None


def _yahoo_series(
    snapshot: YahooFundamentalsSnapshot,
    metric: str,
) -> dict[date, float]:
    return {point.as_of: point.value for point in snapshot.series.get(metric, ())}


def _yahoo_leverage(
    debt: dict[date, float],
    cash: dict[date, float],
    operating_income: dict[date, float],
) -> tuple[float | None, str | None]:
    if not debt or not cash or not operating_income:
        return None, None
    debt_day = max(debt)
    cash_day = max(cash)
    operating_day = max(operating_income)
    denominator = operating_income[operating_day]
    if denominator <= 0:
        return None, None
    return (
        (debt[debt_day] - cash[cash_day]) / denominator,
        max(debt_day, cash_day, operating_day).isoformat(),
    )


def _leverage(
    facts: dict,
    operating_income: dict[date, float],
) -> tuple[float | None, str | None]:
    long_debt = _latest_instant(facts, LONG_TERM_DEBT_TAGS, "USD")
    current_debt = _latest_instant(facts, CURRENT_DEBT_TAGS, "USD")
    cash = _latest_instant(facts, CASH_TAGS, "USD")
    if long_debt is None or current_debt is None or cash is None:
        return None, None
    net_debt = long_debt[1] + current_debt[1] - cash[1]
    operating_days = sorted(operating_income)
    if len(operating_days) >= 4:
        annual_operating_income = sum(
            operating_income[day] for day in operating_days[-4:]
        )
        if annual_operating_income > 0:
            return net_debt / annual_operating_income, max(
                long_debt[0], current_debt[0], cash[0]
            ).isoformat()
    equity = _latest_instant(facts, EQUITY_TAGS, "USD")
    if equity is None or equity[1] <= 0:
        return None, None
    return (long_debt[1] + current_debt[1]) / equity[1], max(
        long_debt[0], current_debt[0], equity[0]
    ).isoformat()


def _quarterly_series(facts: dict, tags: tuple[str, ...], unit: str) -> dict[date, float]:
    selected: dict[tuple[date, date], tuple[str, str, float]] = {}
    for tag in tags:
        rows = _unit_rows(facts, tag, unit)
        for row in rows:
            if row.get("form") not in {"10-Q", "10-K"}:
                continue
            try:
                start = date.fromisoformat(str(row["start"]))
                end = date.fromisoformat(str(row["end"]))
                value = float(row["val"])
            except (KeyError, TypeError, ValueError):
                continue
            duration = (end - start).days
            if not 55 <= duration <= 380:
                continue
            candidate = (str(row.get("filed", "")), str(row.get("accn", "")), value)
            key = (start, end)
            if key not in selected or candidate[:2] > selected[key][:2]:
                selected[key] = candidate
    direct: dict[date, tuple[date, float]] = {}
    for (start, end), (_filed, _accn, value) in selected.items():
        if (end - start).days <= 120 and (
            end not in direct or start > direct[end][0]
        ):
            direct[end] = (start, value)

    derived: dict[date, float] = {}
    by_start: dict[date, list[tuple[date, float]]] = {}
    for (start, end), (_filed, _accn, value) in selected.items():
        by_start.setdefault(start, []).append((end, value))
    for start, rows in by_start.items():
        previous_end = start - timedelta(days=1)
        previous_value = 0.0
        for end, value in sorted(rows):
            segment_days = (end - previous_end).days
            if 55 <= segment_days <= 125 and end not in direct:
                derived[end] = value - previous_value
            previous_end = end
            previous_value = value

    output = dict(derived)
    output.update({end: row[1] for end, row in direct.items()})
    return dict(sorted(output.items()))


def _annual_series(facts: dict, tags: tuple[str, ...], unit: str) -> dict[date, float]:
    selected: dict[date, tuple[str, str, float]] = {}
    for tag in tags:
        for row in _unit_rows(facts, tag, unit):
            if row.get("form") != "10-K":
                continue
            try:
                start = date.fromisoformat(str(row["start"]))
                end = date.fromisoformat(str(row["end"]))
                value = float(row["val"])
            except (KeyError, TypeError, ValueError):
                continue
            if not 250 <= (end - start).days <= 380:
                continue
            candidate = (str(row.get("filed", "")), str(row.get("accn", "")), value)
            if end not in selected or candidate[:2] > selected[end][:2]:
                selected[end] = candidate
    return {end: row[2] for end, row in sorted(selected.items())}


def _latest_instant(
    facts: dict,
    tags: tuple[str, ...],
    unit: str,
) -> tuple[date, float] | None:
    candidates: list[tuple[date, str, str, float]] = []
    for tag in tags:
        for row in _unit_rows(facts, tag, unit):
            if row.get("form") not in {"10-Q", "10-K"}:
                continue
            try:
                end = date.fromisoformat(str(row["end"]))
                value = float(row["val"])
            except (KeyError, TypeError, ValueError):
                continue
            candidates.append(
                (end, str(row.get("filed", "")), str(row.get("accn", "")), value)
            )
    if not candidates:
        return None
    end, _filed, _accn, value = max(candidates)
    return end, value


def _unit_rows(facts: dict, tag: str, unit: str) -> list[dict[str, Any]]:
    fact = facts.get(tag)
    units = fact.get("units") if isinstance(fact, dict) else None
    rows = units.get(unit) if isinstance(units, dict) else None
    return rows if isinstance(rows, list) else []


def _latest_date(series: dict[date, float]) -> str | None:
    return max(series).isoformat() if series else None


def _latest_common_date(*series: dict[date, float]) -> str | None:
    if not series:
        return None
    common = set(series[0])
    for values in series[1:]:
        common &= set(values)
    return max(common).isoformat() if common else None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
