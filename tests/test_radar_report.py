import csv
from datetime import datetime, timedelta

from market_brain.orchestration.universe import EASTERN
from market_brain.runtime.radar_report import append_radar_csv


def _payload(day: int, run_id: str):
    return {
        "run_id": run_id,
        "scheduled_for": datetime(2026, 8, day, 9, 50, tzinfo=EASTERN).isoformat(),
        "status": "COMPLETED",
        "rankings": [
            {
                "rank": 1,
                "symbol": "AAPL",
                "data_status": "OK",
                "catalyst_or_continuation": 0.0,
                "price_momentum": 20.0,
                "volume_liquidity": 15.0,
                "relative_strength_sector": 10.0,
                "entry_invalidation_structure": 15.0,
                "risk_reward": 10.0,
                "total": 70.0,
                "discovery_total": 70.0,
                "reasons": ["CATALYST_UNVERIFIED"],
                "last": 101.0,
                "volume": 1_000_000,
                "relative_volume": 2.0,
                "plan_id": "plan-1",
            },
            {
                "rank": None,
                "symbol": "MSFT",
                "data_status": "MISSING",
                "catalyst_or_continuation": None,
                "price_momentum": None,
                "volume_liquidity": None,
                "relative_strength_sector": None,
                "entry_invalidation_structure": None,
                "risk_reward": None,
                "total": None,
                "discovery_total": None,
                "reasons": ["HTTP_429"],
                "last": None,
                "volume": None,
                "relative_volume": None,
                "plan_id": None,
            },
        ],
    }


def test_full_radar_csv_is_append_only_per_slot_and_retains_ten_sessions(tmp_path):
    report = append_radar_csv(_payload(20, "radar:2026-08-20:0950"), tmp_path)
    append_radar_csv(_payload(20, "radar:2026-08-20:0950"), tmp_path)

    with report.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert [row["symbol"] for row in rows] == ["AAPL", "MSFT"]
    assert rows[0]["risk_reward"] == "10.0"
    assert rows[1]["reasons"] == "HTTP_429"

    start = datetime(2026, 8, 21, tzinfo=EASTERN)
    for offset in range(11):
        stamp = start + timedelta(days=offset)
        payload = _payload(stamp.day, f"radar:{stamp.date().isoformat()}:0950")
        payload["scheduled_for"] = stamp.replace(hour=9, minute=50).isoformat()
        append_radar_csv(payload, tmp_path)
    files = sorted((tmp_path / "radar").glob("*.csv"))
    assert len(files) == 10
    assert files[-1].name == "2026-08-31.csv"
