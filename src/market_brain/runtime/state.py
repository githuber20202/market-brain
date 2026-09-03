from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_brain.ledger.replay import replay_check
from market_brain.ledger.store import PostgresEventStore
from market_brain.orchestration.universe import load_manual_quality


def _run(
    args: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        args,
        cwd=cwd,
        env=env,
        check=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def restore_state(repo: Path, dsn: str, *, ref: str = "origin/shadow-state") -> bool:
    listing = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", ref, "--", "state", "reports"],
        cwd=repo,
        check=False,
        capture_output=True,
    )
    if listing.returncode != 0:
        print("STATE_RESTORE=EMPTY")
        return False
    for raw_path in listing.stdout.decode().splitlines():
        relative = Path(raw_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("STATE_PATH_INVALID")
        result = _run(
            ["git", "show", f"{ref}:{relative.as_posix()}"],
            cwd=repo,
            capture=True,
        )
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(result.stdout)
    dump_path = repo / "state" / "market.dump"
    if not dump_path.exists():
        raise RuntimeError("STATE_DUMP_MISSING")
    _run(
        [
            "pg_restore",
            "--clean",
            "--if-exists",
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
            "--dbname",
            dsn,
            str(dump_path),
        ]
    )
    print(f"STATE_RESTORE=PASS bytes={dump_path.stat().st_size}")
    return True


async def activate_quality_from_state(
    repo: Path,
    target: Path,
    store,
    *,
    now: datetime,
    max_age_days: int = 14,
) -> dict:
    source = repo / "state" / "quality.csv"
    timestamp = _aware(now)
    status: dict
    if not source.exists():
        status = {
            "status": "QUALITY_MISSING",
            "checked_at": timestamp.isoformat(),
            "rows": 0,
        }
    else:
        try:
            records = load_manual_quality(source)
            sources = {record.source for record in records.values()}
            if not sources.issubset({"EDGAR_AUTO", "YAHOO_FUNDAMENTALS"}) or len(sources) > 1:
                raise ValueError("QUALITY_STATE_SOURCE_INVALID")
            as_of_values = [_aware(record.as_of) for record in records.values()]
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("QUALITY_STATE_INVALID") from exc
        if not records or not as_of_values:
            status = {
                "status": "QUALITY_MISSING",
                "checked_at": timestamp.isoformat(),
                "rows": 0,
            }
        else:
            oldest = min(as_of_values)
            stale = oldest > timestamp + timedelta(minutes=5) or (
                timestamp - oldest > timedelta(days=max_age_days)
            )
            if stale:
                status = {
                    "status": "QUALITY_STALE",
                    "checked_at": timestamp.isoformat(),
                    "as_of": oldest.isoformat(),
                    "rows": len(records),
                }
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                status = {
                    "status": "READY",
                    "source": next(iter(sources)),
                    "checked_at": timestamp.isoformat(),
                    "as_of": oldest.isoformat(),
                    "rows": len(records),
                }
    await store.set_runtime_status("quality_state", status)
    print(
        "QUALITY_STATE="
        f"{status['status']} rows={status['rows']} as_of={status.get('as_of')}"
    )
    return status


def _snapshot_files(snapshot_dir: Path) -> list[Path]:
    return sorted(snapshot_dir.glob("market_????-??-??.dump"))


def create_state_dump(repo: Path, dsn: str, session_date: str) -> Path:
    state_dir = repo / "state"
    snapshot_dir = state_dir / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    dump_path = state_dir / "market.dump"
    _run(
        [
            "pg_dump",
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            "--file",
            str(dump_path),
            dsn,
        ]
    )
    snapshot_path = snapshot_dir / f"market_{session_date}.dump"
    shutil.copyfile(dump_path, snapshot_path)
    snapshots = _snapshot_files(snapshot_dir)
    for stale in snapshots[:-14]:
        stale.unlink()
    print(
        f"STATE_DUMP=PASS bytes={dump_path.stat().st_size} "
        f"snapshots={len(_snapshot_files(snapshot_dir))}"
    )
    return dump_path


def publish_state_branch(
    repo: Path,
    *,
    remote: str | None = "origin",
    branch: str = "shadow-state",
) -> str:
    git_dir = Path(
        _run(["git", "rev-parse", "--git-dir"], cwd=repo, capture=True)
        .stdout.decode()
        .strip()
    )
    if not git_dir.is_absolute():
        git_dir = repo / git_dir
    handle, index_name = tempfile.mkstemp(prefix="market-brain-state-index-")
    os.close(handle)
    Path(index_name).unlink()
    env = dict(os.environ)
    env.update(
        {
            "GIT_INDEX_FILE": index_name,
            "GIT_AUTHOR_NAME": "market-brain-actions",
            "GIT_AUTHOR_EMAIL": "actions@users.noreply.github.com",
            "GIT_COMMITTER_NAME": "market-brain-actions",
            "GIT_COMMITTER_EMAIL": "actions@users.noreply.github.com",
        }
    )
    try:
        _run(["git", "read-tree", "--empty"], cwd=repo, env=env)
        paths = ["state"]
        if (repo / "reports").exists():
            paths.append("reports")
        _run(["git", "add", "-f", "--", *paths], cwd=repo, env=env)
        tree = _run(["git", "write-tree"], cwd=repo, env=env, capture=True).stdout.decode().strip()
        commit = _run(
            ["git", "commit-tree", tree, "-m", "state: update shadow runtime snapshot"],
            cwd=repo,
            env=env,
            capture=True,
        ).stdout.decode().strip()
        _run(["git", "update-ref", f"refs/heads/{branch}", commit], cwd=repo)
        if remote:
            _run(
                [
                    "git",
                    "push",
                    "--force",
                    remote,
                    f"refs/heads/{branch}:refs/heads/{branch}",
                ],
                cwd=repo,
            )
    finally:
        Path(index_name).unlink(missing_ok=True)
    print(f"STATE_BRANCH=PASS branch={branch} commit={commit}")
    return commit


async def verify_state(dsn: str) -> list[str]:
    store = PostgresEventStore(dsn)
    try:
        differences = await replay_check(store)
    finally:
        await store.close()
    print(f"STATE_REPLAY_CHECK={differences}")
    return differences


async def persist_state(repo: Path, dsn: str, session_date: str, push: bool) -> None:
    store = PostgresEventStore(dsn)
    try:
        pruned = await store.prune_intraday_bars(5)
    finally:
        await store.close()
    print(f"INTRADAY_PRUNED={pruned}")
    create_state_dump(repo, dsn, session_date)
    publish_state_branch(repo, remote="origin" if push else None)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("restore", "verify", "persist"):
        command = sub.add_parser(name)
        command.add_argument("--dsn", required=True)
        if name != "verify":
            command.add_argument("--repo", type=Path, default=Path.cwd())
        if name == "restore":
            command.add_argument("--ref", default="origin/shadow-state")
        if name == "persist":
            command.add_argument(
                "--session-date",
                default=datetime.now(UTC).date().isoformat(),
            )
            command.add_argument("--push", action="store_true")
    quality = sub.add_parser("activate-quality")
    quality.add_argument("--dsn", required=True)
    quality.add_argument("--repo", type=Path, default=Path.cwd())
    quality.add_argument("--target", type=Path, default=Path("data/quality.csv"))
    quality.add_argument("--now", help="UTC/offset ISO timestamp; tests use only")
    args = parser.parse_args()
    if args.command == "restore":
        restore_state(args.repo.resolve(), args.dsn, ref=args.ref)
    elif args.command == "verify":
        raise SystemExit(1 if asyncio.run(verify_state(args.dsn)) else 0)
    elif args.command == "activate-quality":
        async def activate() -> None:
            store = PostgresEventStore(args.dsn)
            try:
                now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
                await activate_quality_from_state(
                    args.repo.resolve(),
                    args.target.resolve(),
                    store,
                    now=now,
                )
            finally:
                await store.close()

        asyncio.run(activate())
    else:
        asyncio.run(
            persist_state(args.repo.resolve(), args.dsn, args.session_date, args.push)
        )


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


if __name__ == "__main__":
    main()
