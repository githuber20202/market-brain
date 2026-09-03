import asyncio
import os
import subprocess

import pytest

from market_brain.runtime.session_state import verify_handoff, write_handoff
from market_brain.runtime.state import persist_state, publish_state_branch, restore_state


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_phase_a_to_b_real_postgres_dump_restore_and_handoff(pg_store, tmp_path):
    dsn = os.environ["TEST_POSTGRES_DSN"]
    repo = tmp_path / "repo"
    repo.mkdir()
    await asyncio.to_thread(subprocess.run, ["git", "init", "-q"], cwd=repo, check=True)
    await asyncio.to_thread(
        subprocess.run,
        ["git", "config", "user.name", "Test"],
        cwd=repo,
        check=True,
    )
    await asyncio.to_thread(
        subprocess.run,
        ["git", "config", "user.email", "test@example.invalid"],
        cwd=repo,
        check=True,
    )
    await pg_store.set_runtime_status("phase_marker", {"phase": "a"})

    await persist_state(repo, dsn, "2026-09-03", push=False)
    handoff = write_handoff(
        repo / "state",
        session_id="2026-09-03",
        workflow_run_id="501",
        last_completed_tick="2026-09-03T12:50:00-04:00",
    )
    publish_state_branch(repo, remote=None)

    await pg_store.set_runtime_status("phase_marker", {"phase": "mutated"})
    assert restore_state(repo, dsn, ref="shadow-state")
    assert verify_handoff(repo / "state", session_id="2026-09-03") == handoff
    assert await pg_store.get_runtime_status_key("phase_marker") == {"phase": "a"}

    (repo / "state" / "market.dump").write_bytes(b"mismatch")
    with pytest.raises(RuntimeError, match="HANDOFF_MISMATCH"):
        verify_handoff(repo / "state", session_id="2026-09-03")
