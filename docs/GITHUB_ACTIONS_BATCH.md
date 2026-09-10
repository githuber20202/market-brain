# GitHub Actions session runtime

GitHub Actions cron is a wake-up hint, not a minute-resolution clock. Production
evidence showed only six Radar wake-ups across three trading days where roughly 126
were expected. The observed UTC starts were `2026-08-31 19:25/23:17`,
`2026-09-01 17:12/19:58/22:32`, and `2026-09-02 17:02`; Premarket and Digest were
also delayed by hours. For that reason, one admitted workflow run now owns the market
session and waits on the New York clock in-process.

This remains a public, brokerless, delayed-data Shadow system. It does not read an
account, place orders, publish `READY`, or contain secrets. `shadow-radar.yml`,
`premarket-prediction.yml`, and `shadow-digest.yml` remain available for supervised
manual dispatch, but have no schedules.

## Ownership and schedule

`shadow-session.yml` has redundant coarse UTC wake-ups. Its gate rejects closed
NYSE days, times at or after 16:35 ET, a valid lease owned by another run, and another
queued/in-progress Shadow Session from the same ET date. `force=true` bypasses only
this workflow gate; market calendar, causality, state integrity, and policy checks
remain in force.

One admitted workflow run has three jobs, each with its own temporary Postgres:

| Job | ET ownership | Work |
|---|---|---|
| `wait` | before 07:30 | Sleep and renew public control state; no market measurement. |
| `phase_a` | 07:30–13:00 | T-30 at 09:00, T-12 at 09:18, T-3 at 09:27, then ten-minute Radar/plan-watch ticks. |
| `phase_b` | 13:00–16:35 | Ten-minute broad Radar/plan-watch ticks through 15:50 and Digest at 16:20. |

Every job restores `shadow-state` exactly once. Every measurement tick uses the
existing `BatchRuntime` in-process and then creates a database dump, updates the
public heartbeat and lease, and force-pushes the parentless `shadow-state` snapshot.
Heartbeat-only maintenance pulses keep the lease alive during long waits. No passed
measurement is recomputed: expired slots are recorded as `MISSED`.

## Durable control evidence

The public state branch contains:

- `state/heartbeat.json`: session/run identity, phase, last scheduled/completed tick,
  next due tick, policy version, code SHA, lease expiry, and `as_of`;
- `state/lease.json`: owner and a 25-minute expiry; a stale lease may be recovered;
- `state/handoff.json`: A→B ownership plus SHA-256 of the restored `market.dump`;
- `state/latest.json`: separate `workflow_status`, `session_status`,
  `planning_status`, and `learning_status` values;
- `reports/radar/<date>.csv`: the full eligible universe and every score component
  for each discovery slot, retained for the latest ten session files.

Phase B verifies the exact dump hash recorded by phase A. A same-day mismatch fails
closed as `HANDOFF_MISMATCH`, creates an Issue alert, and exits non-zero. A new run
that resumes after 13:00 without a same-day handoff is allowed and logs
`HANDOFF_ABSENT reason=RESUME`.

## Honest coverage

Digest coverage is derived from the fixed schedule, not from the events that happen
to exist: 37 discovery slots, every ten minutes from 09:50–15:50 ET, plus three
Premarket checkpoints. The
Digest and Issue print:

`Session coverage: radar expected=37 ok=… unavailable=… missed=… never_ran=…; premarket expected=3 ok=… missed=…`

The independent states are:

- `workflow_status`: `COMPLETED` or `FAILED`;
- `session_status`: `COMPLETE`, `INCOMPLETE`, or `NEVER_RAN`, based on scheduled
  Discovery and Premarket evidence;
- `planning_status`: `COMPLETE`, `INCOMPLETE`, `BLOCKED`, or `NEVER_RAN`;
- `learning_status`: `READY` only when the scheduled observational evidence is
  complete and usable, otherwise `BLOCKED`.

Each `RADAR_RUN` also stores `discovery_status`, `planning_status`, and candidate-level
`planning_failures`. A Planning data failure keeps plan creation fail-closed for the
entire slot while preserving a valid full-universe ranking for learning. Market-anchor
failure, an excessive universe failure ratio, or no usable ranking remains a Discovery
failure and therefore blocks session learning.

An incomplete session appends `SESSION_INCOMPLETE` with the exact slot list to the
ledger so weekly and replay reports cannot treat the date as complete. A date with no
events begins the Digest text with `NEVER_RAN`, rather than presenting zero activity
as a successful session.

## Recovery domains

`shadow-watchdog.yml` checks every 30 minutes during its UTC window. Before 16:20 ET,
it dispatches `shadow-session.yml` only when there is no valid lease and no active or
queued session run. This is a recovery watchdog, never a catch-up calculator.

The independent live-side Recovery Trigger reads the raw public heartbeat at 08:30
ET. If there is no heartbeat for the current ET date, it commits
`triggers/<YYYY-MM-DD>.json` to `session-trigger`. The resulting `push` event wakes
the same session gate without using Actions cron. It sends no account, order, fill,
position, or other personal field.

Last manual fallback: at 08:30 ET, if today has no heartbeat, open **Actions → Shadow
Session → Run workflow** on `main`. Do not manufacture old checkpoints and do not
edit the lease by hand.

## Historical production-path rehearsal

`python -m market_brain.runtime.batch --mode rehearsal --session YYYY-MM-DD` runs a
completed NYSE session through the same `BatchRuntime` against a separate temporary
database. It never restores or persists `shadow-state`. `YahooReplayMarketData`
exposes only bars at or before the simulated clock, preserving causal replay.
