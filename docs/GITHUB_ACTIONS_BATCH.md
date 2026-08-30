# GitHub Actions batch runtime

The keyless default runs only in Shadow mode. `shadow-radar.yml` uses delayed REST
snapshots and never starts NATS, the stream worker, or `PositionMonitor`. Therefore it
cannot emit second-level `SELL_NOW` alerts; this mode is measurement-only.

The radar workflow starts every ten minutes inside the broad UTC window. The gate
accepts delayed starts, while the batch state decides what is due: discovery runs only
for the latest scheduled slot among the 11 slots from 09:50 through 14:50 ET. Every
accepted run also watches active plans and shadow trades using only their symbols.
The final accepted plan-watch tick is 15:20 ET; discovery still has exactly 11 slots
ending at 14:50 ET.
Older unrecorded discovery slots are persisted as `MISSED` instead of being evaluated
with present-time data.

## Schedule

| Workflow | Cron (UTC) | Runtime decision |
|---|---|---|
| `shadow-radar.yml` | `*/10 13-20 * * 1-5` | Gate accepts the ET radar window; batch runs only the latest due 09:50–14:50 ET discovery slot and performs plan-watch on every accepted run. |
| `shadow-digest.yml` | `20 20,21 * * 1-5` | Dual EDT/EST wake-up; batch emits at most one digest after 16:20 ET. |
| `shadow-weekly.yml` | `30 21 * * 5` | Friday quality refresh plus five-session Replay and weekly Shadow report. |

## Historical production-path rehearsal

`python -m market_brain.runtime.batch --mode rehearsal --session YYYY-MM-DD`
runs one completed NYSE session through the same `BatchRuntime` used by the scheduled
jobs. It executes every ten-minute tick from 09:50 through 15:20 ET and the 16:20 ET
digest against a separate temporary database. It never restores or persists
`shadow-state`. Alerts go to the job log; `--publish-issue` is reserved for a
supervised run and posts one summary comment to a closed `Shadow rehearsal <date>`
Issue.

`YahooReplayMarketData` downloads each symbol/timeframe chart once, then exposes only
bars at or before the simulated clock. Snapshot provenance is `YAHOO_REPLAY`; Cboe is
disabled because its current delayed quote cannot validate an earlier intraday price.
The log records every slot, candidate, rejection, plan-watch transition, Shadow
outcome, HTTP request count, tick duration, full digest text, and exception count.

Manual Radar and Digest dispatches expose `force=true`. Force bypasses only the
stateless workflow gate so a supervised off-hours job can start. It does not bypass
the NYSE calendar, due-slot checks, state replay validation, or any planning gate;
an off-session batch returns `NO_SESSION` and persists the restored state.

Both scheduled workflows share one concurrency group, restore the orphan
`shadow-state` branch, require `replay_check=[]`, and replace that branch with one new
parentless commit after a successful run. The retained state is one current Postgres
dump, 14 dated dumps, `state/latest.json`, and generated reports. Intraday bars are
pruned to the five most recent sessions before each dump.

## Actions-minute estimate

The UTC schedule creates 48 radar gate jobs and two digest gate jobs per weekday.
The ET window lets about 33 radar jobs and one digest job continue. A conservative
one-minute gate/two-minute batch estimate for 22 weekdays is
`(48×22) + (2×22) + (33×22×2) + (1×22×2) = 2,596` runner minutes per month. GitHub
Allowing five ten-minute weekly jobs adds about 50 minutes, for a conservative total
of 2,646 runner minutes per month. Actions usage for this public repository is free;
every full job still prints `BATCH_DURATION_SECONDS` so actual usage remains
measurable.
