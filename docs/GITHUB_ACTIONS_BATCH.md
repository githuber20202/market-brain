# GitHub Actions batch runtime

The keyless default runs only in Shadow mode. `shadow-radar.yml` uses delayed REST
snapshots and never starts NATS, the stream worker, or `PositionMonitor`. Therefore it
cannot emit second-level `SELL_NOW` alerts; this mode is measurement-only.

The radar workflow starts every ten minutes inside the broad UTC window. The gate
accepts delayed starts, while the batch state decides what is due: discovery runs only
for the latest scheduled slot among the 11 slots from 09:50 through 14:50 ET. Every
accepted run also watches active plans and shadow trades using only their symbols.
Older unrecorded discovery slots are persisted as `MISSED` instead of being evaluated
with present-time data.

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
Actions usage for this public repository is free; every full job still prints
`BATCH_DURATION_SECONDS` so actual usage remains measurable.
