# GitHub Actions batch runtime

The keyless default runs only in Shadow mode. `shadow-radar.yml` uses delayed REST
snapshots and never starts NATS, the stream worker, or `PositionMonitor`. Therefore it
cannot emit second-level `SELL_NOW` alerts; this mode is measurement-only.

Both scheduled workflows share one concurrency group, restore the orphan
`shadow-state` branch, require `replay_check=[]`, and replace that branch with one new
parentless commit after a successful run. The retained state is one current Postgres
dump, 14 dated dumps, `state/latest.json`, and generated reports. Intraday bars are
pruned to the five most recent sessions before each dump.

## Actions-minute estimate

The dual UTC schedules create 14 radar gate jobs and two digest gate jobs per weekday.
Only 11 radar jobs and one digest job continue past the time-zone gate. Assuming one
minute per gate and two minutes per full batch, 22 weekdays use approximately
`(14×22) + (2×22) + (11×22×2) + (1×22×2) = 880` runner minutes per month. The target
is below 1,000 minutes; every full job prints `BATCH_DURATION_SECONDS` for measurement.
