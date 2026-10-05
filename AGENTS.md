# Market CORE — Codex project instructions

## Scope and authority

The user authorized a Codex Cloud project on 2026-10-05. `cloud/` is the new
portable launch path. The existing `src/market_brain`, deployment instructions,
old docs, static universes and Position Twin are historical reference; do not
start their daemons or treat their policies as the current strategy.

Read the private `.market-core/policy/01-MARKET_SOURCE_OF_TRUTH.md` completely,
then its Rules, Runtime and Manifest. Those exact canonical resources remain the
policy authority. `cloud/release-lock.json` pins a portable copy, not new trading
rules. Run `python3 cloud/market_core.py verify` before using it. If unavailable
or conflicting, report the blocker and do not invent a substitute algorithm.

The project setup is authorized; this does not establish that a cloud environment
has been published, that connectors work there, or that scheduled operation is
active. Keep manual trading, on-demand requests and all canonical thresholds.
No broker orders, autonomous weight changes, or revival of legacy scheduling.

## Agent workflow

For a Market CORE research/entry request, delegate independent work to subagents:

1. `market_data`: collect and validate source data and instrument identity.
2. `market_news`: investigate company profitability, direct catalysts, and both
   positive and negative news with dated sources.
3. Join their results. `market_risk` evaluates the assembled evidence with the
   pinned deterministic engine. Refresh market inputs after lengthy research.
4. `market_review` independently checks authority, evidence freshness, missing
   fields and gate output before the coordinator writes the final answer.

Maximum three concurrent child agents; data collection and news can overlap,
but risk and final review depend on their inputs. For a short refresh, reuse
only still-current non-live research. Never reuse old quotes as live evidence.
The coordinator alone assembles private receipts and user-facing conclusions.
Subagents return findings and sources; no parallel edits to shared evidence.

Role definitions are in `.codex/agents/*.toml`. Custom TOML agent loading is
documented for local Codex clients; verify it in the selected Cloud surface.
If custom types are unavailable but subagents are exposed, spawn built-in
agents with the exact role instructions from those files. Report which route
actually ran. If delegation is unavailable, report that limitation; do not
claim that agents ran. Inherit the signed-in model and permission controls.

## Data and decisions

Use the current IBKR MARKET_UNIVERSE and IBKR authoritative market evidence.
Use the connected tools exposed in this cloud task; this chat's connectors are
not proof of access in another task. Never switch to Alpaca or manufacture a
broker endpoint, token, source timestamp, account value, or a passed check.

Before live evaluation, freshly list and completely read the canonical source
versions from the user's connected Library, preserving original metadata and
read segments in a trace accepted by the pinned resource-read verifier.
Compare to `.market-core/source-lock.json`; a changed release requires a reviewed
bundle update. A saved read trace is not proof of current source versions.

The Python runner supplies normalization, entry gates and offline regression.
It is not a full autonomous scanner or an exit engine. News/source assessment
remains the agents' job. Entry rejection is not an exit recommendation.
Use `NO_TRADE` only when supported; missing evidence is `DATA_UNAVAILABLE`.
Passing tests do not prove source truth, cloud activation or profitability.

## Commands

- `bash cloud/setup.sh`: verify Python and the public launcher; no services.
- `python3 cloud/market_core.py import-bundle /absolute/private-bundle.zip`
- `python3 cloud/market_core.py verify`
- `python3 cloud/market_core.py regression`
- `python3 cloud/market_core.py normalize --input /absolute/raw-envelope.json`
- `python3 cloud/market_core.py evaluate --evidence /absolute/evidence.json --read-trace /absolute/read-trace.json`

The live command uses the system clock. `--test-now` requires explicitly
synthetic evidence and preserves the `SIMULATED_` output label.

## Privacy, verification and completion

The repository is public. Keep private bundles, policy contents, source IDs,
account/position data, broker responses and live receipts in `.market-core/`
or another private location; never commit them. Stage files explicitly.
No OpenAI API key is needed for this Python runner. Do not add paid API calls.
Codex usage still consumes the signed-in plan allowance; do not promise unlimited
capacity, near-real-time response, continuous runtime or systematic profits.

Run the launcher tests for launcher changes and the full private regression
for policy/engine changes. Preserve canonical helper bytes and hash pins.
Do not merge to main just to complete setup. Use the prepared project branch.

Cloud completion requires a published environment identifier, the selected
repository/branch/commit, a new cloud task identifier, actual test output, and
an observed agent/tool capability check. Keep these distinct from local tests.
Report setup, cloud execution and live-data readiness as separate facts.
