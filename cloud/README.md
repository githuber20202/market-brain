# Market CORE for Codex Cloud

This project packages the existing deterministic Market CORE engine and a
coordinated agent workflow for an on-demand Codex task. The parent coordinates
four roles: market data, news/profitability, risk evaluation, and independent
review. Data and news can run concurrently; risk and review run after inputs
are available. `AGENTS.md` defines the workflow. Model choice is inherited from
the signed-in task, with at most three concurrent child agents.

The public repository contains the launcher, role instructions and release
hashes. The actual policy, canonical source identities and pinned algorithm
remain in a separately supplied private bundle. The bundle is a byte-preserving
deployment copy of the existing release, not another source of trading policy.

## Prepare a private environment

1. In ChatGPT choose **Work in → Cloud → Select environment → Create environment**.
2. Select `githuber20202/market-brain`, branch `codex/market-core-cloud` where the
   UI supports branch selection. Otherwise ask the setup task to check out that
   branch and verify its commit before setup. Keep access **Only me**.
3. Run `bash cloud/setup.sh`. Python 3.11+ and the standard library suffice;
   the legacy application dependencies and services are not needed.
4. Supply the private algorithm ZIP inside this private environment and run:

   ```bash
   python3 cloud/market_core.py import-bundle /absolute/path/to/private-bundle.zip
   python3 cloud/market_core.py verify
   python3 cloud/market_core.py regression
   ```

5. Review the actual setup results and select **Publish**. Confirm that the UI
   says **Environment published**. Saving files alone does not publish it.
6. Start a new task using [CLOUD_FIRST_TASK.md](CLOUD_FIRST_TASK.md). Verify that
   it can see the private bundle and connected tools. Cloud environment access
   and connector availability must be tested there.

Cloud configuration and publishing are service actions; this script does not
create an environment. A successful local regression is not a cloud receipt.
Standalone custom TOML agents are documented for local Codex; hosted tasks use
the explicit role prompts when custom loading is unavailable. Verify actual
subagent availability and execution, not only the files' existence.

## Run the existing algorithm

```bash
python3 cloud/market_core.py normalize --input /private/raw-broker-envelope.json
python3 cloud/market_core.py evaluate \
  --evidence /private/current-evidence.json \
  --read-trace /private/current-canonical-reads.json
```

The original engine and its acceptance fixtures define the evidence schema.
The trace must contain actual current canonical metadata and full read segments,
not a generated success flag. Read the private source-of-truth before collecting
inputs. Fresh quotes are collected after research and refreshed before delivery
if they expire. The runner evaluates one candidate; the parent orchestrates
screening and research. It is not a continuous market feed, autonomous scanner,
order executor or automatic exit engine.

Every imported byte must match `release-lock.json` before any private helper is
executed. Imports reject missing/extra files, duplicate paths, symlinks and
modified code. Existing releases are never overwritten. A future approved
release needs a coordinated canonical update, new bundle and reviewed lock.
Source-version changes block use of an outdated bundled policy.

## Persistence and cost

Commit code changes through normal review. Keep policy and live receipts private.
Do not commit `.market-core`, broker data, account state or credentials. New
cloud tasks have separate workspaces; verify availability of the published
bundle and reconnect data sources as required.

The runner makes no model API calls and needs no OpenAI API key. Codex agents
consume the user's plan allowance; agents and frequent runs can increase usage.
This project does not promise unlimited subscription usage, a response-time SLA,
unattended all-day execution or positive trading expectancy. Scheduling remains
inactive until a supported schedule and its actual behavior are separately set
up and verified.

Official setup references (checked 2026-10-05):

- https://learn.chatgpt.com/docs/environments/cloud-environments
- https://learn.chatgpt.com/docs/agent-configuration/subagents
- https://learn.chatgpt.com/docs/config-file/config-reference
