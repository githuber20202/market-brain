# First task in the published private Codex Cloud environment

Read AGENTS.md. Work on branch codex/market-core-cloud. Verify the checkout's
commit before running anything. Run bash cloud/setup.sh, import the separately
attached private Market-CORE-Algorithm bundle if it is not already installed,
then run python3 cloud/market_core.py regression.

Spawn two independent read-only agents, one for source/byte integrity and one
for the launcher and agent workflow. Use the custom roles if this surface loads
them; otherwise use built-in subagents with the role instructions in the TOML
files. Wait for both and report their actual activity/results.

Inspect the tools available in this new cloud task. Verify read access to the
canonical Library resources and IBKR MARKET_UNIVERSE if those connectors are
available. If unavailable, report the exact missing capability. Do not infer
cloud access from the originating chat. Do not place orders or write watchlists.
Do not use account data in repository files or public logs. This acceptance task
does not need an entry recommendation or a paid API key.

Return a private receipt with environment ID/URL, task ID/URL, repo/branch/commit,
UTC start/end times, test results, actual agent execution mode, source versions,
and connector results. Use null plus an explanation for unobservable fields;
never invent a cloud identifier. Distinguish OFFLINE_TESTS_PASSED,
CLOUD_EXECUTION_VERIFIED and LIVE_DATA_READY. A cloud task is not proof of a
continuously running service or near-real-time latency.
