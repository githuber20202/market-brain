# Deployment Plan — V4

## Stage A — deterministic validation

- unit and API regression;
- runtime configuration validation;
- replay of market snapshots;
- Portfolio Twin reconciliation and fill lifecycle tests;
- market-source divergence and stale-data tests.

## Stage B — shadow host

Deploy the API, market stream worker, PostgreSQL, Redis, NATS, and Temporal on a persistent Linux host. Configure market-data credentials only.

## Stage C — market authority

Discovery can start with a partial feed. Advisory decisions require a full-market subscription or a second independent source for quorum validation.

## Stage D — operator channel

Connect a mobile action inbox or push channel. Every card must carry the Trade Intent or Exit Action id so execution acknowledgements are unambiguous.

## Stage E — portfolio reconciliation

Before the first advisory decision of each session, reconcile cash and open positions. A reconciliation can be entered manually or imported from a user-provided statement or screenshot, but never through account connectivity.

## Stage F — multi-session shadow

Measure:

- discovery recall;
- false positives;
- entry latency;
- stop-first rate;
- MFE and MAE over 5, 15, and 30 minutes;
- missed winners;
- stale twin incidents;
- intent expiry and unacknowledged action rates.

## Stage G — advisory live

Advisory live is allowed only when market authority, Portfolio Twin freshness, alert delivery, and deterministic risk gates are all operational. Automatic execution remains disabled.

