# Market Brain public receipt summaries

This branch is a one-way display feed for the Market Brain dashboard.
It contains no brokerage connection, credentials, account records or trading actions.

`feed.json` contains strictly validated research summaries: symbols, recorded decisions/ranks, receipt timestamps and reported delivery status. It is not a live broker read and does not authorize trading.

Original receipts remain at their private source. No free text, news URLs, quotes, broker identifiers, balances, positions or orders are exported. `tools/` contains the deterministic projection and validation code; it reads explicitly supplied local files and has no network or broker capabilities.

This branch has no GitHub Actions workflows. Do not merge it into application/research branches. Update only feed.json with the contents API and its current blob SHA. Conflicts require a reread; never force a branch update. The website reads this fixed public feed through its backend.

The initial export contains real September 11 receipts. Scheduled publication is pending activation; file presence is not proof of a future scheduled run.
