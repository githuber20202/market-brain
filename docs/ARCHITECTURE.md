# MARKET BRAIN V4 Architecture

## First-principles redesign

The architecture no longer treats an execution account as a source of truth. It treats the market, the decision, and the portfolio as three independent domains.

```text
┌──────────────────────── Market Reality ────────────────────────┐
│ Market data streams │ News │ Filings │ Sector and benchmark    │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────── Decision Fabric ───────────────────────┐
│ Discovery │ Features │ Buffett/Catalyst AI │ Structure │ Risk  │
│           │ Market Authority Quorum │ Trade Passport            │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
┌──────────────────────── Action Outbox ─────────────────────────┐
│ BUY_NOW │ HOLD │ TRIM_NOW │ TAKE_PROFIT │ SELL_NOW             │
└──────────────────────────────┬───────────────────────────────────┘
                               ▼
                      User executes manually
                               │
                               ▼
┌──────────────────────── Portfolio Twin ────────────────────────┐
│ Reconciliation │ Fill acknowledgements │ Positions │ Cash      │
│ Event log │ Pending intents │ Pending exits │ Realized P&L     │
└─────────────────────────────────────────────────────────────────┘
```

## Why this is structurally different

### 1. Market authority replaces account quotes

Discovery data may come from a partial venue. An executable decision requires either:

- one fresh full-market source; or
- a fresh, price-consistent quorum of at least two independent sources.

The consensus engine uses a median price and rejects excessive divergence.

### 2. Portfolio Twin replaces account reads

Cash, positions, and risk are derived from an event-sourced digital twin. The twin changes only through reconciliation and explicit execution acknowledgements. No credentials or account identifiers exist in the runtime.

### 3. Trade Intents replace ad-hoc recommendations

A `BUY_NOW` decision creates a short-lived Trade Intent. The intent reserves twin cash, carries a slippage cap, and expires. A position is created only after a valid fill acknowledgement.

### 4. Exit Actions are durable

A live position is monitored against its stored passport. When an exit condition fires, an Exit Action is issued and remains pending until acknowledged or expired. The twin never assumes that the user sold.

### 5. Agents are analysts, not controllers

Agents can analyze business quality, catalysts, management, news, and narrative risk. They cannot calculate position size, override market authority, create a position, or close a position.

## Event model

```text
MARKET_TRADE
MARKET_QUOTE
BAR_CLOSED_1M
NEWS_RECEIVED
CATALYST_VERIFIED
CANDIDATE_QUALIFIED
TRADE_INTENT_ISSUED
BUY_FILL_ACKNOWLEDGED
EXIT_ACTION_ISSUED
SELL_FILL_ACKNOWLEDGED
PORTFOLIO_RECONCILED
```

## Safety model

- No reconciliation: discovery only.
- No market authority: `ENTRY_READY`, never `BUY_NOW`.
- No risk capacity: `ENTRY_READY`, never `BUY_NOW`.
- No fill acknowledgement: no position.
- Unknown position: no `SELL_NOW` for that symbol.
- Automatic execution is not implemented.

