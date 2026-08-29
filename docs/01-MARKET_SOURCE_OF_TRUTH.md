# MARKET BRAIN V4 — Source of Truth

Version: `BROKERLESS-2026-08-28.1`

## Mission

Find intraday momentum setups in U.S. equities with a plausible 1%–5% move, while prioritizing capital preservation, evidence quality, no-chase discipline and deterministic exits.

## Authority order

1. User-confirmed execution events in the append-only Trade Event Ledger.
2. Materialized Position Twin derived from that ledger.
3. Software-defined Risk Wallet derived from confirmed events.
4. Current authoritative market-data feed.
5. Time-bounded Evidence Cards for catalyst and business quality.

There is no account connector, account snapshot, watchlist synchronization or order interface.

## Position truth

A position exists only after the user confirms an actual fill with plan id, fill price and quantity. Trades made outside the system are invisible and must be imported explicitly as legacy positions. Unknown positions are fail closed.

## Capital truth

Position sizing uses a user-seeded Risk Wallet, not a financial account balance. The wallet reserves cash and risk when BUY_NOW is emitted, converts the reservation after fill confirmation and releases capital after exit confirmation.

## Two-speed cognition

- Slow Brain: company quality, moat, balance sheet, management, valuation and catalyst evidence. It emits expiring Evidence Cards.
- Fast Reflex: price, BBO, volume, VWAP, opening range, retest, relative strength, extension and risk/reward. It is deterministic and runs on every relevant event.

AI is never in the hot execution path and cannot bypass deterministic rules.

## Strategy lanes

- CORE_MOMENTUM: quality score influences risk from 0% to 100% of the standard budget.
- EVENT_MOMENTUM: strong event evidence may trade medium-quality companies at reduced risk.
- SPECULATIVE: disabled by default.

## BUY_NOW

Allowed only when a non-expired Trade Plan has authoritative market data, fresh BBO, acceptable spread, valid retest, price above VWAP, trigger reached, no chase, valid 1:1.5 and 1:2 targets, and Risk Wallet capacity.

## SELL_NOW

Allowed only for a position present in the Position Twin. It is deterministic when stop is breached, breakout fails below VWAP, or the time stop expires while the trade is below entry. Targets produce TRIM or TAKE_PROFIT.

## Learning

Every plan, rejection, reservation, fill confirmation, position decision and exit confirmation is an event. Shadow outcomes are stored separately from real confirmed positions. Model or rule changes require replay and out-of-sample validation.

## Non-negotiable boundaries

- No direct financial-account access.
- No credentials for a financial institution.
- No automatic order submission.
- No averaging down.
- No stop widening.
- No stale plan reuse.
- No trade management for an unknown position.

