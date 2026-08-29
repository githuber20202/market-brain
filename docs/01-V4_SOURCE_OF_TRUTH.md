# MARKET BRAIN V4 — Source of Truth

## Business objective

Discover high-quality short-horizon opportunities, produce deterministic entry and exit decisions, preserve capital, and maintain an auditable lifecycle for every signal.

## Architectural reset

The system has no account connectivity. It does not read balances, positions, orders, fills, or identifiers from any execution venue. It does not submit orders.

Instead, it maintains a credential-free **Portfolio Twin** from only two event classes:

1. **Portfolio reconciliation** — an explicit full-replace statement of cash and open positions.
2. **Execution acknowledgement** — an explicit confirmation that the user executed a previously issued trade or exit action.

## Decision authority

A `BUY_NOW` decision requires all of the following:

- a verified catalyst or continuation case;
- momentum, liquidity, relative-strength, structure, and risk-reward gates;
- a fresh full-market source or a price-consistent quorum of independent market sources;
- a fresh reconciled Portfolio Twin;
- a deterministic risk-envelope pass;
- a complete Trade Passport: quantity, entry, stop, TP1, TP2, slippage cap, and expiry.

A `SELL_NOW` decision requires:

- an open position in the Portfolio Twin;
- a stored position passport;
- a live deterministic exit condition such as stop breach, failed breakout below VWAP, or time-stop invalidation.

## State machines

Candidate:

`RADAR → QUALIFIED → ENTRY_READY → BUY_NOW | NO_TRADE | EXPIRED`

Trade intent:

`ISSUED → FILLED | CANCELLED | EXPIRED`

חלון ההפעלה נגזר מחלון ה־retest שלאחר הפריצה: כאשר נרשם `TRIGGER_HIT`,
תוקף ה־Plan מוארך, אם נדרש, עד חמש דקות לאחר סוף
`RETEST_WINDOW_MINUTES`. Plan שלא נרשם עבורו `TRIGGER_HIT` ממשיך לפוג לפי
ה־TTL המקורי. ההארכה נשמרת בתוך ה־Plan ובשדה `extended_expires_at` של אירוע
`TRIGGER_HIT`, ולכן היא דטרמיניסטית וניתנת לשחזור.

Position:

`ACTIVE → HOLD | TRIM_NOW | TAKE_PROFIT | SELL_NOW → ACKNOWLEDGED`

Shadow trade:

`OPEN → STOPPED | TP1 → STOPPED | TP2 | TIME_STOP`

`shadow_trades` היא ההטלה החומרית של טריידים וירטואליים שנפתחו מאירוע
`BUY_NOW_EMITTED` כאשר `RUN_MODE=shadow`. מחיר המילוי הוא Trigger ועוד 10 bps.
כללי היציאה משותפים ל־Replay ול־Shadow, כולל Stop-first בנר שנוגע גם ב־Stop וגם
ב־Target. הטבלה אינה מייצגת פוזיציה אצל ברוקר ואינה מפעילה פקודה.

אירועי Shadow החדשים:

- `SHADOW_TRADE_OPENED` — נפתח טרייד וירטואלי יחיד עבור Plan;
- `SHADOW_TRADE_EVALUATED` — נרות SIP חדשים נבדקו וה־cursor נשמר;
- `SHADOW_TRADE_TRANSITIONED` — נשמר מעבר ל־`STOPPED`, ‏`TP1`, ‏`TP2` או
  `TIME_STOP`, כולל מצב מלא לצורך Replay.

אירועי בריאות הזרם:

- `STREAM_STALE` — אין הודעת Stream במשך הסף האנושי בשעות המסחר הרגילות;
- `STREAM_RECOVERED` — הנתונים חזרו לאחר מצב Stale.

שני האירועים נוצרים פעם אחת לכל מעבר מצב ונשלחים דרך Alert outbox. השדה
`runtime_status.stream_stale` מוצג גם ב־`/health`.

## Safety invariants

- Unknown cash or position state blocks quantity and `BUY_NOW`.
- Missing market authority blocks `BUY_NOW`.
- Unacknowledged fills never create positions.
- Unacknowledged exits never remove positions.
- No model or agent may bypass deterministic risk, structure, market-authority, or Portfolio Twin gates.
- A day with no trade is valid.
