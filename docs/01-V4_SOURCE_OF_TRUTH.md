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

## Slow Brain lite — documented fundamentals quality

`MANUAL`, ‏`EDGAR_AUTO` ו־`YAHOO_FUNDAMENTALS` הם מקורות איכות מתועדים
שמותרים ב־CORE lane. `QUALITY_SOURCE=yahoo` הוא ברירת המחדל בסביבת GitHub
Actions, משום ש־SEC מחזיר `403` ל־GitHub-hosted runners; ‏`edgar` נשאר מסלול
אופציונלי. קובץ `state/quality.csv` נבנה מחדש בכל ריצת Weekly; ריצת
Radar מעתיקה אותו אל `data/quality.csv` רק כאשר גיל כל שורות `as_of` אינו עולה
על 14 ימים. קובץ חסר או ישן אינו משמש לתכנון ומופיע בדיגסט כ־
`QUALITY_MISSING` או `QUALITY_STALE`.

במסלול EDGAR המקור הוא הדוחות הרשמיים ב־SEC. במסלול Yahoo המקור הוא endpoint
ציבורי ללא מפתח שמציג עיבוד של נתוני הדוחות, ולא את הדוחות עצמם. ה־provenance
נשמר כ־`YAHOO_FUNDAMENTALS`, ואין להציג אותו כמקור SEC רשמי. הכנסות, רווח
תפעולי, חוב, מזומן, FCF ומספר מניות נלקחים מסדרות annual/quarterly; כאשר אין
שמונה רבעונים לצמיחת YoY או לדילול, משתמשים בשתי נקודות annual האחרונות.

ה־Universe מגדיר `instrument_type`. רק `EQUITY` נכנס לרענון איכות. `ETF`
ו־`UNRESOLVED` מדולגים במפורש ואינם נספרים כנתון איכות חסר, משום שציון איכות
חברה אינו חל עליהם.

הציון דטרמיניסטי ומחושב מארבעה מדדים, 0–25 נקודות לכל מדד. נתון חסר מקבל 0
במדד שלו ומסמן `partial=true`; אין השלמת ערכים משוערים.

| מדד | 25 | 20 | 15 | 10 | 5 | 0 |
|---|---:|---:|---:|---:|---:|---:|
| צמיחת הכנסות YoY | ≥20% | ≥10% | ≥5% | ≥0% | ≥-10% | <-10% או חסר |
| מרווח תפעולי | ≥25% | ≥15% | ≥10% | ≥5% | ≥0% | <0% או חסר |
| מינוף | ≤0× | ≤1× | ≤2× | ≤3× | ≤4× | >4× או חסר |
| מרווח FCF | ≥20% | ≥15% | ≥10% | ≥5% | ≥0% | <0% או חסר |

הכנסות, רווח תפעולי, תזרים תפעולי, Capex ומניות בדילול מלא משתמשים בארבעת
הרבעונים האחרונים. מינוף הוא חוב ארוך + חוב שוטף פחות מזומן, חלקי רווח
תפעולי שנתי; אם הרווח התפעולי אינו חיובי, היחס החלופי הוא חוב חלקי הון עצמי.
דילול YoY מפחית 0 נקודות כאשר אינו חיובי, ואז 2/5/10/15 נקודות בספים
≤2%/≤5%/≤10%/>10%. נתון דילול חסר אינו מקבל קנס אך מסמן את הציון כחלקי.

## Plan geometry floors

`build_trade_plan` אוכף את אותן רצפות ב־Radar וב־Replay, לפני שמותר לשמור Plan:

- `MIN_RISK_PCT=0.5`: ‏`(entry − stop) / entry` חייב להיות לפחות 0.5%. כך
  slippage של 10bps אינו עולה על `0.2R`. הרצפה נגזרת מכלל העלויות ואינה
  מכוילת לפי טרייד היסטורי יחיד.
- `MIN_OPENING_RANGE_PCT=0.3`: ‏`(opening_range_high − opening_range_low) /
  opening_range_high` חייב להיות לפחות 0.3%.
- `TP1` חייב להיות גבוה מ־`entry_zone_high` גם לאחר rounding.

הפרות נכשלות סגור עם `RISK_TOO_SMALL`, ‏`OPENING_RANGE_TOO_NARROW` או
`TARGET_BELOW_ENTRY`. הסיבה נשמרת במועמד של אירוע `RADAR_RUN`, וה־Daily Digest
מציג `plan_rejections` לפי reason. הרצפות מסננות רעש בלבד; הן אינן משנות את
גאומטריית `entry=OR high`, ‏`stop=retest low`, ‏`TP1=1.5R`, ‏`TP2=2R`.

## Intraday bar provenance label

השדה הפנימי `source="SIP"` ב־`intraday_bars` מציין נרות מאושרים מהספק שמוגדר
למסלול ההיסטורי, ולא בהכרח מוצר Alpaca SIP. במצב `keyless_delayed` הנרות מגיעים
מ־Yahoo; במצב Alpaca הם מגיעים מ־SIP בהתאם למגבלת הפיגור. ה־snapshot שומר בנוסף
את `source_id`, ‏`fetched_at` ו־`delay_minutes`, והם המקור המדויק ל־provenance.
בחזרה גנרלית היסטורית, `source_id=YAHOO_REPLAY`: אותו chart ציבורי נמשך פעם אחת
לסימבול ול־timeframe, אך בכל tick נחשפים רק נרות שזמנם קטן או שווה לשעון המדומה.
ציטוט Cboe אינו משמש בחזרה, כדי שמחיר מסגירת היום לא יאומת מול מחיר מוקדם יותר.
מצב זה הוא כלי בדיקה למסלול הייצור ואינו משנה את תוצאות ה־Replay האסטרטגי.

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
