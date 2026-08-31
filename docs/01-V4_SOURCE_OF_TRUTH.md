# MARKET BRAIN V4 — Source of Truth

## Business objective

Discover high-quality short-horizon opportunities, produce deterministic entry and exit decisions, preserve capital, and maintain an auditable lifecycle for every signal.

## Architectural reset

The system has no account connectivity. It does not read balances, positions, orders, fills, or identifiers from any execution venue. It does not submit orders.

Instead, it maintains a credential-free **Portfolio Twin** from only two event classes:

1. **Portfolio reconciliation** — an explicit full-replace statement of cash and open positions.
2. **Execution acknowledgement** — an explicit confirmation that the user executed a previously issued trade or exit action.

## Decision authority

### Premarket Prediction Funnel

לפני הפתיחה המערכת מפיקה השערה מדידה, ולא אישור ביצוע. בכל יום מסחר נוצרים
שלושה batches עצמאיים לפי `America/New_York`: ‏`T-30` ב־09:00, ‏`T-12` ב־09:18
ו־`T-3` ב־09:27. כל batch מושך מחדש מחיר ונפח Premarket, חדשות ישירות לסימבול,
פרופיל ADV והקשר SPY/sector. ה־Universe הקנוני נבדק תמיד ב־61 שורות audit,
כולל `MRNA` ו־`MRVL`; גילוי מניות חיצוניות מ־Yahoo נשמר בנפרד ואינו מחליף שורת
audit. מזהה `UNRESOLVED` נשאר `MISSING` ואינו נכנס לדירוג.

הציון הוא 0–100, במשקלים קבועים:

| רכיב | משקל |
|---|---:|
| Catalyst או continuation מאומת | 20 |
| Gap ומומנטום מחיר | 20 |
| נפח Premarket ונזילות | 20 |
| חוזקה יחסית מול sector או SPY | 15 |
| מבנה, VWAP והידרדרות | 15 |
| Risk/Reward | 10 |

לפני הפתיחה אין גאומטריית כניסה תקפה, לכן רכיב Risk/Reward נשאר `MISSING=0`
והציון מוגבל ל־79. ללא Catalyst ישיר, מסווג וממקור אמין, הציון מוגבל ל־74.
הפלט הוא Top 10 ועד שתי מועמדות `PREDICTION/WATCH`; הוא לעולם אינו `READY`,
אינו כולל Trigger/Stop/Targets/quantity ואינו מאפשר פעולה אצל ברוקר.

Premarket Deterioration מאושר כאשר מתקיימים לפחות שניים מהבאים: מרחק של 1% או
יותר מהשיא, תשואת 15 דקות של ‎-0.5% או פחות, ושני lower highs. מועמד כזה חסום
מהגמר. הידרדרות חמורה היא מרחק של 2% או יותר בצירוף תשואת 15 דקות של ‎-1% או
פחות או שני lower highs. כל checkpoint משווה לציון ולמחיר של קודמו; אם batch
קודם חסר, `delta_state=DELTA_UNAVAILABLE` ללא השלמה משוערת.

נתוני Yahoo הציבוריים מסומנים `DELAYED`; כשל ב־SPY או שיעור כשל מעל 20% סוגר
את ה־batch כ־`DATA_UNAVAILABLE`. לאחר הפתיחה בלבד רשאים שערי Opening Range,
VWAP, ‏Retest, סמכות נתון ו־risk envelope להפיק `READY/BUY_NOW`. ה־Daily Digest
מקשר את מועמדות checkpoint האחרון למניות שנראו ב־Radar ולאלו שאושרו לאחר
הפתיחה, לצורך Shadow learning בלבד. לאחר סיום המסחר נשמר
`PREMARKET_LEARNING_REVIEW` עם MFE, ‏MAE, תשואות קדימה אחרי 5/15/30/60 דקות
ו־EOD לכל הופעה ב־Top 10. חוסר ב־checkpoint, מחיר ייחוס או נרות מסומן
`LEARNING_DATA_INCOMPLETE`; הסקירה אינה משנה משקלים ואינה מאפשרת פעולת ברוקר.

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

במצב `keyless_delayed`, שער ההפעלה אינו דורש BBO כאשר הוא חסר: סמכות הנתון
נקבעת בשער ה־keyless, וה־ADV המחמיר נשאר תנאי חובה. כאשר bid/ask תקינים כן
קיימים (למשל מ־Cboe), ה־spread נבדק מול `max_spread_pct`. במסלול Alpaca/IEX
BBO נשאר חובה, וחסרונו מחזיר `BBO_MISSING`.

## Ranking inputs and Radar/Replay parity

ציון ה־Radar משתמש בנתוני ייצור מלאים ולא בערכי ברירת מחדל שמוזנים רק בטסטים:

- בתחילת יום מסחר נשמר לכל ה־Universe פרופיל נזילות יומי עם `adv20`. לאחר מכן
  אותה רשומה ממוחזרת בכל slots של אותו יום; רענון חסר נכשל סגור ומתועד.
- נפח הייחוס לשעה הוא `adv20 × clamp(minutes_since_09:30 / 390, 0.05, 1.0)`.
  לכן `relative_volume` משווה את הנפח המצטבר לנפח שהיה צפוי עד אותו רגע, והרצפה
  0.05 מונעת יחס מנופח בדקות הפתיחה.
- תשואת ה־benchmark מחושבת מ־SPY שנמצא באותו batch ובאותו `as_of`, ומוזרקת
  לכל snapshot לפני `compute_features`. נתוני sector נשארים חסרים כאשר אין מקור.
- אירוע `RADAR_RUN` שומר לכל מועמד את רכיבי הציון
  (`momentum/volume/relative/structure/rr/total`) וכן histogram של
  `0–20`, ‏`20–40`, ‏`40–65`, ‏`65+`; ה־Daily Digest מציג את אותו histogram.

מנוע ה־Replay בונה את אותם inputs מנרות היום ומפרופיל יומי, ואז קורא לאותן
פונקציות `compute_features` ו־`score_features`. אסור להזריק ציון סף קבוע ל־Replay;
Plan שלא הגיע לציון 65 בפועל נפסל בדיוק כמו ב־Radar.

## State machines

Candidate:

`RADAR → QUALIFIED → ENTRY_READY → BUY_NOW | NO_TRADE | EXPIRED`

מכונת ה־Intraday Retest מפרידה בין פסילה לפני ואחרי פריצה:

- במצב `ARMED`, מסחר רגיל בתוך ה־Opening Range אינו פסילה. רק
  `low < ORL − RETEST_INVALIDATION_BUFFER_R × (ORH − ORL)` מעביר ל־`INVALID`
  עם `RANGE_BREAKDOWN`.
- במצבים `BREAKOUT_SEEN` ו־`RETEST_VALID`, ‏failed breakout נשאר טרמינלי:
  `low < ORH − RETEST_INVALIDATION_BUFFER_R × (ORH − ORL)` מעביר ל־`INVALID`
  עם `RETEST_INVALIDATED_BELOW_ORH_BUFFER`.
- `compute_structure` והעדכון האינקרמנטלי `update_intraday_structure` משתמשים
  באותה פונקציית פסילה, כדי ש־Replay, Batch והמסלול הרציף יפיקו אותו state.

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
`BUY_NOW_EMITTED` כאשר `RUN_MODE=shadow`. במסלול batch המושהה בלבד, החלטת
ההפעלה נבחנת כאילו התקבלה בסגירת נר ה-retest: המחיר וה-VWAP הם הערכים המצטברים
באותו נר, המילוי הווירטואלי הוא סגירת הנר ועוד 10 bps, וזמן הפתיחה הוא סוף הנר.
ההתראה שומרת גם את מחיר הגילוי המאוחר ואת הפער ממנו. מסלול live ממשיך לבחון את
מחיר השוק העדכני בזמן ההפעלה. מאחר שנרות Yahoo אינם כוללים VWAP לכל נר,
ה־running VWAP במסלול זה נגזר מ־volume-weighted typical price, באותה נוסחה שבה
משתמש snapshot של Yahoo; נר ללא volume נשאר ללא תרומת VWAP.
נר retest זכאי להפעלת Shadow רק אם נסגר אחרי יצירת ה-plan ואחרי ה-TRIGGER_HIT
שלו. Retest מוקדם יותר נדחה כ-`RETEST_PRECEDES_PLAN_TRIGGER`; אסור לפתוח טרייד
וירטואלי רטרואקטיבית על מבנה שהושלם לפני שה-plan היה קיים.
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
