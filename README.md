# Market Brain

Market Brain הוא רדאר שוק brokerless שפועל כברירת מחדל ב־Shadow mode עם נתונים
ציבוריים מושהים. הוא יוצר תוכניות והתראות דטרמיניסטיות לצורכי מדידה בלבד, אינו
מבצע, משנה או מבטל פקודות אצל ברוקר, ואינו ייעוץ פיננסי.

## מצב ברירת המחדל — GitHub Actions keyless

ה־workflows המתוזמנים משתמשים ב־Yahoo לנתוני intraday, daily ו־fundamentals ללא
מפתח. Postgres זמני משוחזר בתחילת כל job מהענף ה־orphan בשם `shadow-state`, עובר
`replay_check`, ובסיום נשמר חזרה כ־dump יחיד עם snapshots ודוחות. התראות נכתבות
ל־GitHub Issue יומי עם label בשם `shadow`; אין Telegram, חשבון Alpaca או שרת
שנדרשים להפעלה הזו.

ממומשים בפועל:

- טעינה ואימות של Universe, כולל סיווג `EQUITY`, ‏`ETF` ו־`UNRESOLVED`;
- משפך Premarket בשלוש נקודות זמן (`T-30`, ‏`T-12`, ‏`T-3`) עם audit מלא
  של 61/61, חדשות, תנועה, נזילות, חוזקה יחסית והידרדרות;
- discovery מתוזמן, דירוג, Opening Range, ‏trigger ו־retest בצד השרת;
- רצפות גאומטריה משותפות ל־Radar ול־Replay;
- תוכניות, `BUY_NOW` וטריידי Shadow עם event sourcing ו־state replay;
- Replay ודוחות Shadow שבועיים;
- איכות דטרמיניסטית מ־Yahoo Fundamentals עם provenance ו־partial flags;
- batch runtime ב־GitHub Actions, ‏GitHub Issues alert sink ו־state מתמיד;
- Preflight, מסלול Docker Compose ובדיקת ARM64 עבור מצב השרת האופציונלי.

מגבלות ידועות:

- הנתונים הציבוריים מושהים והריצה מתבצעת כל עשר דקות; אין ניטור בזמן אמת ואין
  `SELL_NOW` תוך שניות במצב keyless;
- SEC EDGAR חסום מ־GitHub-hosted runners ב־HTTP 403. הקוד נשאר אופציונלי, אך
  ברירת המחדל המוכחת היא `QUALITY_SOURCE=yahoo`;
- אין operator console;
- המערכת מיועדת ל־Shadow בלבד ואינה שולחת פקודות ביצוע.

## מצב אופציונלי — Oracle + Alpaca

הקוד כולל גם API רציף, Postgres, ‏NATS, stream worker, נתוני Alpaca ו־Telegram.
מצב זה דורש שרת וחשבונות חיצוניים ולכן אינו ברירת המחדל ואינו נדרש להפעלת
GitHub Actions keyless. גם בו execution וגישת חשבון ישירה חסומים בקונפיגורציה.

## נקודות כניסה

- `python -m market_brain.runtime.batch --mode premarket --checkpoint T-30|T-12|T-3`
- `python -m market_brain.runtime.batch --mode radar|digest|weekly`
- `docs/SHADOW_RUNBOOK.md` — הפעלה, צפייה בדוחות וטיפול בתקלות;
- `docs/GITHUB_ACTIONS_BATCH.md` — cadence, ‏state ותקציב הריצות;
- `docs/01-V4_SOURCE_OF_TRUTH.md` — כללי ההחלטה וה־state machines.

ה־API של מצב השרת האופציונלי כולל:

- `GET /health`
- `GET /policy`
- `GET /admin/replay-check`
- `GET /alerts?undelivered=true`
- `POST /screen`
- `POST /wallet/seed`
- `GET /wallet`
- `POST /plans`
- `POST /plans/{plan_id}/activate`
- `POST /plans/{plan_id}/release`
- `POST /fills/confirm`
- `GET /positions`
- `POST /positions/import`
- `POST /reconcile`
- `POST /positions/{position_id}/protect`
- `POST /positions/{position_id}/evaluate`
- `POST /positions/{position_id}/exit`

## ולידציה

```bash
python -m pytest -q
python -m pytest -m postgres -q --strict-markers
python scripts/validate_runtime.py
python scripts/replay_smoke.py --fixture tests/fixtures/replay_bars.json
cp .env.example .env
docker compose config -q
./scripts/compose_smoke.sh
```
