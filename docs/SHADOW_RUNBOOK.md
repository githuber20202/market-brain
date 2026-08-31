# מדריך הפעלה לשבוע Shadow

המדריך הזה מיועד לשבוע הראשון שבו Market Brain רץ עם נתוני אמת, אבל בלי כסף ובלי
פקודות לברוקר. ברירת המחדל היא GitHub Actions עם נתוני Yahoo ציבוריים מושהים.

## מצב GitHub Actions — ברירת המחדל

במצב הזה אין שרת להקים ואין מפתחות למלא. ארבעת ה־workflows נמצאים בלשונית
**Actions** בריפו הציבורי:

| Workflow | תזמון UTC | תפקיד |
|---|---|---|
| `Premarket Prediction` | `0,18,27 13,14 * * 1-5` | שתי משמרות DST; השער מפעיל בדיוק את `T-30/T-12/T-3` לפי שעון ניו־יורק, בודק 61/61 ומפרסם עד שתי מועמדות `PREDICTION/WATCH`. |
| `Shadow Radar` | `*/10 13-20 * * 1-5` | מתעורר כל 10 דקות; הקוד מפעיל discovery רק ב־11 slots של NYSE ועוקב אחר Plans פעילים בשאר הריצות. |
| `Shadow Digest` | `20 20,21 * * 1-5` | שתי משמרות DST; הקוד יוצר digest פעם אחת אחרי 16:20 ET. |
| `Shadow Weekly` | `30 21 * * 5` | רענון איכות, Replay של 5 sessions ודוח Shadow שבועי. |

מה ולמה: כדי לראות התראות, פתח בריפו את **Issues** וסנן לפי label בשם
`shadow`. לכל יום יש Issue בשם `Shadow YYYY-MM-DD`; כל Alert הוא comment עם
הטקסט המלא, כולל `[SHADOW][DELAYED]`, ועם mention ל־`@githuber20202`.

הצלחה: מופיע Issue אחד ליום שבו נוצרה התראה. אחרי ה־digest ה־Issue נסגר; הוא לא
נמחק, ולכן עדיין ניתן לקרוא את כל ההיסטוריה.

מה ולמה: כדי לקרוא את מצב הריצה האחרון, בחר branch בשם `shadow-state` ופתח
`state/latest.json`. איכות החברות נמצאת ב־`state/quality.csv`; הדוחות נמצאים
בתיקייה `reports/` באותו branch.

הצלחה: `latest.json` מכיל `status`, ‏`mode` ו־`updated_at`; ‏`quality.csv` מכיל
שורת כותרת ושורות מניות; דוחות Replay/Shadow הם קובצי Markdown קריאים בדפדפן.

מה ולמה: כדי להשבית זמנית workflow, פתח **Actions**, בחר את שמו, פתח את תפריט
שלוש הנקודות ובחר **Disable workflow**. כדי להחזירו, בחר **Enable workflow**.

הצלחה: workflow מושבת מציג banner מתאים ואינו מקבל ריצות schedule; אחרי Enable
מופיע הכפתור **Run workflow**.

מה ולמה: להרצה ידנית, פתח את workflow, לחץ **Run workflow**, בחר branch `main`
ולחץ שוב **Run workflow**. ב־Premarket בוחרים גם checkpoint; ב־Radar וב־Digest
אפשר לבחור `force=true`. אפשרות זו
עוקפת רק את שער השעה של ה־workflow. ה־batch עדיין בודק לוח NYSE ו־state, ולכן
בשבת התוצאה התקינה היא `NO_SESSION`, ולא סריקת שוק.

הצלחה: ריצה מופיעה ברשימה. בלוג יש `STATE_RESTORE=PASS`, אחריו
`STATE_INTEGRITY=PASS`, ובסיום `STATE_BRANCH=PASS`.

GitHub עשוי להשבית schedule בריפו ציבורי לאחר 60 יום ללא פעילות. ה־commits
האוטומטיים ל־`shadow-state` הם פעילות רגילה כל עוד המערכת רצה. אם בכל זאת מופיע
banner של Disabled, בצע Enable והרצה ידנית אחת. בבדיקה תקופתית של 45 יום ודא
שלכל workflow יש ריצת schedule עדכנית; אין ליצור commits ריקים רק לצורך
keepalive.

מגבלות המצב: Yahoo הוא מקור יחיד ויכול להיות לא זמין; הנתונים מושהים; ה־batch
מתעורר כל 10 דקות; אין stream רציף ואין `SELL_NOW` תוך שניות. לכן המצב הוא
Shadow למדידה בלבד.

## כלל הבטיחות של השבוע

קוראים התראות ולומדים מהן. לא קונים, לא מוכרים ולא מזינים פקודות בעקבות התראה.
המטרה היא למדוד את המערכת, את איכות הנתונים ואת תוצאות הטריידים הווירטואליים בלבד.

## נספח אופציונלי — Oracle + Alpaca

המשך המדריך חל רק אם בעתיד נבחר מצב שרת רציף שדורש Oracle, ‏Alpaca ו־Telegram.
הוא אינו נדרש למצב GitHub Actions.

### יום 0 — בדיקת Preflight אחת

מה ולמה: היכנס לתיקיית הפרויקט כדי שכל הפקודות ישתמשו ב־Compose וב־`.env` הנכונים.

```bash
cd /opt/market-brain
pwd
```

הצלחה: השורה האחרונה היא `/opt/market-brain`.

מה ולמה: הרץ בדיקה מלאה של המארח, Postgres, NATS, Alpaca Paper ו־Telegram לפני
הפעלת המערכת.

```bash
./scripts/preflight.sh
```

הצלחה: כל רכיב מקומי וחיצוני מציג `[PASS]`, אין `[FAIL]`, ובסוף מופיע
`PREFLIGHT=PASS`. בדיקות חיצוניות אינן אמורות להיות `[SKIP]` בהרצה הזו. הודעת
`MARKET BRAIN preflight OK ...` אמורה להגיע לטלגרם.

מה ולמה: אם רוצים לבדוק רק את השרת בלי לשלוח בקשות ל־Alpaca או Telegram, משתמשים
במצב Offline.

```bash
./scripts/preflight.sh --offline
```

הצלחה: Postgres ו־NATS מציגים `[PASS]`; חמש הבדיקות החיצוניות מציגות `[SKIP]`;
בסוף מופיע `PREFLIGHT=PASS`.

### תיקון לפי קוד FAIL

אין להדביק למסך, לצ'אט או ללוג את תוכן `.env`. הפקודות הבאות מציגות מצב בלבד.

| קוד FAIL | משמעות ותיקון |
|---|---|
| `HOST_ENV_FILE` | `.env` חסר. הרץ `cp .env.example .env`, ערוך עם `nano .env`, ושמור. הצלחה: `test -f .env && echo OK` מדפיס `OK`. |
| `HOST_ENV_PERMISSIONS` | ההרשאות אינן 600. הרץ `chmod 600 .env`. הצלחה: `stat -c '%a' .env` מדפיס `600`. |
| `HOST_ENV_GIT_IGNORE` | Git אינו מתעלם מהקובץ. הרץ `git check-ignore -v .env`. הצלחה: מופיעה שורת כלל מ־`.gitignore`; אם לא, עצור ואל תבצע commit. |
| `HOST_DOCKER` | Docker אינו זמין. הרץ `sudo systemctl enable --now docker` ואז `docker info`. הצלחה: מופיע מידע על Server ללא שגיאת חיבור. |
| `HOST_COMPOSE_CONFIG` | יש שגיאה ב־`.env` או ב־Compose. הרץ `docker compose --profile live config -q`. הצלחה: אין פלט ו־`echo $?` מדפיס `0`. |
| `HOST_PORT_BINDINGS` | שירות מאזין מחוץ ל־localhost. הרץ `sudo ss -lntp`. הצלחה: API מופיע רק כ־`127.0.0.1:8080`, ואין `0.0.0.0:8080`, ‏`:5432`, ‏`:4222` או `:8222`. אם `ss` חסר, התקן `sudo apt install -y iproute2`. |
| `INTERNAL_PREFLIGHT` | בדיקה פנימית נכשלה או לא התחילה. תקן קודם את קוד ה־FAIL שמופיע מעליה והריץ שוב; הצלחה: `INTERNAL_PREFLIGHT` עובר. |
| `ENV_ALPACA_API_KEY`, `ENV_ALPACA_API_SECRET` | משתנה חסר. ערוך `nano .env` ומלא מפתחות Alpaca Paper בלבד. הצלחה נבדקת בהרצת Preflight חוזרת; הערך עצמו אינו מודפס. |
| `ENV_TELEGRAM_BOT_TOKEN`, `ENV_TELEGRAM_CHAT_ID` | משתנה Telegram חסר. מלא אותו ב־`.env`. הצלחה: `TELEGRAM_GET_ME` ו־`TELEGRAM_SEND_MESSAGE` עוברים. |
| `ENV_POSTGRES_DSN`, `ENV_POSTGRES_PASSWORD` | הגדרת Postgres חסרה. השווה את שמות המשתנים ל־`.env.example` בלי להעתיק ערכים ללוג. הצלחה: `POSTGRES_CONNECTION` עובר. |
| `ENV_NATS_URL` | כתובת NATS חסרה. בתוך Compose הערך הרגיל הוא `nats://nats:4222`. הצלחה: `NATS_CONNECTION` עובר. |
| `ENV_DATA_PLAN`, `ENV_DIRECT_ACCOUNT_ACCESS_ALLOWED`, `ENV_EXECUTION_ACTIONS_ALLOWED` | משתנה בטיחות חסר. העתק את שם המשתנה מ־`.env.example`. אל תשנה את ערכי הבטיחות. |
| `ENV_HISTORICAL_LAG_MINUTES`, `ENV_STREAM_MAX_SYMBOLS`, `ENV_RUN_MODE`, `ENV_STREAM_STALE_ALERT_SECONDS` | משתנה Runtime חסר. בשבוע Shadow הערכים הם `16`, עד `30`, ‏`shadow`, ו־`120` בהתאמה. |
| `DATA_PLAN_FREE` | `DATA_PLAN` אינו `free`. קבע `DATA_PLAN=free`. הצלחה: הקוד עובר. |
| `EXECUTION_DISABLED` | קבע `EXECUTION_ACTIONS_ALLOWED=false`. אין להפעיל Execution. |
| `ACCOUNT_ACCESS_DISABLED` | קבע `DIRECT_ACCOUNT_ACCESS_ALLOWED=false`. אין להוסיף הרשאת חשבון ישירה. |
| `HISTORICAL_LAG_SAFE` | קבע `HISTORICAL_LAG_MINUTES=16` או יותר. ב־Alpaca Basic אסור לבקש SIP טרי יותר. |
| `STREAM_SYMBOL_CAP_SAFE` | קבע `STREAM_MAX_SYMBOLS` למספר 1–30. |
| `STREAM_STALE_ALERT_SAFE` | קבע `STREAM_STALE_ALERT_SECONDS=120` או מספר חיובי אחר שאושר. אל תשנה את `STREAM_STALE_SECONDS=30`, שמשמש Reconnect פנימי. |
| `POSTGRES_CONNECTION` | בדוק `docker compose --profile live ps postgres` ו־`docker compose --profile live logs --tail=200 postgres`. הצלחה: המצב `healthy` ואין שגיאות Authentication או Disk. |
| `POSTGRES_SCHEMA` | החל את הסכמה המעודכנת: `docker compose --profile live exec -T postgres psql -U market -d market < config/schema.sql`. הצלחה: מופיעות שורות `CREATE TABLE`, `ALTER TABLE` או `NOTICE ... already exists`; Preflight חוזר עובר. |
| `POSTGRES_REPLAY` | המצב החומרי שונה מהאירועים. הרץ `curl -fsS http://127.0.0.1:8080/admin/replay-check` על השרת. הצלחה היא `"ok":true`; אם לא, אל תמשיך לשבוע Shadow ושמור את רשימת `differences` לבדיקת Tech Lead. |
| `NATS_CONNECTION` | בדוק `docker compose --profile live ps nats` ו־`docker compose --profile live logs --tail=200 nats`. הצלחה: המצב `healthy`. |
| `NATS_JETSTREAM` | ודא שב־`docker-compose.yml` פקודת NATS כוללת `-js`, ואז הרץ `docker compose --profile live up -d nats`. הצלחה: Preflight מציג `JetStream account is active`. Preflight אינו יוצר Stream או Consumer. |
| `ALPACA_IEX_QUOTE` | בדוק שמפתחות הדאטה נכונים וש־`DATA_PLAN=free`, ‏`DISCOVERY_FEED=iex`, ‏`DECISION_FEED=iex`. הצלחה: הבדיקה חוזרת עם HTTP 200. |
| `ALPACA_SIP_BARS` | אם ההודעה היא `SUBSCRIPTION_REQUIRED`, ודא שהבקשה היא היסטורית ושהפיגור לפחות 16 דקות. אין לרכוש מנוי. |
| `ALPACA_PAPER_CLOCK` | `PAPER_KEYS_REQUIRED` אומר שהמפתחות אינם של חשבון Paper. החלף אותם במפתחות Paper; אין להשתמש במפתחות Live. |
| `TELEGRAM_GET_ME` | בדוק Bot token ויציאת HTTPS. הצלחה: Preflight מציג את שם הבוט, לא את הטוקן. |
| `TELEGRAM_SEND_MESSAGE` | בדוק `TELEGRAM_CHAT_ID`, פתח שיחה עם הבוט ושלח לו הודעה ידנית אחת. הצלחה: הודעת Preflight מגיעה לצ'אט. |

`HTTPStatusError HTTP 500` הוא קוד שרת ללא גוף תשובה או URL. המתן מספר דקות ונסה
שוב. `TimeoutError` מצביע בדרך כלל על DNS, יציאת HTTPS או שירות שלא עלה. אין
להדפיס URL של Telegram כי הוא מכיל את הטוקן.

### שגרת בוקר — כ־3 דקות

ה־Compose profile בשם `live` מפעיל ומציג גם את `stream-worker`, שהוא השירות שמקבל
את נתוני השוק. לכן כל פקודות ניהול ה־stack במדריך כוללות `--profile live`.

מה ולמה: ודא שה־systemd unit פעיל ושה־containers לא בלולאת Restart.

```bash
systemctl status market-brain.service --no-pager
cd /opt/market-brain
docker compose --profile live ps
```

הצלחה: systemd מציג `active (running)`; `postgres`, ‏`nats`, ‏`api` ו־`stream-worker`
הם `Up`, ושירותי ה־Healthcheck הם `healthy`. ה־`stream-worker` חייב להיות `Up`, כי
הוא הרכיב שמקבל את נתוני השוק.

מה ולמה: במחשב האישי, פתח מנהרת SSH זמנית ל־API המקומי. הפקודה נשארת פתוחה כל עוד
משתמשים במנהרה.

```bash
ssh -i ~/.ssh/oracle_market_brain -L 8080:127.0.0.1:8080 ubuntu@SERVER_PUBLIC_IP
```

הצלחה: אין שגיאת SSH והחלון נשאר מחובר.

מה ולמה: בטרמינל מקומי נוסף קרא את Health בלי לפתוח Ingress חדש.

```bash
curl -fsS http://127.0.0.1:8080/health
```

הצלחה: מתקבל JSON עם `"status":"ok"`, ‏`"run_mode":"shadow"`, גבולות הבטיחות
`false`, ו־`"stream_stale":false` בזמן שהשוק והזרם פעילים.

מה ולמה: בדוק בטלגרם שהודעת הבוקר או הודעת Preflight האחרונה הגיעה ושכל התראה
חדשה מתחילה ב־`[SHADOW]`.

הצלחה: אין התראה לא מסומנת ואין פעולה שנדרשת מול ברוקר.

### במהלך יום המסחר — כ־2 דקות

קוראים את `BUY_NOW`, ‏`SELL_NOW`, ‏`STREAM_STALE` ושאר ההתראות. לא פועלים לפיהן.
רושמים רק האם ההודעה ברורה והאם הנתונים נראים רציפים. `STREAM_RECOVERED` אמור להגיע
פעם אחת כשהזרם חוזר; אין לצפות להצפה של הודעות חוזרות.

מה ולמה: אם צריך לראות את 200 שורות הלוג האחרונות בלי לעקוב לנצח:

```bash
cd /opt/market-brain
docker compose --profile live logs --tail=200 api stream-worker
```

הצלחה: מתקבלות שורות אחרונות ללא Traceback חוזר וללא סודות.

### שגרת ערב — כ־5 דקות

ה־digest נשלח אחרי 16:20 שעון ניו יורק. בישראל זה 23:20 כמעט כל השנה. רק בשבועות
המעבר שבהם מועדי החלפת שעון הקיץ אינם חופפים — באמצע עד סוף מרץ ובסוף אוקטובר עד
תחילת נובמבר — השעה היא 22:20.

בודקים ב־digest:

- `signals`, ‏`trades` ו־`no_trigger` של היום;
- hit rate, expectancy ב־R ו־max drawdown ב־R, היום ובמצטבר;
- פירוט לפי setup;
- מצב הזרם, Alert delivery, Replay check ותזכורת Reconcile.
- סיכום שלושת checkpoints של Premarket והאם המועמדות האחרונות נראו או אושרו
  לאחר הפתיחה.
- כיסוי סקירת התוצאות, MFE/MAE ותשואת EOD ממוצעת של Finalists; נתון חסר נשאר
  `LEARNING_DATA_INCOMPLETE`.

זהו דוח מדידה. אין להסיק ממנו המלצת מסחר ואין לבצע פעולה בעקבותיו.
תוצאות Shadow ו־Replay ברות השוואה כי שתיהן משתמשות באותו מנוע יציאות:
Stop-first בנר דו־משמעי, `time stop` אחרי 30 דקות, ובסגירת היום finalize במחיר
הסגירה של הנר האחרון הזמין.

### שגרה שבועית — כ־10 דקות

מה ולמה: הרץ Replay על 20 ימי המסחר האחרונים לכל ה־Universe. הסקריפט משתמש ב־SIP
היסטורי, בפיגור וב־rate limiter הקיימים.

```bash
cd /opt/market-brain
docker compose --profile live run --rm api python scripts/replay_report.py --days 20
```

הצלחה: מודפס נתיב כמו `reports/replay_2026-08-03_2026-08-28.md` והקובץ מכיל
מדדים, טבלה לפי סימבול ורשימת טריידים.

מה ולמה: הפק דוח Shadow לשבוע הנוכחי מתוך `shadow_trades` וה־event store.

```bash
docker compose --profile live run --rm api python scripts/shadow_report.py
```

הצלחה: מודפס נתיב כמו `reports/shadow_2026-W35.md`; הקובץ מכיל signals, trades,
no_trigger, hit rate, expectancy, max drawdown ופירוט לפי setup.

מה ולמה: הקונטיינר רץ כ־root. אם רוצים לערוך או למחוק את דוחות ה־Markdown כמשתמש
`ubuntu`, מעבירים אליו את הבעלות לאחר יצירת הדוחות.

```bash
sudo chown ubuntu:ubuntu reports/*.md
```

הצלחה: `ls -l reports/*.md` מציג `ubuntu ubuntu` כבעלים וכקבוצה.

מה ולמה: ודא שהגיבוי הלילי האחרון קיים ואינו ריק.

```bash
sudo systemctl status market-brain-backup.timer --no-pager
ls -lh /var/backups/market-brain/market_*.dump | tail -n 3
```

הצלחה: ל־timer יש `NEXT` עתידי, ולפחות קובץ `.dump` אחד מהיממה האחרונה גדול מאפס.

מה ולמה: בדוק מקום בדיסק גם ברמת Linux וגם ברמת Docker.

```bash
df -h /
docker system df
```

הצלחה: המחיצה `/` אינה קרובה ל־100%, ו־Docker מציג סיכום Images, Containers,
Volumes ו־Build cache ללא שגיאה.

מה ולמה: עדכן רק Fast-forward ורק כאשר `git status` נקי.

```bash
cd /opt/market-brain
git status --porcelain
GIT_SSH_COMMAND='ssh -i ~/.ssh/market_brain_deploy -o IdentitiesOnly=yes' git pull --ff-only origin market-brain-v4
sudo systemctl restart market-brain.service
systemctl is-active market-brain.service
```

הצלחה: לפני העדכון אין פלט מ־`git status`; ‏`git pull` מציג `Fast-forward` או
`Already up to date`; השורה האחרונה היא `active`. לאחר מכן מריצים שוב Preflight.

### טבלת תקלות מהירה

| תקלה | פקודות בדיקה | מצב תקין / פעולה |
|---|---|---|
| `DATA_PLAN_VIOLATION` | `journalctl -u market-brain -n 200 --no-pager` וגם `grep -E '^(DATA_PLAN|HISTORICAL_FEED|HISTORICAL_LAG_MINUTES)=' .env` | צריך לראות `free`, ‏`sip`, ו־`16` לפחות. עצור Replay עד שהתצורה תוקנה; אין לרכוש מנוי. |
| `STREAM_STALE` | `journalctl -u market-brain -f` ובחלון נוסף `docker compose --profile live logs --tail=200 stream-worker` | חפש Reconnect ללא חשיפת מפתחות. `STREAM_RECOVERED` אמור להגיע פעם אחת כשהודעות חוזרות. |
| Postgres למטה | `docker compose --profile live ps postgres` ואז `docker compose --profile live logs --tail=200 postgres` | צריך להיות `healthy`. אם הדיסק מלא, טפל בדיסק לפני Restart. |
| NATS למטה | `docker compose --profile live ps nats` ואז `docker compose --profile live logs --tail=200 nats` | צריך להיות `healthy`; הרץ `docker compose --profile live up -d nats` אם הוא stopped. |
| דיסק מלא | `df -h /` ו־`docker system df` | שמור קודם גיבוי Postgres. אל תריץ `docker system prune --volumes`; מחיקת Volumes עלולה למחוק נתונים. |
| Telegram לא מגיע | `docker compose --profile live logs --tail=200 api` ואז `curl -fsS http://127.0.0.1:8080/alerts` | Alerts שלא נמסרו נשארים ב־outbox עם Retry. אל תדפיס Bot token ואל תמחק את הרשומות ידנית. |
| שירות לא עולה | `systemctl status market-brain.service --no-pager` ואז `journalctl -u market-brain -n 200 --no-pager` | חפש את קוד ה־FAIL הראשון, תקן אותו והריץ Preflight; אל תעקוף בדיקה. |

`journalctl -u market-brain -f` ממשיך לעקוב עד `Ctrl+C`. הצלחה אינה "אין לוגים",
אלא שאין כשל חוזר, אין סוד מודפס והמצב חוזר ל־healthy.

### מה מודדים לפני שמחליטים משהו

שומרים את המדידה לפחות כמה שבועות ורק לאחר מספר משמעותי של טריידים וירטואליים.
עוקבים אחרי:

- מספר השבועות ומספר הטריידים הווירטואליים;
- expectancy ב־R, hit rate ו־max drawdown ב־R, כולל לפי setup;
- אפס `DATA_PLAN_VIOLATION`;
- אפס כשלי Reconcile או Replay check;
- רציפות Stream, Alert delivery וגיבויים.

אלה תנאי מדידה ואיכות נתונים בלבד. הם אינם המלצה לסחור, אינם אישור לעבור לכסף
אמיתי ואינם משנים את הארכיטקטורה ה־brokerless.
