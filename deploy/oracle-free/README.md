# פריסת Market Brain על Oracle Cloud Always Free

המדריך מיועד למי שלומד Linux ו־DevOps. בכל שלב מופיעים המטרה, הפקודה, והסימן
שהשלב הצליח. המערכת נשארת Brokerless: היא אינה מתחברת לחשבון מסחר ואינה שולחת
פקודות קנייה או מכירה.

## 1. כללי עלות לפני יצירת השרת

בחר את ה־Home Region בזהירות. משאבי Always Free זמינים רק ב־Home Region. אם מתקבלת
השגיאה `Out of host capacity`, נסה Availability Domain אחר או המתן ונסה שוב.

**אין לשדרג את החשבון ל־Pay As You Go כדי לפתור בעיית Capacity.** אין ליצור משאב
שאינו מסומן `Always Free-eligible`.

מקורות רשמיים:

- [Oracle Always Free resources](https://docs.oracle.com/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)
- [Oracle Free Tier and Home Region](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier.htm)
- [Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

## 2. יצירת VM ב־OCI Console

ב־OCI Console פתח `Compute` → `Instances` → `Create instance`, והגדר:

- Image: `Canonical Ubuntu 24.04`, ‏`aarch64`.
- Shape: `VM.Standard.A1.Flex`, עם תווית `Always Free-eligible`.
- הקצאה בטוחה לחשבון Always Free: עד `2 OCPU` ו־`12 GB RAM` בסך הכול לכל מופעי A1.
- Boot volume: השאר בגודל Always Free; אל תוסיף Performance בתשלום.
- Public IPv4: נדרש לצורך SSH בלבד.
- SSH key: העלה את המפתח הציבורי שלך. המפתח הפרטי נשאר רק במחשב שלך.

ב־Security List מחק כל Ingress שאינו נדרש והשאר כלל Stateful יחיד:

- Source CIDR: כתובת ה־IP הציבורית שלך עם `/32`, לדוגמה `203.0.113.10/32`.
- Protocol: `TCP`.
- Destination port: `22`.

אל תפתח את `8080`, ‏`5432`, ‏`4222` או `8222`. ה־API קשור ל־`127.0.0.1` בלבד.
Oracle מתעדת ש־SSH משתמש ב־TCP/22 וש־Security Lists הן שכבת בקרת ה־Ingress.

הצלחה: ה־Instance מופיע כ־`RUNNING`, ה־Shape הוא `VM.Standard.A1.Flex`, ולידו אין
עלות צפויה.

## 3. התחברות ואימות מערכת ההפעלה

מה ולמה: התחבר כמשתמש ברירת המחדל של Ubuntu. החלף את הנתיב למפתח ואת כתובת ה־IP.

```bash
ssh -i ~/.ssh/oracle_market_brain ubuntu@SERVER_PUBLIC_IP
```

הצלחה: מתקבל prompt שמסתיים ב־`ubuntu@...:~$` ללא בקשת סיסמת שרת.

מה ולמה: ודא שהשרת באמת ARM64; כך נמנע Deployment על Shape שגוי.

```bash
uname -m
```

הצלחה: הפלט הוא `aarch64`.

מה ולמה: ודא שגרסת Ubuntu היא 24.04 LTS.

```bash
. /etc/os-release && echo "$PRETTY_NAME"
```

הצלחה: הפלט כולל `Ubuntu 24.04 LTS`.

## 4. התקנת Git ו־Docker מהמאגר הרשמי

מה ולמה: עדכן את אינדקס החבילות והתקן כלים הנדרשים לאימות המאגר ול־clone.

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git
```

הצלחה: הפקודה מסתיימת ללא `E:` ומציגה שהחבילות הותקנו או כבר מעודכנות.

מה ולמה: צור תיקייה מוגנת למפתח החתימה של Docker.

```bash
sudo install -m 0755 -d /etc/apt/keyrings
```

הצלחה: אין פלט ואין שגיאה.

מה ולמה: הורד את מפתח החתימה הרשמי של Docker ואפשר ל־APT לקרוא אותו.

```bash
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
```

הצלחה: אין שגיאת HTTP או הרשאה.

מה ולמה: הוסף את מאגר Docker המתאים אוטומטית ל־`arm64` ול־Ubuntu Noble.

```bash
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
```

הצלחה: `apt-get update` כולל את `download.docker.com` ואינו מציג שגיאת חתימה.

מה ולמה: התקן Docker Engine, ‏Buildx ו־Docker Compose מהמאגר הרשמי.

```bash
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

הצלחה: השירות `docker.service` מתחיל ללא שגיאה.

מה ולמה: אפשר למשתמש `ubuntu` להריץ Docker ללא `sudo`. ההרשאה נכנסת לתוקף רק
לאחר התחברות מחדש.

```bash
sudo usermod -aG docker ubuntu
exit
```

התחבר שוב באמצעות פקודת ה־SSH משלב 3.

מה ולמה: אמת שה־Engine, ‏Compose ו־Buildx זמינים.

```bash
docker version --format 'Docker Engine {{.Server.Version}}'
docker compose version
docker buildx version
```

הצלחה: מתקבל מספר גרסה בכל אחת משלוש הפקודות ללא `permission denied`.

מה ולמה: הרץ Container בדיקה רשמי והסר אותו בסיום.

```bash
docker run --rm hello-world
```

הצלחה: הפלט כולל `Hello from Docker!`.

## 5. GitHub Deploy Key ו־clone ללא שיתוף Secret

מה ולמה: צור מפתח SSH ייעודי לקריאת הריפו. אל תשלח את המפתח הפרטי לאדם, לצ׳אט,
ל־Issue או ל־commit.

```bash
ssh-keygen -t ed25519 -f ~/.ssh/market_brain_deploy -C market-brain-oracle
```

הצלחה: קיימים `~/.ssh/market_brain_deploy` ו־`~/.ssh/market_brain_deploy.pub`.

מה ולמה: הצג רק את המפתח הציבורי והוסף אותו ב־GitHub תחת
`Repository Settings` → `Deploy keys` כמפתח Read-only. אל תסמן `Allow write access`.

```bash
cat ~/.ssh/market_brain_deploy.pub
```

הצלחה: הפלט מתחיל ב־`ssh-ed25519`; זהו החלק היחיד שמותר להעתיק ל־GitHub.

מה ולמה: בדוק את זהות GitHub ואת הרשאת המפתח. בפעם הראשונה אמת את Fingerprint מול
[התיעוד הרשמי של GitHub](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/githubs-ssh-key-fingerprints).

```bash
ssh -i ~/.ssh/market_brain_deploy -T git@github.com
```

הצלחה: GitHub מאשרת Authentication ומציינת שאין Shell access. Exit code ‏1 הוא
תקין עבור בדיקת `ssh -T` של GitHub.

מה ולמה: צור נתיב קבוע תחת `/opt`, העבר עליו בעלות למשתמש, ושכפל רק את ענף היעד.

```bash
sudo install -d -o ubuntu -g ubuntu -m 0755 /opt/market-brain
GIT_SSH_COMMAND='ssh -i ~/.ssh/market_brain_deploy -o IdentitiesOnly=yes' git clone --branch market-brain-v4 git@github.com:githuber20202/Automation.git /opt/market-brain
cd /opt/market-brain
```

הצלחה: `git branch --show-current` מחזירה `market-brain-v4`.

## 6. יצירת `.env` מקומית ומוגנת

מה ולמה: צור את קובץ הסביבה מהתבנית והגבל קריאה למשתמש בלבד.

```bash
cp .env.example .env
chmod 600 .env
nano .env
```

מלא בעצמך לפחות `ALPACA_API_KEY`, ‏`ALPACA_API_SECRET`, ‏`TELEGRAM_BOT_TOKEN`,
`TELEGRAM_CHAT_ID` וסיסמת PostgreSQL חזקה ב־`POSTGRES_PASSWORD`. ודא שה־DSN משתמש
באותה סיסמה. אין להעביר את הערכים דרך אדם אחר ואין לשמור אותם ב־Git.

השאר תמיד:

```text
EXECUTION_ACTIONS_ALLOWED=false
DIRECT_ACCOUNT_ACCESS_ALLOWED=false
DATA_PLAN=free
DISCOVERY_FEED=iex
DECISION_FEED=iex
HISTORICAL_FEED=sip
```

הצלחה: `ls -l .env` מתחיל ב־`-rw-------`, והפקודה הבאה מחזירה `false`, ‏`false`
ו־`free` בלבד בלי להציג Secrets:

```bash
grep -E '^(EXECUTION_ACTIONS_ALLOWED|DIRECT_ACCOUNT_ACCESS_ALLOWED|DATA_PLAN)=' .env
```

## 7. בניית ARM64 ובדיקת Compose

מה ולמה: בנה את אותו Dockerfile במפורש ל־ARM64. `--load` מכניס את התמונה ל־Engine
המקומי לאחר הבנייה.

```bash
docker buildx build --platform linux/arm64 --load -t market-brain:oracle .
```

הצלחה: השורה האחרונה כוללת `exporting to docker image` ללא שגיאה.

מה ולמה: אמת את קובץ Compose לאחר הרחבת משתני `.env`.

```bash
docker compose config -q
```

הצלחה: אין פלט ו־`echo $?` מחזיר `0`.

מה ולמה: העלה זמנית את כל השירותים, כולל ה־stream-worker בפרופיל `live`.

```bash
docker compose --profile live up -d --build
docker compose --profile live ps
```

הצלחה: `postgres`, ‏`nats`, ‏`api` ו־`stream-worker` מופיעים כ־`Up` או `running`;
PostgreSQL ו־NATS מופיעים גם כ־`healthy`.

מה ולמה: בדוק את ה־API דרך loopback בלבד.

```bash
curl -fsS http://127.0.0.1:8080/health
```

הצלחה: מתקבל JSON עם `"status":"ok"`, ‏`"execution_actions_allowed":false`
ו־`"direct_account_access_allowed":false`.

## 8. העברה לניהול systemd

מה ולמה: עצור את ההרצה הידנית בלי למחוק את Volume הנתונים.

```bash
docker compose --profile live down
```

הצלחה: Containers מוסרים; אין להשתמש ב־`-v`, ולכן Volume PostgreSQL נשמר.

מה ולמה: צור תיקיית לוג בבעלות המשתמש והתקן את Unit הקנוני.

```bash
sudo install -d -o ubuntu -g ubuntu -m 0750 /var/log/market-brain
sudo install -m 0644 deploy/oracle-free/market-brain.service /etc/systemd/system/market-brain.service
sudo systemctl daemon-reload
sudo systemctl enable --now market-brain.service
```

הצלחה: `Created symlink` מופיע בזמן enable והשירות הבא מחזיר `active`:

```bash
systemctl is-active market-brain.service
```

מה ולמה: הצג סטטוס ו־50 שורות לוג אחרונות לצורך Troubleshooting.

```bash
systemctl status market-brain.service --no-pager
tail -n 50 /var/log/market-brain/market-brain.log
```

הצלחה: הסטטוס הוא `active (running)` והלוג אינו מכיל Traceback מתמשך.

## 9. גיבוי PostgreSQL לילי ושמירת 14 ימים

מה ולמה: צור תיקיית גיבוי שרק `ubuntu` יכול לקרוא.

```bash
sudo install -d -o ubuntu -g ubuntu -m 0700 /var/backups/market-brain
chmod 0750 scripts/backup_postgres.sh
```

הצלחה: `ls -ld /var/backups/market-brain` מציג `drwx------ ubuntu ubuntu`.

מה ולמה: התקן Service ו־Timer. ה־Timer מריץ `pg_dump` בכל לילה ב־02:30 לפי שעון
השרת, מאמת את ה־dump באמצעות `pg_restore --list`, ומוחק קבצים שגילם מעל 14 ימים.

```bash
sudo install -m 0644 deploy/oracle-free/market-brain-backup.service /etc/systemd/system/market-brain-backup.service
sudo install -m 0644 deploy/oracle-free/market-brain-backup.timer /etc/systemd/system/market-brain-backup.timer
sudo systemctl daemon-reload
sudo systemctl enable --now market-brain-backup.timer
systemctl list-timers market-brain-backup.timer --no-pager
```

הצלחה: הטבלה מציגה זמן `NEXT` עתידי עבור `market-brain-backup.timer`.

מה ולמה: הרץ גיבוי ידני ראשון ואמת שנוצר קובץ שאינו ריק.

```bash
sudo systemctl start market-brain-backup.service
sudo systemctl status market-brain-backup.service --no-pager
ls -lh /var/backups/market-brain/market_*.dump
```

הצלחה: ה־Service הוא `inactive (dead)` עם `status=0/SUCCESS`, ונראה קובץ `.dump`
בגודל גדול מאפס. זה תקין ש־oneshot חוזר ל־inactive לאחר הצלחה.

## 10. התקנת logrotate

מה ולמה: התקן Policy שמסובב לוגים מדי יום, שומר 14 עותקים ודוחס עותקים ישנים.

```bash
sudo install -m 0644 deploy/oracle-free/market-brain.logrotate /etc/logrotate.d/market-brain
sudo logrotate --debug /etc/logrotate.d/market-brain
```

הצלחה: מצב Debug מציג את `/var/log/market-brain/*.log` ללא שגיאת Syntax או Permission.

## 11. אימות אבטחה ותפעול

מה ולמה: ודא שאין שירות אפליקטיבי שמאזין לכל ממשקי הרשת. `127.0.0.1:8080` תקין;
אין להציג `0.0.0.0:8080`, ‏`:5432`, ‏`:4222` או `:8222`.

```bash
sudo ss -lntp
```

הצלחה: מבחוץ פתוח רק `:22`; ה־API נראה רק כ־`127.0.0.1:8080`.

מה ולמה: אמת שוב את גבולות הבטיחות דרך ה־API המקומי.

```bash
curl -fsS http://127.0.0.1:8080/policy
curl -fsS http://127.0.0.1:8080/admin/replay-check
```

הצלחה: `automatic_execution` הוא `false`, ו־replay-check מחזיר `"ok":true`.

## 12. עדכון גרסה בצורה מבוקרת

מה ולמה: ודא שאין שינוי מקומי בקוד, משוך רק Fast-forward, והפעל מחדש דרך systemd.
ה־`.env` אינו Trackable ולכן נשאר מקומי.

```bash
cd /opt/market-brain
git status --porcelain
GIT_SSH_COMMAND='ssh -i ~/.ssh/market_brain_deploy -o IdentitiesOnly=yes' git pull --ff-only origin market-brain-v4
sudo systemctl restart market-brain.service
systemctl is-active market-brain.service
```

הצלחה: `git status --porcelain` אינו מציג קבצי קוד, `git pull` מסתיים ב־Fast-forward
או `Already up to date`, והשורה האחרונה היא `active`.

## 13. גישה מרחוק ללא פתיחת פורט API

מה ולמה: אם צריך לקרוא `/health` מהמחשב שלך, פתח SSH tunnel זמני במקום Ingress חדש.

```bash
ssh -i ~/.ssh/oracle_market_brain -L 8080:127.0.0.1:8080 ubuntu@SERVER_PUBLIC_IP
```

בטרמינל מקומי נוסף:

```bash
curl -fsS http://127.0.0.1:8080/health
```

הצלחה: מתקבל health JSON, וב־OCI עדיין קיים Ingress יחיד ל־TCP/22 בלבד.

אחרי השלמת ההתקנה, המשך אל [`docs/SHADOW_RUNBOOK.md`](../../docs/SHADOW_RUNBOOK.md) ל־Preflight ולשגרת ההפעלה הבטוחה של שבוע Shadow.
