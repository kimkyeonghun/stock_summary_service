# Stock Info MVP

Private-use stock information MVP for beginner investors.

This project collects:
- Naver News items by stock keyword
- Hankyung Consensus report items (best-effort scraper)
- Naver Finance Research company report items (KR)
- SEC EDGAR filing items for US tickers (free-first)

Then it creates:
- Stock-level 8-line summaries with source tags
- A lightweight Flask dashboard

## Features

- Fixed starter universe of 10 popular KRX stocks
- SQLite storage for documents and summaries
- Deduplication by URL hash
- Rule-based 8-line summary generator
- Optional scheduled collection
- Optional Telegram morning brief delivery
- Universe refresh for KR top-100 and US large caps

## Quick start

1. Create and activate a Python virtual environment.
2. Install packages:

```bash
pip install -r requirements.txt
```

3. Copy env file:

```bash
copy .env.example .env
```

If your environment has SSL inspection/corporate certificates, set:
- `VERIFY_SSL=false` (quick workaround)
- or `CA_BUNDLE_PATH=<your_ca_bundle.pem>` (recommended)

4. Initialize DB:

```bash
python scripts/bootstrap_db.py
```

5. Run one collection pass:

```bash
python scripts/run_collect.py
```

6. Start web app:

```bash
python scripts/run_server.py
```

Open `http://127.0.0.1:5000`.

7. (Optional) Send morning brief manually:

```bash
python scripts/run_brief.py
```

8. (Optional) Refresh KR/US universe manually:

```bash
python scripts/run_universe_refresh.py
```

## Notes on login crawling

For login-required pages, this code supports custom cookies via `.env` (`CONSENSUS_COOKIE`).
If the target site requires interactive login or anti-bot checks, keep to accessible pages only.

For Naver news stability, you can optionally set:
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`
If set, the collector uses Naver OpenAPI first, then falls back to HTML parsing.

Source-specific collection limits:
- `NAVER_NEWS_PER_STOCK` (default `20`)
- `HANKYUNG_REPORTS_PER_STOCK` (default `8`)
- `NAVER_FINANCE_REPORTS_PER_STOCK` (default `8`)
- `SEC_REPORTS_PER_STOCK` (default `6`)

Scheduler-related env vars:
- `ENABLE_SCHEDULER=true`
- `COLLECT_SCHEDULE_KST=00:00,06:00,12:00,18:00`
- `MORNING_BRIEF_TIME_KST=07:00`
- `UNIVERSE_REFRESH_DAY_OF_MONTH=1`
- `UNIVERSE_REFRESH_TIME_KST=05:30`
- `CRAWLER_MAX_RETRIES=1`
- `OPS_ERROR_ALERT_THRESHOLD=5`
- `ENABLE_TELEGRAM_ERROR_ALERT=false`

Telegram env vars:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

SEC env vars:
- `SEC_USER_AGENT=stock-mvp/0.1 (contact: your-email@example.com)`

Ops endpoints:
- `GET /ops/runs?limit=30` (latest pipeline runs)
- `GET /ops/runs/<run_id>` (crawler-level run stats)

## Disclaimer

This tool is for informational support only and is not investment advice.
