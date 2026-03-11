# BriefAlpha

Private-use stock information MVP for beginner investors.

This project collects:
- Naver News items by stock keyword
- Naver Finance Research company report items (KR)
- Naver Finance Research industry report items by sector (KR)
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
- Sector taxonomy + multi-sector mapping per stock (N>=1)
  - KR primary source: Naver upjong crawl
  - US fallback: rule-based mapping (when external theme API is unavailable)
- Sector-level deduplicated document aggregation (M2-T02 baseline)
- Sector-level 8-line summaries + sentiment (LLM-first, rule fallback)
- Financial snapshots (PER/PBR/EPS/ROE/Market Cap) for KR/US
- Stock company descriptions (multi-source profile + docs fallback)
- Daily price-bar collection (KR/US) for chart/backtest foundation
- Backtest engine v1 (buy-and-hold, monthly rebalance, benchmark compare)
- Backtest web screen: periodic contribution (DCA) + multi-portfolio/benchmark comparison chart
- Stock summary format: conclusion/evidence/risk/checkpoints/final sentiment
- Stock detail: latest 10 documents first, with "load more" for additional items

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

5. (Recommended) Sync sector taxonomy and stock-sector mappings:

```bash
python scripts/bootstrap_sectors.py
```

`bootstrap_sectors.py` fetches KR sector mappings from:
`https://finance.naver.com/sise/sise_group.naver?type=upjong`

6. Run one collection pass:

```bash
python scripts/run_collect.py
```

Skip sector-level steps for faster ad-hoc runs:

```bash
python scripts/run_collect.py --stock-codes "005930,AAPL" --skip-sector
```

Run only KR or US market universe:

```bash
python scripts/run_collect.py --market KR
python scripts/run_collect.py --market US --skip-sector
```

Collect/store documents only (skip all summarization agents):

```bash
python scripts/run_collect.py --collect-only
python scripts/run_collect.py --market KR --collect-only
```

You can rebuild sector-level deduped documents directly:

```bash
python scripts/run_sector_aggregate.py --lookback-days 7 --top 20
```

Generate sector summaries directly:

```bash
python scripts/run_sector_summarize.py --lookback-days 7 --limit 30
```

Collect financial snapshots directly:

```bash
python scripts/run_financials.py --stock-codes "005930,AAPL"
```

Collect company descriptions directly (manual run only):

```bash
python scripts/run_profiles.py --market KR
python scripts/run_profiles.py --stock-codes "005930,AAPL"
python scripts/run_profiles.py --stock-codes "005930" --force
```

Collect daily price bars directly:

```bash
python scripts/run_prices.py --market KR
python scripts/run_prices.py --market US
```

Run backtest (ETF/stock portfolio):

```bash
python scripts/run_backtest.py --market US --weights "SPY:60,QQQ:40" --start-date 2024-01-01 --end-date 2025-12-31 --strategy monthly_rebalance --benchmark SPY
python scripts/run_backtest.py --market KR --weights "005930:0.5,000660:0.5" --start-date 2024-01-01 --end-date 2025-12-31 --strategy buy_and_hold
python scripts/run_backtest.py --list-presets
python scripts/run_backtest.py --preset all_weather --start-date 2024-01-01 --end-date 2025-12-31 --strategy monthly_rebalance
python scripts/run_backtest.py --preset all_weather --start-date 2024-01-01 --end-date 2025-12-31 --strategy monthly_rebalance --contribution-amount 500 --contribution-frequency monthly
```

Built-in preset portfolios:
- `all_weather` (SPY/IEF/TLT/GLD/DBC)
- `sixty_forty` (SPY/AGG)
- `three_fund` (VTI/VXUS/BND)
- `permanent` (SPY/TLT/GLD/SHY)
- `golden_butterfly` (SPY/VBR/TLT/SHY/GLD)

7. Start web app:

```bash
python scripts/run_server.py
```

Open `http://127.0.0.1:5000`.
Backtest screen is available at `http://127.0.0.1:5000/backtest`.

8. (Optional) Send morning brief manually:

```bash
python scripts/run_brief.py
```

9. (Optional) Refresh KR/US universe manually:

```bash
python scripts/run_universe_refresh.py
```

`run_universe_refresh.py` also refreshes sector mappings for the active universe
and attempts KR upjong-based sector mapping.

## Notes on login crawling

If a target site requires interactive login or anti-bot checks, keep to accessible pages only.

For Naver news stability, you can optionally set:
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`
If set, the collector uses Naver OpenAPI first, then falls back to HTML parsing.

Source-specific collection limits:
- `NAVER_NEWS_PER_STOCK` (default `20`)
- `NAVER_FINANCE_REPORTS_PER_STOCK` (default `8`)
- `NAVER_INDUSTRY_REPORTS_PER_RUN` (default `60`, KR sector industry reports)
- `SEC_REPORTS_PER_STOCK` (default `6`)

Collection/summary flow controls:
- `COLLECT_STORE_ALL_DOCS=true` (store all collected docs; do not drop by relevance at collect time)
- `SUMMARY_TOP_N_PER_STOCK=10` (LLM summary candidate cap per stock within lookback)
- `SUMMARY_MIN_RELEVANCE=0` (minimum relevance for summary candidate selection)

Company profile fallback policy:
- KR priority: manual > Naver profile > docs-derived summary > placeholder
- US priority: manual > Yahoo profile > docs-derived summary > placeholder
- `run_profiles.py` does not run on scheduler by default (manual execution only)

Scheduler-related env vars:
- `ENABLE_SCHEDULER=true`
- `COLLECT_SCHEDULE_KST=00:00,06:00,12:00,18:00`
- `SECTOR_REFRESH_TIME_KST=00:00` (run sector aggregation/summaries once per day at this collect slot)
- `MORNING_BRIEF_TIME_KST=07:00`
- `UNIVERSE_REFRESH_DAY_OF_MONTH=1`
- `UNIVERSE_REFRESH_TIME_KST=05:30`
- `CRAWLER_MAX_RETRIES=1`
- `CRAWLER_TRUST_ENV=false` (recommended; ignore system proxy env vars for crawler HTTP calls)
- `OPS_ERROR_ALERT_THRESHOLD=5`
- `ENABLE_TELEGRAM_ERROR_ALERT=false`
- `ENABLE_PRICE_COLLECTION=true`
- `PRICE_COLLECT_KR_TIME_KST=16:40` (daily once)
- `PRICE_COLLECT_US_TIME_KST=07:10` (daily once)
- `PRICE_LOOKBACK_DAYS=400`

Telegram env vars:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

SEC env vars:
- `SEC_USER_AGENT=stock-mvp/0.1 (contact: your-email@example.com)`

LLM env vars:
- `LLM_PROVIDER=none|ollama|gemini|openai|openrouter`
- `LLM_MODEL=...`
- `LLM_API_KEY=...` (or provider-specific `GEMINI_API_KEY` / `OPENAI_API_KEY` / `OPENROUTER_API_KEY`)
- `LLM_API_BASE=` (optional override)
- `LLM_TEMPERATURE=0.2`
- `LLM_MAX_TOKENS=900`
- `LLM_REQUEST_TIMEOUT_SEC=30`
- `LLM_TRUST_ENV=false` (set `true` only if you intentionally want system proxy env for LLM calls)
- `ENABLE_FINANCIAL_COLLECTION=true`
- `FINANCIAL_REFRESH_MIN_HOURS=20`

Ops endpoints:
- `GET /ops/runs?limit=30` (latest pipeline runs)
- `GET /ops/runs/<run_id>` (crawler-level run stats)
- `GET /ops/sector-summaries?limit=30` (latest sector summaries)
- `GET /ops/sector-summaries/<sector_code>` (latest sector summary + source mapping)
- `GET /ops/financials?limit=120&sort=as_of_desc` (latest financial snapshots)
- `GET /ops/financials?market=KR&limit=100` (market filter)
- `GET /ops/financials?stock_code=005930` (stock filter)
- `GET /ops/financials?sort=market_rank` (original market/rank order)
- `GET /api/backtest/presets` (built-in portfolio presets for beginners)
- `POST /api/backtest/run` (run backtest with preset or custom weights)

Backtest API example:

```bash
curl -X POST http://127.0.0.1:5000/api/backtest/run ^
  -H "Content-Type: application/json" ^
  -d "{\"preset\":\"all_weather\",\"start_date\":\"2024-01-01\",\"end_date\":\"2025-12-31\",\"strategy\":\"monthly_rebalance\",\"rebalance\":\"monthly\",\"contribution_amount\":500,\"contribution_frequency\":\"monthly\",\"compare_presets\":[\"sixty_forty\",\"permanent\"],\"include_benchmark_in_compare\":true}"
```

Financial snapshot check examples:

```bash
python scripts/run_financials.py --stock-codes "005930,AAPL"
python scripts/run_collect.py --stock-codes "005930,AAPL"
```

## Disclaimer

This tool is for informational support only and is not investment advice.
