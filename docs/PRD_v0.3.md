# BriefAlpha / Phase-2 PRD (v0.3)

- Date: 2026-02-23
- Owner: hanati
- Status: Draft (confirmed scope updates applied)

## 1. Product Summary

Private investment briefing service for beginner investors.
The service collects stock/sector documents, generates explainable summaries with sources, and delivers a morning brief via Telegram.

## 2. Confirmed Decisions (from latest request)

1. US report sources should be free-first.
2. Morning brief delivery channel is Telegram.
3. LLM budget target is "GPT Pro level preference", with free-tier fallback when possible.

## 3. Goals and Non-goals

### Goals

1. Expand from 10 fixed stocks to broad KR/US universe coverage.
2. Fully automate collection and briefing, so users can read every morning.
3. Add sector-level intelligence, charting, backtesting, and basic financial ratios.
4. Replace rule-based sentiment/summary with LLM-based pipeline.

### Non-goals (for this phase)

1. Public launch and multi-tenant account system.
2. Auto-trading or direct buy/sell recommendations.
3. Real-time tick-level ingestion.

## 4. Scope

### 4.1 Universe

1. Korea: KOSPI market-cap top 100 (monthly rebalance).
2. US: staged rollout.
   - Stage A: user watchlist + major large caps.
   - Stage B: S&P 500 and/or Nasdaq-100 expansion.

### 4.2 Data Types

1. News documents.
2. Research/report-like documents (free-first).
3. Financial metrics (PER, PBR, EPS, ROE, market cap).
4. Price history for charting and backtesting.

### 4.3 Sector Intelligence

1. Sector taxonomy (example: Financials, Materials, Energy, Semiconductors, Healthcare, Platform IT).
2. Stock-to-sector mapping.
3. Sector-level summary and positive/negative flow explanation.

## 5. Source Strategy (Free-first)

### 5.1 KR

1. News: Naver News (API first when available, parser fallback).
2. Reports: Hankyung Consensus (best-effort crawler, policy-compliant).

### 5.2 US (free-first)

1. SEC EDGAR filings (10-K, 10-Q, 8-K) as primary "report" source.
2. Company IR press releases and investor updates.
3. Free news feeds/APIs where terms permit.

### 5.3 Policy

1. No paywall bypass, no anti-bot bypass.
2. Keep summary + source links, avoid full-content redistribution.
3. Respect robots/terms for each source.

## 6. Automation Requirements

### 6.1 Collection Scheduler (KST)

1. Regular collection: 06:00 / 12:00 / 18:00.
2. US close sync collection: 06:30 (weekday, DST-aware operational note).
3. Universe refresh: monthly.

### 6.2 Morning Brief

1. Generation time: 07:00 daily.
2. Delivery: Telegram Bot.
3. Brief structure:
   - Market headline (KR/US)
   - Top stock changes
   - Sector highlights
   - Risk notes
   - Link to dashboard

## 7. Functional Requirements

1. FR-01 Universe Manager
   - Maintain KR top-100 and US universe tables.
2. FR-02 Automated Ingestion
   - Scheduled crawler jobs with retry and error counters.
3. FR-03 LLM Summaries
   - Stock-level and sector-level summaries using LLM.
   - Each statement must include supporting source mapping.
4. FR-04 Sentiment
   - LLM-based polarity classification (positive/neutral/negative) with confidence.
5. FR-05 Financial Snapshot
   - PER/PBR/EPS/ROE/market cap in stock detail page.
6. FR-06 Chart and Backtest
   - Portfolio builder (symbols + weights + period).
   - Basic strategies: buy-and-hold and monthly rebalance.
   - Metrics: return, MDD, volatility, Sharpe (simple).
7. FR-07 Telegram Delivery
   - Send daily morning brief and optional alert cards.

## 8. LLM Cost and Quality Plan

### 8.1 Important Constraint

`ChatGPT Pro` subscription is not a backend API billing model.
Automated pipelines should use API-based or local model runtime.

### 8.2 Tiered Execution

1. Tier A (Free-first)
   - Extractive baseline + local/open model where feasible.
2. Tier B (Low-cost API)
   - Use smaller model for daily broad coverage.
3. Tier C (High-quality API)
   - Use stronger model only for top movers/risky items.

### 8.3 Budget Guardrails

1. Daily token budget cap.
2. Per-job cap and fallback-to-cheap model when exceeded.
3. Cache and dedupe to avoid repeated summarization.

## 9. Non-functional Requirements

1. Reliability: scheduler failures must not stop entire pipeline.
2. Transparency: every summary line traceable to source links.
3. Scalability: should support top-100 KR + expanding US set.
4. Compliance: no prohibited scraping behaviors.
5. Observability: logs for fetched/inserted/skipped/error/LLM cost.

## 10. Success Metrics

1. Daily brief delivery success rate >= 98%.
2. Summary generation success rate >= 95% across tracked universe.
3. Source traceability coverage = 100% (all output lines mapped).
4. Duplicate-source display reduction improves readability (qualitative user feedback).
5. LLM monthly spend remains under configured cap.

## 11. Milestones

1. M1 (Week 1-2): universe expansion + scheduler + Telegram pipeline.
2. M2 (Week 3-4): sector summaries + financial snapshot.
3. M3 (Week 5-6): chart/backtest + LLM tiering + budget control.

## 12. Open Items

1. Final US free-source whitelist (domain list + terms review).
2. Default sector taxonomy version (v1) and mapping maintenance owner.
3. Morning brief message size/format limits for Telegram UX.
