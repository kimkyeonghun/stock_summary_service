# AGENTS.md

## Project Purpose

This repository implements a **stock information aggregation and summarization service for beginner investors**.

The system collects market information (news, research reports, filings) and converts it into **structured summaries and evidence-based insights** that help users understand what is happening with a stock.

Primary outputs include:

- structured item summaries
- evidence cards
- daily digests
- agent reports
- dashboard views
- optional Telegram briefs

The goal is **to help users understand market information quickly**, not to provide direct investment advice.

---

# Product Direction

The project is evolving from **simple news summaries** to **investor-oriented guidance summaries**.

Future summaries should aim to answer questions such as:

- What happened?
- Why does it matter?
- How could it affect the company or industry?
- What should an investor pay attention to next?

However:

- The system **must not generate direct investment recommendations**
- It should **explain signals, risks, and uncertainties instead**

Allowed examples:

Good:

> Nvidia announced new AI chip shipments which could strengthen its data center revenue outlook.

Avoid:

> Investors should buy Nvidia.

---

# Summary Philosophy

Summaries should prioritize **clarity, evidence, and investor relevance**.

Key principles:

1. **Explain the signal**

   Focus on why the information matters for the company or industry.

2. **Avoid simple rewriting**

   Summaries must add structure and context.

3. **Highlight uncertainty**

   If implications are unclear, say so explicitly.

4. **Preserve factual grounding**

   Every summary should map back to one or more source items.

5. **Focus on investor relevance**

   Not all news is meaningful for investors.

---

# Target Summary Structure

Future summaries should gradually converge toward the following structure.

### 1. Signal

What happened.

### 2. Context

Background information that explains why this matters.

### 3. Investor relevance

How the signal might affect the company, industry, or market.

### 4. Risk or uncertainty

What is unclear or could change.

### 5. What to watch

Signals investors should monitor next.

---

# System Architecture

The current architecture must be preserved.

Collection → Processing → Agent analysis → Web display

Main components:

Entry points

scripts/run_collect.py  
scripts/run_server.py  
scripts/run_brief.py

Core orchestration

stock_mvp/pipeline.py

Agent layer

stock_mvp/agents/*

Storage

item_summaries  
evidence_cards  
daily_digests  
agent_reports  

UI

Flask + Jinja templates

When extending functionality:

- prefer modifying the **agent layer**
- avoid large pipeline rewrites unless required

---

# Data Sources

Current primary sources include:

- Naver News
- Naver Finance Research
- SEC EDGAR
- Other structured financial sources

When adding new sources:

- ensure relevance to investors
- avoid low-signal news feeds
- preserve source attribution

---

# Evidence Model

All generated insights must trace back to **source evidence**.

Use evidence cards to represent:

- news items
- filings
- research reports
- macro signals

Summaries should reference these evidence objects rather than raw articles.

---

# UI Rules

UI must follow these constraints.

Framework

Flask + Jinja server-rendered templates only.

Do not introduce:

- React
- Next.js
- SPA frameworks

Layout principles

- No sidebar navigation
- Simple dashboard layout
- Clear KR / US market separation

Minimal change rule

If a task is UI-only:

- do not modify pipeline logic
- do not modify database schema

Always check `UI_RULES.md` before editing templates.

---

# Working Style for Agents

Agents modifying this repository should follow these rules.

Prefer:

- small localized changes
- incremental improvements

Avoid:

- broad refactors
- architecture rewrites
- introducing unnecessary dependencies

Before large changes:

Provide a short plan.

---

# Boundaries and Safety

The system provides **information and analysis**, not investment advice.

Do not generate:

- buy/sell instructions
- price targets
- trading signals

Allowed:

- risk discussion
- scenario explanation
- monitoring signals

---

# Environment and Configuration

Environment configuration is controlled via `.env`.

Important variables include:

- LLM provider configuration
- Telegram settings
- scheduler settings
- database configuration

Do not rename environment variables unless explicitly requested.

---

# Validation Steps

After making changes, verify the system works.

Setup

pip install -r requirements.txt

Initialize database

python scripts/bootstrap_db.py

Run collection

python scripts/run_collect.py --market KR

Run server

python scripts/run_server.py

Verify

- dashboard loads
- stock detail page loads
- summaries appear
- evidence references exist

---

# Output Expectations for Agents

When implementing changes, always report:

1. files modified
2. reason for the change
3. commands executed
4. verification results
5. potential risks

---

# Long-Term Direction

The system will evolve toward a **structured investor intelligence service**.

Future capabilities may include:

- thematic clustering
- sector intelligence
- signal ranking
- long-term narrative tracking
- portfolio monitoring

Changes should move the project toward **clearer investor insight**, not simply more content.

---

# Key Rule

Always optimize for:

**clarity for beginner investors**

not:

**maximum data volume**