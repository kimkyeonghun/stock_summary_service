from __future__ import annotations

SUMMARY_STYLE_GUIDE_V1 = """
Use sectioned Korean summary style for beginner investors.
Required section order:
1) 결론
2) 근거 (1~2 lines)
3) 리스크
4) 체크포인트
5) 최종 판단

Rules:
- Explain investor relevance instead of rewriting titles.
- Keep sentences concise and readable.
- Preserve key numbers/dates/tickers when present.
- Explicitly state uncertainty when information is limited.
- Never provide direct investment advice (buy/sell/weight increase).
"""

ITEM_STYLE_GUIDE = """
Always separate FACT and INTERPRETATION.
FACT must be source-grounded and explicit with number/date/subject when possible.
INTERPRETATION must be hedged (e.g., may, could, possibility).
RISK must always be present.
"""

DIGEST_STYLE_GUIDE = """
Produce sectioned digest format (5~6 lines).
Use fixed section order intent: conclusion, evidence, risk, checkpoint, final judgement.
Each summary line must reference evidence aliases (C1, C2...).
Avoid investment recommendation language.
"""

REPORT_STYLE_GUIDE = """
Write concise analyst-style report with:
Executive Summary / Thesis / Bear Case / Key Evidence / What Changed / What to Watch / Uncertainties.
Do not output buy/sell recommendations.
"""
