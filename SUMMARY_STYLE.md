# SUMMARY_STYLE.md

## Purpose

This document defines the **writing style and interpretation rules for AI-generated summaries**.

It **does not change the existing summary schema** implemented in the code.

Instead, it ensures that all summaries:

- provide investor-relevant interpretation
- remain evidence-based
- avoid hallucination
- avoid investment advice

The existing output structure must remain compatible with the system.

Current fields include:

- conclusion
- evidence
- risk
- checkpoints
- final sentiment

This document defines **how these sections should be written**, not how they are structured.

---

# Core Philosophy

Summaries should transform raw financial information into **clear insights for beginner investors**.

The AI should:

- interpret the signal
- explain relevance
- acknowledge uncertainty

The AI should **not simply rewrite the article**.

Focus on answering:

- What happened?
- Why does it matter?
- What should investors watch?

---

# Mapping to Current Summary Fields

The existing summary schema must remain unchanged.

However, the internal logic should follow this interpretation model.

| Concept | Existing Field |
|--------|--------|
| Signal | conclusion |
| Context / relevance | evidence |
| Uncertainty | risk |
| Monitoring signals | checkpoints |
| Overall interpretation | final sentiment |

The AI should incorporate these concepts while keeping the existing format.

---

# Writing Rules for Each Section

## conclusion

Explain **the core signal**.

This should answer:

What happened and why it matters.

Example:

"Nvidia reported strong demand for AI chips, reinforcing expectations of continued growth in data center revenue."

Avoid:

Simply repeating article headlines.

Bad example:

"Nvidia announced its quarterly earnings today."

---

## evidence

Explain **why the signal matters**.

Evidence may include:

- supporting facts
- industry context
- relevant numbers
- supporting articles

Evidence should help the reader understand **the broader meaning of the signal**.

Example:

"Cloud providers continue increasing AI infrastructure spending, which supports demand for advanced GPUs."

Avoid listing article summaries sequentially.

---

## risk

Explain **uncertainty or potential downside**.

Examples:

- regulatory uncertainty
- macroeconomic conditions
- competition
- early-stage developments

Example:

"It remains uncertain whether current demand levels can be sustained over the long term."

Do not invent risks without basis.

---

## checkpoints

Identify **signals investors should monitor next**.

Examples:

- future earnings reports
- policy changes
- product launches
- supply chain changes

Example:

"Investors may watch whether cloud providers expand AI infrastructure investment next quarter."

Checkpoints should guide **what information to monitor**, not suggest trading.

---

## final sentiment

Provide a **balanced overall interpretation**.

This should summarize the situation without making investment recommendations.

Allowed tone:

- cautiously positive
- neutral
- uncertain
- cautiously negative

Example:

"The development supports the company's long-term growth narrative but depends on continued demand expansion."

Avoid:

"This stock is a strong buy."

---

# Tone and Language

Write for **beginner investors**.

Use clear and accessible language.

Avoid excessive financial jargon.

Bad:

"Demand elasticity may materially influence operating margin trajectory."

Better:

"Future demand will likely affect the company's profit margins."

---

# Sentence Structure

Prefer short sentences.

Avoid long compound sentences.

Bad:

"This development, which could affect multiple strategic areas including supply chains and pricing dynamics, may have implications for profitability."

Better:

"This development could affect supply chains and pricing. That may influence profitability."

---

# Length Guidelines

Summaries should remain concise.

Recommended:

- 5–8 sentences total
- align with the current UI summary design

Avoid overly long explanations.

---

# Evidence Integrity

All summaries must be grounded in **real sources**.

Allowed evidence:

- news articles
- financial filings
- research reports
- official announcements

Never fabricate:

- statistics
- sources
- company statements

---

# Handling Multiple Articles

When multiple articles discuss the same topic:

The AI should extract **the underlying signal**.

Bad:

"Article A said X. Article B said Y."

Better:

"Multiple reports indicate that demand for AI chips continues to increase."

---

# Numbers and Data

Preserve meaningful numerical information.

Examples:

- revenue growth
- market share
- deadlines
- policy changes

Avoid unnecessary precision.

Bad:

"Revenue increased by 17.234%."

Better:

"Revenue increased by about 17%."

---

# Proper Names

Use official company names.

Example:

"Nvidia"

Ticker symbols may appear if helpful.

Example:

"Nvidia (NVDA)"

---

# Investment Advice Restriction

The system **must not generate investment advice**.

Forbidden examples:

- "Investors should buy this stock."
- "This is a strong buy."
- "The stock will likely double."

Allowed:

- scenario explanations
- risk discussion
- signals to monitor

Example:

"Investors may watch whether demand continues to grow."

---

# Handling Uncertainty

If information is unclear, state the uncertainty.

Example:

"It remains unclear whether the policy will take effect this year."

Avoid overconfident predictions.

---

# Industry Context

Whenever possible, connect company news to broader industry trends.

Example:

Better:

"Tesla’s price cuts reflect increasing competition in the EV market."

Instead of:

"Tesla reduced prices."

---

# Beginner Investor Focus

Always assume the reader:

- is not a professional investor
- wants quick understanding
- has limited time

Prioritize clarity over complexity.

---

# Final Quality Checklist

Before generating a summary, ensure:

- the main signal is clear
- investor relevance is explained
- uncertainty is acknowledged
- evidence exists
- no investment advice is given