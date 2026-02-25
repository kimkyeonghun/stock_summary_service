# UI RULES (must-follow)

## Goal
Keep the UI minimal, consistent, and SaaS-like with only a top navbar (no sidebar).

## Non-negotiables
- No sidebar. Ever.
- Flask + Jinja server-rendered templates only (no React/Next migration).
- Styling: Tailwind CSS via CDN in `templates/base.html`
- No inline styles.
- Layout container: `max-w-6xl mx-auto px-4` and page padding `py-6`
- Spacing scale: use Tailwind spacing only (4/6/8/12/16/24/32)
- Colors: neutral (gray/white) + ONE accent color at most.
- Components: Card (rounded-2xl shadow-sm), Button, Badge, Table (minimal borders)

## Markets
- KR/US must not mix in one view.
- Use URL prefixes: `/kr/...` and `/us/...`
- Top navbar contains a segmented toggle: `KR | US`
- All navbar links keep the current market prefix.

## Pages
- Dashboard: summary + subscribed tickers/sectors + recent items
- Watchlist: subscribe/unsubscribe UI
- Detail: ticker/sector summary + recent items

## Subscription model (future-proof)
- Current: session-based storage (placeholder)
- Future: swap to DB per user without changing templates/pages
- UI must support empty-states:
  - "No subscriptions yet" + one primary CTA to Watchlist

## PR/Change discipline
- Prefer minimal diff and small file count
- Do not change data collection / DB logic unless explicitly requested
- Add new pages by extending `base.html` and reusing components/macros