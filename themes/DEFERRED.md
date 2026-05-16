# Themes — Deferred Work Tracker

_Single source of truth for everything we've consciously punted on. Each item has a trigger condition that should prompt re-evaluation. Reviewed at the end of each theme cycle._

---

## Stage / web-app build (in order)

| # | Item | Status | Notes |
|---|------|--------|-------|
| 1 | **Mock the tracker page design** | ✅ Done 2026-05-16 | Mock approved, design language locked |
| 2 | **Build `themes_web/` FastAPI app** — 5 page types | ✅ Done 2026-05-16 | All 5 pages render; theme dropdown with optgroups for Active/On Deck/Backlog |
| 3 | **Interactive D3/SVG supply chain diagram** | Deferred — not high priority | Mermaid renders well; revisit if visual upgrade becomes needed |
| 4 | **Tracker page with live data** | ✅ Done 2026-05-16 | Live overlay JS calls /api/themes/{slug}/quotes; news + 13F + earnings from tracker_live.json |
| 5 | **Daily refresh job inside `themes_web/`** | ✅ Done 2026-05-16 | APScheduler cron 18:00 ET weekdays; manual trigger via POST /api/refresh/{slug} |
| 6 | **Deploy `themes_web/` to Railway as separate service** | 📋 Pending user action | See `themes_web/DEPLOY.md` for step-by-step. User creates second Railway service in same project |

---

## Data integration (across all themes)

| # | Item | Status | Notes |
|---|------|--------|-------|
| D1 | **13F overlay** | ✅ Live as of 2026-05-16 | Source: yfinance.Ticker.institutional_holders (FMP endpoint required higher plan, Finnhub returned 403). Quarterly cadence. 5 holders per tracker ticker with quarterly delta in % held |
| D2 | **Earnings calendar overlay** | ✅ Code done; needs FMP key to flow | tracker_refresh.py pulls /stable/earnings; tracker page Upcoming section renders when present |
| D3 | **Insider transactions overlay** | Deferred — phase 2 | Lower priority than 13F; revisit after deploy |
| D4 | **News headlines per tracker name** | ✅ Done 2026-05-16 | Finnhub /company-news with 30-day window; works in sandbox AND deployed env |
| D5 | **ABB (Swiss ADR) data** | Will auto-resolve once deployed | Hand-fix not needed — FMP resolves ABB ADR; sandbox shows `—` because no FMP key |

---

## Research follow-ups (open questions from `ai_data_center/supply_chain.md`)

All five resolved 2026-05-16 — findings appended as dated addendum to `supply_chain.md`. Materially changed the picture for several names. Summary:

| # | Status | Material finding |
|---|--------|------------------|
| R1 | ✅ | CPO shipping in volume from Broadcom (50K Tomahawk 5-Bailly) but pluggables dominate 1.6T rollout — ~2yr runway for LITE/AAOI/COHR |
| R2 | ✅ | 3M PFAS exit completed Mar 2025; **Syensqo (EBR:SYENS)** is the cleanest public replacement supplier |
| R3 | ✅ | **GEV only ~28% AI-relevant revenue** — gas turbines are 50%. Consider trimming GEV tracker position |
| R4 | ✅ | **No IPOs. Acquisitions:** Ecolab→CoolIT $4.75B (Q3 2026), Trane→LiquidStack, Flex→JetCool. **ECL is the new pure-play cooling story** |
| R5 | ✅ | No hyperscaler direct equity in private cooling cos — value flowed to industrial strategics. Positive for "vehicles right" risk |

**Open follow-up action: tracker reshuffle.** User to decide whether to swap GEV → ECL (or another rebalance). Discussion in supply_chain.md addendum.

---

## Methodology / validation

| # | Item | Trigger | Notes |
|---|------|---------|-------|
| M1 | **Backtest the quant-only subset of the rubric** | After 2 themes are active (Space + AI DC) | RS inflection, distance from 52w high, revenue growth, valuation — these are computable historically. Test predictive power across themes. Skip the qualitative criteria — too much hindsight leakage |
| M2 | **6-month rubric performance review** | 2026-11-11 (T+6mo from AI DC lock) | Pull entry scores vs current prices for AI DC tracker names. Which scoring components had real predictive power? Reweight rubric for next theme accordingly |
| M3 | **12-month thesis review — AI DC** | 2027-05-11 | Did the thesis pay off? Compare tracker vs SPY + sector proxies. Update / mark Failed / extend |
| M4 | **Save `scoring_log.json` alongside `scoring.md`** | Now (every time scoring is done) | Machine-readable record of all per-criterion scores for retrospective performance analysis |
| M5 | **Note which scoring components were quantitative vs judgment** | Now | So later we can reweight without conflating algorithm error with my judgment error |

---

## Themes-level (longer horizon)

| # | Item | Trigger | Notes |
|---|------|---------|-------|
| T1 | **Cross-theme allocation framework** | After 3+ themes active | If we have AI DC + Space + Defense each with 5 picks, how do we size across themes? Equal weight? Conviction-weighted? Risk parity? |
| T2 | **"Thesis status" indicator on the tracker** | When tracker is live | Green / Yellow / Red flag per name based on whether any falsifier is firing or near-firing |
| T3 | **Failed-thesis archive** | Whenever a thesis fails | Same as `_archive/` pattern from the v0.1 cleanup — preserves the work + lessons learned |

---

## Review cadence

- **Every time we complete a stage** of the web app build: cross items off, reassess priorities
- **At each theme review** (quarterly per theme): re-check all R-items for that theme
- **At each rubric review** (M2/M3 dates): consolidate methodology learnings

_Last reviewed: 2026-05-16_
