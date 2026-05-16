# Themes — Long-Term Growth Research

This directory holds **thesis-driven research** for finding longer-term growth opportunities in US public equities. It is **completely separate** from the deployed momentum scanner (`/`, `/backtest`, etc.). Nothing in this folder is imported by `app.py`. The deployed Railway app cannot see this work.

## Why this exists

The core idea: the market often has visibility into "what the future will need" before it has visibility into "which public companies supply the rate-limiting input that future depends on." That gap — between a widely-believed thesis and the bottleneck supplier that thesis quietly requires — is where outsized returns live.

This system is **not** a stock screener and **not** an AI thesis-generator. It is a workflow that helps a human:

1. Articulate a future they believe in (the thesis)
2. Map the supply chain that future requires (what does it physically need?)
3. Identify the public-market vehicles to express that thesis (candidates)
4. Score those vehicles against a consistent rubric (scoring)
5. Track ~5 high-conviction names per theme (tracker)

The human owns the thesis. The system organizes the work around it.

## Layout

```
themes/
├── BACKLOG.md             ← prioritized list of future themes to research
├── README.md              ← this file
├── _template/             ← copy this when starting a new theme
│   ├── thesis.md
│   ├── supply_chain.md
│   ├── candidates.md
│   ├── scoring.md
│   └── tracker.md
└── <theme_name>/          ← one directory per active theme
    ├── thesis.md          ← the bet, in 600-1000 words, with falsifiers
    ├── supply_chain.md    ← future → inputs → bottleneck → companies
    ├── candidates.md      ← 10-20 names with fundamentals + price action
    ├── scoring.md         ← rubric + scored table; top 5 → tracker
    └── tracker.md         ← live watchlist for the top 5 names
```

## Workflow per theme

1. **Pick a theme from BACKLOG.md** and promote it to active.
2. **Write thesis.md** — your bet in your voice. ~600-1000 words. Mandatory sections: future you're betting on, why now, supply chain summary, what would make me wrong, re-check triggers, what the market is underestimating, what the market is over-hyping. Refine with Claude before locking. Once locked, the thesis is the contract — don't quietly edit it; write an addendum instead.
3. **Build supply_chain.md** — walk from the future to its physical/digital inputs. Each leaf node maps to public companies. Hand-curated.
4. **Populate candidates.md** — 10-20 names tied to the supply chain. One paragraph each on what they do and how they fit. Pull fundamentals.
5. **Score in scoring.md** — apply the rubric. Promote top ~5 to the tracker.
6. **Maintain tracker.md** — watch the top 5. Annotate when something material changes.
7. **Review quarterly** — is the thesis still intact? Did any falsifier trigger?

## Hard rules

- **The thesis is the human's job.** Claude helps refine but does not generate theses.
- **Never edit a locked thesis.** Write a dated addendum or mark it `Failed` and start fresh.
- **Falsifiers are mandatory.** A thesis without specific, near-term falsifiers is a vibe, not a bet.
- **No commits without explicit user confirmation.** This folder lives in git for version history, but commits and pushes only happen on direct request.
- **No imports from `app.py` or `scanner.py`.** The themes work and the deployed scanner are strictly independent. This is the invariant that makes the parallel work safe.

## Scoring rubric (default — adjust per theme as needed)

Each candidate gets scored 1-5 on each criterion, with weights summing to 100:

| Criterion | Weight | What it measures |
|-----------|--------|------------------|
| Thesis exposure | 20 | % of revenue tied to the theme |
| Bottleneck specificity | 15 | Does the company supply a hard-to-substitute input, or a commoditized one? |
| Revenue growth trajectory | 15 | YoY growth, accelerating vs. decelerating |
| Margin durability | 10 | Pricing power and gross margin trend |
| Valuation runway | 10 | P/S and P/E vs. growth — is upside priced in? |
| RS rank inflection | 10 | Just starting to inflect (better) vs. already pinned at 99 |
| Institutional positioning | 10 | 13F changes — concentrated funds entering, not exiting |
| Catalyst proximity | 10 | Near-term events (earnings, contracts, regulatory) that could re-rate |

Top ~5 by weighted score → tracker. Document the rubric per theme so when you revisit, you know exactly how candidates were selected.

## Start dates and benchmarks

Every thesis records a **start date** and **benchmark anchor prices** (SPY + a sector proxy + one or two key candidates). At review time, you compare your tracker performance against these anchors. A thesis that "worked" but underperformed SPY is not actually a win.
