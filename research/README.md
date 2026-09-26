# research/ — reference tooling for the 2026-09-13 scanner review

- `harness/` — canonical bar-level simulator (bars.py, sim.py, evaluate.py, tests/). Acceptance: re-resolved 8,053 live picks (2026-06-17 → 09-11) with 99.8% result agreement vs the production resolver (`ACCEPTANCE_summary.json`). Use it, not `backtest.py`, for any strategy evaluation. Conventions: entry at next-bar open, costs per side on the trade's own stop, 15:55 exit, day-cluster bootstrap CIs.
- `scripts/` — data builders used by the review: `build_master.py` (joins performance_log to the raw daily files), `fetch_bars.py` (60-day 5-min cache for the scanner universe + ETFs), `fetch_broad_universe.py` (US common-stock list → 2y daily bars → ex-ante gap screen → 5-min bars for gappers), `explore_inplay_baserate.py` (discovery-window base rate on gap days), `V_all_claims.py` (independent recomputation of the review's headline claims), `A5_build_market.py` (daily market/breadth table).
- Data caches live outside the repo (session scratchpad); rebuild with the scripts. yfinance limits 5-min history to 60 days.
- Evidence: `../research_findings_100day_review.md`. Plan and locked rule family: `../research_plan_v4_inplay.md`, `../preregistration_v4.json`.

Ignition Watch research (swing-horizon, daily bars; feeds `ignition/` and themes_web `/ignition`):

- `ignition_discovery/` — 2026-09-20. What precedes Dell/Micron-style explosive moves: 628 episodes of +80% in 63 sessions (S&P 500+400, 2015–26) → the ignition signal and its 5-signal backtest. Pipeline, `results.json`, standalone `report.html`.
- `ignition_exits/` — 2026-09-26. When has an ignition failed? 28 exit rules × 748 trades on train/holdout/live splits → the "close below pre-ignition base" sell rule; dropping off the signal list is not a sell.
