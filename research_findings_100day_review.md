# 100-Day Live Review of the Momentum Scanner (2026-04-20 → 2026-09-11)

**Date:** 2026-09-13
**Data:** every live pick in `performance_log.json` from the `data-backups` branch (12,203 picks, 100 trading days), joined 1:1 to the raw per-day emission files; 5-minute bars incl. pre-market for the 236-name universe + index/sector ETFs + VIX for the last 60 sessions (2026-06-17 → 09-11); 2 years of daily bars.
**Method:** twelve independent analysis lenses (statistics, code audit, measurement fairness, trade construction, market regime, research-process audit, universe, payload features, timing, whole-universe opportunity set, multi-day context, intraday context), each headline claim independently recomputed by an adversarial verifier, then a completeness critic. Every rule was tested with a temporal split (discovery ≤ 06-30 or 06-17→07-31 for bar-level work; test ≥ 07-01 or 08-01→09-11) and day-cluster bootstrap CIs. Scripts and full reports are in the session scratchpad (`research/F*_report.md`, `A5_regime_report.md`, `V_report.md`).
**Conventions:** `R` = P&L / (entry − stop). WIN = 2.5R target touched first; LOSS = stop touched first (−1R); EOD = exit at the 15:55 close. `avgR` is the decision metric. The log books zero costs.

---

## 1. Headline: what the recommendations actually earned

| Set | n | Hit 2.5R | Stopped | avgR (log, zero cost) | 95% CI |
|---|---|---|---|---|---|
| All picks, 100 days | 12,203 | 12.3% | 53.4% | **−0.034** | [−0.087, +0.019] |
| Test half (≥ 07-01) | 6,790 | 11.4% | 54.6% | **−0.075** | [−0.133, −0.007] |
| STRONG (live since 05-12) | 3,090 | 14.5% | 55.5% | −0.019 | — |
| TRADEABLE (live since 07-08) | 756 | 16.7% | 62.6% | −0.093 | — |
| ELITE (live since 07-08) | 116 | 15.5% | 66.4% | −0.155 | — |

Monthly avgR: Apr +0.09, May +0.09, Jun −0.05, Jul −0.08, Aug −0.03, Sep −0.17. The median pick stops out (−1R). Average price change after entry is ≈ 0%: the picks have no directional drift.

**What a trader following the picks would have earned.** An independent 5-minute-bar replica reproduces the log on 99.88% of 8,047 post-06-17 picks, so the log is an honest record of its own rules. Its rules are not attainable: entry is booked ~30 s into the forming bar, before any alert exists, and costs are zero. With entry at the next bar's open (slippage ≈ 0.01R, neutral) and 5 bp per side:

| Step (post-06-17, n≈7,900) | avgR | 95% CI |
|---|---|---|
| Log as recorded | −0.086 | [−0.139, −0.027] |
| Next-bar-open entry | −0.085 | [−0.141, −0.023] |
| + 5 bp/side | **−0.234** | [−0.290, −0.173] |
| + 10 bp/side | −0.383 | — |

At the median 0.80% stop, 5 bp/side is 0.149R per trade (0.2–0.3R on the 22.7% of picks with stops under 0.5%). Realistically the book lost about −0.18% of notional per trade, −31.5R per day, and was profitable on 12% of days. Every month and every tier is negative net.

## 2. Why: four verified facts

**2.1 The trade structure is negative from every bar on this universe.** A long entered at the close of *any* RTH bar (09:35–14:30) in the 211-name universe with a 2×ATR14 stop and a 2.5R target earns **−0.066R** [−0.126, −0.005] before costs (753,480 bars, 60 days), negative in every 15-minute bucket, every month and every volatility tercile. Hit 10.5%, stopped 54%.

**2.2 The scanner adds no selection.** Its emitted bars earn −0.011R [−0.041, +0.021] relative to random bars at the same time on the same day (verifier: −0.022). Its daily avgR correlates 0.87 with a naive "buy every universe name at the same scan times" portfolio. The scanner is slightly better than chance at being in a stock that has a winning bar that day (×1.16, stable every month) and worse than chance at choosing the bar (×0.92); 70% of first emissions come *after* the day's first winning bar. Net effect on avgR: zero.

**2.3 Daily P&L is beta.** SPY open-to-close alone explains a third of day-to-day avgR (+0.28R per +1% SPY); the universe's share of names up on the day explains 42%. Expectancy is positive only on days SPY closes > +0.3% above its open (+0.19R, 29 days); flat days (−0.11) are as bad as down days (−0.15). Yet only 3.6% of *pick-level* R variance is "which day": regime decides whether to trade, never which stock. The May→Sep decline is market + noise + in-sample selection, not skill decay; April–May (+0.09, 28 days, CI includes 0) sat on a friendly tape, a different cron layout and split-adjustment artifacts.

**2.4 Costs finish the job.** Because the stop is a 5-minute ATR (median 0.8%, decaying to 0.4% by 14:30), retail costs are 0.15R per trade. Gross ≈ −0.07R plus 0.15R of cost = the −0.23R observed.

## 3. What was tested and does not work (out of sample)

- **Every shipped tier.** STRONG (13-day memo claim: 53.6% hit / +1.19R) → live 14.5% / −0.02. TRADEABLE (claim +0.285) → test −0.064. ELITE v3.7.5 (claim +0.512 on n=76) → test −0.114. Anti-extension "good shape" is worse (−0.157 vs −0.032) inside TRADEABLE. All 23 avgR claims in the memos have live CI upper bounds below the claim; 3 of 4 tiers shipped with 0 shadow days; the STRONG claim rests on backfilled flags.
- **Every gate the tiers use.** 09:30–10:00 window (+0.178 in discovery → +0.049 test; it raises hit *and* stop rate: faster resolution, not expectancy), category D, rvol [2,5), rsi ≥ 68, stop ≥ 0.9%: all test diffs within ±0.05R. 0 of 36 pre-registered rules survive; an exhaustive search of 5,344 slices finds 10 OOS "survivors" where 134 are expected by chance, and 0 of 247 discovery survivors carry over.
- **Everything in the payload.** 0 of 28 fields are sign-stable (max |Spearman| with R in test 0.03); the composite score correlates 0.009 with R and inverts in test; 12 models (logistic, ridge, gradient boosting; day-blocked CV and temporal split) have OOS AUC ≤ 0.575 and top-decile OOS avgR ≤ 0 (≤ −0.12 net).
- **Multi-day context** (20-day return, distance from highs, realized vol, gap, ADV, sector/market trend, VIX): no feature reaches |ρ| 0.04 in test; every discovery-significant one shrinks to ~0 or flips. Random-date cross-validation inflates their value 2–5× by identifying the month.
- **Prior-day regime rules** (VIX vs MA20, prior-day return, breadth, trend, weekday): 172 rules, Spearman(discovery, test) = 0.04. The only ex-ante tape signal (SPY/QQQ above prior close at 09:35) is absent in discovery, equal in size in the naive benchmark, and leaves the kept book at −0.14R net: beta management, not edge.
- **Exits, stops, targets, timing.** 0 of 163 exit constructions (stops 1–4×ATR / signal-bar / day-low / none; targets 1–4R / none; breakeven; trails; time stops; MAE exit; scale-out) are positive net in test; best −0.149R vs baseline −0.203R. Entering 1/2/3 bars later or on a breakout of the emission bar: −0.21 to −0.22R. Price drifts *against* the pick from the first bar (−0.066R at 5 min, −0.20R by the close without a stop); 42% reach +1R, 11% reach +2.5R.
- **Ticker and sector history.** Winner tickers do not persist (ρ ≈ 0); the 13-day memo's sector exclusion is worth +0.014R; the discovery "good" sectors (Semis/Tech/AI) were the worst in test. High-beta, high-vol, gap-up slices flip sign between halves: beta, not quality.
- **The all-bars dip anatomy.** The first winning bar of a day is a dip below VWAP (the mirror of STRONG), but the real-time "buy the dip" rule built from it is at base rate (OOS AUC 0.51); the July 60-day study had already found the same inversion. The 12:00–13:00 bucket is reliably the worst (−0.13R, both halves, all six months).

## 4. Pipeline defects (real, but not the cause)

| Severity | Location | Defect | Effect |
|---|---|---|---|
| critical | `config.py` gates + `scanner.py` sort/cap | Gates barely bind (25th-pct rvol 1.53 vs gate 1.33; cap binds in 4.9% of scans): 19.6 scans × 6.2 picks = **122 picks/day = 52% of the universe** | The "scanner" is a lister; P&L is tape beta |
| critical | performance log / memos | No cost model anywhere; all claims are gross | 0.15R/trade hidden |
| major | `scanner.py:497-527` indicators on a 5-day `prepost=True` frame | ~162 extended-hours bars per session inside EMA/RSI/MACD/BB/ATR: opening ATR 22% **wider** than RTH, RSI shifted 11.7 pts, EMA-stack disagrees 31%; Wilder ATR decays to 0.4% stops by afternoon | Wrong indicators; cost-dominated stops |
| major | `scanner.py:567-620` `calculate_rvol` | 09:35/09:40 denominators include 09:20/09:25 pre-market bars (RVOL ~2× inflated; gate pass 69.5% vs 23.7% clean); fallback divisor yields rvol up to 195 | Gate is noise at the open |
| major | `scanner.py:997` composite | Score unrelated to R (corr 0.009); technical score quantised to ~12 values; negative sentiment clipped to 0; 1,427 of 6,240 score ≥ 60 picks owe the tier to sentiment | No selection |
| major | leadership / earnings modules | 52% UNKNOWN (3,798 unmapped incl. 79/80 mid-caps); LEADER < UNKNOWN in both windows; earnings flag never true in 100 days | Dead weight |
| major | `config.py` ELITE history, `/api/backfill_strong` | Pre-05-12 STRONG flags backfilled; ELITE v3.7.1 tuned on ~128/188 backfilled picks; v3.7.5 thresholds from cells of n=22–62 | Look-ahead-contaminated tiers |
| major | `backtest.py` | 2 bp/side, no commissions, 15:55 vs 16:00 exit, technicals-only, no walk-forward; its "winning" MAE exit is worth +0.002R live | Unfit as a validation tool |
| minor | `calculate_trade_levels` | Entry is a ~30 s print of the forming bar (no R effect); 9% of picks are re-emits while the position is open; 20 excluded-sector names never emittable; 5 delisted names in the universe | Hygiene |

## 5. Why every iteration failed: the process

Each tier was discovered on the same short live window it was evaluated on (3, 13, 37 days), on a rising tape, with hit rate rather than expectancy as the metric, no cost model, no clustered errors, dozens of candidate rules per memo, and 0–2 shadow days before shipping. Claims shrank 2–4× live (median hit claim 33% → 15%; median avgR claim +0.94 → −0.02). Statistical power was never there: at the observed day-level dispersion, detecting +0.15R/trade at 10 picks/day needs ≈ 74 trading days; at 3 picks/day it cannot be validated within a year. The July 60-day study (`research_findings_missed_winners_60d.md`) had already concluded that no robust intraday-long rule exists in this search space; this review confirms it with stronger evidence and adds that the structure itself is negative on random bars.

## 6. Design constraints for any replacement (verified evidence → rules)

1. **Accounting:** entry at the open of the first bar after the signal; skip if that open is beyond stop/target; ≥ 5 bp/side (report 10 bp) on the pick's own stop; dedupe re-emits; 15:55 exit; per-pick avgR **and** per-capital P&L under an explicit concurrency cap; day-cluster bootstrap ≥ 1,000 reps.
2. **Benchmarks:** report every candidate as excess over (a) the time-matched naive all-universe portfolio and (b) the same-day-same-bucket random bar, net of costs. Raw avgR and hit rate are not decision metrics.
3. **Validation:** pre-registered rule file (thresholds, features with live-since dates, family size, metric) before any test computation; temporal split, then monthly walk-forward; random-date CV banned; ≥ 300 picks and ≥ 40 days per cell; Holm correction across the family; pass = net avgR lower CI > 0 in test; shadow ≥ 40 days / ≥ 400 picks before display, ≥ 80 days with lower CI > 0 before capital; kill on trailing-40-day CI upper < 0.
4. **Pick volume:** fixed K (10–20/day) by rank, not threshold gates.
5. **Edge target:** ≥ +0.15R gross per trade at a 0.8% stop, or a stop construction with cost < 0.10R (floor ≥ 0.6% or a daily-ATR fraction).
6. **No more work on** exits, trails, time stops, breakeven, scale-out, entry delay, confirmation breakouts, freshness gates, ticker/sector lists, payload re-weighting, multi-day context, or prior-day regime features **for the current pick set**. The 2×ATR / 2.5R / EOD structure is negative on random bars in this universe: a v4 must either show positive excess over the random bar in both windows or change the universe/structure and re-establish a base rate first.
7. **No stops < 0.5%; no 12:00–13:00 entries; ban backfilled features; indicators on an RTH-only frame; RVOL denominator from same-minute RTH bars; join daily bars by UTC date.**

## 7. Bottom line

The scanner's recommendations have no edge: they are statistically indistinguishable from buying random bars of random large caps, in a trade structure that loses on those bars before costs and loses ~0.23R per trade after them. Nothing inside the current data or code can be re-weighted into an edge; the July study and this review agree on that from independent data. Improvement therefore means changing what is traded, not how the same picks are scored. See `research_plan_v4_inplay.md` for the pre-registered plan.
