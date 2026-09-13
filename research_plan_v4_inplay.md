# v4 Plan and Validation Record: Stocks-in-Play Universe, Swing Alternative, Control Arm

**Date:** 2026-09-13. Pre-registration written at 08:15 ET before any test-window computation; validation run once the same morning; red-team review of the registration completed in parallel (see §6.5).
**Companions:** `research_findings_100day_review.md` (the diagnostic evidence), `preregistration_v4.json` (the locked rule family), `research/validation/` (scripts, cell tables, things-tried log), `research_findings_missed_winners_60d.md` (July's independent exhaustion of the current search space).

---

## 1. The problem, in one paragraph

Over 100 live days the scanner emitted 12,203 picks that earned −0.03R per trade before costs and about −0.23R after realistic entry and 5 bp per side. That is not a scoring problem. A long entered at *any* bar of the current 211-name large/mid-cap universe with the scanner's 2×ATR stop and 2.5R target loses −0.07R before costs, and the scanner's picks are indistinguishable from those random bars. The stop is a 5-minute ATR (median 0.8%), so costs are 0.15R per trade. Nothing in the payload, in multi-day context, in exits, in timing, or in prior-day regime carries out-of-sample information. Re-scoring the same bets cannot fix that; improvement had to mean changing what is traded.

## 2. The hypothesis that was registered, and the honest state of its evidence

**Thesis.** Intraday momentum should work where there is a reason for the stock to move: a catalyst. The current universe almost never contains such a day for a given name; the broad US market has 100–150 of them daily. Matching the strategy class (continuation, breakout, pullback-in-strength) to a dynamic universe of *stocks in play* was the one change the diagnostic evidence permitted.

**Exploratory look (discovery window only, 2026-06-17 → 07-31).** Before registration, the scanner's trade structure was applied to every decision bar 09:35–11:00 on ex-ante gap days (RTH open ≥ 3% above the prior close, prior close ≥ $5, 20-day dollar ADV ≥ $20M) across a 2,814-name universe. Averaged over all such bars it showed +0.08R gross on gap-ups, +0.13R on gaps ≥ 5%, +0.16R on gap-ups under $100M ADV, versus −0.07R on the static universe. **This lift did not survive the registered accounting, and the reason is instructive:**

| Same weeks, same bars, one change at a time | n | gross avgR | net avgR |
|---|---|---|---|
| All gap-ups, entry at signal close, 09:35–11:00 (the exploratory number) | 33,161 | +0.081 | +0.010 |
| → entry at next bar open instead | 33,161 | +0.079 | +0.007 |
| → **top-20 gappers per day only** (the registered watchlist) | 9,791 | **−0.044** | **−0.106** |
| → plus decision bars through 14:30 (the registered benchmark) | 32,634 | −0.097 | −0.189 |

Entry timing and slippage are irrelevant (median 0 bp). The entire exploratory lift was **pick-weighting**: days with many gap-ups are strong market days, and an average over all their bars overweights them. Weighting days equally, as any tradeable watchlist must, the in-play base rate is negative before the first rule is applied. Costs then take a further 0.06–0.09R. This is exactly the failure mode the pre-registration was designed to catch, and it caught it before the test window was opened.

## 3. The registered algorithm (kept here as the record of what was tested)

**Universe, rebuilt every morning ex-ante:** US common stocks, prior close ≥ $5, ADV20 ≥ $20M; watchlist = names gapping ≥ +3% at the RTH open, top 20 by gap. Cost tiers: 5 bp/side (ADV ≥ $100M), 10 bp/side ($20–100M).
**Signals (first trigger, one position per ticker-day):** R0 benchmark (every watchlist name at the 10:05 open); R1 gap-and-hold (10:00 close above RTH open, above VWAP, ≥ 0.5% above the open; enter 10:05); R2 first VWAP pullback 10:00–12:00; R3 15-minute opening-range breakout before 11:00 above VWAP; R4 = R1 on $20–100M ADV names; R5 = R1 on gaps ≥ 5%.
**Structures:** S_a = 2×ATR14 stop, 2.5R target, exit 15:55; S_b = day-low stop with a 1% floor, no target, exit 15:55.
**Portfolio:** K = 5 concurrent, 1% risk each; per-pick R and per-capital P&L both reported.
**Alternatives registered in the same family:** U3, a two-year multi-day swing family on the same broad universe (gap continuation 3-day, 20-day breakout with relative strength 5-day, big-gap drift 5-day; T+1 open entry, 10 bp/side, excess over SPY, 8 quarterly folds); U2, the current picks as a "do less harm" control arm (fixed K = 10, no 12:00–13:00 entries, 0.6% stop floor, with/without an opening-tape gate).

## 4. Validation protocol (as locked)

Test once on the untouched window (intraday: 2026-08-01 → 09-11, 29 sessions; swing: 8 quarters 2024-09 → 2026-09). Accounting: next-bar-open entry, skip if the open is beyond a barrier, same-bar ambiguity → loss, gap-through stops filled at the open, exit at the 15:55 bar close, costs per side on the trade's own stop, R in own-risk units, day-cluster bootstrap CIs (1,000 reps). Pass = net avgR lower 95% CI > 0 **and** excess over both benchmarks (R0 and the same-day-same-bucket random watchlist bar) > 0 **and** n ≥ 300 **and** ≥ 25 days, Holm-corrected across the 11 intraday cells. Swing pass = quarter-cluster CI of net excess over SPY > 0 and ≥ 6 of 8 quarters positive. Every extra variant evaluated is logged in `things_tried.csv` (6 entries).

## 5. Production changes that still apply

Only the parts that do not depend on a passing rule: (a) stand the current scanner down as a signal source, or keep it purely as a logger; no configuration of it should be traded (§6.3); (b) if anything is shadow-logged in future, log the full per-signal payload listed in the review (§5.2 of the previous draft is preserved in `preregistration_v4.json`'s spirit: universe version, gap, ADV, cost tier, rule id, signal and entry bars, stop/target, ATR method, VWAP, data source, latency, rank, counts, config version; never backfilled); (c) evaluation plumbing adopts the realistic accounting and reports net R with day-cluster CIs and per-capital P&L; the reference simulator is `research/harness` (99.8% agreement with the live resolver), not `backtest.py`. Any production change requires operator approval; every push is a deploy.

## 6. Results of validation (run once, 2026-09-13)

### 6.1 U1 in-play intraday family: **no cell passes**

Test window 2026-08-01 → 09-11, 29 sessions, 490 watchlist ticker-days with bars. Net of costs. Random-bar benchmark on the watchlist (S_a, all decision bars): **−0.168R** (n = 28,990).

| Cell | n | days | net avgR | 95% CI | hit | stopped | pnl%/trade | excess vs R0 | excess vs random bar | pass |
|---|---|---|---|---|---|---|---|---|---|---|
| R0 all-watchlist / S_a (benchmark) | 483 | 29 | −0.158 | [−0.317, +0.014] | 4.6% | 46.8% | −0.40 | — | −0.04 | — |
| R0 / S_b | 483 | 29 | −0.196 | [−0.340, −0.043] | 0% | 47.4% | −0.39 | — | −0.08 | — |
| R1 gap-and-hold / S_a | 156 | 27 | −0.181 | [−0.431, +0.096] | 5.8% | 53.8% | −0.38 | −0.02 | −0.07 | no |
| R1 / S_b | 156 | 27 | −0.112 | [−0.301, +0.091] | 0% | 24.4% | −0.52 | +0.08 | 0.00 | no |
| R2 VWAP pullback / S_a | 307 | 28 | −0.255 | [−0.440, −0.053] | 17.3% | 68.1% | −0.34 | −0.10 | −0.16 | no |
| R2 / S_b | 307 | 28 | −0.326 | [−0.502, −0.128] | 0% | 69.7% | −0.44 | −0.13 | −0.23 | no |
| R3 ORB-15 / 2R | 162 | 28 | −0.146 | [−0.318, +0.003] | 1.9% | 21.0% | −0.44 | +0.01 | −0.07 | no |
| R3 / no target | 162 | 28 | −0.142 | [−0.329, +0.037] | 0% | 21.0% | −0.37 | +0.05 | −0.06 | no |
| R4 small-ADV tilt / S_a | 78 | 22 | −0.370 | [−0.595, −0.105] | 2.6% | 59.0% | −1.09 | −0.21 | −0.22 | no |
| R4 / S_b | 78 | 22 | −0.164 | [−0.360, +0.067] | 0% | 24.4% | −1.03 | +0.03 | −0.01 | no |
| R5 gap ≥ 5% tilt / S_a | 103 | 23 | −0.055 | [−0.378, +0.242] | 5.8% | 48.5% | −0.11 | +0.10 | −0.04 | no |
| R5 / S_b | 103 | 23 | −0.041 | [−0.259, +0.181] | 0% | 20.4% | −0.26 | +0.16 | −0.02 | no |

Holm-adjusted one-sided p = 1.0 for every non-benchmark cell. Per-capital at K = 5 and 1% risk: every cell lost between −6% and −36% over the 29 sessions, with 23–43% positive days. Discovery-window values were the same sign (R0/S_a −0.117, R1/S_a −0.266, R2/S_a −0.111, R3/S_a −0.080; only R4/S_b was marginally positive at +0.010 on n = 63). Removing the day-length filter the red team flagged (which drops halted names) changes R0 by −0.01R and nothing else. Both months of the test window are negative for every cell except R5 in September (n = 14).

**Reading:** on a properly weighted watchlist of the day's biggest gappers, the momentum-continuation structure loses before and after costs, in discovery and in test, under both stop constructions. The universe change does not create an edge; if anything the biggest gappers mean-revert intraday from 10:00 onward.

### 6.2 U3 multi-day swing family: **no cell passes**

2,700 names with a full two-year history (4.1% of the 2,814 excluded for survivorship; the bias direction is favourable to the rules, so the true numbers are worse). Net of 10 bp/side, excess over SPY across the identical holding window.

| Rule | trades | mean return | mean SPY | **excess** | quarter-cluster CI | quarters positive | K=10 total return |
|---|---|---|---|---|---|---|---|
| W0 gap ≥ 3%, hold 3 (benchmark) | 17,077 (14,903 under the one-open-position rule) | +0.49% | +0.64% | −0.15% (−0.18% verified) | [−0.34, +0.05] | 4 / 8 (2 / 8 verified) | +3.1% |
| W1 gap ≥ 5%, strong close, 2× volume, hold 3 | 954 (938 verified) | −0.22% | +0.35% | −0.57% (−0.38% verified; the 16 overlapping-position trades averaged −11.5%) | [−1.06, −0.09] (verified [−0.76, +0.02]) | 3 / 8 (2 / 8 verified) | −19.9% |
| W2 20-day breakout + RS + MA50, hold 5 | 4,348 | −0.47% | +0.22% | −0.69% | [−1.46, +0.07] | 2 / 8 | −44.3% |
| W3 gap ≥ 8%, 3× volume, green, hold 5 | 502 | −1.50% | +0.38% | −1.88% | [−3.00, −0.73] | 1 / 8 | −50.7% |

**Reading:** continuation after gaps and breakouts underperforms simply holding SPY at every horizon tested; the biggest gap-ups revert hardest over the following week. This closes the "same idea, longer horizon" alternative on two years of data with far more power than any intraday test in this project.

### 6.3 U2 control arm (current picks, realistic accounting): **negative in every configuration**

| Configuration | window | n | net avgR | 95% CI | K=10 total return (29 sessions) |
|---|---|---|---|---|---|
| C0 all picks | TEST | 3,827 | −0.200 | [−0.284, −0.112] | −28.6% |
| C1 K=10, no 12:00–13:00, 0.6% stop floor | TEST | 290 | −0.098 | [−0.277, +0.082] | −27.6% |
| C2 = C1 + opening-tape gate (SPY & QQQ above prior close) | TEST | 120 (12 days) | −0.195 | [−0.421, +0.060] | −21.9% |

Discovery-window values: −0.256, −0.076, −0.161. **Even the most conservative configuration of the existing scanner loses roughly 1% of capital per day at 1% risk per trade.** There is no "do less harm" setting worth running with money.

### 6.4 Exploratory only, not registered: the short side

Because drift after every long entry tested is negative, fading was checked on the discovery window only (equal-per-day weighting, next-bar-open entry, costs plus a borrow proxy): shorting the static universe's extended names at 10:05 earned +0.08 to +0.10R net [−0.09, +0.30], entirely from June (+0.26/+0.32) with July at 0.00; shorting the gap-up watchlist was ~0 (June −0.36, July +0.17); fading the scanner's own picks −0.05. Sign-unstable across two months, therefore **not registered and not proposed**. It is recorded in `things_tried.csv` so nobody rediscovers it as new.

### 6.5 Red-team review of the registration (completed in parallel)

Sixteen risks were raised; the material ones for the numbers above: (1) the exploratory base rate used signal-close entry and a day-length filter that drops halted names; both were addressed (next-bar-open entry in all registered cells; sensitivity without the filter reported); (2) VWAP is effectively RTH-only in yfinance data, a live-vs-backtest divergence; (3) the swing universe filters must use T−1 data only (they do); (4) tie-breaks and window boundaries were tightened; none changes any conclusion. **Independent verification (completed 2026-09-13, separate agent, own code from raw bars):** 11 cells recomputed; 10 CONFIRMED within 0.03R / 0.1 pp (all eight U1 cells with identical n and day counts; U3 W0; U2 C0), 1 PARTIALLY_CONFIRMED (U3 W1: the orchestrator's per-trade metric let a new signal open while a position was still open, which added 16 trades averaging −11.5% and overstated the loss; under the locked rule W1 is −0.38% [−0.76, +0.02], still negative, still failing; W0 n is 14,903 not 17,077 and 2 of 8 folds positive). No look-ahead found in 30 + 30 sampled trades and whole-file checks: entries strictly after the signal bar, ex-ante watchlist inputs, swing ADV through T−1, SPY excess on identical dates. Two minor departures from the locked table were noted and are immaterial: VAL1 uses a 1% instead of 0.5% floor for the R2 no-target stop (that cell is −0.33 with CI upper −0.13) and a ≥70-bar day filter (effect < 0.01R). The accounting convention (entry cost embedded in the fill, stop set relative to the adjusted fill) is conservative by ≤ 0.03R. **Survivors after verification: none.**

### 6.6 Decision

No registered rule meets the pass criteria. Under the protocol (§7 of the previous draft, now §7 below) the conclusion is a **stand-down, not another tier.**

## 7. What this means and what remains open

1. **The intraday long-momentum product has no retail-cost edge in any universe we can observe:** not in liquid large caps (100 live days, 753k random bars), not in the day's biggest gappers (60 days, both windows), not as a multi-day continuation (two years, 2,700 names). Every construction that "should" work (breakout, pullback, ORB, gap-and-go, day-low stops, hold-to-close) is negative after costs. The consistent finding across all of it is mild mean reversion after upward extension.
2. **Recommendation for the live system:** stop treating its output as tradeable; turn off notifications; keep it running only if its logging is useful as a control record. Do not ship the control-arm settings; they still lose.
3. **What would count as genuinely new information** (each would need its own pre-registration and ≥ 40 days of shadow before any display): news *content* classification per gapper (earnings beat vs. dilution vs. M&A; counts alone showed nothing in July); an earnings-event calendar to isolate post-earnings drift from generic gaps; order-flow or Level-2 data; a systematic short/fade study with real borrow costs if the operator can short. The zero-cost step that builds toward all of these is logging the daily in-play watchlist with the full payload from now on.
4. **Process rule going forward:** no rule enters the dashboard without a registered file, a temporal split, day-cluster CIs, a cost model, a benchmark, and a things-tried log. This round shows the discipline working: an attractive exploratory number (+0.16R) was traced to a weighting artefact before a single test-window trade was looked at.

## 8. Timeline

- **2026-09-13:** review, registration, single validation run, red-team review: complete. Independent verification of the headline cells: next usage window; verdicts appended to §6.5.
- **Operator decisions:** commit these documents; stand down the scanner as a signal source; whether to start zero-cost in-play watchlist logging (needs a small `app.py` job and operator approval to deploy).
- **Revisit trigger:** a new information source from §7.3, or ≥ 6 months of logged watchlist data.
