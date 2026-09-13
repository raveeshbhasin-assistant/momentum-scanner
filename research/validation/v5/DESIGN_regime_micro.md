# DESIGN_regime_micro — Auction-to-auction catalyst carry with a pre-registered regime brake

Designer lens: **market microstructure and regime**. KEY prefix for all files: `RM_`.
Status: proposal only. Nothing below has been fitted or run. All thresholds are stated a priori; where a number
could only be sensible after looking at the seen 2014-2024 yearly table, that is said explicitly.

Inputs read (3 tool calls): `OVERNIGHT_holdout_report.md`, header of `OVERNIGHT_holdout_trades.csv.gz`
(`date,ticker,event,ret,on1,nc1,date_s,spy_on,spy_nc,X1_net,X2_net,X1_raw,X2_raw,q,y`), event/universe/trade definitions in
`preregistration_overnight.json`, daily-bar schema (`date,Open,High,Low,Close,Volume`), `bars1d/SPY.csv` present,
`bars1d_2004_2013` currently 695 files (partial), `earnings_dates.csv` absent.

---

## 1. Thesis (why the overnight leg after a catalyst carries, and why its sign flipped)

The verified facts to explain: after a big catalyst day the **close(T) -> open(T+1)** leg is positive (+0.79% net excess,
n=722, 2014-2024), the following intraday session is ~0 to negative, holding to the next close is worse in every cell, the
effect was **negative every year 2015-2019** and **positive every year 2020-2024** except 2023, and the same shape (overnight
positive, intraday reversing) shows in the 2024-2026 decomposition.

The overnight leg is not a "return"; it is the price difference between two auctions with **different clienteles**. Who
transacts at the closing auction of a catalyst day and who transacts at the next opening auction is what sets the sign.

**Buyers at the next open (push the overnight leg up):**
1. **Attention-driven retail.** Catalyst names appear on top-gainer screens, broker "movers" lists and social media *after
   the close*. Retail orders placed in the evening are market orders that execute in the opening auction or the first
   minutes (Barber & Odean 2008; Berkman, Koch, Tuttle & Zhang 2012 document exactly this pattern: high-attention names have
   positive overnight and negative intraday returns because the open is bid up and fades). This channel scales with the
   retail share of volume: roughly ~10% of US volume in 2015-2019, ~20-25% in 2020-2021 after zero commissions (Oct 2019),
   fractional shares, app brokers and stimulus, then ~15-20% in 2022-2024.
2. **Sell-side revisions.** Analysts publish upgrades / target raises pre-market on T+1 after an earnings-driven catalyst
   (the most informative revisions cluster in the first days after the announcement, Ivkovic & Jegadeesh 2004). This is a
   structural, all-regime contributor, but small on its own.
3. **Short covering.** A >=8% gap on 3x volume breaches risk limits and triggers borrow recalls; covering that could not be
   completed on T resumes at the next open. The 2020-2021 regime added crowd-coordinated squeezes that made this channel
   far larger than in 2015-2019.
4. **Dealer delta hedging of retail call buying.** Catalyst days draw small-lot call buying; dealers short those calls buy
   stock as the price rises and as pre-market indications rise. Single-stock option volume roughly tripled 2019 -> 2021 and
   stayed elevated. Not observable on disk; proxied by the attention variables below.
5. **Investor distraction (Hirshleifer, Lim & Teoh 2009).** When many catalysts occur simultaneously, each one is
   under-reacted to on day T and drifts afterwards. This predicts that the **count of catalysts per day is itself a
   positive conditioning variable** — independently of the retail story.

**Sellers at the next open / buyers at the close (push the overnight leg down):**
6. **Institutional day-1 momentum completion in the closing auction.** Quant and momentum desks buy the day's winners via
   MOC; the closing auction share of US volume grew from ~4% (2010) to >10% (2019+). A buy-imbalanced close over-prints the
   close price; the open reverts it.
7. **Professional gap-fading / liquidity provision.** In low-volatility, low-dispersion regimes (2017 is the archetype),
   HFT and market makers dominate the opening auction, lean against overnight order imbalances and earn the reversal.
   When retail order flow overwhelms them (2020-2021) they widen or step aside and the imbalance prints.

The observed sign is the net of (1-5) against (6-7). In 2015-2019 the professional side dominated: low retail share, low
dispersion, low VIX, growing passive/close-auction flow -> the overnight leg after catalysts was a **liquidity-provision
premium earned by the fader**, i.e. negative for the momentum buyer. From 2020 the retail/options/squeeze/distraction side
dominated -> positive. 2022 stayed positive on high dispersion and volatility despite a shrinking retail share; 2023
turned slightly negative on low VIX and narrow (mega-cap-only) participation; 2024 was positive again with AI-driven
dispersion. This is consistent with a **two-factor regime: retail/attention intensity and cross-sectional dispersion**,
both of which are observable daily from OHLCV alone.

Why this is the right trade shape for the operator's brief: it is the *earliest* rule-based entry (day 1 of the catalyst),
the exit is a fixed, mechanical horizon (next opening auction), there is no discretionary intraday management, and it is
explicitly not a hold. It also has unusually high backtest-to-live fidelity: MOC fills at the official close and MOO fills
at the official open are *exactly* the `Close` and `Open` fields in the daily bars, so the model's prices are the prices
you get — the residual is imbalance impact, covered by the 5-10 bp/side assumption.

What the evidence already rules out (not re-proposed): any intraday continuation entry, holding to the next close or
beyond at the T+1 open, the 5-minute scanner, top-20 movers and strong-close events.

---

## 2. Observable DAILY regime variables (all computable from `bars1d_*` + `bars1d/SPY.csv`, data through T-1)

Eligible universe on day T (unchanged from the pre-registration): prev close >= $5, ADV20$ >= $50M, >= 250 rows.

| id | variable | computation | mechanism it proxies | expected sign on overnight excess |
|----|----------|-------------|----------------------|-----------------------------------|
| B1 | **Strategy's own trailing net excess** | mean `X1_net` of ALL E2 events (traded or not) whose T+1 open is <= T-1, over the trailing 63 trading days; if < 30 events, extend to 126 days | regime persistence of the clientele balance (the yearly table shows multi-year sign persistence — a seen fact) | + ; threshold a priori = 0 |
| B2 | **Catalyst rate** | trailing 63-day mean of (# E2 events per day) / (eligible names / 1,000) | retail/attention intensity and investor distraction | + |
| B3 | **Cross-sectional dispersion** | trailing 21-day mean of the cross-sectional std of close-to-close returns across the eligible universe, expressed as a percentile of its own trailing 504-day window | information per catalyst; dominance of stock-pickers vs index flow | + |
| B4 | **Attention concentration** | trailing 21-day mean of (dollar volume of the day's top-20 movers by return) / (total eligible-universe dollar volume) | retail herding into movers, options/gamma crowding | + |
| D1 | SPY 21-day realised vol (annualised) | std of SPY daily log returns x sqrt(252) | market volatility (VIX substitute; VIX not on disk) | ambiguous (high in 2020/2022 positive years, but also 2018Q4) — diagnostic only |
| D2 | Breadth | share of eligible names above their 50-day MA | risk-on / risk-off | diagnostic only |
| D3 | SPY trend | SPY close vs 200-day MA | bull/bear | diagnostic only (2022 was positive in a bear market) |

Design choice: **B1 is the only brake in v1** because its threshold (zero) needs no calibration and it directly tests the
claim "the regime is persistent enough at quarterly horizon to be tradeable". B2-B4 are the *mechanism* variables; they
are tested as monotonic relationships (rank correlation), not as knife-edge thresholds, precisely to avoid fitting a
cut-off on a seen window. If B2-B4 confirm on the unseen window, a v2 brake (2-of-3 "hot") can be pre-registered later.

Honesty note: the choice of B1's 63-day window and the expectation that B2 is positive were formed after seeing the yearly
event counts (E2: 174-251/yr in 2015-2019 vs 327-535/yr in 2020-2024) and yearly means. Every test of B1-B4 on 2014-2024
is therefore labelled **refinement on a seen window**; the unseen 2004-2013 window is the confirmatory test.

---

## 3. Algorithm (rule set, v1)

**Universe.** US common stocks from `us_common_symbols.csv` (no ETFs/ADRs/SPACs/warrants); prev close >= $5; ADV20$ >= $50M;
>= 250 trading days of history. Cost tier: 5 bp/side if ADV20$ >= $100M, else 10 bp/side. Exclude a name if an earnings
release is scheduled between close T and open T+1 (only if `earnings_dates.csv` arrives; otherwise no exclusion, and the
exposure is logged in shadow).

**Event / signal (evaluated 15:45 ET on day T, universe from data through T-1).**
- S1 (= E1_big_catalyst): `Open_T/Close_{T-1} - 1 >= 0.08` AND `Vol_T(through 15:45) >= 3 x mean(Vol_{T-20..T-1})` AND
  `Px_15:45 > Open_T`.
- S2 (= E2_gapup_held): gap >= 0.03 AND `Px_15:45 >= Open_T` AND `Vol_T(through 15:45) >= 2 x ADV20sh`.
- The backtest uses the actual close; the 15:45 proxy is an implementation necessity (NYSE MOC cut-off 15:50, Nasdaq 15:55).
  The flip rate between 15:45 and the close is measured in build step 6, not assumed.

**Regime / brake.** Trade only if B1 > 0 (computed from events resolved through T-1). Otherwise submit nothing and log the
day as OFF. Operational circuit breaker (untested, safety only): if the sum of the last 20 realised trade net-excess
returns < -8 percentage points, pause 20 trading days regardless of B1.

**Ranking and capacity.** K = 5 slots. Fill S1 names first (descending day return `Close_T/Close_{T-1}-1`), then S2 names
(descending day return). Never force fills: if only 2 qualify, hold 2 and cash. At most one entry per name per day; a name
may re-qualify on later days (that is RM4, tested separately, not v1).

**Entry timing and order type.** Market-on-close order for each selected name, submitted 15:46-15:49 ET on T. Fill = official
closing print (= `Close_T` in the bars). No intraday entries of any kind.

**Exit rules.** Market-on-open (opening-auction) sell for every position, submitted 09:15-09:25 ET on T+1. Fill = official
opening print (= `Open_{T+1}`). No stops (none are possible overnight), no targets, no holds past the open, no exceptions.
The exit is a fixed horizon, not a price rule, because the decomposition shows the intraday session after the catalyst
carries zero or negative expectancy: any price-based exit that lets a position run into the session gives back the edge.

**Position sizing and risk.** Equal weight 20% of equity per slot, 100% gross max, no margin. Per-name gap-risk cap: define
`ATR20%` = 20-day mean true range / close; if `0.20 x 3 x ATR20% > 0.02` (a 3-ATR adverse gap would cost more than 2% of
equity), scale the weight to `0.02 / (3 x ATR20%)`. Portfolio overnight risk therefore <= ~10% of equity under a 3-ATR
simultaneous shock on 5 names; typical realised per-trade sd ~3.5% x 20% = 0.7% of equity per position.

**Benchmarks (per trade, identical leg).** (a) SPY `Open_{T+1}/Close_T - 1`; (b) matched non-event names: 5 eligible names
drawn on the same day from the same ADV20$ decile that had no S1/S2 flag, same leg, same costs. Excess is reported against both.

**Daily operations.**
- 15:40 ET: pull last price and cumulative volume for the eligible universe (FMP/Finnhub in prod); compute S1/S2 flags with
  the 15:45 proxy; read today's B1 state (pre-computed overnight from data through T-1); rank, size; submit MOC orders by
  15:49; log the candidate list including names *not* taken (needed for the untraded-event population that feeds B1).
- 09:15 ET (T+1): submit MOO sells for all open positions; record fills against the official open.
- Nightly: append daily bars; recompute eligible universe, E2 population overnight returns, B1-B4/D1-D3 series; update
  `RM_shadow_log.csv` (modelled vs realised fills, 15:45-vs-close flip flags, regime state) and `things_tried.csv`.
- Weekly: reconcile shadow slippage in bp and flip rate; no rule changes during the shadow window.

**Data required.** Daily OHLCV for the universe (on disk 2014-2026; 2004-2013 partial download in progress; FMP/yfinance
nightly in production), SPY daily (`bars1d/SPY.csv`; fetch 2004-2013 via yfinance with retries), a 15:45 intraday snapshot
in production (FMP quote endpoint; Finnhub fallback), optional `earnings_dates.csv`. Not needed: 5-minute bars (except
for the flip-rate study), news, order flow, options data, short interest (all would strengthen the mechanism variables
but none is required for v1).

---

## 4. Pre-registered rules and tests

Common statistics for every cell: mean net excess per trade (%), 95% CI by quarter-cluster bootstrap (>=1000 reps) AND by
day-cluster bootstrap (report both; a cell passes only on the wider CI), share of quarters positive, n, K=5 per-capital
series with max drawdown, yearly table, both benchmarks. Costs on the trade's own risk (5/10 bp/side by ADV tier). Holm
correction across the confirmatory cells RM1, RM2, RM4, RM5 (m=4; m=5 if RM6 runs). Power: overnight sd ~3.5% -> SE ~0.23%
at n=225; a +0.5% effect needs n >= 200 for 2 SE; cells with n < 150 are reported but cannot pass.

| id | definition | exit | test window | pass criterion |
|----|------------|------|-------------|----------------|
| RM1 | S1 (E1) events on days with B1 > 0 (B1 from events resolved through T-1), K=5 fill S1-first, costs by ADV tier | MOO at Open_{T+1} | Primary: 2004-2013 (unseen; run once when >= 80% of the symbol files are downloaded, file count recorded). Secondary: 2014-2024 labelled refinement-on-seen-window | lower 95% CI (wider of day/quarter cluster) > 0; n >= 150; quarters positive >= 55% among ON quarters; mean(ON) > mean(OFF, RM5); Holm-adjusted p < 0.05 |
| RM2 | S2 (E2) events on days with B1 > 0, same fills/costs | MOO at Open_{T+1} | same as RM1 | same as RM1 with n >= 500 |
| RM3 | Mechanism test: quarter-level Spearman rank correlation between each of B2, B3, B4 (quarter mean) and the quarter mean `X1_net` of all E2 events | n/a | 2004-2013 primary; 2014-2024 refinement | rho > 0 with one-sided p < 0.05 for at least 2 of 3 after Holm (m=3). Seen-window sanity check (not a pass criterion): B1 must be OFF on >= 60% of trading days in 2015-2019 and ON on >= 60% of days in 2020-2021, else the brake does not capture the regime it was designed for and RM1/RM2 are void |
| RM4 | Overnight ladder: for each S1 event, additional legs k = 1..3: buy MOC on T+k, sell MOO on T+k+1, only if `Close_{T+k} >= Close_T` (catalyst gain held) and B1 > 0 | MOO at Open_{T+k+1} | 2014-2024 (new cell, seen window -> refinement) then 2004-2013 confirmatory | lower CI > 0; n >= 300; quarters positive >= 55%; each of k = 1, 2, 3 individually >= 0 (no leg-picking) |
| RM5 | Brake validity: S2 events on days with B1 <= 0 (the OFF cell) | MOO at Open_{T+1} | 2004-2013 primary; 2014-2024 refinement | Expectation: mean(OFF) <= 0 or at least mean(OFF) < mean(ON). If OFF is also significantly positive, the brake is unnecessary and the regime story is wrong (fine for the trade, fatal for this design's claim) |
| RM6 | Conditional on `earnings_dates.csv`: S1 events where T is an earnings reaction day vs S1 events that are not | MOO at Open_{T+1} | whatever window the file covers (likely 2024-2026 -> exploratory, seen) | difference (earnings minus non-earnings) > 0 with day-cluster CI excluding 0; otherwise reported only |

Evidential asymmetry to record in the report: survivorship grows with lookback, so 2004-2013 is *more* biased toward long
continuation than 2014-2024. A **negative** result there is strong evidence against; a **positive** result there is weaker
evidence for, and must be discounted by the matched non-event benchmark (which shares the same survivorship).

All six rules, with these exact thresholds, are to be appended to `things_tried.csv` with status `pre-registered` and this
file's SHA-256 recorded before any return series is computed for the 2004-2013 window.

---

## 5. Expected economics

- Frequency: S1 ~35-120 events/yr (regime-dependent; the count is itself the regime), S2 ~175-535/yr; with the B1 brake
  expect to be active ~40-55% of trading days over a full cycle, ~0-3 positions on an average active day, 5 on busy days.
- Per-trade: if the 2020-2024 regime persists, +0.3% to +0.8% net excess per S1 trade and +0.1% to +0.3% per S2 trade; in an
  OFF regime the brake should make the strategy flat (cash). The seen holdout's K=5 series (E1-first) returned 236% over
  10 years with -16.8% max drawdown, but essentially all of it came from 2020-2024; a fair forward expectation for the
  braked strategy is **+5% to +12%/yr excess in hot regimes, ~0% in cold regimes, ~-2%/yr in the transition quarters**
  where the brake lags the regime.
- Risk: per-position overnight sd ~3.5%; a 5-slot night has ~1.5% equity sd; single-name gap tails of -20% to -30% happen a
  few times a year in this event class (sized to <= 2% equity each by the ATR cap).
- Capacity: irrelevant at retail size ($50M+ ADV names, auction fills).
- Shadow statistics: 60 days yields only ~15-30 S1 shadow trades (SE ~0.7%) — useless for a verdict. The shadow verdict
  cell is therefore S2 (n ~100 in 60 days, SE ~0.35%), with S1 tracked as a sub-cell.

---

## 6. Biggest risks

1. **The regime flips back.** 2015-2019 was negative every year; if retail participation, options activity and dispersion
   normalise, the edge is negative and B1 will lag the turn by one to two quarters (that lag is the expected cost of any
   trailing brake).
2. **B1 is a trailing-performance brake and therefore looks like curve-fitting.** It is pre-registered with a zero threshold
   and a fixed window precisely so it cannot be tuned; RM3's sanity check and RM5 are what make it falsifiable.
3. **Survivorship.** Absent delisted names inflate long continuation; the overnight leg is less exposed than multi-day
   holds but not immune (the names most likely to have delisted are the meme/squeeze names whose overnight legs were
   largest). Mitigant: matched non-event benchmark shares the bias; the 2004-2013 asymmetry rule above.
4. **15:45 signal vs 16:00 close.** Some S1/S2 flags will flip in the last 15 minutes (green -> red, volume threshold missed).
   Build step 6 measures this; if the flip rate is > 15% the live rule must be re-registered with the 15:45 proxy inside the
   backtest.
5. **After-hours binary events.** The name may have its own earnings after the close of T (or an offering, a halt, an FDA
   letter). Without an earnings calendar this is unhedged; the ATR cap and 5-slot limit bound it.
6. **Auction imbalance impact.** MOC/MOO fills on catalyst names carry imbalance-driven slippage that is not a spread; 5-10
   bp is an assumption, and shadow fills are the only way to measure it.
7. **Statistical fragility.** Holm p 0.105 on the seen window, 41% of quarters positive: the base effect is a wide-CI,
   heavy-tailed mean. A handful of trades per year drive it. Taxes (all short-term) and wash-sale rules further reduce net.
8. **Operator discipline.** The design requires never holding into the session. One "let it run" override converts the
   trade back into the intraday continuation that the evidence says loses.

---

## 7. What would kill it

- RM3 fails on the seen window's sanity check (B1 is not OFF for most of 2015-2019): the brake does not capture the regime
  and the design is void before the unseen window is touched.
- On 2004-2013 (unseen): RM1 and RM2 both fail AND RM5's OFF cell is not below the ON cell — i.e. the regime variables carry
  no information out of sample; the effect is then an unexplained 2020-2024 artefact and should not be traded.
- RM3 shows zero or negative rank correlation for B2, B3 and B4 on 2004-2013: the microstructure story is wrong even if the
  trade happens to work; keep the trade only as an unexplained empirical rule with a much tighter risk budget.
- Shadow (>= 60 days, n >= 100 S2 trades) mean net excess <= 0, or realised auction slippage > 15 bp/side, or 15:45 flip rate
  > 15% that is not neutral in return terms.
- 2004-2013 download stalls below 80% of names: run RM1-RM5 anyway, label the window "partial", and treat any pass as
  provisional until the download completes and the cells are re-run once (pre-declared here so it is not a second look).

---

## 8. Build steps (in order; none touches conditional returns before step 5)

1. `RM_regime_features.py` — for each window (`bars1d_2004_2013`, `bars1d_10y`, `bars1d_all`): daily eligible universe,
   E1/E2 flags, per-day n_E2, eligible count, B2, B3, B4, D1-D3. Output `RM_daily_regime_<window>.csv`. No return columns.
2. `RM_brake_state.py` — walk-forward B1 (from E2 overnight legs resolved through T-1; the 2014-2024 legs already exist in
   `OVERNIGHT_holdout_trades.csv.gz` — re-derive from bars for the other windows). Output ON/OFF per day and the yearly
   ON-share table. Check RM3's seen-window sanity criterion here, before any conditional return is computed.
3. Append RM1-RM6 to `things_tried.csv` as `pre-registered`; record this file's SHA-256 in `RM_prereg_hash.txt`.
4. Fetch SPY 2004-2013 daily via yfinance with retries (backoff, 5 attempts); write `RM_SPY_2004_2013.csv`. Record the
   `bars1d_2004_2013` file count; proceed when >= 80% of `us_common_symbols.csv` names that have data in that era are present,
   otherwise run and label partial (see kill list).
5. `RM_holdout_run.py` — one run of RM1-RM5 on 2004-2013, then on 2014-2024 (labelled refinement), day- and quarter-cluster
   CIs, both benchmarks, Holm across cells, yearly tables, K=5 capital series. Output `RM_holdout_report.md`,
   `RM_holdout_cells.csv`, `RM_holdout_trades.csv.gz`. Update `things_tried.csv` with outcomes.
6. `RM_1545_flip_study.py` on `bars5m_inplay` (929 gappers, 60 days): share of E1/E2 flags at 15:45 that differ from the
   close-based flag; distribution of `Close/Px_15:45 - 1`. Implementation-fidelity only; no return conditioning.
7. If RM1 or RM2 passes on the unseen window (or passes provisionally on a partial window): production shadow >= 60 trading
   days in the scanner service (a new, isolated module; no import coupling with themes; no display). Log candidates,
   B1 state, modelled and realised auction fills, flip flags. Verdict cell = S2 with n >= 100.
8. Only after a passing shadow: expose as a notifier line ("MOC candidates, regime ON/OFF") — the operator places the MOC
   and MOO orders manually; no auto-execution. Any subsequent rule change re-enters at step 3.
