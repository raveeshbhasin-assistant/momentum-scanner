# DESIGN_execution_risk — Catalyst-Close-to-Open (CCO), designed from the execution and risk seat

Author lens: execution and risk practitioner. Written 2026-09-13 before any new test window is touched.
Inputs read: preregistration_overnight.json, OVERNIGHT_holdout_report.md, OVERNIGHT_holdout_trades.csv.gz (measurement only — tail quantiles, events per night, nightly portfolio distribution under capped sizing; no thresholds were tuned).

## 0. What the evidence allows, restated from the risk seat

The only continuation that survived this project's own tests is the overnight leg after a big-catalyst day (E1: gap >= 8%, volume >= 3x ADV20, green close; buy official close T, sell official open T+1). The per-event legs show what kind of edge it is, and that changes how it must be executed and sized:

| statistic (E1 / X1, net of 5 bp/side, excess over SPY, n=722, 2014-09..2024-08) | value |
|---|---|
| mean | +0.79% |
| median | -0.22% |
| win share | 41.8% |
| per-trade sd | 7.9% (yearly sd 0.8-3.0% in 2015-19, 7-12% in 2020-22, 3.6% 2023, 7.1% 2024) |
| q01 / q05 / q95 / q99 | -15.6% / -6.7% / +13.3% / +33.9% |
| min / max | -26.3% / +68.6% |
| share of trades < -10% / < -15% / < -20% | 2.5% / 1.25% / 0.55% |
| mean with top 1% / 5% / 10% of trades removed | +0.28% / -0.64% / -1.09% |
| mean with both 5% tails trimmed | 0.00% |
| events per night (535 nights) | 1: 77%, 2: 17%, 3: 4.5%, >=4: 1.9% (max 19 on 2020-11-09) |
| SPY overnight on event nights | sd 0.78%, q01 -1.74%, min -7.45% |

Three consequences:

1. **The edge is a right-tail (lottery-ticket) payoff.** The trader loses 58% of nights and is paid by ~6% of nights that gap +10% or more. Any execution choice that systematically misses winners (limit prices that fail to fill on names ramping into the auction, discretionary "too extended" skips, taking profits pre-market) removes the only thing that makes money. Conversely no stop-loss exists overnight, so risk is controlled only by position size and position count.
2. **The dangerous names are the profitable names.** Within the seen window the mean comes from day-return >= 25% names (+3.05%, sd 14%, n=199); the 8-25% names are ~0 (this is a seen-window slice and is registered below as a hypothesis, not acted on). Per-position worst case therefore has to be planned at the offering/short-report/halt scale, not at the 3-4% "normal overnight" scale.
3. **Live validation of the edge is not feasible on a useful timescale.** At sd 7.9% and ~72 events/year, distinguishing +0.8% from 0 at 2 SE needs ~390 events (about 5 years). A 60-day shadow (roughly 15-25 events) can validate *execution fidelity* (fills vs backtest prices, proxy conversion) but cannot validate the edge. The decision to size up must rest on the unseen 2004-2013 confirmatory test plus a regime brake, and the operator must know that in advance.

## 1. Universe

- US common stocks from us_common_symbols.csv, primary listing NYSE/Nasdaq/NYSE American; no ADRs-by-default exclusion needed but exclude ETFs, SPAC units/warrants, and names with a scheduled reverse split.
- Data through T-1: previous close >= $5; ADV20 dollar >= $50M (the registered holdout universe). The $20-50M tier is an *extension* and is only admitted if EXR-2 passes; it carries 10 bp/side in all accounting.
- Excluded regardless of signal: names halted (LULD or news) at the 15:44 decision snapshot; names with a company earnings release scheduled between close T and open T+1 (FMP earnings calendar in production; earnings_dates.csv in backtests when it exists); names where day T's high-low range / open <= 2% (takeover pin — the stock is glued to a deal price, overnight return ~0, pure risk-budget waste; registered as EXR-6b).

## 2. Event definition (unchanged from the pre-registration; no new thresholds)

E1 at close T: `Open_T/Close_{T-1} - 1 >= 0.08` AND `Volume_T >= 3 x mean(Volume_{T-20..T-1})` AND `Close_T > Open_T`.

Because the two closing conditions are only known at 16:00 and MOC cutoffs are earlier, the live system evaluates a **15:44 ET proxy**: gap >= 8% (known since the open), cumulative volume through 15:44 >= 2.6x ADV20 (the closing auction and last 15 minutes typically carry 8-15% of a heavy day's volume), last price >= Open_T x 1.01 (1% cushion above the green threshold). The proxy-to-close conversion is a pre-registered fidelity test (EXR-3), and the fallback entry if it fails is described in section 3.

## 3. Entry timing and order type

**Primary: market-on-close (MOC), official closing auction.**
- NYSE-listed names: MOC entered by 15:49 ET (exchange cutoff 15:50). Nasdaq-listed names: MOC entered by 15:54 ET (cutoff 15:55). After cutoff only imbalance-offsetting LOCs are accepted — do not use them; a missed cutoff means no trade that night.
- Why MOC and not LOC: the fill is the official closing print, which is exactly the backtest entry price, with zero spread cost; the retail order is a price-taker in an auction that on a 3x-volume day clears $5-15M in a $50M-ADV name, so a $5-10k order has no measurable impact (< 2 bp). An LOC at last x 1.005 would protect against a closing spike but non-fills would be concentrated in names with buying pressure into the auction — precisely the right-tail candidates. With a right-tail payoff, guaranteed participation beats price protection.
- Why not 15:58 market: it pays half the spread (5-15 bp in $20-100M names, 1-3 bp in liquid names) plus 5-10 bp sd of noise versus the closing print, for the benefit of verifying the event with 2 more minutes of data. It is the **fallback** only if EXR-3 shows the 15:44 proxy converts < 85% of the time; then use a marketable limit at last x 1.003 at 15:58:30, and add 10 bp to the cost assumption.
- Position count: at most 3 names per night, ranked by day return descending (the fill order already registered for the K5 series). 92% of E1 events fall on nights with <= 3 events, so the cap rarely binds, and when it does the night is a macro cluster (2020-11-06/09/23, 2024-08-07) where correlation, not selection, is the risk.

## 4. Exit rules

- **Market-on-open (MOO) at the official opening auction of T+1, always.** Enter the MOO between 08:30 and 09:20 ET (Nasdaq accepts MOO until 09:28; NYSE until 09:30 — do not rely on the last minutes). The fill is the official opening print, which is exactly the backtest exit price. Opening auctions the day after a catalyst are typically 1-2% of that day's volume ($1-3M in these names), so a $7.5k order is 0.3-0.8% of the auction: budget 2-5 bp impact in $20-100M names, ~0 in liquid names.
- **No pre-market trading, no discretion.** If an offering or downgrade hits overnight, the pre-market book is thin and the opening auction is normally the best liquidity of the morning; selling at 07:00 into a 2% wide market forfeits more on average than it saves. If the name is halted at the open, the MOO rests and fills at the reopening auction.
- **Never hold to the next close.** The registered X2 leg is worse in every cell (E1: +0.39% vs +0.79%, maxDD -32% vs -17%). No "sell above a threshold" logic exists in this design because the holding period is fixed at one auction-to-auction leg; the operator's "fixed return or technical exit" requirement is satisfied trivially: the exit is the open.
- **No stop-loss.** None is executable overnight; sizing is the stop.

## 5. Position sizing and risk (the rule)

Per-position weight `w`:
- Full size: **w = 7.5% of sleeve equity per name, max 3 names per night, max 22.5% gross overnight.**
- Halve to 3.75% if previous close < $10, or if the name is in the $20-50M ADV tier (only after EXR-2 passes; before that the tier is excluded).
- Pilot phase: w = 2.5% for the first 100 live events after shadow. Shadow phase: w = 0.
- No leverage. Cash account behaviour: proceeds from the 09:30 sale are available for the 15:50 buy (settled-funds rules do not bite because the same cash is only cycled once per day; if the broker enforces T+1 settlement on the sale proceeds, keep sleeve cash at 1.3x the maximum nightly exposure).

Measured on the seen window with fill order by day return (net of 5 bp/side, excess over SPY, additive):

| rule | avg exposure | mean/night | sd/night | worst night | q0.5% night | additive 10y | additive maxDD | yearly range |
|---|---|---|---|---|---|---|---|---|
| K=3, w=7.5% | 9.7% | +0.083% | 0.71% | -1.76% | -1.43% | +44% | -6.7% | -2.3% (2019) to +27.6% (2020) |
| K=3, w=10% | 13.0% | +0.110% | 0.94% | -2.34% | -1.91% | +59% | -9.0% | -3.0% to +36.9% |
| K=4, w=5% | 6.6% | +0.053% | 0.47% | -1.17% | -1.03% | +28% | -4.5% | -1.5% to +17.5% |
| K=5, w=20% (registered K5) | 26.4% | +0.217% | 1.87% | -4.69% | -3.82% | +116% | -18.0% | -6.0% to +72.6% |

**Worst-case planning numbers (plan, not observed average):**
- Per position: **-30%** (observed min -26%; mechanism: overnight follow-on offering priced at a 10-20% discount after a big up day, short-seller report, fraud/regulatory halt reopening, broken deal). Expect ~2 trades per year worse than -10% and one trade every ~2.5 years worse than -20%.
- Per night at full size (7.5% x 3): **plan for -3.0% of sleeve equity** (observed worst -1.76% across 535 nights; a -7.45% SPY overnight, as in March 2020, at beta 1.5 on 22.5% gross is -2.5%). Catastrophic bound with all three names at -30%: -6.75%.
- Per year at full size: plan for **-4%** in a 2015-2019-type regime (observed -2.3% worst year plus friction drift); the brake below is what limits this.
- Sleeve drawdown limits: -6% additive from peak → halve w; -10% → w = 0 and a written review before restart.

## 6. Regime brake (pre-registered; both variants)

The effect was negative every year 2015-2019 and positive 2020-2022 and 2024. The same years split on realized dispersion of the trade returns themselves (sd 0.8-3.0% vs 7-12%), which is consistent with the right-tail mechanics: when post-catalyst overnight dispersion is low there is no tail to be paid by.

- **B1 (dispersion brake, primary):** trading is ON when the trailing-40-event sd of realized E1 overnight net excess (computed each morning from the events that occurred, whether traded or not) is >= 5%; OFF (w = 0, keep logging) when < 5%. The 5% threshold sits between the 3.6% (2023, negative year) and 7.1% (2024, positive year) observed values and is therefore **fitted on the seen window**; it is confirmed only if EXR-4 passes on 2004-2013. Lag: ~6 months of events.
- **B2 (performance brake, fallback, hypothesis-free):** OFF when the trailing-100-event mean net excess <= 0; back ON when the trailing-40-event mean > +0.3%. With sd 7.9% this false-stops a true +0.8% edge about 15% of the time per check and correctly stops a zero edge 50% of the time per check — it is a slow brake and is stated as such.
- Macro binary skip (risk rule, not alpha): no new positions on the night before a CPI or payrolls release (08:30 ET prints land inside the holding period). Cost: roughly 10% of events; the excess-over-SPY edge is unaffected by construction, but the unhedged trader's realized gap tail is reduced. Registered as EXR-6c with a measurement, applied regardless of outcome unless it removes more than 0.1% of mean.

## 7. Friction ledger — what survives of the gross overnight move

Gross raw overnight (E1, seen window) +0.99%; SPY overnight on those nights +0.10%; so gross excess +0.89%.

| friction | estimate | note |
|---|---|---|
| commission | 0-3 bp round trip | IBKR Pro $0.005/sh; zero at Lite/most retail |
| closing auction spread/impact | 0-2 bp | MOC fills at the official print; order is < 0.2% of the auction |
| 15:44 proxy contamination | 5-15 bp | 10-15% of fills expected to fail the green-close/volume test at 16:00; their overnight leg is assumed ~0 |
| opening auction impact | 2-5 bp ($20-100M names), 0-1 bp (liquid) | order is 0.3-0.8% of the opening auction |
| SEC/TAF fees | < 1 bp | sells only |
| regulatory/halt tail | not a mean cost; a variance cost | covered by sizing |
| **expected to the trader** | **≈ +0.80% raw, ≈ +0.70% over SPY per trade** | ~80% of the gross move survives; the binding risk is regime, not friction |

If the 15:58 marketable-limit fallback is used instead of MOC, subtract a further 10 bp (half-spread + noise) in the $20-100M tier and 3 bp in liquid names.

Capacity: in a $50M-ADV name the strategy tolerates ~$50k per position before opening-auction impact approaches 10 bp; at w = 7.5% that is a ~$650k sleeve. Not binding for one trader; it is binding if the sleeve is ever levered or pooled.

## 8. Pre-registered rules (thresholds fixed now; test windows labelled)

Common protocol: costs 5 bp/side for ADV >= $100M, 10 bp/side for $20-100M, charged on the trade's own notional; benchmark A = SPY over the identical leg; benchmark B = matched non-event names (same day, same ADV decile, no E1/E2 event, equal-weight) over the identical leg; day-cluster and quarter-cluster bootstrap 90% and 95% CIs; every cell appended to things_tried.csv with its label ("confirmatory / unseen 2004-2013" or "refinement on a seen window 2014-2024") before results are read.

| id | definition | exit | test window | pass criterion |
|---|---|---|---|---|
| EXR-1 | E1 as registered; universe prev close >= $5, ADV20 >= $50M; buy official close T, sell official open T+1 | MOO open T+1 | 2004-01..2013-12 (unseen, confirmatory; partial download acceptable if >= 7 full years) | mean net excess over SPY > +0.30% AND day-cluster 90% CI lower bound > 0 AND >= 50% of quarters positive AND mean over matched non-event names > +0.30%. Power: expected n 300-450 → SE 0.25-0.30% at blended sd 5%; detects +0.8% at ~3 SE, cannot resolve +0.3%. |
| EXR-2 | EXR-1 with universe ADV20 $20-50M only, 10 bp/side | MOO open T+1 | 2004-2013 confirmatory; 2014-2024 as "refinement on a seen window" (tier was outside the registered universe) | mean net excess >= +0.30% with 90% CI lower bound > -0.10% AND q01 of trade returns not worse than -20% AND n >= 150 in the confirmatory window. Otherwise the tier stays excluded. |
| EXR-3 | Execution fidelity: at 15:44 ET, proxy = gap >= 8%, cum volume >= 2.6x ADV20, last >= open x 1.01; measure P(E1 satisfied at 16:00 | proxy) and mean(close/last_15:44 - 1) | n/a (measurement) | bars5m_inplay (929 gappers, 60 days; ADV20 from bars1d_all) — unseen for this purpose | conversion >= 85% AND |mean drift 15:44 → close| <= 0.15% AND mean overnight leg of proxy-but-failed names >= -0.5%. Fail → switch entry to 15:58:30 marketable limit (last x 1.003) and add 10 bp to costs. |
| EXR-4 | Dispersion brake B1: ON when trailing-40-event sd of E1 net excess >= 5%, else OFF | MOO open T+1 | 2004-2013 confirmatory (the 2014-2024 split is already known and is a refinement on a seen window) | ON-state mean net excess minus OFF-state mean >= +0.50% AND ON-state day-cluster 90% CI lower bound > 0 AND ON state covers >= 25% and <= 75% of events (otherwise the brake is not discriminating). Fail → use B2 only and keep w at pilot size. |
| EXR-5 | Extreme-mover concentration hypothesis: E1 events with day return (Close_T/Close_{T-1} - 1) >= 25% vs 8-25% | MOO open T+1 | 2004-2013 confirmatory only (seen-window values +3.05% vs ~0 are the origin of the hypothesis) | >= 25% subset mean net excess exceeds the 8-25% subset by >= +1.0% AND its 90% CI lower bound > 0. Pass → nothing changes in sizing (equal weight), but the 8-25% names may be dropped in a later, separately registered version. Fail → no change. Not acted on before confirmation. |
| EXR-6 | Risk exclusions measured for cost: (a) company earnings between close T and open T+1 (needs earnings_dates.csv); (b) takeover pin: (High_T - Low_T)/Open_T <= 2%; (c) night before CPI/payrolls (payrolls approximated as first Friday; CPI dates from BLS calendar) | MOO open T+1 | 2014-2024 labelled refinement on a seen window (measurement of an exclusion, not a new signal); repeat on 2004-2013 | Each exclusion is adopted if it removes <= 0.10% from the mean net excess of the remaining set AND (for a and c) the excluded set's q05 is worse than the included set's by >= 2%, or (for b) the excluded set's |mean| < 0.3% and sd < 2%. |
| EXR-7 | Live shadow and pilot: shadow >= 60 calendar days logging 15:44 proxy decisions, hypothetical MOC/MOO fills vs official prints, brake state; pilot w = 2.5% x <= 3 for 100 events | MOO open T+1 | live, forward | Shadow pass: >= 90% of proxy decisions reproducible from the daily-bar backtest definition; median |hypothetical fill - official print| <= 5 bp. Pilot pass: realized fills within 10 bp of official prints on >= 90% of legs; no operational miss (missed cutoff, unintended pre-market trade) in the last 50 events. Edge is NOT a pilot pass criterion (underpowered by construction). |

## 9. Daily operations (ET)

- 09:35 — from the opening prints, list names with gap >= 8% and prev close >= $5, ADV20 >= $50M (typically 2-15 names). Check earnings calendar for tonight; drop scheduled reporters.
- 15:40 — snapshot last price and cumulative volume for that list (existing scanner's Finnhub/FMP quote feed suffices; 15 names, one call each). Check halt status.
- 15:44 — apply proxy; rank by day return; take top 3; apply brake state (B1/B2) and sleeve drawdown rule to set w.
- 15:46 — submit MOC for NYSE names; 15:52 — submit MOC for Nasdaq names. Log intended vs submitted.
- 16:05 — record fills vs official close; any difference > 5 bp is an incident.
- 08:30-09:20 next day — submit MOO for every open position. No exceptions for news, halts, or pre-market prints.
- 09:35 — record fills vs official open; append to the trade log; recompute trailing-40 sd and trailing-100 mean; publish tomorrow's brake state.
- Weekly — reconcile the trade log with the backtest definitions; append anything new to things_tried.csv.

## 10. Expected economics (honest)

At full size (7.5% x <= 3), on the seen window: ~+4.4%/year additive on sleeve equity on average, composed of -1.1%/year in 2015-2019 and +9.9%/year in 2020-2024, sd of nightly return 0.71% on ~54 active nights/year, additive maxDD -6.7%. Per trade: ≈ +0.70% over SPY after all frictions, 42% win rate, median -0.22%. On a $100k sleeve that is roughly +$4k/year averaged, with individual years from -$2.3k to +$28k. If the brake works (EXR-4), the 2015-2019-type years become ~0 with the upside retained. This is a small, positively skewed sleeve that pays for itself only if the operator does not intervene on losing nights — not a primary income strategy.

## 11. Biggest risks

1. Regime dependence: negative every year 2015-2019; the dispersion brake is fitted on that split and unconfirmed.
2. Right-tail dependence: remove the top 5% of trades and the edge is negative; any missed winner (limit non-fill, discretionary skip, pre-market exit) is disproportionately expensive; the operator will experience 58% losing nights.
3. Overnight offerings and short reports in the exact names that carry the edge: -10% or worse in 2.5% of trades, -20% or worse in 0.55%; no stop exists.
4. Survivorship: E1 is a long-continuation rule on 2026 survivors; the bias grows with lookback and is largest in the 2004-2013 confirmatory window; the matched-non-event benchmark is the only partial control.
5. Proxy risk: the decision uses 15:44 information for a 16:00 condition; if conversion is < 85% the entry changes and costs rise ~10 bp.
6. Correlated nights: macro clusters (elections, vaccine day, Aug 2024) put all three positions on the same gap; a -7.45% SPY overnight is in-sample.
7. Underpowered live validation: ~5 years of live events to confirm the edge at 2 SE; the operator must accept that the go/no-go rests on the historical confirmatory test.

## 12. What would kill it

- EXR-1 fails on 2004-2013 and EXR-4 does not separate ON from OFF there: the effect is a 2020-2022 artifact and the design is shelved (logged, not re-cut).
- EXR-3 conversion < 70%: the event cannot be traded at the close by a retail order and the fallback costs eat the median trade.
- Live: 10 consecutive incidents of fills more than 10 bp from official prints, or any unintended pre-market/next-close hold — an execution process that cannot replicate the auction prints has no edge to capture.
- Sleeve reaches -10% additive drawdown at pilot size.

## 13. Build steps

1. Append EXR-1..EXR-7 to things_tried.csv with labels and pass criteria (before any 2004-2013 file is opened).
2. Write EXR_confirm_2004_2013.py: load bars1d_2004_2013 + SPY (yfinance with retries, cached to research/EXR_SPY_2004_2013.csv), reproduce E1/X1 exactly per the registration, add matched-non-event benchmark, day/quarter-cluster CIs; emit EXR-1, EXR-2, EXR-4, EXR-5, EXR-6b/c cells in one run; write EXR_confirm_report.md. Run once.
3. Write EXR_proxy_fidelity.py on bars5m_inplay + bars1d_all for EXR-3; write EXR_proxy_report.md.
4. If earnings_dates.csv appears, run EXR-6a on both windows (labelled) and append.
5. Build the live module as a separate process from the scanner (no shared code): 09:35 candidate list, 15:40 snapshot, 15:44 decision, order tickets (MOC/MOO) with exchange-aware cutoffs, fill reconciliation vs official prints, brake state, trade log to a new data file; shadow mode by default (w = 0).
6. Shadow >= 60 days; pilot at 2.5% x <= 3 for 100 events; then 7.5% x <= 3 with the drawdown ladder; every stage gated by the criteria in EXR-7.
