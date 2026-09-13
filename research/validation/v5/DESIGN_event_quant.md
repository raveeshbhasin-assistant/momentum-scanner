# DESIGN_event_quant — Catalyst-Close-to-Open (CCO)

Lens: event-driven quant. Designer key: `EVENTQ`. Written 2026-09-13 before any cell below was run.
Status of windows: 2024-09..2026-09 = DISCOVERY (spent). 2014-09..2024-08 = HOLDOUT, now PARTIALLY SEEN (aggregate yearly results of E1..E4 x X1/X2). 2004-2013 = UNSEEN (download in progress, 682/3,179 files at time of writing).

## 0. Reframing the operator's ask under the evidence

The operator wants to "pick up trades early, when they are building momentum, and sell above a threshold / technical / fixed return." The verified evidence says where that momentum actually pays in US single names:

| Leg after a catalyst day | Evidence | Sign |
|---|---|---|
| Intraday continuation, 5-min bars, any hour | 753k bars, live -0.23R/trade over 100 days | negative after cost |
| Day's top-20 gappers, intraday | discovery + test windows | negative |
| Multi-day continuation from T+1 OPEN | 2-year family | -0.15% to -1.9% vs SPY per trade |
| **Close T -> Open T+1 after big catalyst** | holdout n=722 | **+0.79% net excess [+0.08, +1.48]** |
| Close T -> Close T+1 (same events) | holdout | +0.39% [-0.30, +1.09] (intraday leg is -0.40%) |
| 5 days after catalyst | discovery | -1.25% |

So "early" = the close of the catalyst day itself (the first bar where the event is fully observable), and the only leg with a measured positive expectation is the overnight one. Every extension (hold intraday, hold 5 days, technical trail) has been measured and is worse. The CCO algorithm therefore treats the T+1 opening auction as the primary exit for every event type, and the operator's "fixed return / technical / time" menu is applied *before* the open (pre-market limit) or *at* the open, never after it.

Entry and exit are both auction prints (MOC buy, MOO sell). That is the strongest implementability feature of this design: the backtest's Close_T and Open_{T+1} are literally the prices a small retail order receives, so the 5 bp/side cost assumption covers commission plus auction impact with margin in $50M+ ADV names.

## 1. Universe (data through T-1 only)

- US common stock from `us_common_symbols.csv`; exclude ADRs? No — keep as-is to match the holdout; note ADR overnight behaviour is driven by home-market hours (log as a covariate, do not filter).
- Prev close >= $5; ADV20 dollar >= $50M (identical to the holdout); >= 250 rows of history.
- Two cost tiers for accounting: ADV20 >= $100M -> 5 bp/side; $50-100M -> 10 bp/side. (The holdout used 5 bp uniformly; the confirmatory run will report both.)
- Exclude names with a split factor on T or T+1 (a reverse split is not a gap).

## 2. Event detection at the close of T

Signals are computed at 15:40 ET from a live snapshot (open, high, low, last, cumulative volume) and finalized with official OHLCV after the close.

Primary event **E1 big catalyst** (identical to the holdout definition):
- gap = Open_T / Close_{T-1} - 1 >= 8%
- Volume_T >= 3 x mean(Volume_{T-20..T-1})  [live: cumulative volume at 15:40 already >= 3x ADV20 — a strictly stricter live proxy, so live can only miss events, never add them]
- Close_T > Open_T  [live: last at 15:40 >= Open_T x 1.005 — the 0.5% buffer absorbs the last 20 minutes; the close-flip rate is a shadow metric]

Secondary event **E2 gap-up-held** (holdout definition): gap >= 3%, Close_T >= Open_T, Volume_T >= 2 x ADV20sh. E2 fills empty slots only, at half weight (section 5).

Not used: E3 strong-close (-0.02%), E4 top-20 return-with-volume (-0.04%). A close location or "strength" score without a gap is dead (E3); a top-N-by-return rank without a gap/volume threshold is dead (E4). This is why the ranking in section 4 is applied *within* E1/E2, never as a substitute for them.

Mechanistic exclusion **deal-pin**: gap >= 15% AND (High_T - Low_T)/Open_T <= 2% -> likely a cash acquisition target pinned to deal price; expected overnight ~0, so it only dilutes. Registered as its own cell (R3) so the base E1 stays comparable with the holdout.

## 3. Separating earnings from other catalysts

Why: the exit menu and the risk differ. Earnings gaps are scheduled, are disclosed *outside* market hours, and sit on the best-documented drift in the literature (PEAD). Non-earnings 8% gaps in the E1 population (mean day-T return +23.5%, sd 20%, from `OVERNIGHT_holdout_trades.csv.gz`) are dominated by biotech/FDA, M&A, and news; those names have a specific overnight hazard — the follow-on offering priced overnight after a big up day — which is the most plausible source of E1's 41.8% win rate and the -1.25% five-day return.

Classification, in order of preference:
1. **`earnings_dates.csv`** (if the acquisition agent succeeds): T is an earnings event if a report is dated T (BMO) or T-1 (AMC). Ambiguous timing -> either date counts.
2. **SEC EDGAR daily form index** (free, no key, needs only a User-Agent header; back to the 1990s): an 8-K filed on T or T-1 with Item 2.02 (Results of Operations) -> earnings; 8-K with Item 1.01/8.01 and no 2.02 -> non-earnings news; no 8-K -> unknown. CIK->ticker via `company_tickers.json` from the SEC. This is the recommended path for the 2004-2013 confirmatory window.
3. **Bar-only quarterly-periodicity proxy** (usable today): T is `earnings_proxy=1` if at least 2 of the 3 lag windows [T-58..T-68], [T-121..T-131], [T-184..T-194] trading days contain a day with Volume >= 2.5 x that day's own ADV20. Earnings recur quarterly; almost nothing else does. Validate the proxy's precision on 2024-2026 against whichever true source lands; report the confusion matrix before using it in any cell.

Event types and their exit menus:

| Type | Definition | Default exit | Fixed-return option | Technical | Time |
|---|---|---|---|---|---|
| A. Earnings E1 | E1 and earnings=1 | MOO T+1 | Pre-market limit sell at entry x (1+3%) from 07:00 ET, cancel-replace to MOO at 09:26 if unfilled (shadow-logged variant, not default) | none intraday (evidence 1) | T+1 open, always |
| B. Non-earnings E1 | E1 and earnings=0 | MOO T+1 | none — the tail is the whole return (win 41.8%, mean from large opens); a cap removes it | none | T+1 open, always |
| C. E2 fill-in | E2 and not E1 | MOO T+1 | none | none | T+1 open, always |

No PEAD extension is proposed for trading. The measured 5-day leg after E1 is -1.25% and X2 < X1 for every event; an earnings-only multi-day cell may be *registered* on the unseen window (R2b) but is research, not a trade.

## 4. Cross-sectional ranking and capacity

Event arrival is lumpy: 34-123 E1 events/year in the holdout (0.1-0.5/day) with clusters on earnings-season days. Most days have 0-2 qualifying names; capacity of 5 slots binds only on cluster days.

Fill order: all type A/B (E1) first, then type C (E2). Within a type, rank by relative volume Volume_T / ADV20sh (attention proxy consistent with the retail-demand-at-the-open mechanism in Lou, Polk & Skouras 2019). Take the top 5. The ranking is registered as a non-inferiority cell (R4), because the choice is forced by capacity and the rank only needs to not hurt.

## 5. Position sizing and risk (costs on the trade's own risk)

- Per-trade net overnight sd for E1 is 7.9% (holdout trades file); win share 41.8%; the distribution is right-skewed with a fat left tail (overnight offerings, guidance reversals).
- Size: 10% of capital per E1 position, 5% per E2 position; max 5 positions (50% gross max). Expected average exposure ~5-15% of capital; the rest is idle by design — CCO is an overlay, not a fully invested book.
- Worst-case accounting: a -40% overnight gap on a 10% position = -4% of capital; two on the same night = -8%. Position sizing is the *only* overnight defense — there is no stop between 20:00 and 04:00.
- Costs in R units: 10 bp round trip vs 7.9% sd = 0.013 sd per trade; the edge (+0.79%) is 0.10 sd per trade. This is a low-information, many-trades edge: it needs ~80+ events/year to be visible above noise within a year.
- Per-capital reference: the registered K5 series (20% each) returned +236% over 10 years with -16.8% max drawdown for E1 (includes the five negative years 2015-2019); the E2 series +685% / -25.1%. Halving weights to 10%/5% roughly halves both.

## 6. Regime brake

The effect was negative every year 2015-2019 (-0.07% to -0.50%) and positive 2020-2024 (+2.9, +1.5, +0.4, -0.1, +1.1). No external regime variable is registered (it would be fitted on the seen window). The brake is the strategy's own track record:
- Full size when the trailing-100-event mean of X1_net (SPY-adjusted, net of the trade's own cost tier) > 0; half size when <= 0. Never zero, so the ledger keeps learning.
- Halt-and-review when the trailing 12-month net excess <= 0 with >= 60 events.
- Registered as R5 across the full 2004-2026 event series; pass = braked series has max drawdown lower than unbraked and Sharpe not lower by more than 0.10.

## 7. Pre-registered rules (thresholds fixed here; windows named)

Statistics for every cell: mean excess return per trade net of cost tier; benchmark = SPY over the identical leg; second benchmark = matched non-event names (same day, same ADV20 decile, |gap| < 1%, Volume_T < 1.5 x ADV20sh; mean of their overnight return); quarter-cluster bootstrap 95% CI (>= 1,000 reps) and day-cluster CI; share of quarters positive; yearly table; ADV tier split (>= $500M / $100-500M / $50-100M) as the survivorship check; Holm across all cells registered in this document (R1, R2a, R2b, R3, R4, R5, R6 = 7 cells).

Power (honest): with sd 7.9%, SE = 7.9/sqrt(n): n=200 -> 0.56%, n=400 -> 0.40%, n=722 -> 0.29%. A true +0.79% needs n ~ 385 iid trades for a 95% lower bound above zero, more with quarter clustering. 2004-2013 with a smaller surviving universe will likely yield 150-400 E1 events, so the unseen window can confirm only a *large* effect on its own; the decision therefore uses the unseen window as a no-contradiction test and the pooled 2004-2026 series as the significance test.

| id | definition | exit | test window | pass criterion |
|---|---|---|---|---|
| R1 | E1 big catalyst, holdout definition, universe of section 1, 5 bp/side (10 bp reported for the $50-100M tier) | MOO T+1 | 2004-01..2013-12 UNSEEN (partial coverage reported) then pooled 2004-2026 | Unseen: point estimate of net excess >= 0 AND matched-control excess >= 0. Pooled: quarter-cluster lower CI > 0, >= 50% of calendar years positive, matched-control lower CI > 0, effect present (point > 0) in the >= $500M ADV tier. All four required. |
| R2a | R1 split by earnings flag (file > EDGAR 8-K 2.02 > periodicity proxy; the source used is recorded) | MOO T+1 | unseen + pooled; on 2014-2024 labelled "refinement on a seen window" | Earnings subset: pooled lower CI > 0. Non-earnings subset: point estimate > -0.30%. If the non-earnings subset fails, type B is dropped and only type A trades. |
| R2b | Earnings-only E1, research extension | Close T -> Open T+6 (5 sessions) | unseen + pooled | Research cell: pass = lower CI > 0 AND better than R2a-earnings by > 0.5%. Not traded regardless unless it passes AND survives a second shadow. |
| R3 | Deal-pin exclusion: gap >= 15% AND (High-Low)/Open <= 2% | MOO T+1 | pooled | Mechanistic check: excluded set has |mean overnight| < 0.30% and n >= 30; if so, exclusion is adopted operationally (it removes dilution, not risk). |
| R4 | On days with > 5 qualifying E1, top-5 by Volume_T/ADV20sh vs the rest | MOO T+1 | pooled | Non-inferiority: mean(top-5) >= mean(rest) - 0.20%. If it fails, fall back to equal-weight all qualifying names capped at 5 by smallest gap (registered fallback). |
| R5 | Regime brake: trailing-100-event mean X1_net > 0 -> full size else half; halt at 12-month <= 0 with >= 60 events | n/a | full 2004-2026 series | Braked max drawdown < unbraked AND Sharpe(braked) >= Sharpe(unbraked) - 0.10. |
| R6 | E2 gap-up-held, holdout definition, half weight, fills empty slots | MOO T+1 | unseen then pooled | Unseen: point >= 0. Pooled: lower CI > 0, >= 50% years positive, n >= 1,500. Fails -> E2 tier is dropped and the book runs E1 only. |

Not allowed after this file is saved: changing any threshold above; adding exits beyond MOO/pre-market-limit-variant; using Volume_T or Close_T in the universe filter; random-date cross-validation; treating 2024-2026 as evidence.

## 8. Daily operations

1. 15:35 ET — refresh ADV20/universe from bars through T-1 (done nightly, checked here); pull live snapshot for the universe (production FMP or broker feed).
2. 15:40 — compute gap, cumulative RVOL, green-with-buffer, day range; tag earnings (file / EDGAR 8-K on T or T-1 / proxy); apply deal-pin exclusion; rank; size (brake state from the ledger).
3. 15:45 — submit MOC buys (NYSE cutoff 15:50, Nasdaq 15:55). Write the intended list with snapshot values to the ledger *before* the close.
4. 16:10 — reconcile fills; recompute the event with official OHLCV; mark `rule_miss` if Close_T <= Open_T or Volume_T < 3x (position is still exited at the open; rule-miss rate is a shadow KPI, target < 10%).
5. 07:00-09:26 — (variant, shadow-logged only) pre-market limit sell at +3% for type A; 09:26 cancel-replace to MOO.
6. 09:25 — submit MOO sells for every open position (cutoff 09:28 on both exchanges). No exceptions, no discretionary holds.
7. 09:40 — reconcile; compute realized net excess vs SPY open/close and vs matched controls; update the trailing-100 brake; append to `things_tried.csv` only when a *rule* changes, and to the trade ledger every day.
8. Nightly — daily bars refresh; weekly symbol-list and split check; monthly EDGAR CIK map refresh.

Data required: daily OHLCV for the universe (existing), SPY daily open/close, one 15:40 intraday snapshot per day (open/high/low/last/cum-volume for ~3,000 names — one batch quote call), split/corporate-action flags, earnings dates (file or EDGAR 8-K daily index), broker MOC/MOO support (confirmed). Not required: news text, order flow, 5-minute bars.

## 9. Expected economics (anchored)

- E1: +0.79%/trade net excess (holdout mean; [+0.08, +1.48]); sd 7.9%; ~50-120 events/yr in a 3,000-name universe, of which ~90% are tradeable within 5 slots. At 10% weight: +0.08% of capital per event, ~+4% to +8%/yr contribution in a 2020-2024-type regime, roughly -0.3% to -2%/yr in a 2015-2019-type regime.
- E2 tier at 5% weight: +0.32%/trade [+0.05, +0.62], ~300/yr but slot-limited; +2% to +5%/yr in the good regime, about -0.5%/yr in the bad one.
- Whole-book: expect low-to-mid single-digit annual excess on total capital, ~1-2% annualised vol from the overlay, max drawdown of the overlay in the -8% to -15% range at these weights (K5 at 20% saw -16.8% / -25.1%). Sharpe of the overlay ~0.8-1.0 if the 2020-2024 regime persists, ~0 if it does not. The brake is what converts "~0" into "small negative with small drawdown" rather than "five consecutive losing years."

## 10. Biggest risks

1. Regime dependence: five straight negative years (2015-2019). The effect is real in aggregate but not shown to be structural.
2. Survivorship: the 2026-09 symbol list drops every name that delisted; big-gap names that later failed are exactly the ones missing, and the bias grows with lookback — the 2004-2013 window is the most exposed. The ADV >= $500M tier check and the matched-control benchmark are the only mitigations available without a delisted-names source.
3. Fat left tail with a mechanism: overnight follow-on offerings after a big up day, guidance walk-backs on the call, pre-market news reversals. Sizing is the only defense.
4. Low power in the confirmatory window (150-400 events at sd 7.9%); the pooled test leans on a partially seen window.
5. Live-vs-backtest mismatch at 15:40 (close-flip, volume projection, snapshot latency) and auction impact in the $50-100M tier.
6. Earnings misclassification if only the periodicity proxy is available.
7. Multiple-testing creep: seven cells are registered here; every later cell must be added to the Holm family in `things_tried.csv`.

## 11. What would kill it

- Unseen 2004-2013: E1 overnight point estimate < 0, or matched-control excess <= 0 (the "edge" is just the generic overnight premium every stock earns).
- Pooled 2004-2026 quarter-cluster CI includes zero after Holm across the 7 cells.
- Effect present only in the $50-100M ADV tier and absent above $500M (survivorship signature).
- Shadow >= 60 days: realized mean excess more than one SE below the backtest expectation for the same period, rule-miss rate > 10%, or auction slippage > 15 bp/side vs the official print.
- Two consecutive trailing-100-event windows <= 0 after go-live.

## 12. Build steps

1. Freeze this file; copy section 7 into `EVENTQ_preregistration.json` with thresholds verbatim; append one row to `things_tried.csv` ("EVENTQ registered, not yet run"). Do this before opening `bars1d_2004_2013`.
2. Earnings source: use `earnings_dates.csv` if it lands; otherwise build `EVENTQ_edgar_8k.py` (SEC daily form index, 8-K item filter, CIK map) for 2004-2026; in parallel implement the periodicity proxy and measure its precision/recall on 2024-2026 against the best available truth. Record which source each cell used.
3. `EVENTQ_engine.py` on daily bars: universe (T-1 info only), E1/E2 flags, earnings tag, deal-pin flag, split check, matched controls, cost tiers, SPY legs (fetch SPY 2004-2013 via yfinance with retries; cache to `EVENTQ_SPY_2004_2013.csv`).
4. Run once on 2004-2013 when the download completes (report file coverage; if < 1,500 names, say so in the report header). Then the pooled run. Outputs: `EVENTQ_cells.csv`, `EVENTQ_report.md`, `EVENTQ_trades.csv.gz`; log every cell to `things_tried.csv`.
5. Decision gate on section 7. Drop tiers/types that fail; do not tune.
6. If R1 passes: new independent service (no shared code with scanner or themes) — 15:40 snapshot, event engine, MOC/MOO order scripts, ledger, brake; shadow mode >= 60 trading days with intended-vs-realized, rule-miss, slippage, and close-flip KPIs; no display until shadow is complete.
7. Go-live at half the section-5 weights for the first 100 events; full weights only if the brake is in the "full" state at event 100.
