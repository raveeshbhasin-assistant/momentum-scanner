# v6 Plan: Whale Flow — follow identifiable informed money, conditioned on trend and news context

**Date:** 2026-10-03. **Status:** design reviewed, registration drafted and red-teamed, **not locked**. No real return has been computed and no product code exists. The binding definitions are in `research/whale_flow/PREREG_WF1.md` (draft v3) and `wf1_run.py`; where this plan and the registration differ (context cell, test statistics, gates, windows), the registration governs.
**Operator brief:** a contact reports that "whales' volume and trend, plus context like news and some logical moves" is a good predictor. Build it as a real, back-testable model; get the strategy right and test it right before coding it in.

---

## 1. What the brief has to become to be testable

Taken literally ("big volume + uptrend + news"), the idea is one this project has already tested and closed: catalyst days, gap-and-hold, 20-day breakouts with relative strength, overnight continuation and news-type drift were all negative or regime artefacts at retail costs (`research_plan_v5_overnight_catalyst.md` §6, `research_findings_news_track.md`). Raw volume does not say *who* traded. 13F holdings — the usual "institutional whale" data — were also tested point-in-time and add nothing (`research_findings_13f.md`; median staleness 118 days).

So the refinement is a change of trigger, not of parameters:

| Brief | Refined, testable definition |
|---|---|
| "Whales" | A trade by an **identifiable party with an information or size advantage, disclosed in a dated public record** — not a volume bar. |
| "Volume" | The **size of that party's trade** (dollars, % of their holding, number of distinct parties), and the stock's **off-exchange flow** on the days around it. |
| "Trend" | A **state at the time of the trade** (above/below the 200-day average, distance from the 52-week high), used as a conditioner. It is not a trigger; trend-as-trigger is closed. |
| "News context" | The **8-K and earnings record already on disk**: what was filed in the prior 30 days, and whether the stock fell with or without news. Used for exclusions and one interaction. |
| "Logical moves" | **Rules written down in advance** that separate conviction from routine: role, size, clustering, first purchase in years, open-market vs offering. No discretion at test time. |

## 2. Whale data sources: what is real, dated and obtainable

| Leg | Source | History | Point in time? | Cost | Status |
|---|---|---|---|---|---|
| **W1 Insider open-market purchases** (Form 4, code P) | SEC Insider Transactions Data Sets (quarterly zips) | 2006Q1 → 2026Q1 | Yes: filing date; enter next session's open | Free | Built. 2,400–5,200 officer/director purchase filings of ≥ $100k per year across all issuers |
| **W2 Off-exchange ("dark") flow** | FINRA Reg SHO daily short-sale volume files | 2009-08 → today (per-facility files; consolidated from 2018-08) | Yes: published the same evening; enter next open | Free | Built. This is the raw input behind "dark pool" indicators such as DIX |
| W3 5% stakes (13D/13G) | EDGAR form index | 2004 → today | Yes | Free | Not probed. Small n; most of the return is on the filing day. Phase 2 at most |
| W4 Options flow ("unusual options activity") | ThetaData / Polygon / ORATS | 2012–2018 → today depending on vendor | Yes (trade prints) | ≈ $80–100 per month | **Not available today.** Needs an operator subscription; see §7 |
| ~~13F holdings~~ | — | — | — | — | Closed (IN13F, 2026-09-27) |
| ~~Raw volume spikes~~ | — | — | — | — | Closed (v4, v5) |

**Data built 2026-10-03 (`research/whale_flow/`, data in `C:\dev\Trader-v3-data\whale_flow\`; no return has been computed):**
- `insider_trans.pkl`: 4.54M Form 4 non-derivative transactions 2006-01 → 2026-03 (837k purchases, 2.6M sales, 953k option exercises, 149k gifts), all issuers, keyed by issuer CIK. The 2026 Q2+ data sets are not yet posted by the SEC.
- `darkflow.pkl`: 11.3M stock-days of off-exchange short and total volume, 2009-08-03 → 2026-10-02 (4,301 trading days; one file missing). Median short share 37–51% by year. Files before March 2011 have no short-exempt column.
- `bars/`: a new single daily panel 2004 → 2026 for the 5,245-symbol 2026 list, with dividend-adjusted close and split events (download in progress). The older panels are **not** used: 1,027 names in the 2024–26 panel (JPM, XOM, ABT, PG among them) are absent from the 2014–24 panel, and none carries splits or dividends. Raw Form 4 prices and FINRA volumes cannot be compared with split-adjusted bars without the split factors (KO's off-exchange share reads 11% before its 2012 split and 29% after).
- `bars_delisted/`: FMP price history for issuers no longer on the 2026 list (fetch in progress; 9,990 issuer-symbol pairs, 8,292 issuers). **Survivorship on the event side is severe if ignored:** only 22–38% of 2006–2013 purchase rows, and 50–63% of 2018–2026 rows, belong to an issuer that still has a 2026 ticker. Symbols get reused, so a delisted match is accepted only where Form 4 trade prices agree with the bars. FMP's delisted *list* is paywalled, so control stocks remain survivors only.
- Already on disk and reused: 8-K events 2004–2026 (701k filings, item codes, ET timing) and earnings dates (107k rows).

**What the published evidence says (priors, not results):**
- Insider purchases are the best-documented informed-flow signal. Non-routine ("opportunistic") buys and multi-insider clusters carry the effect; routine buys carry none. It is strongest in small caps and accrues over one to six months, not days.
- Daily short-volume share predicts returns in the cross-section, but with the sign *opposite* to the popular "dark pool buying" reading in at least one study (heavily shorted names underperform over the next 20 days). The two readings conflict, so this leg is registered two-sided and the data decide.
- Following *publicly flagged* unusual options activity has been found to lose money (price pops on the day of coverage, negative afterwards). Signed open/close option volume does predict short-horizon returns in academic data that retail feeds do not carry.

## 3. The strategy as proposed

**Trade.** Long only. Enter at the **open of the first session after the whale record becomes public**. Exit on time: primary **63 trading days**, secondary 21. No stops, targets or technical exits in the primary cells (every such exit tested in this project was negative; exits get their own study only if the entry passes).

**Universe.** US common stocks, prior close ≥ $5, 20-day dollar volume ≥ $10M, all filters through T−1. Costs per side: 5 bp (ADV ≥ $100M), 10 bp ($50–100M), 25 bp ($10–50M).

**Whale event definitions (W1).** From Form 4 non-derivative transactions, code P, acquired, original filings only:
- *Insider* = officer or director. Filings by 10% owners alone are funds, not insiders; they go to a secondary cell.
- Drop token buys (< $25k), buys priced more than 5% outside the day's traded range (offerings and private placements, not open market), and *routine* buyers (bought in the same calendar month in each of the prior three years).
- **Cluster buy:** ≥ 2 distinct insiders, combined ≥ $250k, within 30 calendar days; event date = the filing that completes the cluster.
- **Conviction buy:** CEO, CFO or chair, single purchase ≥ $500k or ≥ 10% increase in their direct holding.

**Dark-flow measure (W2).** Per stock-day: `DPI = off-exchange short volume / off-exchange total volume`; signal = 5-day mean DPI, z-scored against the stock's own trailing 252 days. Secondary: off-exchange share of total volume, same z-score.

**Trend state.** UP = close > 200-day average and within 15% of the 52-week high. DOWN = close < 200-day average and ≥ 25% below the high. Everything else = NEUTRAL.

**News context.** (a) Exclude events with a dilution or executive-departure 8-K in the prior 30 days (measured strongly negative in the news track). (b) "No-news drop": stock fell ≥ 15% over the prior 20 sessions with no earnings release and no 8-K in that window.

## 4. Primary cells (six; Holm-corrected as one family)

| Cell | Hypothesis | Test |
|---|---|---|
| WF-1 | Cluster buys earn positive excess | mean 63-day net excess > 0 |
| WF-2 | Conviction buys earn positive excess | same |
| WF-3 | **Trend matters:** insider buys in UP vs DOWN state differ | difference, two-sided (literature says insiders are contrarian; the brief says follow the trend) |
| WF-4 | **Context matters:** insider buys after a no-news drop beat other insider buys | difference, one-sided |
| WF-5 | Dark-flow extremes predict 21-day returns | top vs bottom decile of DPI z, two-sided |
| WF-6 | **Confluence (the brief itself):** insider buy *with* supportive dark flow (DPI z > 0 over the prior 10 days, sign fixed by WF-5 on discovery) beats insider buy without | difference, one-sided |

Secondary, descriptive only: 21-day horizon, 10%-owner buys, role and size gradients, liquidity terciles, insider *sales*.

## 5. How it is tested (house rules, unchanged)

1. **Windows.** Discovery 2014-01 → 2019-12 (may be looked at, thresholds may be adjusted once). **Confirmatory A 2020-01 → 2026-03, run once.** Confirmatory B 2009-08 → 2013-12 (2006 for insider-only cells), run once, used for sign consistency.
2. **Benchmarks.** Excess over SPY for the identical holding period, **and** over matched controls: five non-event names on the same day from the same liquidity decile and the same trend state (fixed seed). The 13F lesson: a spread that a price-only sort reproduces is not a signal.
3. **Pass bar per cell.** Holm one-sided p < 0.05; lower 95% CI > 0 (wider of day- and quarter-clustered) in Confirmatory A; same sign in B; ≥ 60% of calendar years positive; matched-control excess lower CI > 0; no five names supplying more than 30% of the total; n ≥ 300.
4. **Placebos.** Option exercises (code M) and gifts as fake "buys"; filing dates shifted ±60 days; dark-flow series shuffled within stock.
5. **Portfolio check before any product.** K-slot ledger (K swept 10–30), equal weight, tie-break order and start date swept, 10 bp; report CAGR, drawdown, exposure vs SPY.
6. **Process.** Red-team the registration → hash-lock rules, data files and code → discovery run → single confirmatory run → independent verifier with its own engine → verdict computed in code: GREEN (shadow page, no capital) / AMBER (log forward only) / RED (stop).

**Known weaknesses, stated up front.** (i) Survivorship: event-side prices for delisted issuers come from FMP where it has them and the match validates; issuers with no usable price history are counted and reported, and a bound is computed by assigning them −100%. Control stocks are 2026 survivors only, which makes the matched-control comparison conservative for a long rule. (ii) The insider effect is strongest in names below the $10M ADV floor. (iii) Overlapping 63-day holds make events dependent; hence clustered intervals and the portfolio ledger. (iv) Insider data ends 2026-03 until the SEC posts the next quarters. (v) Form 4 fields contain gross errors (single "purchases" worth 10^16 dollars); values are checked against price × volume and clipped by pre-declared rules. (vi) Returns use the dividend-adjusted close for stocks and SPY alike.

## 6. Honest prior

Insider cluster/conviction buying (WF-1, WF-2) is the only leg with decades of independent support; a plausible outcome is a modest positive excess over one to three months that survives costs in mid caps and is too thin in large caps. The trend, context and confluence cells (WF-3, 4, 6) are the brief's actual claim and have no strong prior either way. The dark-flow leg alone (WF-5) is more likely to be an exclusion than a long. Overall chance that at least one cell reaches GREEN: roughly one in three.

## 7. Operator decisions

1. **What did the contact mean by "whales"?** If options flow or dark-pool prints from a flow service, W4 needs a paid historical feed (about $80–100 for one or two months, long enough to pull the history). I cannot create the account or pay; the operator would subscribe and supply the key. The free legs (W1, W2) go ahead either way and W4 can join as a second registration.
2. **Horizon.** This design holds for one to three months (the horizon at which the insider effect exists). If a days-long hold is required, only W2 and W4 apply and the prior is weaker.
3. **Where a passing result would live.** Proposed: a standalone module with a page next to Ignition Watch (no shared code), shadow-only first.

## 8. Results (2026-10-04)

**WF1 discovery (2014-01 → 2019-09), registration locked first.** No cell significant; smallest Holm p 0.31. Against matched non-event stocks over 63 sessions: cluster buys +0.26% [−1.79, +2.03]; conviction buys +1.04% [−0.51, +2.50]; uptrend minus downtrend −1.29% (n.s., wrong sign for "follow the trend"); after a sharp drop minus other −1.83% (n.s.); supportive dark flow minus not +0.81% (n.s.). Dark flow alone: high off-exchange short share → lower 21-session returns (spread −0.16%, p 0.05), neither extreme beats the universe after costs. One descriptive lead: all insider events at 21 sessions +0.79% [+0.21, +1.33] against peers.

**WF2 (operator request): mine on 70%, confirm on 30%.** The WF1 confirmatory windows were not run; 2006–2013 became training data and 2020–2026 the single holdout (`research/whale_flow/WF2_DESIGN.md`). Success = a trade beats SPY after costs; precision = successes ÷ signals; base rate = random eligible stocks bought on the same days. Four strategies were mined on 2006-02 → 2019-09, frozen and hashed, then run once on 2020-01 → 2026-04.

| Strategy | Train precision vs base | Holdout signals | Holdout precision | Base rate | Lift, points [95%] | Holm p | Mean excess per trade [95%] |
|---|---|---|---|---|---|---|---|
| S1 rule: insider buy, stock down ≥ 44% in 126 sessions, ≥ 12 sessions after earnings; hold 21 | 59.4% vs 50.1% | 375 | 52.3% | 45.7% | +6.6 [−4.4, +13.3] | 0.32 | +2.05% [−2.04, +4.91] |
| S2 gradient-boosted model on insider-purchase days, top 30%; hold 63 | 52.0% vs 49.4% | 2,499 | 49.0% | 46.6% | +2.4 [−0.6, +5.4] | 0.22 | +0.95% [−1.64, +3.10] |
| S3 every cluster/conviction event; hold 21 (nothing fitted) | 51.8% vs 48.7% | 1,844 | 44.6% | 44.6% | 0.0 [−2.6, +3.0] | 0.51 | −1.03% [−3.18, +0.98] |
| S4 gradient-boosted model on all stock-weeks, top 0.5%; hold 21 | 61.0% vs 56.1% | 4,575 | 50.9% | 49.3% | +1.7 [−4.3, +5.3] | 0.49 | +0.63% [−2.40, +2.20] |

**Verdict: all four NOT CONFIRMED.** None reached 55% precision, none has a lift distinguishable from zero after correcting for four tests, none has a mean excess whose lower bound is above zero. The one lead from WF1 discovery (S3) has zero lift on the holdout. S1 keeps a positive point estimate (60 signals a year, +6.6 points) but 45% of its holdout trades are from 2020 and its precision by year runs 29% to 93%.

**Reading.** Out-of-fold ranking power on train was AUC 0.51–0.53: insider purchases, off-exchange flow, price state and the 8-K record carry almost no information about which stock beats SPY over the next one to three months at the $5 / $10M liquidity floor. All insider-purchase days together beat SPY 45.4% of the time on the holdout against a 44.1% base rate.

**What is and is not left.** The 2020–2026 holdout is now spent: any further rule mined from this data has no clean test until new months accumulate. Not tested: options flow (needs a paid feed), names below the liquidity floor (where the published insider effect is strongest and costs are highest), and anything using the content of filings rather than their existence.

