# WF2: Mine strategies on 70% of the history, confirm on the untouched 30%

**Written 2026-10-04, before any mining.** Operator brief: with all the data on hand, reverse-engineer what would have been a good strategy on 70% of the data and test it on the other 30%; develop more than one strategy; the objective is **precision, not recall** (when it says buy it should be right; few signals is fine).

This replaces the WF1 confirmatory step. WF1's discovery run (2014-01 → 2019-09) found no significant cell. Its two confirmatory windows were never run: 2006–2013 now becomes training data and 2020–2026 becomes the WF2 holdout, used once. `PREREG_WF1.md` and its lock are left as they are.

## The split

| | Entry dates | Share of the calendar | Seen before? |
|---|---|---|---|
| **Train (mine freely)** | 2006-02-01 → 2019-09-30 | 69% | 2014–2019 seen in WF1 discovery; 2006–2013 unseen |
| *Embargo* | 2019-10-01 → 2019-12-31 | | no entries; keeps train returns out of the holdout |
| **Holdout (test once)** | 2020-01-01 → 2026-04-01 | 31% | never joined to any signal |

The split is by time, not random. A random split would put the same weeks in both halves, and stocks move together within a week, so the "test" would be contaminated by the training outcome of its neighbours.

The mining code reads only the train files. The holdout files are written by the feature builder and opened by exactly one script, `wf2_holdout.py`, after the strategies are frozen and hashed.

## What counts as success

- A **signal** is a buy at the next session's open, held a fixed number of sessions (21 or 63, chosen per strategy on train), sold at the close. Costs as in WF1 (5 / 10 / 25 bp per side by liquidity). Universe as in WF1 (as-traded price ≥ $5, ≥ $10M average daily dollar volume).
- A signal **succeeds** if its return after costs beats SPY over the same days.
- **Precision** = successes ÷ signals. **Base rate** = the same figure for random eligible stocks bought on the same days.
- One open position per company per strategy: a new signal for a company already held is skipped.

## Data available to every strategy (all known by the close of the signal day)

Insider purchases and sales (who, how much, how many insiders, role, holding increase, time since the last buy); price history (returns over 1 day to 1 year, distance from the 52-week high and low, moving averages, volatility, liquidity, volume surge); off-exchange short-volume z-score; the 8-K record (earnings timing, counts and item types in the last 20 sessions); market state (SPY trend, returns, volatility).

## Strategies to be mined (four go to the holdout, whatever train shows)

| | Universe | How it is found |
|---|---|---|
| **S1 Rule** | Days with a qualifying insider purchase | A readable rule of at most three conditions, found by search on 2006–2015 and required to hold on 2016–2019 |
| **S2 Model** | Same | Gradient-boosted classifier; buys only the highest-confidence slice, cut-off chosen on walk-forward out-of-fold predictions |
| **S3 Simple** | WF1's cluster and conviction events | No fitting: buy every event, hold 21 sessions (the one lead from WF1 discovery) |
| **S4 Broad model** | Every eligible stock, every 5th session, no insider trigger required | Gradient-boosted classifier on all features; highest-confidence slice |

Train procedure: walk-forward folds (fit ≤ 2011 → score 2012–13; ≤ 2013 → 2014–15; ≤ 2015 → 2016–17; ≤ 2017 → 2018–19), 63 sessions purged between fit and score. Hyper-parameters, hold length and confidence cut-off are chosen on the out-of-fold results only; the final model is refit on all of train with those choices. Selection target for the cut-off: the highest out-of-fold precision that still gives ≥ 25 signals a year (S1, S2) or ≥ 100 a year (S4).

## Holdout gates (fixed now; computed by `wf2_holdout.py`)

A strategy is **CONFIRMED** only if all hold on 2020–2026:

1. ≥ 100 signals.
2. Precision ≥ 55%.
3. Precision minus base rate > 0, Holm-adjusted one-sided p < 0.05 across the four strategies (bootstrap clustered by half-year, quarter and day; the least favourable is used).
4. Mean return over SPY after costs has a 95% lower bound > 0.
5. Precision above the base rate in ≥ 60% of calendar years with ≥ 10 signals.
6. Still positive mean excess with its five best companies removed.

**PARTIAL** = gates 1 and 3 hold but not all others (a real lift that is not yet a usable strategy). Otherwise **NOT CONFIRMED**.

Reported for every strategy regardless: train out-of-fold precision next to holdout precision (the size of the drop is the measure of over-fitting), absolute win rate, precision against matched peers, mean and median, share of trades worse than −10%, signals per year, results by year, and a label-shuffle placebo.

## Known limits

- The training period has thinner price coverage (44% of insider purchase filings in 2006 have a price series, 76% in 2014, 95%+ from 2019), so rules learned there lean on companies that either survived or that FMP still carries.
- S4's universe is 2026-listed stocks only. Its base rate carries the same bias, so the *lift* is fair but the absolute precision is flattered.
- 2020–2026 contains one strong small-cap rebound (2020–21) and one sharp fall (2022); results by year are reported for that reason.
- Mining tests thousands of candidate rules on train. Train results are therefore optimistic by construction; only the holdout numbers are evidence.

## Amendments made on train, before the freeze (holdout unopened; gates unchanged)

1. **Cut-off chosen by lift, not raw precision (2026-10-04).** The first mining pass chose each model's confidence cut-off by raw out-of-fold precision. For S4 that picked a slice that fires almost only during market sell-offs, when random stocks beat SPY 56% of the time anyway: 61% precision but only +5 points over the same-day base rate, positive in 2 of 4 folds. That is market timing, not stock selection, and gate 3 measures lift. The cut-off is now the one with the largest out-of-fold lift over the same-day base rate, subject to the same minimum signals per year.
2. **Zero adjusted prices repaired (2026-10-04).** FMP returns a dividend-adjusted close of 0 for 17 of the 4,252 delisted series, which made 44 training returns NaN or infinite (counted as failures). The last valid adjustment factor is now carried forward; rows with a non-finite return are dropped and counted. The feature tables were rebuilt and the mining re-run.

