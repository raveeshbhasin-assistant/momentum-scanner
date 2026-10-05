# Top-N largest stocks, rebalanced on a schedule (TN1) — 2026-10-05

**Question (Raveesh):** hold the top 10 / 20 / 30 stocks of the S&P 500 and of the Nasdaq-100, rebalance monthly,
compare with the benchmark; then refine (how many stocks, how often) and say whether it is worth doing.

**Verdict.** S&P 500: no. Nasdaq-100 top 10: it beat QQQ by about 3 points a year, but that is a concentrated bet
on the megacap-technology era, not a selection edge. Rebalance frequency does not matter. Fewer names raised
the return in this sample and raised the single-stock risk with it.

"Top" means largest by market capitalisation among index members on the day.

## Method

- Membership, point in time: S&P 500 from fja05680/sp500 (daily snapshots since 1996); Nasdaq-100 rebuilt
  backwards from today's list with Wikipedia's change table, which reaches February 2007.
- Market cap and total-return prices: FMP, 921 symbols. FMP understates the cap before a large spin-off
  (it lowers old prices but not old share counts); 40-odd spin-offs are corrected (GE, Abbott, ConocoPhillips,
  Time Warner, Kraft ...). Year-end top-10 lists were checked against history.
- Rank on the last close of the month, trade at the next close. Weights drift in between. 5 bps a side.
- S&P test: 2005-01 to 2026-10 (21.7 years). Nasdaq test: 2007-03 to 2026-10 (19.6 years).
- Held-month returns agree with the Yahoo-based blank-slate panel on 14,182 of 14,199 name-months (within 2 points).

## Base case: monthly rebalance

| Index | Portfolio | CAGR | Excess | Volatility | Max drawdown | t-stat | Years ahead |
|---|---|---|---|---|---|---|---|
| S&P 500, from 2005 | SPY | 10.9% | | 18.9% | -55% | | |
| | top 10 equal weight | 11.6% | +0.7 | 20.1% | -53% | 0.45 | 12 of 21 |
| | top 20 equal weight | 11.2% | +0.3 | 18.6% | -53% | 0.28 | 10 of 21 |
| | top 30 equal weight | 11.4% | +0.5 | 18.3% | -50% | 0.54 | 11 of 21 |
| | top 10 cap weight | 12.3% | +1.4 | 20.8% | -51% | 0.85 | 12 of 21 |
| Nasdaq-100, from 2007-03 | QQQ | 16.6% | | 22.2% | -53% | | |
| | top 10 equal weight | 19.6% | +3.0 | 24.6% | -48% | 2.00 | 13 of 19 |
| | top 20 equal weight | 17.6% | +1.0 | 22.7% | -47% | 0.93 | 11 of 19 |
| | top 30 equal weight | 15.2% | -1.4 | 22.7% | -53% | -1.23 | 6 of 19 |
| | top 10 cap weight | 19.4% | +2.8 | 24.5% | -52% | 1.97 | 13 of 19 |

Excess CAGR over the benchmark by half of the sample (points a year):

| | to 2015 | 2016 on |
|---|---|---|
| S&P top 10 | -2.4 | +4.1 |
| S&P top 20 | -0.9 | +1.7 |
| S&P top 30 | +0.1 | +1.0 |
| Nasdaq top 10 | +1.5 | +4.4 |
| Nasdaq top 20 | +1.7 | +0.5 |
| Nasdaq top 30 | -1.2 | -1.6 |

The S&P top 10 lagged SPY for eleven years, then led for eleven. In 2022 the top-10 portfolios lost 37% (S&P)
and 43% (Nasdaq) against 18% and 33% for the benchmarks.

## Refinement

**How many names** (equal weight, monthly; excess over benchmark, points a year):

| N | S&P 500 | Nasdaq-100 |
|---|---|---|
| 3 | +5.5 | +2.9 |
| 5 | +2.3 | +4.2 |
| 10 | +0.7 | +3.0 |
| 15 | +1.3 | +1.6 |
| 20 | +0.3 | +1.0 |
| 30 | +0.5 | -1.4 |
| 50 | 0.0 | -1.7 |

Fewer names, higher return, in this window. Top 3 had a 66% drawdown on the Nasdaq side. At 20 to 30 names the
portfolio is the index with extra work.

**How often** (top 10 equal weight; CAGR, averaged over every possible start month):

| Months between rebalances | S&P 500 | Nasdaq-100 | Turnover a year |
|---|---|---|---|
| 1 | 11.6% | 19.6% | 68-80% |
| 3 | 12.1% | 18.8% | 39-44% |
| 6 | 12.1% | 18.3% | 27-31% |
| 12 | 12.3% | 18.0% | 19-20% |

The two indexes disagree on the sign, so frequency has no reliable effect on return. It changes turnover fourfold.
Trading cost is immaterial either way (20 bps a side instead of 5 costs 0.2 to 0.3 points a year). Tax is not
modelled and is the real cost of monthly trading in a taxable account.

**Other variants** (top 10, monthly unless stated):

| Variant | S&P CAGR | Nasdaq CAGR | Note |
|---|---|---|---|
| base, equal weight | 11.6% | 19.6% | |
| cap weight | 12.3% | 19.4% | turnover 44% / 23% |
| keep a name until it leaves the top 15 | 11.6% | 17.1% | turnover 32% / 38% |
| 10 strongest 12-1 momentum of the top 30 | 12.8% | 20.2% | turnover 283%, t about 1.0-1.3 |
| 10 weakest momentum of the top 30 (placebo) | 9.5% | 12.9% | |
| 200-day trend switch to short Treasuries | 9.7% | 15.4% | drawdown -36% / -27% |

The trend switch is the only variant that changes the risk: it cuts the worst drawdown by a third to a half and
costs about 2 to 4 points a year of return. The momentum tilt points the right way against its placebo but is
not statistically distinguishable from the base and trades four times as much.

## Robustness

- **One stock.** Nasdaq top 10 without Nvidia: +1.3 a year instead of +3.0. Without Apple: +1.3. Every
  leave-one-out stays positive. S&P top 10 without Nvidia or Apple: zero.
- **Interval.** Block bootstrap of the excess return, 95%: S&P top 10 [-2.9, +4.2]; Nasdaq top 10 equal weight
  [-0.3, +6.3], cap weight [+0.3, +5.2]. About 75 configurations were tried, so the Nasdaq interval is flattering.
- **ETFs.** From 2007-03: SPY 11.0%, top-50 ETF (XLG) 11.6%, S&P 100 (OEF) 11.5%, QQQ 16.5%. The S&P top 10 made
  12.4%. QQQ alone beat every S&P top-N variant.
- **Era.** The sample starts after the dot-com bust. An S&P 100 ETF lagged SPY by 2.2 points a year in
  2000-10 to 2004-12 and by 0.7 in 2005-2014, then led by 1.1 from 2015. Earlier data could not be built cleanly.
- **Missing names.** FMP has no history for Wachovia, Merrill Lynch, Wyeth, BellSouth, old Dell, Sears Holdings,
  Genzyme, Sun Microsystems and old News Corp / 21st Century Fox (Yahoo was recovered from an archived Yahoo
  Finance file). Putting old Dell and Wachovia back from year-end closes lowers CAGR by 0.0 to 0.2 points
  (largest: Nasdaq top 20, 17.6% to 17.5%). The top-10 results are essentially unaffected; top 20 and top 30
  before 2010 are slightly flattered.

## Reproduce

```
railway run python research/top_n/tn_fetch.py     # FMP caps + adjusted prices, about 50 minutes
python research/top_n/tn_build.py                 # panel + membership
python research/top_n/tn_run.py qa | base | grid | qa_ret
python research/top_n/tn_robust.py
```

Data and outputs: `C:\dev\Trader-v3-data\top_n\` (`base_table.csv`, `grid_n_freq.csv`, `variants.csv`,
`holdings_*.csv`, `out_*.txt`).

---

# TN2 — other definitions of "top" (2026-10-05)

**Question (Raveesh):** is there another definition of "top" that picks index members which then beat the index?
Build hypotheses, test them; build trackers and pages only if a rule holds.

**Verdict: no rule.** Seven definitions were fixed in advance (`research/top_n/PREREG_TN2.md`, hashed before
any result). Two passed the 2005-2015 training screen; both lost to SPY on 2016-2026. No definition beat its
index in both windows on the S&P 500. No tracker or page was built.

## Design

- Universes: S&P 500 members (primary, top 20) and Nasdaq-100 members (replication, top 10). Equal weight,
  monthly, rank at month-end close, trade next close, 10 bps a side.
- Benchmarks: the index ETF (defines success) and an equal-weight basket of the same eligible members (EWU,
  separates selection from the equal-weight and survivorship tilts).
- Train 2005-02..2015-12 (free to look). Promote at most three definitions that beat both SPY and EWU, plus
  their composite. Holdout 2016-01..2026-09, run once, block-bootstrap p with Holm correction.
- Fundamentals: FMP quarterly statements, used from the filing date (887 of 927 symbols have them).

## Excess return over the index ETF, points a year

| "Top" means | S&P train | S&P holdout | Nasdaq train | Nasdaq holdout |
|---|---|---|---|---|
| MOM — strongest 12-1 month return | -1.8 | +0.1 | +1.8 | +2.8 |
| LOWVOL — lowest volatility | +0.9 | -7.0 | -0.4 | -10.7 |
| PROF — gross profit / assets | +1.4 | -4.7 | -0.3 | -9.0 |
| VALUE — earnings / market cap | **+8.4** | **-5.4** | -4.3 | -10.3 |
| GROWTH — revenue growth | -1.1 | -4.8 | +0.3 | -2.4 |
| YIELD — dividends + net buybacks | **+5.0** | **-2.5** | -5.9 | -11.2 |
| EARN — largest dollar profits | +0.1 | -0.4 | -0.7 | -2.1 |
| VALUE+YIELD composite | (promoted) | **-3.8** | | -13.3 |

Bold = promoted on train and tested on the holdout. Index ETF returns: SPY 7.2% (train) and 15.1% (holdout);
QQQ 12.4% and 20.3%.

## Confirmatory tests (S&P 500, holdout)

| Definition | CAGR | vs SPY | vs EWU | 95% range vs SPY | p (one-sided) | Holm p |
|---|---|---|---|---|---|---|
| VALUE | 9.6% | -5.4 | -2.4 | -9.5 to +4.9 | 0.73 | 1.0 |
| YIELD | 12.6% | -2.5 | +0.6 | -7.4 to +4.5 | 0.72 | 1.0 |
| VALUE+YIELD | 11.3% | -3.8 | -0.7 | -8.4 to +4.2 | 0.69 | 1.0 |

None meets "rule found" or "candidate" (which needed a positive excess against both SPY and EWU).

## What the table says

- **Every definition changes sign or stays negative** between the two windows on the S&P 500. Value and
  shareholder yield won 2005-2015 and lost 2016-2026; momentum did the opposite.
- **Nothing equal-weighted kept up with SPY after 2016.** The equal-weight basket itself made 12.0% against
  15.1%. Momentum and biggest-earners beat that basket by about 3 points and still only matched SPY.
- **Momentum on the Nasdaq-100 is the one consistent sign**: ahead of QQQ and of the equal-weight basket in
  both windows (+1.8 / +2.8 against QQQ). It was not promoted (promotion was on the S&P 500), the t-statistics
  are 0.6 and 0.9, turnover is 320-360% a year, volatility 34% and the 95% range on the holdout is -6.5 to
  +19.1. It is a lead for forward observation, not a finding.
- The train window flatters everything a little: 318-425 of 500 members have data before 2015, and the
  equal-weight basket of those made 10.1% against SPY's 7.2%.

## Reproduce

```
railway run python research/top_n/tn2_fetch.py    # quarterly statements, about 25 minutes
python research/top_n/tn2.py build | train | holdout
```

Outputs: `tn2_train.csv`, `tn2_holdout.csv`, `tn2_promoted.json`, `tn2_holdings_*.csv` in `C:\dev\Trader-v3-data\top_n\`.

---

# TN3 — do definitions of "top" work in phases, and can the leader be followed? (2026-10-05)

**Raveesh's challenge to TN2:** recent years should be in training, and a pattern may hold for a few years
rather than for decades.

**Verdict: no rule.** Definitions do have multi-year winning streaks in hindsight, but the streaks are no
longer than shuffled data produces, and last year's leader beats the index less than half the time. A rule
that follows the recent leader lost to SPY in all six configurations. Pre-registration: `PREREG_TN3.md`.

## Design

- Eight sleeves per universe as in TN2 (seven definitions plus SIZE), full history, plus the index ETF as a
  ninth choice ("nothing is working, hold the index").
- Walk-forward: each month rank the nine by return over the trailing L months, hold the best K next month.
  Every month is tested using only earlier data, so recent years are both learned from and tested on.
- L = 12, 36, 60 months; K = 1, 2. Primary L = 12, K = 2. Window: S&P 2010-02..2026-09, Nasdaq 2012-03..2026-09.

## Part A — are there phases?

S&P 500, return minus SPY, points a year, in three-year blocks:

| | 2006-08 | 2009-11 | 2012-14 | 2015-17 | 2018-20 | 2021-23 | 2024-26 |
|---|---|---|---|---|---|---|---|
| MOM | -13.1 | -6.5 | +3.2 | -3.1 | -5.9 | -1.8 | +13.0 |
| LOWVOL | +7.1 | +1.5 | -4.6 | +0.2 | -3.5 | -8.0 | -10.5 |
| PROF | +3.9 | +9.7 | -6.3 | -5.2 | +0.3 | +1.0 | -15.7 |
| VALUE | +3.9 | +13.8 | +7.8 | -2.6 | -15.9 | +3.7 | -4.6 |
| GROWTH | -7.6 | +2.7 | +0.7 | -5.5 | -13.7 | 0.0 | +3.0 |
| YIELD | +3.5 | +6.1 | +5.2 | -3.0 | -12.2 | +3.4 | +2.5 |
| EARN | +5.8 | -1.9 | -1.4 | -0.5 | -4.4 | -1.0 | +3.5 |
| SIZE | +3.3 | -3.0 | -3.5 | +1.8 | -1.4 | +1.9 | +3.8 |

| Persistence measure | S&P 500 | Nasdaq-100 | Chance |
|---|---|---|---|
| A sleeve's yearly excess keeps last year's sign | 51% | 47% | 50% |
| Last year's best sleeve beats the index this year | 9 of 20 | 6 of 18 | |
| ... by, on average | -1.8 pts | -5.5 pts | |
| Correlation, one 3-year block with the next | +0.17 (48 pairs) | -0.11 (40 pairs) | 0 |
| Average run of a 12-month lead or lag, months | 8.3 | 9.3 | 8.0 / 8.5 when months are shuffled |
| Rank correlation, trailing 12 months vs next month | -0.01 (p 0.67) | +0.06 (p 0.008) | 0 |
| Same, trailing 36 months | 0.00 (p 0.50) | -0.03 (p 0.88) | 0 |
| Same, trailing 60 months | +0.02 (p 0.22) | +0.02 (p 0.24) | 0 |

Value led SPY nine calendar years in a row (2006-2014) and then lagged for five. That looks like a phase, but
streaks of that kind are as common in shuffled data, and nothing at the time marked the turn.

## Part B — following the leader (walk-forward, net of costs)

| Universe | Look-back | Hold best | CAGR | vs index ETF | vs all eight sleeves | First half | Second half | p |
|---|---|---|---|---|---|---|---|---|
| S&P 500 | 12 | 1 | 11.8% | -2.6 | -1.2 | -2.3 | -2.8 | 0.73 |
| S&P 500 | **12** | **2** | **12.7%** | **-1.7** | -0.4 | -1.0 | -2.4 | 0.80 |
| S&P 500 | 36 | 1 | 12.7% | -1.7 | -0.4 | +1.0 | -4.4 | 0.59 |
| S&P 500 | 36 | 2 | 11.5% | -2.9 | -1.5 | -1.1 | -4.7 | 0.89 |
| S&P 500 | 60 | 1 | 13.2% | -1.2 | +0.1 | -0.9 | -1.5 | 0.57 |
| S&P 500 | 60 | 2 | 14.3% | -0.1 | +1.2 | -0.3 | +0.1 | 0.46 |
| Nasdaq-100 | 12 | 1 | 21.2% | +2.0 | +4.5 | -0.5 | +4.8 | 0.22 |
| Nasdaq-100 | **12** | **2** | **22.8%** | **+3.6** | +6.0 | +0.1 | +7.3 | 0.09 |
| Nasdaq-100 | 36 | 1 | 16.0% | -3.2 | -0.7 | -2.3 | -4.1 | 0.71 |
| Nasdaq-100 | 36 | 2 | 15.8% | -3.4 | -0.9 | -2.2 | -4.6 | 0.84 |
| Nasdaq-100 | 60 | 1 | 21.2% | +2.0 | +4.4 | +3.6 | +0.3 | 0.17 |
| Nasdaq-100 | 60 | 2 | 18.4% | -0.8 | +1.7 | +1.1 | -2.8 | 0.54 |

SPY made 14.4% and QQQ 19.2% over these windows. Turnover is 240-535% a year.

- S&P 500: zero of six beat SPY. The pre-set conditions for "rule found" and "candidate" both fail.
- Nasdaq-100: the primary is ahead by 3.6 points (95% range -1.4 to +10.2), but the 36-month versions lose by
  more than 3. A result that flips with the look-back is not a rule. It lines up with the one significant
  persistence figure above (12 months, Nasdaq), which is one of six such tests.

## Reading

Phases are real as description and useless as prediction, on this evidence. Three separate framings now agree
(TN1 size, TN2 fixed definitions, TN3 adaptive): on the S&P 500 nothing beats holding SPY. The recurring
exception is the Nasdaq-100, where size and 12-month momentum keep coming out ahead of QQQ without reaching
significance. Only new months can settle that.

Reproduce: `python research/top_n/tn3.py sleeves | phases | follow`. Outputs `tn3_*.csv` in the data folder.
