# TN2 pre-registration — other definitions of "top" (written 2026-10-05, before any TN2 result)

**Question (Raveesh):** is there a definition of "top" other than size that picks index members which then
beat the index? Build hypotheses, test them, and only if a rule holds build trackers and pages.

## What is already known (so it is not counted as new evidence)

- TN1: largest-by-cap adds nothing on the S&P 500; Nasdaq top 10 beat QQQ by 3 points a year (era bet).
  A 12-1 momentum pick among the 30 or 50 largest names was positive over the full sample, t 1.0 to 1.7.
- BS1 (train 2006-2019, all US stocks): momentum, 52-week-high and low-volatility deciles showed no selection
  lift net of costs.
- General knowledge of 2016-2026: growth and momentum led, value and low volatility lagged. I cannot un-know
  this, so the holdout is weaker than a true out-of-sample test. Forward tracking is the only clean test.

## Universes, portfolio, benchmarks

- Universe A (primary): S&P 500 members on the day. Universe B (replication): Nasdaq-100 members.
- A member is eligible when it has a live price, a market cap and the data its definition needs.
- Portfolio: the top N by the definition, equal weight, monthly. Rank on the last close of the month, trade at
  the next close, 10 bps a side. **N = 20 for the S&P 500, N = 10 for the Nasdaq-100.** N = 10 and 30
  (S&P) are reported as sensitivity only.
- Benchmark that defines success: the index ETF (SPY, QQQ), total return.
- Diagnostic benchmark: equal weight of every eligible member, same schedule and costs (EWU). It separates
  stock selection from the equal-weight and survivorship tilts.

## The seven definitions (each one fixed, no tuning)

| Code | "Top" means | Signal | Rationale |
|---|---|---|---|
| MOM | strongest price trend | total return from 252 to 21 sessions ago, highest | momentum (Jegadeesh-Titman) |
| LOWVOL | steadiest | standard deviation of daily returns over 252 sessions, lowest | low-risk anomaly |
| PROF | most profitable | gross profit (last 4 quarters) / total assets, highest | gross profitability (Novy-Marx) |
| VALUE | cheapest on earnings | net income (last 4 quarters) / market cap, highest | value |
| GROWTH | fastest growing | revenue (last 4 quarters) / revenue (the 4 before) - 1, highest | growth persistence |
| YIELD | returns most cash | 12-month total return minus 12-month growth in market cap (dividends + net buybacks), highest | shareholder yield |
| EARN | biggest earner | net income (last 4 quarters) in dollars, highest | fundamental size instead of price size |

Fundamentals are used from max(filing date, period end + 30 days), next session. A trailing-four-quarter
figure needs four quarters with the latest period ending within 200 days; GROWTH needs eight.

## Stage 1 — train (S&P 2005-01..2015-12; Nasdaq 2007-03..2015-12)

Free to look. A definition is **promoted** when, on the S&P 500, its top-20 portfolio beats both SPY and EWU
net of costs. At most the three best by t-statistic against EWU are promoted, plus one **composite**: the mean
of the promoted definitions' percentile ranks. If nothing qualifies, the answer is "no rule" and stage 2 is
descriptive only.

Known weakness: 2005-2010 coverage is 330-410 of 500 S&P members (delisted names FMP does not carry), so
train is a filter, not evidence.

## Stage 2 — holdout (2016-01..2026-09), run once

Coverage 457-500 of 500. For each promoted definition, one-sided test that the mean monthly excess over the
index ETF is above zero (12-month block bootstrap, 10,000 draws), Holm-adjusted across the promoted set.

**Rule found** = all of:
1. Holm-adjusted p < 0.05 against SPY on the S&P 500;
2. excess over EWU above zero;
3. excess over SPY still above zero after removing the single name that contributed most;
4. same sign against QQQ and EWU on the Nasdaq-100.

**Candidate** = holdout excess above zero against both SPY and EWU but (1) fails: worth a forward tracker
labelled unproven, nothing more.

**No rule** = anything else. Non-promoted definitions are reported on the holdout for information and cannot
be confirmed.

Trackers and pages are built for a "rule found", and for a "candidate" only as a clearly labelled forward test.

## Amendment 1 (2026-10-05, after the signal build, before any performance number)

- The S&P train window starts 2005-02, not 2005-01: the 12-month signals need 252 sessions of history.
- Data guards, set after eyeballing the top-ranked names on two dates (no returns seen): VALUE outside
  [-100%, +50%], YIELD beyond +/-50%, GROWTH above 500% and PROF beyond +/-3 are treated as missing. They are
  symbol-lineage and one-off-gain errors (Tyco filed under JCI, the Keurig special dividend, AIG's tax asset).
