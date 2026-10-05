# TN3 pre-registration — do definitions of "top" work in phases? (written 2026-10-05, before any TN3 result)

**Raveesh's challenge to TN2:** recent years should be part of training, and a pattern may hold for a few years
rather than for decades. A fixed 2005-2015 / 2016-2026 split cannot see that.

**Restated as a testable claim:** the definition of "top" that has been winning recently keeps winning for a
while. If true, a rule that follows the recent leader beats the index without needing any one definition to
work for 20 years.

## What I already know (not new evidence)

- TN2's two-window table: value and shareholder yield led 2005-2015, momentum and size led 2016-2026.
  So I know there was at least one switch. I have not seen any year-by-year series, any persistence statistic
  or any leader-following result.
- Published work on "factor momentum" (Ehsani and Linnainmaa; Arnott and co-authors) reports that factors with
  good recent returns keep outperforming, mostly at a 1 to 12 month look-back. That is why 12 months is primary.
- Every year to 2026-09 has been looked at in some form. A walk-forward test uses only past data at each step,
  but the design choices below are made by someone who knows the era. Forward tracking is the only clean test.

## Building blocks

Eight "sleeves" per universe, exactly as in TN2 (equal weight, monthly, rank at month-end close, trade next
close, 10 bps a side): MOM, LOWVOL, PROF, VALUE, GROWTH, YIELD, EARN, plus SIZE (largest market cap).
S&P 500 members top 20 (from 2005-02); Nasdaq-100 members top 10 (from 2007-03). A ninth candidate is the
index ETF itself (SPY / QQQ), so the rule can choose "no definition is working, hold the index".

## Part A — are there phases? (descriptive plus one test)

- Calendar-year excess return of every sleeve over the index ETF.
- Run lengths: months in a row that a sleeve's trailing-12-month return is above (or below) the ETF's.
- **Persistence test:** each month, the Spearman rank correlation across the eight sleeves between trailing
  L-month return and the next month's return, L = 12, 36, 60. Mean and one-sided block-bootstrap p. Chance = 0.

## Part B — can phases be followed? (walk-forward)

Each month: rank the nine candidates by total return over the trailing L months (known at the month-end close),
hold the best K in equal parts for the next month. Traded at the holdings level with the same costs.

- Grid: L = 12, 36, 60 months; K = 1, 2. Six configurations, all reported.
- **Primary: L = 12, K = 2.**
- Test window, the same for all six: S&P 500 2010-02..2026-09; Nasdaq-100 2012-03..2026-09 (after the
  60-month warm-up). Halves: split at the midpoint.
- Benchmarks: the index ETF (defines success); the equal mix of all eight sleeves (does choosing add anything
  over holding everything); the equal-weight basket of all members.

**Adaptive rule found** = on the S&P 500 all of: primary beats SPY with one-sided block-bootstrap p < 0.05;
at least five of the six configurations beat SPY; primary beats SPY in both halves; primary beats the equal
mix of sleeves; and the primary beats QQQ on the Nasdaq-100.

**Candidate** = primary beats SPY and the equal mix on the S&P 500 and beats QQQ on the Nasdaq-100, but the
p-value or one of the other conditions fails. Worth a forward tracker labelled unproven.

**No rule** = anything else.

No parameter is changed after the first run. Trackers and pages follow the same condition as before.
