# Pre-registration: trimming winners in the Ignition portfolio (2026-09-26)

Written before any trimming variant was simulated.

## Question
The portfolio test (PORTFOLIO_PREREG.md) found the current rule is CAPITAL-bound,
not slot-bound. It never trims winners, so rising positions absorb the book. At
K=20 it skipped ~37% of new fires for lack of cash, and the skipped fires had
higher average returns than the ones taken. Does trimming winners, while keeping
the current exit rule, compound better?

## Fixed from PORTFOLIO_PREREG.md (unchanged)
Entries, fills (next open), exit rule R0 (floor = close < pre-ignition base, plus
252-session max hold), slots K, sizing (slot = equity at prior close / K; size =
min(slot, cash); skip if < 50% of slot), same-day ranking by 5-day return, costs
10 bps per side (on trims too), daily mark-to-market, cash earns 0, periods
(TRAIN 2016-2021 / HOLDOUT 2022-2026-09-25, each starting all cash),
metrics, K = 20 primary and K = 10 robustness.

## Trimming variants (all keep R0 exits). Weight = position value / equity.
Weights are measured at the PRIOR close (known before the open). Trims execute at
the OPEN by selling a fraction of shares. The remaining shares keep the same exit
triggers. Session order: exits, then trims, then entries.
- T0 no trim (= R0; must reproduce the portfolio study's R0 exactly)
- T1 cap 2x -> 1x : any position with weight > 2/K is sold down to 1/K
- T2 cap 3x -> 1x : weight > 3/K sold down to 1/K
- T3 hard cap 2x  : weight > 2/K sold down to 2/K (trim only the excess)
- T4 hard cap 3x  : weight > 3/K sold down to 3/K
- T5 trim to fund : only when an entry candidate has a free slot but cash < its
                    slot size. Sell from positions with weight > 1/K, largest
                    weight first, each down to no less than 1/K, only as much as
                    needed to fund the full slot. If still short, the 50% rule applies.
- T6 quarterly trim-only rebalance: on the first session of each calendar quarter,
                    every position with weight > 1/K is sold down to 1/K. No top-ups.
- T7 monthly full equal-weight rebalance: on the first session of each month, every
                    position is set to 1/K of equity (trims winners AND tops up
                    losers, subject to cash). Included to contrast "trim-only" with
                    "rebalance".

## Decision (locked before running)
TRAIN, 10 bps. An eligible variant (T1-T7) PASSES if all of:
- P1: K=20 CAGR > T0's K=20 CAGR
- P2: K=20 max drawdown no more than 5 pp deeper than T0's
- P3: K=10 CAGR > T0's K=10 CAGR
- P4 (the known outlier): with GME removed from the candidate pool of ALL variants
      (including T0), K=20 CAGR > T0's. The portfolio study showed that one trade
      (GME, fired 2020-10-08) decides the train comparison, and trimming directly
      caps such a position.
Locked winner = the highest train K=20 CAGR among passers.
CONFIRMED only if, on HOLDOUT (K=20, 10 bps), it has CAGR > T0's and max drawdown
no more than 5 pp deeper. K=10 and no-GME holdout results are reported.
Exploratory only: the winner (or, if none, the best train variant) combined with
A10 exits (R2) is reported on both periods.

## Verification
Two independent builds: one on portfolio_A.py, one on portfolio_B.py. They must
agree on train (CAGR within 1.0 pp, maxDD within 1.5 pp at K=20). A skeptic then
runs the holdout after the decision is locked, plus tie-break, K=15/30, 25 bps,
start-date shifts and a look-ahead audit.

## Known biases
Survivorship (current index members). One regime (2020-21) with extreme bunching:
236 of 477 train candidates fall in Mar-Apr 2020.
