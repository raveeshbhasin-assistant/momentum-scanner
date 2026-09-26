# Pre-registration: does selling earlier pay off at the PORTFOLIO level? (2026-09-26)

Written before any portfolio simulation was run.

## Question
The per-trade study (PREREGISTRATION.md, 0/60 passed) measured return PER TRADE.
But time-gated exits free capital sooner: "sell at edge expiry if green" earned
0.31 per 100 sessions held vs 0.21 for the current rule. If there are enough new
ignition fires to redeploy into, a faster-turnover rule could compound faster even
though each trade makes less. Only a capital-constrained portfolio simulation
can answer that.

## Rules compared (ALL taken from the per-trade study; no new variants)
"floor" = close < pre-ignition base (close 5 sessions before the fire), the current rule.
Every rule also closes at 252 sessions after entry (hard max hold, as in engine.py).
- R0 current   : floor
- R1 hold252   : no floor (reference only; NOT eligible: it drops risk control)
- R2 A10       : floor OR (k >= 126 AND 0 < ret < 0.30)
- R3 A09       : floor OR (k >= 63  AND 0 < ret < 0.30)
- R4 A08       : floor OR (since_fire >= 63 AND ret > 0)        (clock resets on re-fire)
- R5 A02       : floor OR (k >= 63 AND ret > 0)
- R6 A06       : floor OR (k >= 63 AND ret > 0 AND not refired)
(k = sessions since entry, ret = close/entry - 1; definitions exactly as engine.py.)

## Portfolio mechanics
- Entry candidates: the engine's fresh-ignition entries (a fire with no fire in the
  prior 20 sessions), skipped if that ticker is already held.
- Fills: buy at the open of the session after the fire; a close-triggered exit sells
  at the next session's open (same as engine.py). Missing open -> use that day's close.
- Order each session: exits at the open first, then entries (freed capital is reusable
  the same morning).
- Slots: K equal slots. An entry is taken only if open positions < K. Size =
  min(equity at the prior close / K, available cash); skip if that is < 50% of
  equity/K. Same-day candidates are ranked by 5-day return at the fire, highest first.
- Daily mark-to-market at the close (forward-fill missing closes). Cash earns 0.
- Costs: 10 bps per side (primary). 0 bps is also reported.
- PRIMARY K = 20. Robustness K = 10.

## Periods (each starts flat, all cash)
- TRAIN: first session of 2016 through 2021-12-31. Entries whose fire is in that
  window. Open positions are marked to market at the 2021-12-31 close.
- HOLDOUT: first session of 2022 through the last date in the data (2026-09-25).
  Entries whose fire is on or after 2021-12-31. HOLDOUT IS SEALED until the train
  decision below is locked.

## Metrics
CAGR, max drawdown (daily closes), Sharpe (daily mean/std x sqrt(252), rf = 0),
MAR (CAGR / |maxDD|), average exposure (invested / equity), trades taken, candidates
skipped for lack of a slot. Reference: an equal-weight universe index (mean daily
close-to-close return across liquid tickers).

## Decision (locked before running)
TRAIN, K = 20, 10 bps. An eligible rule (R2-R6) PASSES if:
- P1: CAGR > R0's CAGR, AND
- P2: max drawdown no more than 5 pp deeper than R0's, AND
- P3 (robustness): CAGR > R0's CAGR at K = 10 as well.
Locked winner = the highest train CAGR (K = 20) among passers.
CONFIRMED only if, on HOLDOUT (K = 20, 10 bps), it has CAGR > R0's and max drawdown
no more than 5 pp deeper. K = 10 holdout is reported. If nothing passes on train, the answer
is "capital recycling does not rescue profit-taking".

## Verification
Two independent implementations (one built on engine.py, one from raw prices
without engine.py) must agree on train (CAGR within 1.0 pp, maxDD within 1.5 pp for
every rule at K = 20). A skeptic then tests tie-break order, K = 15/30, 25 bps costs
and start-date shifts.

## Known biases
Survivorship (current index members) flatters holding and understates any sell rule.
Small-sample: train has ~6 years; one regime can dominate a CAGR.
