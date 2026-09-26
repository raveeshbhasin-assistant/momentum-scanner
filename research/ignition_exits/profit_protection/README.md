# Can Ignition Watch sell with some gain left? (profit-protection study, 2026-09-26)

**Question (operator):** "Are we sure the sell is only after it's given back all
its gains? Can there be a logic to exit with some positive still left if X days
have passed? I'm worried we only sell mainly when the gains are gone."

## Short answer

- **The worry is correct.** The sell line (close below the pre-ignition base)
  sits a median **13.9% below the entry price**. Every sell under the current
  rule is therefore a loss (average −16.7%). The only profitable exit is the
  12-month age-out. Of 334 sells in 748 trades, **58 had been up ≥ +20% first**
  (average peak +41%) and were sold at about −19%.
- **But no profit-protecting rule beat the current one under the pre-registered bar.**
  60 variants in 5 families were tested on train (2016-21). **0 passed.** Every
  rule that at least halved the "up 20%, sold at a loss" trades cost 7-34
  points of mean return. The few that kept the mean did not fix the round-trips.
- **Why:** the payoff is extremely right-skewed. The stocks a profit lock sells
  usually keep going: their post-exit 63-session return was **+11% to +28%**
  across the families. Capping them cuts the tail that pays for the ~60% of
  trades that fail.
- **Closest to the idea, exploratory only:** "after 126 sessions, sell if
  up 0-30%; let bigger winners run" (A10). It halved round-trips in both train
  and holdout, improved the holdout median and worst-10%, and matched the live
  results. It costs about 6-7 points of mean. It failed the pre-registered
  mean gate by 4.2 points, so it is **not adopted**.

## Protocol
See `PREREGISTRATION.md`, written before any new rule was run. In brief:
- Same 748 trades as `../README.md`: fires 2016-2026, buy next open, close
  triggers fill at the next open, max hold 252 sessions.
- The current rule stays on as a floor under every candidate.
- Splits: TRAIN 2016-21 (477 trades), HOLDOUT 2022 to the last complete window
  (200), LIVE (71, open).
- A candidate had to pass all three gates on train, relative to the current
  rule: give up at most 3 pp of mean (G1), at least halve roundtrip20 (G2), and
  keep worst-10% within 2 pp (G3). Among passers, the best median wins, and the
  winner then gets a one-shot holdout confirmation.
- The engine (`engine.py`) reproduces the published study exactly: base_fail
  train/holdout/live mean +36.7% / +33.7% / +12.7%.
- Each family was built by an independent agent on train only, with at most 12
  variants declared up front and all of them reported. No file touches the holdout.

## Train results (all 60; roundtrip20 = trades up ≥ 20% that exited at a loss)

Reference: **current rule** mean +36.7%, median +20.6%, worst10 −18.8%,
roundtrip20 35. **Hold 252** mean +54.0%, median +34.9%.

| Family | Idea | Best mean kept | Round-trips halved? | Cost of halving them |
|---|---|---|---|---|
| A · time-gated (your idea) | after X sessions, if up, sell / raise the stop | +31.5% (sell at day 189 if up; rt 29) | yes: day 126 if up 0-30% (rt 17) | −7.2 pp mean |
| B · ratchet locks | once up G, lock a level | +35.4% (+20% locked at +50%; rt 28) | yes: keep ⅓ of peak gain once +25% (rt 14) | −8.8 pp |
| C · armed trailing / giveback | trail only after it has worked | **+40.4%** (35% trail once +50%; rt 31) | yes: sell after giving back half the gain once +30% (rt 15) | −9.2 pp |
| D · structure | sell line moves up on re-fire; gated 50-DMA breaks | +29.0% (moving base; rt 31) | yes: new 20-session low once +20% (rt 8) | −21.4 pp |
| E · scale-outs | bank ⅓-½ at +X% | +32.5% (½ at +100%; rt 33) | yes: ⅔ of the position on a +20%→+10% lock (rt 14) | −8.9 pp (best median: thirds at +30/+60%, +23.6%) |

Per-variant tables: run `family_A.py` through `family_E.py`.

Patterns seen in every family:
- **Breakeven stops make round-trips worse** (35 → 43-56). The next-open fill
  sits just below entry, and the stops sell ordinary dips that later recover.
- **Worst-10% barely moves.** The tail comes from trades that never reached
  +20% and hit the floor.
- **Moving the sell line up on each re-fire barely helps** (rt 31) because
  re-fires are rare. When it does trigger, it sells the strongest names
  (post-exit +32%).
- **Faster exits recycle capital:** "sell at edge expiry if green" earns 0.31
  per 100 sessions held, against 0.21 for the current rule. Whether that
  matters depends on having new fires to redeploy into. That is a
  portfolio-level question this per-trade study cannot answer.

## Exploratory holdout
Four candidates were declared before this ran, one per idea; see
`holdout_exploratory.py`. None of these rows is a confirmation.

| Rule | Train mean / median / rt | Holdout mean / median / rt / worst10 | Live mean |
|---|---|---|---|
| current rule | +36.7 / +20.6 / 35 | +33.7 / −12.2 / 31 / −25.4 | +12.7 |
| hold 252 | +54.0 / +34.9 / 28 | +50.3 / +14.6 / 31 / −31.3 | +18.0 |
| **A10: day 126, sell if up 0-30%** | +29.5 / +11.7 / 17 | **+27.8 / −6.9 / 16 / −22.4** | +13.3 |
| C09: keep half the gain once +30% | +27.5 / +16.8 / 15 | +13.7 / +4.6 / 7 / −19.8 | +1.8 |
| E05: thirds at +30/+60% | +24.4 / +23.6 / 17 | +17.2 / −8.5 / 20 / −19.8 | +7.1 |
| C05: 25% trail once +50% | +38.6 / +19.9 / 29 | +17.9 / −11.9 / 26 / −24.8 | +4.1 |

- **A10** is the most stable across the splits. It halves round-trips, improves
  the median and worst-10%, and costs about 6 points of mean.
- **C05**, the only rule that raised the train mean, collapsed on holdout
  (−15.8 pp). This is why train-only winners need a holdout.

## Honest limits
- **Survivorship bias works against every sell rule here.** The universe is
  current index members, so stocks that ran up and then collapsed out of the
  index are missing. Those are exactly the trades a profit lock would have
  saved. Hold-252 beats every exit in every split, which is the bias talking.
  The true cost of a profit lock is probably smaller than shown.
- No costs or slippage. The per-trade view ignores capital recycling.

## Portfolio test: does selling earlier free up capital that pays? (same day)

The per-trade view ignores capital: an earlier exit makes less per trade but
frees cash for the next fire. `PORTFOLIO_PREREG.md` was written before any run.
- **Setup:** K equal slots, sized at 1/K of equity, 10 bps per side, starting
  all cash.
- **Train:** 2016-21. **Holdout:** 2022 to 2026-09.
- **Rules:** the five time-gated exits from family A, compared with the
  current rule (R0) and hold-252 (reference only).
- **Two independent builds:** `portfolio_A.py` (on engine.py) and
  `portfolio_B.py` (from raw prices) agree **bit-for-bit** on all 56 rows.

| K=20, 10 bps | Train CAGR | Train maxDD | Train Sharpe | Holdout CAGR | Holdout maxDD | Holdout Sharpe |
|---|---:|---:|---:|---:|---:|---:|
| **R0 current rule** | **23.3%** | **−37.0%** | **1.09** | **24.5%** | **−31.7%** | **0.90** |
| R2 = A10 (6-month review as a sell) | 27.6% | −45.7% | 0.82 | **37.0%** | **−29.2%** | **1.24** |
| R3 = A09 (day 63, sell if up 0-30%) | 23.1% | −44.3% | 0.75 | 37.4% | −34.6% | 1.27 |
| R4 = A08 (edge expired and up) | 26.9% | −46.4% | 0.84 | 21.5% | −24.9% | 1.03 |
| R5 = A02 (day 63 and up) | 15.8% | −25.8% | 0.81 | 17.5% | −23.2% | 0.98 |
| R6 = A06 (day 63, up, no re-fire) | 28.4% | −44.4% | 0.88 | 30.4% | −32.3% | 1.09 |
| R1 hold 252, no sell line (reference) | 35.7% | −44.5% | 0.97 | 37.8% | −30.6% | 1.18 |
| Equal-weight universe | 21.5% | −40.7% | 1.03 | 13.1% | −20.1% | 0.77 |

At K=10 the current rule makes 29.8% on train and 16.6% on holdout. A10 makes
23.5% and 37.7%.

**Pre-registered verdict: no rule passed on train.**
- R2, R4 and R6 beat R0's CAGR, but their drawdowns were 7.4-9.4 pp deeper, beyond
  the 5 pp limit.
- R2 also lost at K=10.
- The skeptic could not overturn this under 36 same-day tie-break orders,
  K=15/30, 25 bps costs, two later start dates, or dropping each rule's best
  trade. There are no look-ahead or accounting bugs; a truncated-data rerun
  matched exactly.

**The train result rests on one trade.**
- In Oct 2020, R0 was fully invested: it had bought the Mar-2020 wave and was
  still holding it. So it skipped **GME** (fired 2020-10-08) for lack of cash.
- The faster rules had recycled capital, so they took GME, which grew to
  52-56% of their equity.
- The Jan-Feb 2021 collapse is their entire drawdown failure. The same trade
  is also their entire CAGR edge.
- Without GME, their drawdowns are shallower than R0's but their CAGR is lower.
  Train says "no" either way.

**The holdout says the opposite, and more robustly.**
- A10 made 37.0% vs 24.5% CAGR, with a shallower drawdown and a Sharpe of 1.24
  vs 0.90.
- It held under 23 of 30 random tie-break orders, at K=15 and K=30, from a
  2022-07 start, and after dropping its best trades.
- In the holdout, profits are spread across trades: the top trade is 17-36% of P&L.
- R6 passed under 30 of 30 orders.
- Reported only, per the pre-registration; it cannot overturn the train
  decision.

**Why recycling can matter: the current rule is capital-bound.** It never
trims winners, so rising positions absorb the book. At K=20 it skipped ~37%
of new fires for lack of cash (slots almost never bind). The fires it skipped
had *higher* average returns than the ones it took: +45% vs +21% on train,
+38% vs +20% on holdout.

Hold-252 with no sell line beat R0 in both periods at K=20. That is mostly the
survivorship bias noted above, and is a reference only.

## Decision
- **The current sell rule is unchanged.** Neither study passed its
  pre-registered bar.
- **The 6-month review is shown on `/ignition` as a flag with a what-if line**
  (themes_web v1.7.0). Positions 126+ sessions in and up 0-30% are flagged,
  and the ledger is also scored as if every flag had been sold.

The evidence is now split by regime. 2016-21 (one GME-sized outlier) says no;
2022-26 says A10 compounds faster *and* with a shallower drawdown. Per trade,
it costs ~6 pp of mean. As a portfolio, its recycled capital more than repaid
that in the holdout.

The honest next step is forward evidence, not more backtest variants. Track
the flag's live what-if against the live rule, and revisit when a meaningful
number of flagged positions has resolved.
