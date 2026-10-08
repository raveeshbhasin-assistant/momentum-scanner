# Re-fire add study — should a re-fire mean "buy more"?

_Exploratory, not pre-registered. Run 2026-10-08 on Yahoo adjusted prices,
2015-01-02 to 2026-10-07, ignition/universe.txt (903 tickers). Script:
`refire_add_study.py`._

**Question.** The exit study found a re-fire is the strongest hold state. It
never asked whether to *add*. This replays every fire with the live ledger rule
(buy next open; sell at the open after a close below the pre-ignition base, or
after 252 sessions without a re-fire) and treats every re-fire as an extra
tranche bought at the next open and held to the same exit.

**The comparison that matters** is the add tranche against a *fresh fire
elsewhere* — the other thing you could do with the money. Comparing the add to
the original tranche is meaningless (the add buys higher, later, by
construction).

| | Train 2016-21 | Holdout 2022-25 | Live (open) |
|---|---|---|---|
| Trades / first re-fires | 474 / 99 | 188 / 39 | 82 / 16 |
| **Fresh fire**: mean / median / win | +41.7% / +19.0% / 61% | +22.7% / −12.4% / 37% | +37.2% / −9.0% / 39% |
| **Add at 1st re-fire**: mean / median / win | +43.9% / +24.5% / 71% | +17.7% / −0.7% / 49% | +71.1% / +19.0% / 62% |
| Fwd 63-session excess vs universe: fresh / add | +1.0% / +5.2% | +6.2% / +7.0% | +11.8% / +29.0% |
| ≥20% drawdown within 63 sessions: fresh / add | 16% / 16% | 28% / 36% | 24% / 50% |

Share of trades that ever re-fire: 21%. The first tranche of trades that
went on to re-fire returned +102% (train) and +72% (holdout) — that is the
"re-fire = strongest hold" finding again, and it is conditioning on the stock
having already risen, not a forecast.

**By when the re-fire came** (add tranche, mean / win):

| Sessions after the first fire | Train | Holdout |
|---|---|---|
| 0-20 | +34% / 75% (n=65) | +18% / 31% (n=16) |
| 20-63 | +14% / 48% (n=21) | +29% / 58% (n=19) |
| 63+ (edge window over) | +49% / 59% (n=54) | +12% / 53% (n=38); excess +22% |

**By how far the position was up at the re-fire** (add tranche, mean / win):

| Position at re-fire | Train | Holdout |
|---|---|---|
| ≤ +10% | +28% / 67% (n=36) | +20% / 25% (n=12) |
| +10% to +30% | +33% / 78% (n=41) | +32% / 68% (n=22) |
| > +30% | +46% / 53% (n=60) | +12% / 47% (n=38) |

## Reading

- **No edge to adding.** An add tranche earns about what a fresh fire earns,
  in all three windows. It is not worse; it is not better by anything that
  would survive the sample sizes (99 / 39 / 16 first re-fires).
- **More risk per dollar.** Out of sample the add tranche drew down 20% or
  more within three months in 36% of cases (holdout) and 50% (live), against
  28% and 24% for a fresh entry. You are buying an extended stock, and the
  position's stop (the original pre-ignition base) is far below the add price.
- **If you add anyway**, the small-sample pattern is: best when the position
  is up 10-30% at the re-fire, weakest when it is already up more than 30% or
  when the re-fire comes within 20 sessions in the holdout. Treat these as
  hints, not rules; the cells are 12-60 trades.
- **Guidance stays: a re-fire means hold and don't trim.** Adding is a
  concentration decision, not an edge, and the 2× cap review already pushes
  the other way on exactly these names.

## Limits
Same as the exit study: today's index members (survivorship), no costs, no
slippage beyond next-open fills, single universe. Not pre-registered — the
splits and cuts were chosen before looking, but no bar was set in advance.
