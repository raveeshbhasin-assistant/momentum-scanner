# When has an ignition breakout failed? (exit study, 2026-09-26)

**Question (operator):** once a stock fires the ignition signal, what tells
us the breakout is over and we should sell? In particular, if a fire drops
off the list the next day (CNH after 2026-09-08), is that a sell?

**Data.** S&P 500 + 400 (903 tickers), daily Yahoo bars 2015-2026. Every
ignition fire becomes a trade: buy at the next session's open, sell at the
open after an exit trigger, max hold 252 sessions. A new entry needs 20
quiet sessions since the last fire, giving **748 trades**.

**Protocol.** Rules were compared on **train** (entries 2016-2021, 477
trades). The pick was then checked on the **holdout** (2022 up to the last
entry with a complete 252-session window, 200 trades) and on **live**
entries after that (71, still open and marked to market). Code:
`exit_study.py` (28 rules) and `state_study.py` (forward excess return by
position state).

## Answers

### 1. Dropping off the list is NOT a sell
- 39.4% of fires are already off the list the next day. That's normal: the
  5-day return rolls off.
- Fires that were off the next day still returned **+11.6%** over the next
  63 sessions, versus +15.4% for fires that stayed on and +4.6% for all
  stocks.
- Selling on drop-off was the **worst rule of 28** in every split: −1.8%
  per trade in train, −0.5% in holdout, −1.1% live. The stocks it sold went
  on to gain +20.6% (train) and +10.8% (holdout) over the next 3 months.

CNH is therefore a hold. It stays a hold unless it closes below its
pre-ignition base, $11.83.

### 2. Tight exits hurt
Closes below the 20- or 50-day average, 15-20% trailing stops and a 3×ATR
chandelier stop all shook out positions that then rose further. Their
post-exit 63-session returns were +15% to +27% in train. Momentum stocks
are volatile, and ordinary pullbacks are not failures.

### 3. The one robust sell: "ignition failed"
**Rule:** a close below the pre-ignition base, meaning the close 5
sessions before the fire. The stock has given back the entire breakout
week. Sell at the next open.

| Rule | Train mean | Holdout mean | Live mean | Holdout worst 10% |
|---|---:|---:|---:|---:|
| hold 252 sessions | +54.0% | +50.3% | +18.0% | −31.3% |
| **ignition failed (base)** | **+36.7%** | **+33.7%** | **+12.7%** | **−25.4%** |
| stop −15% | +31.7% | +32.9% | +11.9% | −19.4% |
| trailing −25% | +35.1% | +18.1% | +8.3% | −25.8% |
| death cross (50<200) | +18.3% | +37.7% | +16.9% | −28.2% |
| close < 50-DMA | +1.1% | +7.1% | +3.1% | −13.4% |
| drop off the list | −1.8% | −0.5% | −1.1% | −7.2% |

Only "ignition failed" ranked near the top in all three splits. The
−15% stop is a close second, but it depends on where you enter rather than
on the stock's own structure.

### 4. Re-fire = strongest hold; the edge fades after about 3 months
- **Re-fire:** a fresh ignition after at least 5 sessions off. Within 20
  sessions of a re-fire, the forward 63-session excess return vs the
  universe was **+4.3%** (train) and **+16.7%** (holdout), against +0.3% and
  +5.3% for other held days.
- **Edge window:** in train, excess returns were +4.1% to +4.6% during
  sessions 0-63 after a fire, then +0.2% to −1.6% after that. The holdout did not decay, because
  2022-25 momentum stayed strong. So "edge expired" at 63 sessions is a
  review flag, not a sell.

## Honest limits
- **Survivorship bias.** The universe is today's index members, so stocks
  that collapsed and were removed are missing. This flatters buy-and-hold
  (hold 252 wins every split) and **understates the value of any sell
  rule**: the disasters a stop would have saved you from aren't in the
  sample. Treat the sell rule as risk control. Price-based states did not
  reliably predict *under*-performance: stocks in deep drawdowns
  mean-reverted on average.
- **Skewed payoff.** In the live ledger (2025-01 to 2026-09, 107
  positions), the average return is +23.8% but the median is −11.6%, and
  only 37% finished up. Most ignitions fail small; a few win very big.
  Position sizing matters more than any exit rule.
- No costs, slippage or gaps beyond next-open execution.

## What shipped (themes_web v1.6.0)
`ignition/scan.py` replays every fire since 2025-01-02 as a position:
statuses NEW / HOLD / REFIRED / EDGE_EXPIRED / SELL_PENDING, sells on
IGNITION_FAILED, and ages out after 252 sessions without a re-fire. The
ledger is rebuilt from prices each run, so a missed run cannot lose an
event. Each run is committed to the `ignition-data` branch by
`.github/workflows/ignition-daily.yml`.

## Follow-up: profit protection (2026-09-26)
Can the system sell with some gain left, instead of only below the base
(which sits ~14% under entry)? 60 pre-registered variants in 5 families were
tested: time-gated, ratchet locks, armed trails, structure, scale-outs.
**None passed.** Every rule that halved the "was up 20%, sold at a loss" trades
cost 7+ points of mean return, because sold winners kept rising. Closest,
exploratory only: sell at session 126 if up 0-30%.

**Portfolio test:** capital-constrained, two independent builds. No rule
passed on train, and that result hinges on one trade (GME 2020). On the
2022-26 holdout, the 126-session rule beat the current rule: 37.0% vs 24.5%
CAGR, with a shallower drawdown. The current rule is capital-bound: it never
trims winners and skips ~37% of new fires. Full study:
`profit_protection/README.md`.
