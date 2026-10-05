# Blank-slate strategy search (BS1): findings

**2026-10-05.** Operator brief: forget the whale idea; with all the data on hand, start from blank and reverse-engineer a worthy strategy; precision over recall. Design, frozen rules and gates: `research/blank_slate/BS_DESIGN.md`. Data: `C:\dev\Trader-v3-data\blank_slate\`.

## Verdict

**Nothing passed the pre-set bar. One rule came close and is the only thing in this project whose out-of-sample result matches its training result:**

> **D1.** When a broad index ETF has fallen 3% or more in five sessions and is still above its 200-day average, buy at the next open and sell at the close five sessions after the signal.

| | Training 2006–2019 | Holdout 2020–2026 (run once) |
|---|---|---|
| Trades (separate weeks) | 348 (131) | 190 (75) |
| Hit rate (trade made money after costs) | 65.5% | **66.8%** |
| Base rate (same ETFs, any day, same hold) | 55.6% | 55.9% |
| Lift, 95% range | +9.9 pts [+1.1, +18.3], p 0.013 | +11.0 pts [−1.2, +21.9], p 0.037, Holm-adjusted 0.11 |
| Average trade, 95% range | +0.61% [+0.08%, +1.14%] | +0.74% [−0.05%, +1.47%] |
| Portfolio, five 20% slots | +3.0% a year, 8% of capital deployed, worst drawdown −13% | +4.2% a year, 9% deployed, worst drawdown −11% |
| SPY buy-and-hold, same years | +8.4% a year, worst drawdown −55% | +15.4% a year, worst drawdown −34% |

It failed three of six gates, each narrowly: the lift is not significant after correcting for three tests; the average trade's lower bound is −0.05%; hit rate beat the base rate in 4 of 7 years (2022: 44% on 9 trades; 2023: 39% on 23 trades). An independent recomputation from the raw ETF files, without the slot limit, gives 69.3% on 218 holdout trades and 62.5% on 413 training trades.

**What kind of strategy this is.** High precision, small contribution: about 29 trades a year across eight near-identical funds, which is roughly 11 separate market pullbacks a year. It is idle 91% of the time. It is a way to deploy cash on pullbacks, not a replacement for holding the index. Its worst trades are the first leg of a crash, before price has fallen through the 200-day average (24 Feb and 2 Mar 2020: −7% to −12%).

## What the blank-slate map found (training years only)

**Stocks: nothing.** 5,149 listed and 3,241 delisted companies, about 45 conditions, holds of 1 to 63 sessions, two liquidity tiers (`map_train.csv`).
- Buying oversold stocks (lowest decile of 1-, 5-, 21-day return, RSI-2, distance from recent highs) does not beat buying the average stock; hit rates sit at or below the base rate.
- Twelve-month momentum, 52-week-high proximity and trend measures give no selection lift at 21 or 63 sessions in 2006–2019.
- Low-volatility stocks win more often (65% vs 59% over 63 sessions among stocks trading ≥ $100M a day) with the same average return as the rest: more precision, no extra return.
- High-volatility stocks gain overnight and lose intraday; the overnight gain sits in names where a round trip costs 0.5%.

**Calendar: nothing.** Turn-of-month and day-of-week are noise for SPY.

**ETFs: one family.** An index or sector ETF that is sharply oversold while above its 200-day average wins more often over the next few sessions. Below the 200-day average the lift is gone. This is timing the market, not picking stocks.

## Holdout results for everything that was frozen

| Strategy | Trades | Hit rate | Base | Lift | p (raw) | Avg trade | Verdict |
|---|---|---|---|---|---|---|---|
| **D1** index ETFs, 5-day return ≤ −3%, uptrend, hold 5 | 190 | 66.8% | 55.9% | +11.0 | 0.037 | +0.74% | NOT CONFIRMED (near miss) |
| **D2** index ETFs, 2 sd below 20-day average, uptrend, hold 5 | 151 | 54.3% | 56.5% | −2.2 | 0.62 | +0.17% | NOT CONFIRMED |
| **D3** index + sector ETFs, 5-day return ≤ −3%, uptrend, hold 5 | 608 | 57.4% | 54.7% | +2.7 | 0.17 | +0.33% | NOT CONFIRMED |
| D1s, SPY/QQQ/IWM only (secondary) | 97 | 68.0% | 56.6% | +11.4 | 0.028 | +0.96% | — |
| D1c, entered at the close (secondary) | 190 | 65.8% | 56.5% | +9.3 | 0.057 | +0.79% | — |
| D1x, no uptrend filter (secondary) | 383 | 59.5% | 55.9% | +3.7 | 0.23 | +0.50% | — |
| D1L, held 21 sessions (secondary) | 137 | 69.3% | 62.8% | +6.6 | 0.17 | +1.59% | — |

Two things to read from this table. The uptrend filter matters out of sample as it did on train (D1 vs D1x). And the effect depends on how "oversold" is measured: the 5-day-return trigger held up, the 20-day-band trigger (D2, second best on train) did not. That fragility is the main reason not to treat D1 as established.

## What this adds up to

- Across every study in this repo (intraday momentum, gaps, overnight, earnings, news, 13F, insiders, off-exchange flow, and now a blank-slate map of daily bars), **stock selection has shown no usable edge at retail costs**. The only repeatable structure is at the index level: short pullbacks inside an uptrend tend to recover.
- D1 is not confirmed and should not be treated as proven. The honest next step is to log its signals forward with no capital; at about 11 pullbacks a year it needs roughly two more years before the evidence changes materially.
- Every stretch of history on disk has now been used as a test at least once. Further mining of the same data cannot be confirmed except by waiting.

## Limits

- 128 combinations were simulated on train before D1 was chosen; a best p of 0.013 among them is about what chance produces. The training evidence was the consistency across index variants, and one of those variants then failed.
- Delisted coverage is partial: 3,244 of 7,270 delisted price histories passed the check against insider trade prices; 2,886 failed and were not diagnosed. This affects the stock map (already null), not the ETF results.
- The ETF list is funds that exist today. All eight index funds have traded since before 2006.
- Returns ignore interest on idle cash and taxes; five-session trades are short-term gains.
- No independent agent re-derived the stock map.
