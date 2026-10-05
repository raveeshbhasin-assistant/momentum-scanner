# Blank-slate strategy search (BS1)

**Started 2026-10-04.** Operator brief: forget the whale idea; with all the data on hand, start from blank and reverse-engineer what would be a worthy strategy. Objective stays precision, not recall.

## Method

1. **Map before strategy.** On the training years only (signal days 2006-01-03 → 2019-09-30), measure for every stock-day how ~45 conditions known at the close relate to the next 1–63 sessions, and do the same for 41 ETFs and for the calendar. No strategy is proposed until the map shows where any lift exists.
2. **Strategies only from the map**, a handful at most, simulated as real portfolios on train.
3. **Freeze, hash, and test once** on the holdout (signal days 2020-01-02 → 2026-06-30).

No period of this history is untouched by earlier studies; 2020–2026 was used once on 2026-10-04 for four insider strategies. It has not been used for any ETF or dip-buying rule. Forward logging is the only fully clean test from here.

## Data

- One daily panel 2004–2026: 5,149 stocks listed in 2026, 3,241 delisted companies (FMP history for every issuer that ever filed a Form 4 and has no 2026 ticker, accepted only where Form 4 trade prices agree with the bars), 41 ETFs. Delisted names are 16–27% of the tradeable universe each year 2005–2019.
- Tradeable stock = as-traded price ≥ $5, 20-day dollar volume ≥ $10M, a year of history. Costs 5 / 10 / 25 bp per side by liquidity; ETFs 2 bp per side.
- Not used: 5-minute bars (three months of 2026 only; too short to discover and confirm anything).

## What the training map showed

**Stocks (`map_train.csv`, 1,494 rows).** No condition gives a usable lift in hit rate at any horizon after costs.
- Oversold stocks (lowest decile of 1-, 5-, 21-day return, RSI-2, distance from recent highs) do not beat the average stock: selection return within ±0.1% over 5 days, hit rate at or below the base rate.
- 12-month momentum, distance from the 52-week high and trend measures: no selection lift at 21 or 63 sessions.
- Low-volatility stocks win more often in absolute terms (65% vs 59% over 63 sessions among stocks trading ≥ $100M a day) with the same average return as everything else: higher precision, no edge.
- High-volatility stocks gain overnight and give it back intraday (close-to-close +0.12% a day gross, next-open-to-close −0.07%); the overnight part sits in names where a round trip costs 0.5%.

**Calendar.** Turn-of-month and day-of-week are noise for SPY. SPY's return came mostly overnight (2.9 bp a night vs 1.1 bp intraday).

**ETFs (`etf_map_train.csv`).** One family stands out: an index or sector ETF that is sharply oversold **while above its 200-day average** wins more often over the next 2–5 sessions than the same ETF on an ordinary day (lifts of 4–17 points in hit rate across four oversold measures). Below the 200-day average the lift disappears. This is market-level timing, not stock selection.

**Portfolio simulation of that family (`dip_train_grid.csv`, 128 declared combinations: 4 universes × 4 triggers × uptrend filter on/off × hold 5/21 × entry at close/next open; five equal slots, one position per ETF).**
- Broad index ETFs, 5-day hold, uptrend filter on: hit rate 58–66% against a 56–57% base in every one of the eight trigger/entry combinations. Best: 5-day return ≤ −3%, next-open entry: 348 trades over 131 separate weeks, 65.5% vs 55.6%, +0.61% a trade, lift p 0.013.
- Sector ETFs: lift 1–3.5 points, not distinguishable from zero.
- 21-day holds and no uptrend filter: no lift.
- Money terms: 2–4% a year on capital with 7–8% of capital deployed on average and a worst drawdown of 8–13%; SPY buy-and-hold made about 8% a year with a 55% drawdown over the same years.
- Caveat: the best of 128 correlated combinations at p ≈ 0.013 is about what chance alone can produce. The support is the consistency across all index variants and the published record of this effect, not that one p-value.

## Frozen strategies for the holdout (`dip_frozen.json`)

Signal at the close; buy at the next session's open; sell at the close five sessions after the signal; one position per ETF; five slots of 20%; most oversold first.

| | Universe | Trigger (and price above its 200-day average) | Status |
|---|---|---|---|
| D1 | 8 broad index ETFs (SPY QQQ IWM DIA MDY IJR VTI RSP) | 5-day return ≤ −3% | primary |
| D2 | same | close ≥ 2 standard deviations below its 20-day average | primary |
| D3 | 8 index + 19 sector ETFs | 5-day return ≤ −3% | primary |
| D1c | as D1, entered at the signal day's close | | secondary |
| D1x | as D1, no uptrend filter | | secondary (the filter should matter) |
| D1L | as D1, held 21 sessions | | secondary |
| D1s | SPY, QQQ, IWM only | 5-day return ≤ −3% | secondary |

## Holdout gates (fixed before the holdout is opened; computed by `bs_dip.py holdout`)

A primary strategy is **CONFIRMED** only if: ≥ 60 trades; hit rate ≥ 60%; hit rate minus the same ETFs' any-day base rate > 0 with Holm-adjusted p < 0.05 across the three primaries (bootstrap over whole weeks and whole months of signals, the less favourable used); average trade after costs has a 95% lower bound > 0; average trade minus the any-day average has a lower bound > 0; hit rate above base in ≥ 60% of years with ≥ 5 trades. **PARTIAL** = enough trades and a significant lift, but not every gate. Otherwise **NOT CONFIRMED**.

Stock-level strategies are not carried to the holdout: the map gave no candidate worth a test.
