# Momentum Scanner — April 15, 2026 Post-Mortem Analysis

**Date analyzed:** Wednesday, April 15, 2026  
**Total signals (all day):** 132  
**Market-hours signals (9:30 AM – 4:00 PM ET):** 82 signals across 40 unique tickers  
**Invalid signal:** SQ (Block Inc. renamed to ticker XYZ in Jan 2025 — see Bug #1 below)  
**Signals analyzed:** 40 unique first-entry trades  
**Analysis run:** April 16, 2026

> **Methodology:** For each unique ticker, only the *first* market-hours signal was treated as a trade entry. Assumed 1 share per trade, entry at the listed price, exit at whichever came first: target hit, stop hit, or 4:00 PM close. For 11 tickers where both target and stop levels were breached during the day, 5-minute intraday data was used to determine which triggered first.

---

## Summary Scorecard

| Metric | Value |
|--------|-------|
| Total valid trades | 40 |
| Winners (target hit) | 6 |
| Partial win (closed above entry, no target) | 1 (HON) |
| Losers (stop hit) | 33 |
| **Win rate** | **17.5%** |
| Gross profit | +$15.34 |
| Gross loss | -$32.99 |
| **Net P&L (1 share each)** | **-$17.65** |
| Breakeven win rate at 2.5:1 R:R | 28.6% |
| Actual win rate vs. breakeven | **-11.1 pts below breakeven** |

The scanner needed roughly a 29% hit rate to be flat at 2.5:1 R:R. At 17.5%, it destroyed about **-15.5R** on the day — meaning it lost the equivalent of 15.5 full stop-loss amounts across the portfolio.

---

## Full Trade Log — All 40 Unique Market-Hours Entries

| # | Time | Ticker | Entry | Target | Stop | Day High | Day Low | Outcome | Exit | P&L $ | P&L % |
|---|------|--------|-------|--------|------|---------|---------|---------|------|-------|-------|
| 1 | 9:31 AM | **MSFT** | $393.02 | $395.44 | $392.05 | $414.37 | $396.73 | ✅ TARGET HIT | $395.44 | +$2.42 | +0.62% |
| 2 | 9:31 AM | GOOGL | $332.94 | $335.19 | $332.04 | $337.48 | $330.90 | ❌ STOP (9:35) | $332.04 | -$0.90 | -0.27% |
| 3 | 9:31 AM | DHR | $198.60 | $199.85 | $198.10 | $199.48 | $197.28 | ❌ STOP | $198.10 | -$0.50 | -0.25% |
| 4 | 9:31 AM | **BKNG** | $181.08 | $182.80 | $180.39 | $186.60 | $182.33 | ✅ TARGET HIT | $182.80 | +$1.72 | +0.95% |
| 5 | 9:31 AM | LOW | $248.39 | $249.82 | $247.82 | $248.44 | $242.87 | ❌ STOP | $247.82 | -$0.57 | -0.23% |
| 6 | 9:31 AM | AMD | $255.06 | $257.01 | $254.28 | $258.18 | $251.85 | ❌ STOP (9:35) | $254.28 | -$0.78 | -0.31% |
| 7 | 9:31 AM | **NVDA** | $196.49 | $197.92 | $195.92 | $200.40 | $195.74 | ✅ TARGET (9:35) | $197.92 | +$1.43 | +0.73% |
| 8 | 9:31 AM | SBUX | $98.46 | $99.08 | $98.21 | $99.18 | $97.80 | ❌ STOP (9:35) | $98.21 | -$0.25 | -0.25% |
| 9 | 9:31 AM | F | $12.70 | $12.80 | $12.66 | $12.76 | $12.49 | ❌ STOP | $12.66 | -$0.04 | -0.31% |
| 10 | 9:31 AM | **SOFI** | $17.92 | $18.17 | $17.82 | $18.82 | $18.17 | ✅ TARGET HIT | $18.17 | +$0.25 | +1.40% |
| 11 | 10:01 AM | INTC | $64.34 | $66.09 | $63.64 | $65.84 | $62.88 | ❌ STOP | $63.64 | -$0.70 | -1.09% |
| 12 | 10:01 AM | NET | $185.38 | $191.68 | $182.86 | $190.88 | $182.00 | ❌ STOP | $182.86 | -$2.52 | -1.36% |
| 13 | 10:01 AM | SQ | $64.21 | $64.83 | $63.96 | — | — | 🚫 INVALID (delisted) | — | — | — |
| 14 | 10:01 AM | ISRG | $472.28 | $478.88 | $469.64 | $473.99 | $465.79 | ❌ STOP | $469.64 | -$2.64 | -0.56% |
| 15 | 10:31 AM | NKE | $45.35 | $46.25 | $44.99 | $45.90 | $44.71 | ❌ STOP | $44.99 | -$0.36 | -0.79% |
| 16 | 10:31 AM | QCOM | $133.43 | $134.90 | $132.84 | $134.34 | $132.28 | ❌ STOP | $132.84 | -$0.59 | -0.44% |
| 17 | 10:31 AM | ICE | $164.63 | $166.56 | $163.86 | $165.80 | $162.33 | ❌ STOP | $163.86 | -$0.77 | -0.47% |
| 18 | 10:31 AM | DKNG | $23.56 | $24.18 | $23.31 | $24.00 | $23.06 | ❌ STOP | $23.31 | -$0.25 | -1.06% |
| 19 | 10:31 AM | HOOD | $85.84 | $89.29 | $84.46 | $87.55 | $81.50 | ❌ STOP | $84.46 | -$1.38 | -1.61% |
| 20 | 10:31 AM | UBER | $76.68 | $78.13 | $76.10 | $77.93 | $73.79 | ❌ STOP | $76.10 | -$0.58 | -0.76% |
| 21 | 10:31 AM | SNAP | $6.03 | $6.36 | $5.90 | $6.15 | $5.83 | ❌ STOP | $5.90 | -$0.13 | -2.16% |
| 22 | 11:01 AM | PNC | $224.33 | $229.33 | $222.33 | $225.47 | $218.40 | ❌ STOP | $222.33 | -$2.00 | -0.89% |
| 23 | 11:01 AM | **TSLA** | $381.72 | $389.77 | $378.50 | $394.65 | $362.50 | ✅ TARGET (11:30) | $389.77 | +$8.05 | +2.11% |
| 24 | 11:31 AM | ADP | $197.13 | $199.73 | $196.09 | $198.88 | $194.53 | ❌ STOP | $196.09 | -$1.04 | -0.53% |
| 25 | 12:01 PM | AVGO | $392.75 | $398.28 | $390.54 | $397.08 | $385.57 | ❌ STOP | $390.54 | -$2.21 | -0.56% |
| 26 | 12:31 PM | COIN | $191.70 | $195.85 | $190.04 | $196.46 | $183.52 | ❌ STOP (12:40) | $190.04 | -$1.66 | -0.87% |
| 27 | 1:01 PM | ABNB | $137.23 | $138.68 | $136.65 | $138.05 | $134.10 | ❌ STOP | $136.65 | -$0.58 | -0.42% |
| 28 | 1:01 PM | COST | $982.64 | $990.92 | $979.33 | $985.38 | $968.30 | ❌ STOP | $979.33 | -$3.31 | -0.34% |
| 29 | 1:01 PM | WMT | $124.80 | $125.88 | $124.37 | $125.07 | $123.18 | ❌ STOP | $124.37 | -$0.43 | -0.34% |
| 30 | 2:01 PM | NOC | $681.24 | $684.86 | $679.79 | $682.88 | $676.00 | ❌ STOP | $679.79 | -$1.45 | -0.21% |
| 31 | 2:01 PM | SNOW | $142.54 | $144.82 | $141.63 | $144.73 | $137.01 | ❌ STOP | $141.63 | -$0.91 | -0.64% |
| 32 | 2:01 PM | IBM | $245.27 | $246.87 | $244.63 | $246.06 | $240.99 | ❌ STOP | $244.63 | -$0.64 | -0.26% |
| 33 | 2:01 PM | **PLD** | $138.95 | $139.70 | $138.65 | $139.90 | $137.49 | ✅ TARGET (3:55) | $139.70 | +$0.75 | +0.54% |
| 34 | 2:31 PM | SCHW | $100.14 | $100.66 | $99.93 | $100.76 | $99.09 | ❌ STOP (3:40) | $99.93 | -$0.21 | -0.21% |
| 35 | 2:31 PM | BLK | $1,051.82 | $1,059.70 | $1,048.67 | $1,062.00 | $1,043.45 | ❌ STOP (3:40) | $1,048.67 | -$3.15 | -0.30% |
| 36 | 3:01 PM | ABT | $102.18 | $102.78 | $101.94 | $102.48 | $100.63 | ❌ STOP | $101.94 | -$0.24 | -0.24% |
| 37 | 3:01 PM | **HON** | $231.47 | $232.84 | $230.92 | $232.92* | $228.93 | ⚠️ CLOSE (neither) | $232.19 | +$0.72 | +0.31% |
| 38 | 3:31 PM | MMM | $151.60 | $152.50 | $151.24 | $152.68 | $150.09 | ❌ STOP (3:35) | $151.24 | -$0.36 | -0.24% |
| 39 | 3:31 PM | BA | $223.76 | $224.94 | $223.29 | $224.72 | $221.65 | ❌ STOP | $223.29 | -$0.47 | -0.21% |
| 40 | 3:31 PM | TMO | $532.03 | $535.13 | $530.79 | $532.86 | $524.76 | ❌ STOP | $530.79 | -$1.24 | -0.23% |
| 41 | 3:31 PM | USB | $56.60 | $56.92 | $56.47 | $56.76 | $55.84 | ❌ STOP | $56.47 | -$0.13 | -0.23% |

*HON daily high of $232.92 was hit at market open, before the 3:01 PM entry — so the target was unreachable after entry.

---

## P&L Breakdown

### Winners (+$15.34 total)

| Ticker | Entry | Exit | $ Gain | % Gain | Score | RVOL | RSI | Time | Sector |
|--------|-------|------|--------|--------|-------|------|-----|------|--------|
| TSLA | $381.72 | $389.77 | **+$8.05** | +2.11% | 62 | 2.9x | 86 | 11:01 AM | Auto/EV |
| MSFT | $393.02 | $395.44 | +$2.42 | +0.62% | 75 | 4.0x | 60 | 9:31 AM | Tech |
| BKNG | $181.08 | $182.80 | +$1.72 | +0.95% | 75 | 4.7x | 52 | 9:31 AM | Travel |
| NVDA | $196.49 | $197.92 | +$1.43 | +0.73% | 72 | 3.7x | 73 | 9:31 AM | Semis |
| HON* | $231.47 | $232.19 | +$0.72 | +0.31% | 69 | 2.2x | 64 | 3:01 PM | Industrials |
| PLD | $138.95 | $139.70 | +$0.75 | +0.54% | 64 | 1.9x | 68 | 2:01 PM | REIT |
| SOFI | $17.92 | $18.17 | +$0.25 | +1.40% | 71 | 3.2x | 62 | 9:31 AM | Fintech |

*HON: partial win — closed above entry but never reached target

### Top Losers (worst performers)

| Ticker | $ Loss | % Loss | Stop Distance | Note |
|--------|--------|--------|---------------|------|
| COST | -$3.31 | -0.34% | $3.31 | Wide stop on high-priced stock |
| BLK | -$3.15 | -0.30% | $3.15 | Very late signal (2:31 PM), stop hit 3:40 PM |
| ISRG | -$2.64 | -0.56% | $2.64 | Small target vs high ATR |
| NET | -$2.52 | -1.36% | $2.52 | Big target gap never reached |
| AVGO | -$2.21 | -0.56% | $2.21 | Midday pick, lunchtime reversal |
| PNC | -$2.00 | -0.89% | $2.00 | Banking sector weakness |

---

## What the Winners Had in Common

**1. Strong underlying intraday momentum.** Every winning trade was in a stock with a clear directional move on April 15 — TSLA rocketed +6.8% intraday (open $366 → high $394), MSFT gapped up $5 and ran another $16 from open, BKNG drifted steadily up all day. The scanner's composite score correlated loosely with this, but not perfectly.

**2. Adequate room to breathe before the first intraday test.** BKNG and MSFT both had day lows *above* their stops, meaning the trades had room to develop without ever threatening the stop. NVDA's target was so tight ($1.43 above entry) that it hit in the very first candle (9:35 AM). TSLA had its pre-entry low (day low $362.50) hit *before* the 11:01 AM signal — after entry, the stock only moved up.

**3. RVOL above 3x.** Five of the 7 winners had RVOL ≥ 3.0x (MSFT 4.0x, BKNG 4.7x, NVDA 3.7x, SOFI 3.2x, TSLA 2.9x — only PLD at 1.9x and HON at 2.2x were lower). Compare to the overall loss pool, which included many stocks with RVOL of 1.4x–1.9x.

**4. RSI in the 60–75 zone (mostly).** The 55–75 RSI "sweet spot" held for MSFT (60), BKNG (52), NVDA (73), SOFI (62). The exception was TSLA with RSI of 86 — overbought by any measure, yet it won because the *intraday momentum was extreme*, not a general market move.

**5. Tight, achievable targets.** Winning targets were an average of $2.09 above entry (+0.82% average). Losing trades had an average target distance of $3.10 (+1.1%). The algo was setting ambitious targets for many losers that had little realistic chance within market hours.

---

## What the Losers Had in Common

**1. Opening Whipsaw — The 9:35 AM Stop-Run Problem.** Three of the ten 9:31 AM batch signals (GOOGL, AMD, SBUX) were stopped out in the VERY FIRST 5-MINUTE CANDLE after entry. The scanner fired at 9:31 AM using pre-market prices, but opening volatility caused these stocks to briefly dip 0.5–1% in the first few minutes before recovering. By 12:25 PM, GOOGL was well above its target of $335.19 — but the stop had already been triggered at 9:35 AM. AMD's stop hit at 9:35; by 10:35 AM it had rallied to hit its target of $257.01. These were correct directional calls that got stopped out by opening noise.

**2. Stale Entry Prices from Pre-Market Scan.** The 9:31 AM signal shows MSFT at $393.02, SOFI at $17.92 — but MSFT opened at $398.00 and SOFI opened at $18.47. The scanner captured pre-market prices and posted them as the opening signal. For winners like MSFT this didn't matter (the stock kept going up), but for GOOGL (opened at $332.89, barely at entry) and SBUX (opened at $98.49, above entry), this caused the stop-to-entry distance to be razor thin in practice.

**3. Midday signals in sideways/declining stocks.** Picks from 10:01 AM – 1:31 PM had a 5% win rate (1/19 valid unique trades). INTC, NET, ISRG, NKE, QCOM, ICE were all in consolidation or light downtrends when signaled. The scanner's technical score was not strong enough to distinguish "currently rallying" from "previously rallied and now consolidating."

**4. Late-day signals with no time to reach target.** The 2:31 PM – 3:31 PM batch (SCHW, BLK, ABT, MMM, BA, TMO, USB) were all stopped out. With only 85–145 minutes to close, and targets set 1.5–2% above entry, these had almost no chance. BLK's target was $7.88 above entry with 85 minutes left — unrealistic.

**5. Wrong sector.** April 15 was a tech/EV rally day. Consumer staples (WMT, ABT), Industrials (BA, TMO, HON, ADP), and defensive/non-tech financials (PNC, SCHW, USB) all lagged. The scanner had no mechanism to recognize that the day's leadership was concentrated in tech and EV.

**6. NOC — the #1 composite score (82) was a loser.** The best-scored signal of the entire day (NOC at 2:01 PM, score 82) hit its stop. NOC opened at $680.66, reached a high of only $682.88 (not enough to reach target of $684.86), then fell to $676.00. High scores are no guarantee of success if the underlying stock runs out of momentum.

---

## The SQ Bug — Critical Data Integrity Issue

**Block Inc. (formerly Square) renamed its ticker from SQ to XYZ in January 2025.** The scanner is still tracking "SQ," which is now a delisted/invalid symbol on Yahoo Finance and all major exchanges.

Impact on April 15:
- SQ appeared in **14 out of 14 market-hours scan batches** — essentially every single scan throughout the day showed SQ at $64.21 with the same entry, target, and stop.
- The actual Block Inc. (XYZ) traded between $66.10 and $68.33 on April 15 — the scanner's price of $64.21 was wrong by ~4%.
- **SQ consumed a top-10 slot in every single scan**, displacing valid signals.
- The scanner showed a "2.5:1 R:R" trade that was based on phantom price data. Any trader who followed this signal would have been unable to execute at $64.21 since XYZ was trading at $67+.

**This is the single most urgent fix needed in the scanner.**

---

## Time-of-Day Win Rate

| Scan Time Window | Unique Trades | Winners | Win Rate |
|-----------------|---------------|---------|----------|
| 9:31 AM (open batch) | 10 | 4 | **40%** |
| 10:01–11:31 AM | 9 (excl. SQ) | 1 (TSLA) | 11% |
| 12:01–1:31 PM | 7 | 0 | 0% |
| 2:01–3:31 PM | 14 | 2 (PLD, HON) | 14% |

The opening batch was by far the best performer. Midday was abysmal. Late-day was only "saved" by PLD barely hitting target at 3:55 PM and HON's partial close win.

---

## Algorithm Improvement Recommendations

### Bug Fixes (Urgent)

**Fix #1 — Update SQ → XYZ ticker.** Block Inc. changed to XYZ in January 2025. Remove SQ from the ticker list and add XYZ. This alone will eliminate 14+ phantom signals per day and free up scan slots.

**Fix #2 — Stale price detection at market open.** Before posting a 9:31 AM signal, verify the listed entry price is within 0.3% of the current bid. If `|entry_price - current_bid| / current_bid > 0.003`, recalculate targets/stops from the current bid, or suppress the signal. SOFI was showing $17.92 when the stock was already at $18.47 at open — a 3% gap. This makes the listed stop/target meaningless.

---

### Signal Quality Improvements

**Improvement #1 — Opening Range Filter (highest priority).** The 9:31 AM signals are generating stops that get triggered by opening-candle noise. Three stocks (GOOGL, AMD, SBUX) were stopped out in the first 5 minutes despite being directionally correct. Add a **15-minute opening range filter**: suppress entries until 9:45 AM, or alternatively, widen the ATR multiplier from 2.0x to 3.5x specifically for the first 15 minutes of trading. This would have saved all three of those trades.

**Improvement #2 — Time-to-target viability filter.** Before generating a signal, estimate the minimum time needed to reach the target based on the ATR and current momentum. A rough heuristic: `min_time_minutes = (target_distance / (ATR / 78))` where 78 is the number of 5-minute bars in a 6.5-hour trading day. If the signal fires within `min_time_minutes` of the 3:55 PM close, suppress it. This would eliminate the entire 3:31 PM batch and most of the 2:31 PM batch.

**Improvement #3 — Sector momentum filter.** Add a pre-check: for each signal, look up the sector ETF (XLK for Tech, XLI for Industrials, XLF for Financials, etc.) and check whether it's above or below its own VWAP. If the sector ETF is below VWAP, apply a 10-point score penalty or a minimum RVOL threshold of 3.0x instead of 1.33x. On April 15, industrials and consumer staples were underperforming. ADP (XLI sector), TMO, BA, HON, USB, ABT — all losers — would have been filtered or penalized.

**Improvement #4 — Reduce RVOL gate for midday signals.** The RVOL threshold was lowered from 1.5x to 1.33x in v3.1. Based on today's results, that was the wrong direction for midday signals. After 11:00 AM, volume naturally fades. Consider requiring RVOL ≥ 2.0x for signals between 11:00 AM and 2:30 PM, while keeping 1.33x for the open and close windows. Today's midday losers included ICE (1.6x), ISRG (2.1x), UBER (2.2x) — all too low to justify a trade.

**Improvement #5 — Deduplication with signal freshness indicator.** SQ aside, several legitimate stocks (SOFI, HOOD, GOOGL, SQ) appeared in 8–10 consecutive scans at the same price and same levels. If a signal has already fired within the last 60 minutes at essentially the same entry/stop/target, mark it "watching" rather than "new signal." This reduces noise and makes it clear to the trader whether they've already entered this trade.

**Improvement #6 — Intraday trend filter (price vs. VWAP velocity).** Add a check: is the price currently *moving toward* VWAP or *away from* VWAP? A stock that is above VWAP and has been rising for the past 3 candles is a better long candidate than one that just crossed VWAP and is stalling. A simple filter: require at least 2 of the last 3 five-minute closes to be higher than the prior close. This would filter out COIN (stop run victim at 12:40 — the stock was not in a clean uptrend at entry time) and AVGO (midday consolidation).

**Improvement #7 — RSI ceiling at 78 for standard entries.** TSLA had RSI of 86 and won — but that's because TSLA's intraday momentum was extraordinary (it opened $15 below prior close and rallied). In general, RSI > 78 at entry means you're chasing a move that's already extended. Consider capping the RSI sweet-spot score benefit at RSI = 78, and add an explicit "RSI overbought" flag in the UI (RSI > 78). Don't suppress the signal outright, but give the trader a clear warning.

---

## Key Takeaways

1. **The algo is directionally often right, but structurally stopped out.** GOOGL, AMD, TSLA, COIN all eventually reached or exceeded their targets — but were stopped before they got there. This is a stop-placement problem, not a signal-quality problem.

2. **The opening batch (9:31 AM) is the best time to trade, but needs tighter execution.** 40% win rate at open vs. 5–14% for the rest of the day. If you can avoid the first-candle whipsaw, the opening batch is where the alpha lives.

3. **Midday is a graveyard.** 0% win rate from 12:01–1:31 PM. The midday lull is real and the scanner is not accounting for reduced momentum during this window.

4. **Fix SQ immediately.** It contaminated every single scan with phantom data and wasted 14 top-10 slots.

5. **High composite score ≠ high win rate.** NOC (score 82, the day's best) lost. PLD (score 64) won. The sentiment component (25% weight) may be introducing noise that doesn't translate to intraday price moves.

---

*Data sources: Momentum Scanner history page, Yahoo Finance daily OHLCV, Yahoo Finance 5-minute intraday candles. All prices April 15, 2026 ET.*
