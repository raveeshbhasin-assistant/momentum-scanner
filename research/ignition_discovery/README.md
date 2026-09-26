# Ignition discovery — what precedes Dell/Micron-style explosive moves (2026-09-20)

The study that produced the ignition signal. It is now live as
`ignition/scan.py` and the `/ignition` page; the exit rule came later, in
`../ignition_exits/`. Moved here from the operator's Test1 repo
(branch `claude/stock-momentum-patterns-op28f6`) on 2026-09-26.

**Question (operator):** find major stocks that surged in a short time
after years of drifting (Dell ~3x, Micron ~7x). What could have predicted
the move and its timing, how does that backtest, and which stocks should
be watched now?

## Contents

| File | What it is |
|---|---|
| `pipeline/download_data.py` | ~11y of daily OHLCV for the S&P 500 + 400 (yfinance, chunked, re-runnable) |
| `pipeline/analyze.py` | Episode detection, feature study, 5-signal backtest, current screen |
| `results.json` | Study output as of 2026-09-18: backtest tables, per-year robustness, famous and top episodes, the 2026-09 watchlist |
| `report.html` | The full write-up as a standalone page (open locally in a browser) |

## Findings (2015–2026, 903 tickers, 628 episodes of +80% within 63 sessions)

1. **The best entry day looks weak and can't be known in real time.** The
   median breakout was 29% off its high, below its 50-DMA, RSI ≈ 38.
2. **The tradeable tell is the ignition:** a +12% week on ≥ 1.5× volume
   inside an uptrend (50-DMA > 200-DMA). Mean 63-session return +17.0% vs
   +4.5% baseline. 26.9% gained another +40% (5.8× the base rate) and 8.2%
   gained another +80% (13×). Positive in 10 of 11 years.
3. **Quiet breakouts don't work.** A tight base breaking to a new high
   without a volume shock underperformed the baseline. Volume carries the
   signal.
4. **Catalysts do the work:** earnings inflections, product and AI cycles,
   index inclusion, M&A, sector supercycles. The signal finds the
   footprint; the news says whether it's real.

Caveats: the universe is current index membership (survivorship bias), and
the results exclude costs.

## Reproduce

```bash
pip install yfinance pandas numpy requests lxml
export MOMENTUM_DATA=./data        # caches stay out of the repo
python research/ignition_discovery/pipeline/download_data.py
python research/ignition_discovery/pipeline/analyze.py
```

`analyze.py` recreates `episodes.csv`, `backtest.json` and
`current_screen.csv` in `$MOMENTUM_DATA`. `download_data.py` scrapes the
constituent lists from Wikipedia, so the ticker set follows today's index
membership. The live scan pins its universe in `ignition/universe.txt`
instead.
