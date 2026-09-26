# Ignition Watch

Daily scan for the one pre-move pattern that survived an 11-year backtest of
"Dell/Micron-style" explosive moves. Served by themes_web at `/ignition`.

## The research (2015–2026, S&P 500 + 400, 903 tickers)

- **628 episodes** of a stock gaining ≥ 80% within 63 trading days. About
  half were crash rebounds (2020, late 2022); 327 were breakouts from a flat
  base or an existing uptrend (Dell Dec-2023, Micron Apr-2025, PLTR, SMCI,
  VST, GEV, CEG, AMD).
- **The ideal entry is unknowable in real time.** The day before the best
  entry, the median breakout stock was 29% off its high, below its 50-DMA,
  RSI ≈ 38, with a negative 3-month return.
- **The first violent week is catchable.** Most big runs broke cover with
  a +12% week on heavy volume inside an uptrend.
- **Quiet breakouts don't work.** A tight base breaking to a new high
  *without* a volume shock had lower odds of +40% than the baseline
  (2.9% vs 4.6%). Volume is the load-bearing ingredient.

## The signal

```
IGNITION = 5-day return > +12%
           AND 21-day avg volume > 1.5x its 126-day avg
           AND 50-DMA > 200-DMA
           (price > $3, 21-day avg dollar volume > $5M)
```

| Signal (2016–2026) | n | P(+40% in 63d) | P(+80%) | Mean 63d | P(−20% DD) |
|---|---:|---:|---:|---:|---:|
| **Ignition** | 2,271 | **26.9%** | **8.2%** | **+17.0%** | 22.7% |
| Volume shock (2× vol, +10% month) | 2,681 | 21.3% | 8.2% | +8.9% | 29.1% |
| 52w high + volume + RS | 34,297 | 10.3% | 2.4% | +6.6% | 17.1% |
| RSI<35 dip in uptrend | 11,021 | 5.0% | 0.2% | +5.0% | 13.1% |
| Baseline (any liquid day) | 2,178,204 | 4.6% | 0.6% | +4.5% | 11.7% |
| Quiet tight breakout | 447,403 | 2.9% | 0.4% | +3.3% | 9.7% |

Ignition beat the same-year baseline in 10 of 11 years (2017 was the
exception). Caveats: no costs or slippage, and the universe is *current*
index membership, so there is survivorship bias. Treat the edge as an upper
bound.

## Operation

- `python ignition/scan.py` downloads ~2.5y of Yahoo daily bars for
  `universe.txt` (~1–2 min) and writes `data/latest.json` plus
  `data/history/<date>.json`.
- themes_web runs it weekdays at **17:00 ET** and once at boot when
  `latest.json` is missing, because Railway's disk is wiped on redeploy. You
  can also trigger it with `POST /api/refresh_ignition`.
- Fires are derived from price history, so the 90-session log rebuilds
  itself after a redeploy. Only "first seen" and the NEW badge depend on the
  previous `latest.json`. Without one, NEW means "fired in the last 3
  sessions".
- `universe.txt` is pinned (S&P 500 + 400 as of 2026-09-26). Refresh it when
  index membership drifts.
- Standalone module: it imports nothing from the scanner or themes, and
  themes_web calls it only by subprocess.

Research pipeline and full episode catalog:
github.com/raveeshbhasin-git/Test1, branch `claude/stock-momentum-patterns-op28f6`, `momentum/`.
