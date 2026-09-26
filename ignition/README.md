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

## Positions and the sell rule (v1.6.0)

Every fire since 2025-01-02 is replayed as a position: bought at the next
open, then tracked. Full study: `research/ignition_exits/README.md`.

| Status | Meaning |
|---|---|
| NEW | Fired on the last session; entry is at the next open |
| HOLD | Ignition intact, within 63 sessions of the last fire |
| REFIRED | Fresh ignition within the last 20 sessions: the strongest hold state |
| EDGE_EXPIRED | 63+ sessions since the last fire. A review flag, not a sell |
| SELL_PENDING | Sell signal on the last close; sell at the next open |
| CLOSED | Sold, for one of two reasons (below) |

**IGNITION_FAILED** means a close below the pre-ignition base (the close 5
sessions before the fire). It was the only exit near the top in train,
holdout and live data. **TIME** closes the position after 252 sessions
without a re-fire. Dropping off the signal list is *not* a sell.

### 6-month review (v1.7.0, flag only)
The sell line sits a median 14% *below* entry, so IGNITION_FAILED only ever
sells at a loss. The **6-month review** flags a position 126+ sessions after
entry that is up, but by less than +30%. It is the one take-profit rule that
held up across train, holdout and live data. It never closes a position.

- **Why flag only:** it failed the pre-registered bar. It gives about half as
  many "was up 20%, sold at a loss" trades and a better median, but it costs
  ~6 pp of mean per trade (`research/ignition_exits/profit_protection/`).
- **What-if stats:** the ledger scores the whole history as if every flag had
  been sold at the next open (`summary.checkpoint_whatif`), shown next to the
  live rule on the page.
- **Data:** each position carries `checkpoint` (date), `checkpoint_ret`,
  `checkpoint_exit` and `whatif_ret`. A `CHECKPOINT` event is logged once
  per position.

## Operation

- **Scan of record:** `.github/workflows/ignition-daily.yml` runs
  `python ignition/scan.py` weekdays at 21:30 UTC, after the close. It
  commits `latest.json`, `runs.jsonl` (one line per run) and
  `history/<asof>.json` to the **`ignition-data` branch**. Git keeps every
  run, and pushes to that branch don't redeploy Railway. You can also run
  it on demand from the Actions tab (workflow_dispatch).
- **themes_web** syncs from that branch every 30 minutes and at boot
  (`scheduler.refresh_ignition`). If GitHub is unreachable and there is no
  data on disk, it falls back to a local scan, and the page says so.
  `POST /api/refresh_ignition` syncs now; `?local=1` scans on the container.
- **No lost days:** the ledger is rebuilt from prices on every run. If runs
  are skipped, the next run reports everything since the previous run's
  data date as new.
- `universe.txt` is pinned (S&P 500 + 400 as of 2026-09-26). Refresh it when
  index membership drifts.
- Standalone module: it imports nothing from the scanner or themes, and
  themes_web calls it only by subprocess.

Discovery study (the 628-episode research that produced this signal):
`research/ignition_discovery/`. Exit study: `research/ignition_exits/`.
