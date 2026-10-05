# Pullback Watch

Daily tracker for one rule on eight broad US index funds. Served by themes_web
at `/pullback`. Standalone module: it imports nothing from the scanner, themes
or ignition, and nothing imports it (themes_web runs it by subprocess and reads
its JSON).

## The rule

```
SIGNAL (at the close) = 5-session return <= -3%  AND  close > 200-session average
BUY   at the next session's open
SELL  at the close five sessions after the signal
one position per fund; no stop, no target
```

Funds: SPY, QQQ, IWM, DIA, MDY, IJR, VTI, RSP.

## Where it came from

A blank-slate search of daily bars (`research/blank_slate/`, write-up in
`research_findings_blank_slate.md`): about 45 conditions across 8,400 listed and
delisted stocks, 41 ETFs and the calendar, mapped on 2006–2019. Stocks and the
calendar showed nothing. Index funds that are sharply oversold while above
their 200-day average was the one family with a lift. Three versions were
frozen and run once on 2020–2026.

| | Training 2006–2019 | Holdout 2020–2026 |
|---|---|---|
| Trades (separate weeks) | 348 (131) | 190 (75) |
| Hit rate after costs | 65.5% | 66.8% |
| Same funds, any day | 55.6% | 55.9% |
| Average trade | +0.61% | +0.74% (95% range −0.05% to +1.47%) |
| Five 20% slots | +3.0% a year, 8% of capital in use | +4.2% a year, 9% in use |

**Status: not confirmed.** It failed three of six pre-set gates narrowly (lift
p 0.037 raw, 0.11 after correcting for three rules; average-trade lower bound
just below zero; hit rate above base in four of seven years). The version
triggered by a 20-day band failed outright, and without the 200-day condition
the lift mostly disappears. The tracker exists to collect new evidence.

## What the scan writes (`pullback/data/`, on the `pullback-data` branch)

- `latest.json`: each fund's state today (`SIGNAL`, `HOLDING`, `WATCH`, `OFF` =
  below its 200-day average), the full trade ledger, the forward summary and
  the back-test record.
- `runs.jsonl`: one line per run (new signals, sells, counts).
- `history/<asof>.json`: a copy of every run.

Trades from `LEDGER_START` (2026-07-01, the first session after the holdout
window) to the day before `LIVE_FROM` (2026-10-05) are replayed from history
and flagged `live: false`; later ones are recorded as they happen.

## Rules the scan keeps

- **Append-only.** A signal or closed trade recorded by an earlier run is never
  dropped or re-priced (`carry_forward`), even if Yahoo revises its bars.
- **No thin publishes.** The scan refuses to write if any fund's latest bar is
  missing or the data is more than five days old (`check_coverage`), and it
  drops a half-finished session when run during market hours.
- **Data never goes to main.** The GitHub Actions job
  (`.github/workflows/pullback-daily.yml`, weekdays 21:40 UTC) commits to
  `pullback-data`; a push to main would redeploy Railway.

## Run it

```
python pullback/scan.py
```

Returns include dividends and a 0.04% round-trip cost. Each fund is tracked on
its own; the back-tested portfolio held at most five at a time.
