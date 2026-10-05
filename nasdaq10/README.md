# Nasdaq Leaders

Daily tracker for two monthly top-ten lists of Nasdaq-100 stocks. Served by
themes_web at `/signals` (section 2). Standalone module: it imports nothing
from the scanner, themes, ignition or pullback, and nothing imports it
(themes_web runs it by subprocess and reads its JSON).

## The rules

```
RANK   at the last close of the month, among today's Nasdaq-100 members
TRADE  at the next session's close: ten names in equal parts, held until the next rebalance

Ten largest    rank by market value (shares outstanding x close)
Ten strongest  rank by total return from 252 to 21 sessions before the ranking day
```

A second share class of a company already in the list is ignored (GOOG next
to GOOGL). No stop, no target, no market-timing switch.

## Where they came from

Three studies of "hold the top N index members" (`research/top_n/`, write-up
in `research_findings_top_n.md`): size, seven other definitions of "top", and
a rule that follows whichever definition led recently. On the S&P 500 nothing
beat SPY. On the Nasdaq-100 these two came out ahead of QQQ in both halves of
the sample without reaching significance.

| 2007-03 to 2026-09, 10 bps a side | Ten largest | Ten strongest | QQQ |
|---|---|---|---|
| Return a year | 19.5% | 18.8% | 16.5% |
| Lead over QQQ, 2007–2015 / 2016–2026 | +1.4 / +4.3 | +1.8 / +2.8 | |
| Calendar years ahead | 13 of 19 | 9 of 19 | |
| Worst drawdown | −48% | −57% | −53% |
| Volatility | 25% | 32% | 22% |

**Status: not confirmed.** The 95% range of the lead is −0.3 to +6.3 points a
year for ten largest and −6.5 to +19.1 (2016–2026) for ten strongest. About a
hundred variations were tried on the same history. Ten largest is a
concentrated bet on big technology (its lead drops from +3.0 to +1.3 without
Nvidia). Ten strongest replaces about a third of its names every month. The
tracker exists to collect new evidence: the back-tests end 2026-09-30 and the
ledger starts with the rebalance traded on 2026-10-01.

## What the scan does each day

`python nasdaq10/scan.py`, weekdays after the US close
(`.github/workflows/nasdaq10-daily.yml`, 21:50 UTC), committed to the
`nasdaq10-data` branch. themes_web syncs from that branch every 30 minutes.

- Members: Wikipedia's "List of NASDAQ-100 companies", read on every run;
  `universe.txt` is the fallback when the page cannot be read or does not look
  like the index.
- Prices: Yahoo daily bars. Shares outstanding: Yahoo, with the last known
  figure (`data/shares.json`, seeded by `shares_seed.json`) when it does not answer.
- Most days: the lists as held, each name's return since it was bought, and
  what would change if the month ended today.
- Last session of the month: next month's lists are fixed and the page shows
  the buys and sells for the next close.

History is append-only: a month's picks are never re-picked and a finished
month keeps its recorded returns, even if Yahoo revises prices. The scan
refuses to publish when bars or share counts are missing for more than 10% of
members or the latest bar is stale.

## Files

| | |
|---|---|
| `scan.py` | members, download, ranking, ledger, output |
| `universe.txt` | fallback member list |
| `shares_seed.json` | fallback shares outstanding |
| `data/latest.json` | current state (data branch only) |
| `data/runs.jsonl`, `data/history/` | run log and a copy of every run |
| `tests/test_nasdaq10.py` | rules, ledger, calendar, page |
