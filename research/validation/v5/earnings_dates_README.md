# earnings_dates.csv - coverage report

Generated 2026-09-13 10:38 local. Columns: `ticker,date,timing,source`. timing in {bmo,amc,unknown}.

## Sources

1. **nasdaq** - `https://api.nasdaq.com/api/calendar/earnings?date=YYYY-MM-DD`, one request per weekday, browser-like headers, 5 threads. Date = calendar date Nasdaq lists the report on. Timing from the `time` field (`time-pre-market`->bmo, `time-after-hours`->amc, `time-not-supplied`->unknown).
2. **sec_8k** - SEC `data.sec.gov/submissions/CIK##########.json` (plus its archived pages), one company at a time for every universe ticker that maps to a CIK via `sec.gov/files/company_tickers.json`. Kept: forms 8-K / 8-K/A whose `items` include 2.02 (Results of Operations and Financial Condition), filed 2004+. Date = `reportDate` (8-K 'date of earliest event reported' = press-release date); falls back to filingDate if missing/implausible (>30d before filing). Timing inferred from `acceptanceDateTime` converted to US/Eastern: accepted on the event date before 09:30 -> bmo, at/after 16:00 -> amc, intraday -> unknown; accepted 1-3 days after the event date before 09:30 -> amc (after-close release filed next morning); otherwise unknown.
   EDGAR full-text search (`efts.sec.gov/LATEST/search-index`, q="Results of Operations", forms=8-K) was tested and works (418 hits for 2019-04-25, `period_ending` + `items` present) but was dropped in favour of the submissions API: ~1.3 req/s single-threaded and no acceptance time.
3. yfinance - not used (Yahoo throttling; not needed).

## Merge / dedupe rules

- Universe filter: only tickers in `us_common_symbols.csv` (column `yf`), symbols normalised (`.` and `/` -> `-`).
- Exact (ticker,date) matches across sources -> one row, `source=nasdaq+sec_8k`; Nasdaq timing kept when it is bmo/amc, otherwise the SEC-inferred timing is used.
- SEC rows within +/-3 calendar days of a Nasdaq row for the same ticker are treated as the same event and dropped (Nasdaq date kept). Dropped rows are in `earn/sec_rows_dropped_near_nasdaq.csv` for inspection.
- SEC rows with no Nasdaq row within 3 days are kept as `source=sec_8k`.

## Totals

- Universe tickers: 5245; tickers with >=1 date: 3025
- Final rows: 107367  (nasdaq-only 36642, nasdaq+sec_8k 14381, sec_8k-only 56344)
- Timing: {'unknown': 54086, 'amc': 29773, 'bmo': 23508}
- Nasdaq calendar days fetched: 1282 (min 2014-01-01 max 2018-11-29); distinct symbols seen (all, before universe filter): 3428
- SEC CIKs fetched: 942; 8-K filings with Item 2.02 since 2004: 72202; distinct (ticker,date) rows: 71562
- SEC rows exactly matching Nasdaq: 14381; within 1-3 days (dropped): 837; SEC-only kept: 56344
- SEC acceptance-hour histogram (ET, all 2.02 8-Ks): [(0, 2), (2, 2), (3, 12), (4, 23), (5, 28), (6, 5114), (7, 9299), (8, 9253), (9, 5163), (10, 2828), (11, 2064), (12, 1583), (13, 1569), (14, 1732), (15, 1943), (16, 23024), (17, 5755), (18, 1251), (19, 731), (20, 484), (21, 340), (22, 2)]

## Coverage per year (final file)

| year | rows | tickers | nasdaq | nasdaq+sec | sec_only | bmo | amc | unknown |
|---|---|---|---|---|---|---|---|---|
| 2004 | 719 | 552 | 0 | 0 | 719 | 137 | 217 | 365 |
| 2005 | 2553 | 594 | 0 | 0 | 2553 | 572 | 754 | 1227 |
| 2006 | 2603 | 613 | 0 | 0 | 2603 | 580 | 831 | 1192 |
| 2007 | 2671 | 632 | 0 | 0 | 2671 | 631 | 849 | 1191 |
| 2008 | 2746 | 651 | 0 | 0 | 2746 | 689 | 921 | 1136 |
| 2009 | 2782 | 665 | 0 | 0 | 2782 | 799 | 968 | 1015 |
| 2010 | 2891 | 702 | 0 | 0 | 2891 | 876 | 1078 | 937 |
| 2011 | 3054 | 741 | 0 | 0 | 3054 | 936 | 1162 | 956 |
| 2012 | 3259 | 776 | 0 | 0 | 3259 | 1075 | 1250 | 934 |
| 2013 | 3387 | 818 | 0 | 0 | 3387 | 1101 | 1330 | 956 |
| 2014 | 9936 | 2482 | 6608 | 2838 | 490 | 1150 | 1367 | 7419 |
| 2015 | 10426 | 2611 | 7119 | 2865 | 442 | 1159 | 1385 | 7882 |
| 2016 | 10604 | 2688 | 7290 | 2863 | 451 | 1199 | 1420 | 7985 |
| 2017 | 10960 | 2775 | 7659 | 2949 | 352 | 1210 | 1450 | 8300 |
| 2018 | 11293 | 2894 | 7966 | 2866 | 461 | 1214 | 1498 | 8581 |
| 2019 | 3462 | 833 | 0 | 0 | 3462 | 1253 | 1602 | 607 |
| 2020 | 3610 | 839 | 0 | 0 | 3610 | 1289 | 1693 | 628 |
| 2021 | 3548 | 842 | 0 | 0 | 3548 | 1308 | 1675 | 565 |
| 2022 | 3563 | 847 | 0 | 0 | 3563 | 1296 | 1770 | 497 |
| 2023 | 3560 | 844 | 0 | 0 | 3560 | 1302 | 1760 | 498 |
| 2024 | 3549 | 846 | 0 | 0 | 3549 | 1341 | 1739 | 469 |
| 2025 | 3553 | 851 | 0 | 0 | 3553 | 1364 | 1749 | 440 |
| 2026 | 2638 | 844 | 0 | 0 | 2638 | 1027 | 1305 | 306 |

## Nasdaq raw `time` field by year (all symbols, before universe filter)

| year | time-pre-market | time-after-hours | time-not-supplied | other |
|---|---|---|---|---|
| 2014 | 0 | 0 | 10670 | 0 |
| 2015 | 0 | 0 | 11218 | 0 |
| 2016 | 0 | 0 | 11300 | 0 |
| 2017 | 0 | 0 | 11693 | 0 |
| 2018 | 0 | 0 | 11928 | 0 |

## Per-source rows per year (before merge, universe-filtered)

| year | nasdaq rows | nasdaq tickers | sec rows | sec tickers | sec bmo | sec amc |
|---|---|---|---|---|---|---|
| 2004 | 0 | 0 | 719 | 552 | 137 | 217 |
| 2005 | 0 | 0 | 2553 | 594 | 572 | 754 |
| 2006 | 0 | 0 | 2603 | 613 | 580 | 831 |
| 2007 | 0 | 0 | 2671 | 632 | 631 | 849 |
| 2008 | 0 | 0 | 2746 | 651 | 689 | 921 |
| 2009 | 0 | 0 | 2782 | 665 | 799 | 968 |
| 2010 | 0 | 0 | 2891 | 702 | 876 | 1078 |
| 2011 | 0 | 0 | 3054 | 741 | 936 | 1162 |
| 2012 | 0 | 0 | 3259 | 776 | 1075 | 1250 |
| 2013 | 0 | 0 | 3387 | 818 | 1101 | 1330 |
| 2014 | 9446 | 2457 | 3497 | 833 | 1164 | 1430 |
| 2015 | 9984 | 2591 | 3522 | 831 | 1172 | 1462 |
| 2016 | 10153 | 2673 | 3477 | 834 | 1220 | 1480 |
| 2017 | 10608 | 2758 | 3449 | 829 | 1227 | 1508 |
| 2018 | 10832 | 2879 | 3469 | 832 | 1236 | 1556 |
| 2019 | 0 | 0 | 3462 | 833 | 1253 | 1602 |
| 2020 | 0 | 0 | 3610 | 839 | 1289 | 1693 |
| 2021 | 0 | 0 | 3548 | 842 | 1308 | 1675 |
| 2022 | 0 | 0 | 3563 | 847 | 1296 | 1770 |
| 2023 | 0 | 0 | 3560 | 844 | 1302 | 1760 |
| 2024 | 0 | 0 | 3549 | 846 | 1341 | 1739 |
| 2025 | 0 | 0 | 3553 | 851 | 1364 | 1749 |
| 2026 | 0 | 0 | 2638 | 844 | 1027 | 1305 |

## Failed / missing (dates for nasdaq, CIKs for sec)

- none

## Caveats

- Tickers are *current* symbols (universe list and SEC mapping are current); companies that changed ticker are covered only under their current symbol. Nasdaq rows carry the symbol as of the report date, so a company's pre-rename Nasdaq rows are dropped by the universe filter, while its SEC rows (keyed by CIK) are complete.
- Nasdaq calendar coverage is thin before ~2009 (no records in 2004-2005 probes) and its historical `time` field is mostly `time-not-supplied`; see the table above for how much bmo/amc is populated per year.
- SEC dates are press-release/event dates from the 8-K (`reportDate`); the 8-K may be filed up to 4 business days later. SEC timing is a proxy from the 8-K acceptance timestamp, not the press-release timestamp; when the Nasdaq calendar supplies bmo/amc it takes precedence.
- Foreign private issuers (20-F/6-K filers) do not file 8-Ks, so their dates come only from Nasdaq. SPACs and companies without a 2.02 8-K habit are under-covered on the SEC side.
- Scripts: `earn/nasdaq_fetch.py`, `earn/sec_sub_fetch.py` (both resumable, checkpoint to `earn/*_raw*.jsonl`), `earn/assemble.py` (this report).
