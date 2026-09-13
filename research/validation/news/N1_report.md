# N1: news vs no-news continuation (Chan 2003) on 8-K filings, KEY N1

Registered: `preregistration_news_vs_nonews.json`, SHA-256 `9a61eb6a7653279929a6149f13ba1c2c928969b043c8aecdb1361c279614498e` (sidecar `N1_prereg_sha256.txt`). Single run 2026-09-13 13:02. Units: percent net excess over SPY per trade after round-trip costs (10 bp if ADV20 >= $100M else 20 bp); CI = 95% wider-of-day/quarter cluster bootstrap (2000 reps).

Filings: 701,481 8-K-family rows; 675,356 mapped to bar tickers (722 filing tickers with no bars). Universe through T-1: prev close >= $5, ADV20 >= $50M, >= 250 prior rows. Events = ticker-days.

## 0. Verdict (plain)

- TEST 2015-01..2026-08, day-0 excess >= +8%: 13,939 events; NEWS (8-K reacting on T) 3,169 (23%; 2.02 earnings 2,328, non-2.02 841), NO-NEWS (no 8-K in T-1..T+1) 9,262 (66%), ambiguous 1,508.
- NEWS arm, MOC T entry: h1 +0.12% [-0.10,0.38], h5 -0.22% [-0.68,0.22], h20 -0.18% [-1.31,1.06]; years positive 50%/58%/67%; Holm p 0.722/1.000/1.000.
- NO-NEWS arm, MOC T entry: h1 +0.00% [-0.26,0.28], h5 -0.82% [-1.49,-0.10], h20 -1.59% [-3.82,0.70]; years positive 50%/25%/17%; Holm p 1.000/0.084/0.378.
- NEWS minus NO-NEWS: h1 +0.11% [-0.18,0.41], h5 +0.60% [-0.12,1.25], h20 +1.40% [-0.07,3.09].
- PASS (tradeable NEWS long): h1=False, h5=False, h20=False. NO-NEWS reverts (upper CI < 0): h1=False, h5=True, h20=False.


**Plain verdict. 0 of 6 family cells pass; nothing here is a tradeable long.** News-tagged +8% movers do not continue: net over SPY they are +0.12% next day, -0.22% at 5 sessions and -0.18% at 20 sessions (every CI through zero, years positive 50-67%). No-news +8% movers revert: -0.82% at 5 sessions with the whole CI below zero (one-sided p 0.014, Holm 0.084 across the six-cell family), -1.59% at 20 sessions (CI through zero once quarter-clustered), positive in only 3 of 12 years at h5 and 2 of 12 at h20. The news-minus-no-news gap is therefore positive (+0.60% h5, +1.40% h20) but its lower CI just misses zero at the MOC-T entry; at the MOO-T+1 entry the gap clears zero at every horizon (+0.42/+0.87/+1.70%, lower CIs +0.13/+0.22/+0.25) because the no-news arm loses a further -0.42% in the T+1 session. Two thirds of all +8% excess moves in the $50M-ADV universe (9,262 of 13,939 in TEST) carry no 8-K within T-1..T+1. Chan (2003) replicates in sign - no-news moves revert, news moves do not revert - but the news half is flat, not positive, so the only actionable content is an exclusion: do not hold a continuation position on a big move with no 8-K. Down side: news-tagged -8% drops keep falling (-0.37% next day, -1.16% at 20 sessions, both CIs below zero long-side; non-2.02 news drops -2.48% at h20), no-news drops are flat (+0.04/+0.31/-0.24%), not a bounce. Earnings (2.02) news at h20 is the only positive-pointing long cell (+0.31%, +0.55% with the N5 exclusion, 75-83% of years positive) and it is far from clearing zero, consistent with the PEAD result.

## 1. Registered family (TEST, UP8, MOC_T, no exclusion)

| arm | h | n | net excess % | lo | hi | cluster | news-nonews % | d lo | d hi | years+ | yrs | H1 | p | Holm p | PASS | REVERTS |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| NEWS | 1 | 3169 | 0.117 | -0.103 | 0.380 | quarter | 0.115 | -0.179 | 0.410 | 0.500 | 12 | >0 | 0.180 | 0.722 | False | False |
| NONEWS | 1 | 9262 | 0.002 | -0.265 | 0.278 | day |  |  |  | 0.500 | 12 | <0 | 0.496 | 1.000 | False | False |
| NEWS | 5 | 3164 | -0.222 | -0.683 | 0.224 | quarter | 0.595 | -0.118 | 1.246 | 0.583 | 12 | >0 | 0.871 | 1.000 | False | False |
| NONEWS | 5 | 9260 | -0.817 | -1.491 | -0.101 | day |  |  |  | 0.250 | 12 | <0 | 0.014 | 0.084 | False | True |
| NEWS | 20 | 3131 | -0.181 | -1.306 | 1.057 | quarter | 1.405 | -0.066 | 3.086 | 0.667 | 12 | >0 | 0.695 | 1.000 | False | False |
| NONEWS | 20 | 9124 | -1.586 | -3.819 | 0.703 | quarter |  |  |  | 0.167 | 12 | <0 | 0.075 | 0.378 | False | False |

## 2. Arm sizes (share of big moves with no 8-K)

| window | row | n | NEWS | NEWS_202 | NEWS_non202 | NONEWS | AMBIG | share_NONEWS | share_NEWS | share_bad60 | tickers |
|---|---|---|---|---|---|---|---|---|---|---|---|
| TEST | UP8 | 13939 | 3169 | 2328 | 841 | 9262 | 1508 | 0.664 | 0.227 | 0.217 | 1534 |
| TEST | UP3V | 15169 | 5695 | 4761 | 934 | 7428 | 2046 | 0.490 | 0.375 | 0.144 | 1600 |
| TEST | DN8 | 12302 | 3304 | 2448 | 856 | 7740 | 1258 | 0.629 | 0.269 | 0.225 | 1613 |
| DISC | UP8 | 2526 | 733 | 554 | 179 | 1526 | 267 | 0.604 | 0.290 | 0.221 | 368 |
| DISC | UP3V | 5018 | 1835 | 1470 | 365 | 2622 | 561 | 0.523 | 0.366 | 0.147 | 427 |
| DISC | DN8 | 2246 | 646 | 473 | 173 | 1336 | 264 | 0.595 | 0.288 | 0.201 | 368 |

## 3. All cells, both windows, both entries (UP rows with/without the N5 exclusion; DN8 long-side)

| window | row | exclN5 | entry | h | arm | n | gross % | net % | lo | hi | cluster | vs NONEWS % | d lo | d hi | years+ | net short % (DN8) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| TEST | UP8 | False | MOC_T | 1 | NEWS | 3169 | 0.25 | 0.12 | -0.10 | 0.38 | quarter | 0.11 | -0.18 | 0.41 | 0.50 |  |
| TEST | UP8 | False | MOC_T | 1 | NEWS_202 | 2328 | 0.22 | 0.09 | -0.13 | 0.30 | day | 0.09 | -0.24 | 0.41 | 0.50 |  |
| TEST | UP8 | False | MOC_T | 1 | NEWS_non202 | 841 | 0.33 | 0.20 | -0.61 | 0.99 | quarter | 0.19 | -0.54 | 0.93 | 0.42 |  |
| TEST | UP8 | False | MOC_T | 1 | NONEWS | 9262 | 0.14 | 0.00 | -0.26 | 0.28 | day |  |  |  | 0.50 |  |
| TEST | UP8 | False | MOC_T | 1 | AMBIG | 1508 | 0.11 | -0.03 | -0.70 | 0.62 | quarter |  |  |  | 0.33 |  |
| TEST | UP8 | False | MOC_T | 1 | ALL | 13939 | 0.16 | 0.02 | -0.19 | 0.26 | day |  |  |  | 0.42 |  |
| TEST | UP8 | False | MOC_T | 5 | NEWS | 3164 | -0.08 | -0.22 | -0.68 | 0.22 | quarter | 0.60 | -0.12 | 1.25 | 0.58 |  |
| TEST | UP8 | False | MOC_T | 5 | NEWS_202 | 2323 | -0.05 | -0.18 | -0.56 | 0.19 | quarter | 0.63 | -0.10 | 1.34 | 0.58 |  |
| TEST | UP8 | False | MOC_T | 5 | NEWS_non202 | 841 | -0.19 | -0.32 | -1.47 | 0.80 | day | 0.49 | -0.62 | 1.65 | 0.50 |  |
| TEST | UP8 | False | MOC_T | 5 | NONEWS | 9260 | -0.68 | -0.82 | -1.49 | -0.10 | day |  |  |  | 0.25 |  |
| TEST | UP8 | False | MOC_T | 5 | AMBIG | 1508 | -0.63 | -0.77 | -2.01 | 0.69 | quarter |  |  |  | 0.25 |  |
| TEST | UP8 | False | MOC_T | 5 | ALL | 13932 | -0.54 | -0.68 | -1.20 | -0.11 | day |  |  |  | 0.33 |  |
| TEST | UP8 | False | MOC_T | 20 | NEWS | 3131 | -0.04 | -0.18 | -1.31 | 1.06 | quarter | 1.40 | -0.07 | 3.09 | 0.67 |  |
| TEST | UP8 | False | MOC_T | 20 | NEWS_202 | 2297 | 0.45 | 0.31 | -0.70 | 1.35 | quarter | 1.90 | -0.18 | 3.98 | 0.75 |  |
| TEST | UP8 | False | MOC_T | 20 | NEWS_non202 | 834 | -1.40 | -1.54 | -4.47 | 1.58 | quarter | 0.04 | -2.01 | 1.97 | 0.25 |  |
| TEST | UP8 | False | MOC_T | 20 | NONEWS | 9124 | -1.45 | -1.59 | -3.82 | 0.70 | quarter |  |  |  | 0.17 |  |
| TEST | UP8 | False | MOC_T | 20 | AMBIG | 1488 | -1.75 | -1.88 | -4.76 | 1.12 | quarter |  |  |  | 0.08 |  |
| TEST | UP8 | False | MOC_T | 20 | ALL | 13743 | -1.16 | -1.30 | -3.15 | 0.74 | quarter |  |  |  | 0.17 |  |
| TEST | UP8 | False | MOO_T1 | 1 | NEWS | 3169 | 0.13 | -0.00 | -0.18 | 0.18 | day | 0.42 | 0.13 | 0.78 | 0.42 |  |
| TEST | UP8 | False | MOO_T1 | 1 | NEWS_202 | 2328 | 0.24 | 0.11 | -0.08 | 0.29 | quarter | 0.53 | 0.23 | 0.89 | 0.50 |  |
| TEST | UP8 | False | MOO_T1 | 1 | NEWS_non202 | 841 | -0.17 | -0.31 | -0.76 | 0.14 | day | 0.11 | -0.39 | 0.60 | 0.33 |  |
| TEST | UP8 | False | MOO_T1 | 1 | NONEWS | 9262 | -0.28 | -0.42 | -0.79 | -0.12 | quarter |  |  |  | 0.08 |  |
| TEST | UP8 | False | MOO_T1 | 1 | AMBIG | 1508 | -0.56 | -0.69 | -1.16 | -0.22 | day |  |  |  | 0.17 |  |
| TEST | UP8 | False | MOO_T1 | 1 | ALL | 13939 | -0.22 | -0.36 | -0.64 | -0.13 | quarter |  |  |  | 0.33 |  |
| TEST | UP8 | False | MOO_T1 | 5 | NEWS | 3164 | -0.21 | -0.35 | -0.76 | 0.03 | quarter | 0.87 | 0.22 | 1.49 | 0.42 |  |
| TEST | UP8 | False | MOO_T1 | 5 | NEWS_202 | 2323 | -0.03 | -0.17 | -0.55 | 0.22 | quarter | 1.05 | 0.37 | 1.71 | 0.58 |  |
| TEST | UP8 | False | MOO_T1 | 5 | NEWS_non202 | 841 | -0.70 | -0.84 | -1.95 | 0.22 | day | 0.38 | -0.70 | 1.47 | 0.25 |  |
| TEST | UP8 | False | MOO_T1 | 5 | NONEWS | 9260 | -1.08 | -1.22 | -1.83 | -0.58 | day |  |  |  | 0.17 |  |
| TEST | UP8 | False | MOO_T1 | 5 | AMBIG | 1508 | -1.28 | -1.42 | -2.55 | -0.22 | quarter |  |  |  | 0.08 |  |
| TEST | UP8 | False | MOO_T1 | 5 | ALL | 13932 | -0.91 | -1.04 | -1.53 | -0.53 | day |  |  |  | 0.17 |  |
| TEST | UP8 | False | MOO_T1 | 20 | NEWS | 3131 | -0.13 | -0.27 | -1.33 | 0.92 | quarter | 1.70 | 0.25 | 3.29 | 0.67 |  |
| TEST | UP8 | False | MOO_T1 | 20 | NEWS_202 | 2297 | 0.47 | 0.33 | -0.71 | 1.37 | quarter | 2.30 | 0.36 | 4.25 | 0.83 |  |
| TEST | UP8 | False | MOO_T1 | 20 | NEWS_non202 | 834 | -1.78 | -1.92 | -4.70 | 0.94 | quarter | 0.05 | -2.04 | 1.91 | 0.25 |  |
| TEST | UP8 | False | MOO_T1 | 20 | NONEWS | 9124 | -1.83 | -1.97 | -4.05 | 0.26 | quarter |  |  |  | 0.17 |  |
| TEST | UP8 | False | MOO_T1 | 20 | AMBIG | 1488 | -2.45 | -2.58 | -5.15 | -0.07 | quarter |  |  |  | 0.08 |  |
| TEST | UP8 | False | MOO_T1 | 20 | ALL | 13743 | -1.51 | -1.65 | -3.40 | 0.30 | quarter |  |  |  | 0.17 |  |
| TEST | UP8 | True | MOC_T | 1 | NEWS | 2553 | 0.17 | 0.03 | -0.19 | 0.25 | day | 0.04 | -0.25 | 0.35 | 0.50 |  |
| TEST | UP8 | True | MOC_T | 1 | NEWS_202 | 1997 | 0.21 | 0.07 | -0.14 | 0.29 | day | 0.09 | -0.22 | 0.45 | 0.58 |  |
| TEST | UP8 | True | MOC_T | 1 | NEWS_non202 | 556 | 0.00 | -0.14 | -0.83 | 0.44 | quarter | -0.12 | -0.73 | 0.50 | 0.30 |  |
| TEST | UP8 | True | MOC_T | 1 | NONEWS | 7332 | 0.12 | -0.02 | -0.29 | 0.26 | day |  |  |  | 0.25 |  |
| TEST | UP8 | True | MOC_T | 1 | AMBIG | 1024 | 0.13 | -0.01 | -0.61 | 0.60 | day |  |  |  | 0.33 |  |
| TEST | UP8 | True | MOC_T | 1 | ALL | 10909 | 0.13 | -0.01 | -0.25 | 0.23 | day |  |  |  | 0.42 |  |
| TEST | UP8 | True | MOC_T | 5 | NEWS | 2549 | 0.08 | -0.06 | -0.50 | 0.40 | quarter | 0.51 | -0.20 | 1.23 | 0.67 |  |
| TEST | UP8 | True | MOC_T | 5 | NEWS_202 | 1993 | 0.08 | -0.06 | -0.46 | 0.37 | quarter | 0.51 | -0.25 | 1.26 | 0.67 |  |
| TEST | UP8 | True | MOC_T | 5 | NEWS_non202 | 556 | 0.07 | -0.08 | -1.34 | 1.19 | day | 0.49 | -0.82 | 1.95 | 0.40 |  |
| TEST | UP8 | True | MOC_T | 5 | NONEWS | 7330 | -0.43 | -0.57 | -1.22 | 0.14 | day |  |  |  | 0.25 |  |
| TEST | UP8 | True | MOC_T | 5 | AMBIG | 1024 | -0.69 | -0.83 | -1.82 | 0.14 | day |  |  |  | 0.25 |  |
| TEST | UP8 | True | MOC_T | 5 | ALL | 10903 | -0.34 | -0.48 | -1.00 | 0.07 | day |  |  |  | 0.42 |  |
| TEST | UP8 | True | MOC_T | 20 | NEWS | 2520 | 0.59 | 0.45 | -0.70 | 1.71 | quarter | 1.38 | -0.37 | 3.24 | 0.67 |  |
| TEST | UP8 | True | MOC_T | 20 | NEWS_202 | 1970 | 0.69 | 0.55 | -0.40 | 1.56 | quarter | 1.48 | -0.70 | 3.69 | 0.83 |  |
| TEST | UP8 | True | MOC_T | 20 | NEWS_non202 | 550 | 0.26 | 0.12 | -2.95 | 3.17 | quarter | 1.05 | -1.19 | 3.30 | 0.40 |  |
| TEST | UP8 | True | MOC_T | 20 | NONEWS | 7221 | -0.79 | -0.93 | -3.29 | 1.49 | quarter |  |  |  | 0.33 |  |
| TEST | UP8 | True | MOC_T | 20 | AMBIG | 1008 | -0.12 | -0.25 | -3.09 | 2.80 | quarter |  |  |  | 0.33 |  |
| TEST | UP8 | True | MOC_T | 20 | ALL | 10749 | -0.40 | -0.54 | -2.46 | 1.56 | quarter |  |  |  | 0.33 |  |
| TEST | UP8 | True | MOO_T1 | 1 | NEWS | 2553 | 0.12 | -0.02 | -0.20 | 0.17 | day | 0.34 | 0.02 | 0.76 | 0.42 |  |
| TEST | UP8 | True | MOO_T1 | 1 | NEWS_202 | 1997 | 0.23 | 0.09 | -0.10 | 0.28 | quarter | 0.45 | 0.12 | 0.87 | 0.50 |  |
| TEST | UP8 | True | MOO_T1 | 1 | NEWS_non202 | 556 | -0.29 | -0.43 | -0.96 | 0.07 | day | -0.07 | -0.66 | 0.52 | 0.20 |  |
| TEST | UP8 | True | MOO_T1 | 1 | NONEWS | 7332 | -0.22 | -0.36 | -0.78 | -0.04 | quarter |  |  |  | 0.25 |  |
| TEST | UP8 | True | MOO_T1 | 1 | AMBIG | 1024 | -0.26 | -0.40 | -0.85 | 0.06 | day |  |  |  | 0.25 |  |
| TEST | UP8 | True | MOO_T1 | 1 | ALL | 10909 | -0.15 | -0.28 | -0.58 | -0.05 | quarter |  |  |  | 0.25 |  |
| TEST | UP8 | True | MOO_T1 | 5 | NEWS | 2549 | 0.02 | -0.12 | -0.53 | 0.31 | quarter | 0.80 | 0.13 | 1.47 | 0.50 |  |
| TEST | UP8 | True | MOO_T1 | 5 | NEWS_202 | 1993 | 0.10 | -0.04 | -0.43 | 0.38 | quarter | 0.88 | 0.20 | 1.56 | 0.67 |  |
| TEST | UP8 | True | MOO_T1 | 5 | NEWS_non202 | 556 | -0.25 | -0.39 | -1.57 | 0.83 | day | 0.52 | -0.72 | 1.93 | 0.50 |  |
| TEST | UP8 | True | MOO_T1 | 5 | NONEWS | 7330 | -0.78 | -0.92 | -1.52 | -0.28 | day |  |  |  | 0.08 |  |
| TEST | UP8 | True | MOO_T1 | 5 | AMBIG | 1024 | -1.08 | -1.21 | -2.20 | -0.32 | quarter |  |  |  | 0.33 |  |
| TEST | UP8 | True | MOO_T1 | 5 | ALL | 10903 | -0.62 | -0.76 | -1.24 | -0.27 | day |  |  |  | 0.17 |  |
| TEST | UP8 | True | MOO_T1 | 20 | NEWS | 2520 | 0.56 | 0.42 | -0.70 | 1.59 | quarter | 1.69 | -0.03 | 3.52 | 0.75 |  |
| TEST | UP8 | True | MOO_T1 | 20 | NEWS_202 | 1970 | 0.71 | 0.57 | -0.38 | 1.58 | quarter | 1.84 | -0.23 | 4.00 | 0.83 |  |
| TEST | UP8 | True | MOO_T1 | 20 | NEWS_non202 | 550 | 0.01 | -0.14 | -3.08 | 2.70 | quarter | 1.13 | -1.11 | 3.34 | 0.40 |  |
| TEST | UP8 | True | MOO_T1 | 20 | NONEWS | 7221 | -1.13 | -1.27 | -3.55 | 1.03 | quarter |  |  |  | 0.25 |  |
| TEST | UP8 | True | MOO_T1 | 20 | AMBIG | 1008 | -0.65 | -0.78 | -3.37 | 1.86 | quarter |  |  |  | 0.33 |  |
| TEST | UP8 | True | MOO_T1 | 20 | ALL | 10749 | -0.69 | -0.83 | -2.65 | 1.17 | quarter |  |  |  | 0.33 |  |
| TEST | UP3V | False | MOC_T | 1 | NEWS | 5695 | 0.07 | -0.07 | -0.18 | 0.05 | day | 0.12 | -0.10 | 0.35 | 0.33 |  |
| TEST | UP3V | False | MOC_T | 1 | NEWS_202 | 4761 | 0.08 | -0.06 | -0.17 | 0.06 | day | 0.13 | -0.09 | 0.38 | 0.33 |  |
| TEST | UP3V | False | MOC_T | 1 | NEWS_non202 | 934 | 0.02 | -0.12 | -0.61 | 0.36 | quarter | 0.07 | -0.37 | 0.50 | 0.50 |  |
| TEST | UP3V | False | MOC_T | 1 | NONEWS | 7428 | -0.05 | -0.19 | -0.38 | 0.01 | day |  |  |  | 0.42 |  |
| TEST | UP3V | False | MOC_T | 1 | AMBIG | 2046 | 0.04 | -0.10 | -0.52 | 0.35 | day |  |  |  | 0.50 |  |
| TEST | UP3V | False | MOC_T | 1 | ALL | 15169 | 0.01 | -0.13 | -0.28 | 0.01 | quarter |  |  |  | 0.25 |  |
| TEST | UP3V | False | MOC_T | 5 | NEWS | 5690 | -0.08 | -0.21 | -0.45 | 0.01 | quarter | 0.28 | -0.11 | 0.76 | 0.50 |  |
| TEST | UP3V | False | MOC_T | 5 | NEWS_202 | 4756 | -0.08 | -0.22 | -0.46 | 0.01 | quarter | 0.27 | -0.18 | 0.82 | 0.33 |  |
| TEST | UP3V | False | MOC_T | 5 | NEWS_non202 | 934 | -0.05 | -0.19 | -0.92 | 0.59 | quarter | 0.30 | -0.48 | 1.08 | 0.50 |  |
| TEST | UP3V | False | MOC_T | 5 | NONEWS | 7424 | -0.35 | -0.49 | -0.95 | -0.08 | quarter |  |  |  | 0.25 |  |
| TEST | UP3V | False | MOC_T | 5 | AMBIG | 2045 | -0.31 | -0.45 | -1.09 | 0.32 | day |  |  |  | 0.33 |  |
| TEST | UP3V | False | MOC_T | 5 | ALL | 15159 | -0.24 | -0.38 | -0.68 | -0.09 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | False | MOC_T | 20 | NEWS | 5633 | -0.16 | -0.29 | -0.89 | 0.29 | quarter | 0.87 | -0.32 | 2.02 | 0.50 |  |
| TEST | UP3V | False | MOC_T | 20 | NEWS_202 | 4708 | 0.17 | 0.03 | -0.56 | 0.63 | quarter | 1.20 | -0.15 | 2.48 | 0.75 |  |
| TEST | UP3V | False | MOC_T | 20 | NEWS_non202 | 925 | -1.80 | -1.94 | -3.64 | -0.12 | quarter | -0.78 | -2.08 | 0.67 | 0.25 |  |
| TEST | UP3V | False | MOC_T | 20 | NONEWS | 7358 | -1.03 | -1.17 | -2.57 | 0.34 | quarter |  |  |  | 0.25 |  |
| TEST | UP3V | False | MOC_T | 20 | AMBIG | 2017 | -0.94 | -1.08 | -2.41 | 0.22 | quarter |  |  |  | 0.33 |  |
| TEST | UP3V | False | MOC_T | 20 | ALL | 15008 | -0.69 | -0.83 | -1.77 | 0.20 | quarter |  |  |  | 0.25 |  |
| TEST | UP3V | False | MOO_T1 | 1 | NEWS | 5695 | 0.03 | -0.11 | -0.21 | 0.01 | quarter | 0.35 | 0.14 | 0.60 | 0.17 |  |
| TEST | UP3V | False | MOO_T1 | 1 | NEWS_202 | 4761 | 0.09 | -0.04 | -0.15 | 0.07 | quarter | 0.41 | 0.18 | 0.68 | 0.33 |  |
| TEST | UP3V | False | MOO_T1 | 1 | NEWS_non202 | 934 | -0.30 | -0.44 | -0.81 | -0.09 | quarter | 0.01 | -0.37 | 0.37 | 0.33 |  |
| TEST | UP3V | False | MOO_T1 | 1 | NONEWS | 7428 | -0.31 | -0.45 | -0.71 | -0.27 | quarter |  |  |  | 0.08 |  |
| TEST | UP3V | False | MOO_T1 | 1 | AMBIG | 2046 | -0.28 | -0.42 | -0.71 | -0.12 | day |  |  |  | 0.33 |  |
| TEST | UP3V | False | MOO_T1 | 1 | ALL | 15169 | -0.18 | -0.32 | -0.49 | -0.19 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | False | MOO_T1 | 5 | NEWS | 5690 | -0.11 | -0.25 | -0.49 | -0.03 | quarter | 0.46 | 0.04 | 0.94 | 0.42 |  |
| TEST | UP3V | False | MOO_T1 | 5 | NEWS_202 | 4756 | -0.06 | -0.20 | -0.45 | 0.04 | quarter | 0.51 | 0.03 | 1.06 | 0.50 |  |
| TEST | UP3V | False | MOO_T1 | 5 | NEWS_non202 | 934 | -0.37 | -0.51 | -1.21 | 0.22 | day | 0.20 | -0.55 | 0.94 | 0.33 |  |
| TEST | UP3V | False | MOO_T1 | 5 | NONEWS | 7424 | -0.57 | -0.71 | -1.16 | -0.29 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | False | MOO_T1 | 5 | AMBIG | 2045 | -0.59 | -0.72 | -1.29 | -0.11 | day |  |  |  | 0.17 |  |
| TEST | UP3V | False | MOO_T1 | 5 | ALL | 15159 | -0.40 | -0.54 | -0.84 | -0.26 | quarter |  |  |  | 0.08 |  |
| TEST | UP3V | False | MOO_T1 | 20 | NEWS | 5633 | -0.17 | -0.31 | -0.91 | 0.29 | quarter | 1.04 | -0.12 | 2.15 | 0.42 |  |
| TEST | UP3V | False | MOO_T1 | 20 | NEWS_202 | 4708 | 0.18 | 0.05 | -0.56 | 0.66 | quarter | 1.40 | 0.09 | 2.63 | 0.75 |  |
| TEST | UP3V | False | MOO_T1 | 20 | NEWS_non202 | 925 | -1.98 | -2.12 | -3.83 | -0.33 | quarter | -0.78 | -2.24 | 0.73 | 0.17 |  |
| TEST | UP3V | False | MOO_T1 | 20 | NONEWS | 7358 | -1.21 | -1.35 | -2.72 | 0.11 | quarter |  |  |  | 0.25 |  |
| TEST | UP3V | False | MOO_T1 | 20 | AMBIG | 2017 | -1.29 | -1.43 | -2.60 | -0.26 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | False | MOO_T1 | 20 | ALL | 15008 | -0.83 | -0.97 | -1.90 | 0.02 | quarter |  |  |  | 0.25 |  |
| TEST | UP3V | True | MOC_T | 1 | NEWS | 4879 | 0.03 | -0.11 | -0.22 | 0.01 | day | 0.13 | -0.08 | 0.34 | 0.25 |  |
| TEST | UP3V | True | MOC_T | 1 | NEWS_202 | 4173 | 0.07 | -0.07 | -0.19 | 0.05 | day | 0.17 | -0.05 | 0.41 | 0.42 |  |
| TEST | UP3V | True | MOC_T | 1 | NEWS_non202 | 706 | -0.21 | -0.35 | -0.81 | 0.03 | quarter | -0.11 | -0.51 | 0.27 | 0.33 |  |
| TEST | UP3V | True | MOC_T | 1 | NONEWS | 6412 | -0.10 | -0.24 | -0.45 | -0.06 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | True | MOC_T | 1 | AMBIG | 1693 | -0.02 | -0.15 | -0.55 | 0.19 | quarter |  |  |  | 0.58 |  |
| TEST | UP3V | True | MOC_T | 1 | ALL | 12984 | -0.04 | -0.18 | -0.32 | -0.06 | quarter |  |  |  | 0.25 |  |
| TEST | UP3V | True | MOC_T | 5 | NEWS | 4875 | -0.12 | -0.25 | -0.52 | -0.01 | quarter | 0.15 | -0.31 | 0.70 | 0.33 |  |
| TEST | UP3V | True | MOC_T | 5 | NEWS_202 | 4169 | -0.03 | -0.17 | -0.43 | 0.09 | quarter | 0.23 | -0.26 | 0.83 | 0.50 |  |
| TEST | UP3V | True | MOC_T | 5 | NEWS_non202 | 706 | -0.62 | -0.76 | -1.46 | -0.12 | quarter | -0.36 | -1.09 | 0.29 | 0.33 |  |
| TEST | UP3V | True | MOC_T | 5 | NONEWS | 6408 | -0.26 | -0.40 | -0.87 | 0.02 | quarter |  |  |  | 0.25 |  |
| TEST | UP3V | True | MOC_T | 5 | AMBIG | 1692 | -0.30 | -0.43 | -0.93 | 0.05 | day |  |  |  | 0.42 |  |
| TEST | UP3V | True | MOC_T | 5 | ALL | 12975 | -0.21 | -0.35 | -0.62 | -0.08 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | True | MOC_T | 20 | NEWS | 4823 | -0.08 | -0.21 | -0.79 | 0.37 | quarter | 0.60 | -0.60 | 1.79 | 0.67 |  |
| TEST | UP3V | True | MOC_T | 20 | NEWS_202 | 4125 | 0.21 | 0.07 | -0.52 | 0.66 | quarter | 0.89 | -0.45 | 2.18 | 0.75 |  |
| TEST | UP3V | True | MOC_T | 20 | NEWS_non202 | 698 | -1.77 | -1.92 | -3.38 | -0.55 | quarter | -1.10 | -2.36 | 0.18 | 0.25 |  |
| TEST | UP3V | True | MOC_T | 20 | NONEWS | 6350 | -0.67 | -0.81 | -2.22 | 0.64 | quarter |  |  |  | 0.33 |  |
| TEST | UP3V | True | MOC_T | 20 | AMBIG | 1669 | -0.35 | -0.49 | -1.52 | 0.51 | quarter |  |  |  | 0.33 |  |
| TEST | UP3V | True | MOC_T | 20 | ALL | 12842 | -0.41 | -0.55 | -1.43 | 0.40 | quarter |  |  |  | 0.42 |  |
| TEST | UP3V | True | MOO_T1 | 1 | NEWS | 4879 | 0.05 | -0.08 | -0.19 | 0.02 | quarter | 0.33 | 0.13 | 0.57 | 0.25 |  |
| TEST | UP3V | True | MOO_T1 | 1 | NEWS_202 | 4173 | 0.09 | -0.04 | -0.15 | 0.07 | day | 0.37 | 0.15 | 0.64 | 0.42 |  |
| TEST | UP3V | True | MOO_T1 | 1 | NEWS_non202 | 706 | -0.18 | -0.32 | -0.68 | 0.01 | quarter | 0.09 | -0.28 | 0.45 | 0.25 |  |
| TEST | UP3V | True | MOO_T1 | 1 | NONEWS | 6412 | -0.27 | -0.41 | -0.66 | -0.24 | quarter |  |  |  | 0.08 |  |
| TEST | UP3V | True | MOO_T1 | 1 | AMBIG | 1693 | -0.23 | -0.37 | -0.66 | -0.09 | quarter |  |  |  | 0.42 |  |
| TEST | UP3V | True | MOO_T1 | 1 | ALL | 12984 | -0.14 | -0.28 | -0.44 | -0.16 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | True | MOO_T1 | 5 | NEWS | 4875 | -0.10 | -0.23 | -0.50 | 0.01 | quarter | 0.32 | -0.16 | 0.88 | 0.42 |  |
| TEST | UP3V | True | MOO_T1 | 5 | NEWS_202 | 4169 | -0.01 | -0.14 | -0.42 | 0.12 | quarter | 0.41 | -0.11 | 1.01 | 0.58 |  |
| TEST | UP3V | True | MOO_T1 | 5 | NEWS_non202 | 706 | -0.62 | -0.76 | -1.40 | -0.13 | quarter | -0.21 | -0.89 | 0.45 | 0.17 |  |
| TEST | UP3V | True | MOO_T1 | 5 | NONEWS | 6408 | -0.41 | -0.55 | -1.02 | -0.13 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | True | MOO_T1 | 5 | AMBIG | 1692 | -0.48 | -0.62 | -1.10 | -0.16 | quarter |  |  |  | 0.33 |  |
| TEST | UP3V | True | MOO_T1 | 5 | ALL | 12975 | -0.30 | -0.44 | -0.71 | -0.17 | quarter |  |  |  | 0.17 |  |
| TEST | UP3V | True | MOO_T1 | 20 | NEWS | 4823 | -0.05 | -0.18 | -0.76 | 0.40 | quarter | 0.76 | -0.42 | 1.94 | 0.58 |  |
| TEST | UP3V | True | MOO_T1 | 20 | NEWS_202 | 4125 | 0.24 | 0.10 | -0.51 | 0.70 | quarter | 1.04 | -0.26 | 2.32 | 0.75 |  |
| TEST | UP3V | True | MOO_T1 | 20 | NEWS_non202 | 698 | -1.71 | -1.86 | -3.26 | -0.52 | quarter | -0.91 | -2.22 | 0.45 | 0.25 |  |
| TEST | UP3V | True | MOO_T1 | 20 | NONEWS | 6350 | -0.80 | -0.94 | -2.30 | 0.47 | quarter |  |  |  | 0.33 |  |
| TEST | UP3V | True | MOO_T1 | 20 | AMBIG | 1669 | -0.63 | -0.77 | -1.70 | 0.16 | quarter |  |  |  | 0.33 |  |
| TEST | UP3V | True | MOO_T1 | 20 | ALL | 12842 | -0.50 | -0.63 | -1.49 | 0.27 | quarter |  |  |  | 0.33 |  |
| TEST | DN8 | False | MOC_T | 1 | NEWS | 3304 | -0.23 | -0.37 | -0.61 | -0.15 | quarter | -0.41 | -0.88 | 0.07 | 0.17 | 0.10 |
| TEST | DN8 | False | MOC_T | 1 | NEWS_202 | 2448 | -0.19 | -0.33 | -0.56 | -0.10 | quarter | -0.37 | -0.91 | 0.11 | 0.17 | 0.06 |
| TEST | DN8 | False | MOC_T | 1 | NEWS_non202 | 856 | -0.36 | -0.50 | -1.09 | 0.12 | day | -0.54 | -1.25 | 0.17 | 0.50 | 0.21 |
| TEST | DN8 | False | MOC_T | 1 | NONEWS | 7740 | 0.18 | 0.04 | -0.44 | 0.57 | day |  |  |  | 0.50 | -0.32 |
| TEST | DN8 | False | MOC_T | 1 | AMBIG | 1258 | -0.13 | -0.27 | -0.98 | 0.46 | day |  |  |  | 0.50 | -0.01 |
| TEST | DN8 | False | MOC_T | 1 | ALL | 12302 | 0.04 | -0.10 | -0.46 | 0.30 | day |  |  |  | 0.25 | -0.17 |
| TEST | DN8 | False | MOC_T | 5 | NEWS | 3296 | -0.18 | -0.32 | -0.86 | 0.22 | quarter | -0.63 | -1.64 | 0.31 | 0.33 | 0.05 |
| TEST | DN8 | False | MOC_T | 5 | NEWS_202 | 2446 | -0.20 | -0.34 | -0.83 | 0.20 | quarter | -0.65 | -1.85 | 0.40 | 0.33 | 0.07 |
| TEST | DN8 | False | MOC_T | 5 | NEWS_non202 | 850 | -0.13 | -0.27 | -1.80 | 1.36 | quarter | -0.58 | -1.84 | 0.64 | 0.42 | -0.01 |
| TEST | DN8 | False | MOC_T | 5 | NONEWS | 7730 | 0.45 | 0.31 | -0.76 | 1.50 | day |  |  |  | 0.50 | -0.58 |
| TEST | DN8 | False | MOC_T | 5 | AMBIG | 1256 | 1.12 | 0.98 | -0.92 | 2.79 | quarter |  |  |  | 0.58 | -1.25 |
| TEST | DN8 | False | MOC_T | 5 | ALL | 12282 | 0.35 | 0.21 | -0.56 | 1.20 | day |  |  |  | 0.25 | -0.48 |
| TEST | DN8 | False | MOC_T | 20 | NEWS | 3252 | -1.02 | -1.16 | -2.15 | -0.18 | quarter | -0.91 | -2.82 | 1.13 | 0.33 | 0.88 |
| TEST | DN8 | False | MOC_T | 20 | NEWS_202 | 2414 | -0.56 | -0.70 | -1.58 | 0.09 | quarter | -0.46 | -2.71 | 1.91 | 0.17 | 0.43 |
| TEST | DN8 | False | MOC_T | 20 | NEWS_non202 | 838 | -2.33 | -2.48 | -4.71 | -0.12 | quarter | -2.24 | -4.41 | -0.07 | 0.25 | 2.19 |
| TEST | DN8 | False | MOC_T | 20 | NONEWS | 7628 | -0.10 | -0.24 | -2.65 | 2.18 | quarter |  |  |  | 0.25 | -0.03 |
| TEST | DN8 | False | MOC_T | 20 | AMBIG | 1244 | -0.73 | -0.86 | -4.34 | 2.40 | quarter |  |  |  | 0.17 | 0.59 |
| TEST | DN8 | False | MOC_T | 20 | ALL | 12124 | -0.41 | -0.55 | -2.55 | 1.51 | quarter |  |  |  | 0.25 | 0.28 |
| TEST | DN8 | False | MOO_T1 | 1 | NEWS | 3304 | -0.27 | -0.41 | -0.64 | -0.19 | quarter | -0.29 | -0.72 | 0.15 | 0.25 | 0.13 |
| TEST | DN8 | False | MOO_T1 | 1 | NEWS_202 | 2448 | -0.21 | -0.35 | -0.55 | -0.15 | quarter | -0.23 | -0.73 | 0.21 | 0.17 | 0.08 |
| TEST | DN8 | False | MOO_T1 | 1 | NEWS_non202 | 856 | -0.43 | -0.58 | -1.10 | -0.06 | day | -0.46 | -1.07 | 0.17 | 0.25 | 0.29 |
| TEST | DN8 | False | MOO_T1 | 1 | NONEWS | 7740 | 0.02 | -0.12 | -0.54 | 0.39 | day |  |  |  | 0.33 | -0.16 |
| TEST | DN8 | False | MOO_T1 | 1 | AMBIG | 1258 | -0.22 | -0.36 | -1.00 | 0.33 | day |  |  |  | 0.25 | 0.08 |
| TEST | DN8 | False | MOO_T1 | 1 | ALL | 12302 | -0.08 | -0.22 | -0.56 | 0.16 | day |  |  |  | 0.17 | -0.05 |
| TEST | DN8 | False | MOO_T1 | 5 | NEWS | 3296 | -0.21 | -0.35 | -0.88 | 0.19 | quarter | -0.55 | -1.55 | 0.35 | 0.25 | 0.07 |
| TEST | DN8 | False | MOO_T1 | 5 | NEWS_202 | 2446 | -0.21 | -0.35 | -0.87 | 0.18 | quarter | -0.55 | -1.76 | 0.47 | 0.42 | 0.08 |
| TEST | DN8 | False | MOO_T1 | 5 | NEWS_non202 | 850 | -0.20 | -0.34 | -1.88 | 1.27 | quarter | -0.55 | -1.73 | 0.60 | 0.42 | 0.06 |
| TEST | DN8 | False | MOO_T1 | 5 | NONEWS | 7730 | 0.34 | 0.20 | -0.82 | 1.40 | day |  |  |  | 0.42 | -0.48 |
| TEST | DN8 | False | MOO_T1 | 5 | AMBIG | 1256 | 1.19 | 1.05 | -0.70 | 3.04 | day |  |  |  | 0.58 | -1.32 |
| TEST | DN8 | False | MOO_T1 | 5 | ALL | 12282 | 0.28 | 0.14 | -0.64 | 1.13 | day |  |  |  | 0.42 | -0.42 |
| TEST | DN8 | False | MOO_T1 | 20 | NEWS | 3252 | -1.06 | -1.20 | -2.19 | -0.24 | quarter | -0.89 | -2.80 | 1.17 | 0.25 | 0.92 |
| TEST | DN8 | False | MOO_T1 | 20 | NEWS_202 | 2414 | -0.57 | -0.71 | -1.59 | 0.09 | quarter | -0.39 | -2.63 | 1.95 | 0.25 | 0.44 |
| TEST | DN8 | False | MOO_T1 | 20 | NEWS_non202 | 838 | -2.48 | -2.62 | -4.70 | -0.47 | quarter | -2.31 | -4.37 | -0.24 | 0.25 | 2.33 |
| TEST | DN8 | False | MOO_T1 | 20 | NONEWS | 7628 | -0.17 | -0.31 | -2.77 | 2.15 | quarter |  |  |  | 0.50 | 0.04 |
| TEST | DN8 | False | MOO_T1 | 20 | AMBIG | 1244 | -0.48 | -0.62 | -4.00 | 2.65 | quarter |  |  |  | 0.25 | 0.35 |
| TEST | DN8 | False | MOO_T1 | 20 | ALL | 12124 | -0.44 | -0.58 | -2.62 | 1.45 | quarter |  |  |  | 0.25 | 0.31 |
| DISC | UP8 | False | MOC_T | 1 | NEWS | 733 | -0.16 | -0.30 | -0.66 | 0.07 | day | 0.24 | -0.26 | 0.76 | 0.30 |  |
| DISC | UP8 | False | MOC_T | 1 | NEWS_202 | 554 | -0.07 | -0.22 | -0.50 | 0.07 | day | 0.32 | -0.23 | 0.88 | 0.10 |  |
| DISC | UP8 | False | MOC_T | 1 | NEWS_non202 | 179 | -0.42 | -0.56 | -1.73 | 0.68 | day | -0.02 | -1.15 | 1.13 | 0.40 |  |
| DISC | UP8 | False | MOC_T | 1 | NONEWS | 1526 | -0.39 | -0.54 | -1.08 | -0.05 | day |  |  |  | 0.00 |  |
| DISC | UP8 | False | MOC_T | 1 | AMBIG | 267 | -0.18 | -0.33 | -1.54 | 0.49 | quarter |  |  |  | 0.33 |  |
| DISC | UP8 | False | MOC_T | 1 | ALL | 2526 | -0.30 | -0.45 | -0.85 | -0.06 | day |  |  |  | 0.00 |  |
| DISC | UP8 | False | MOC_T | 5 | NEWS | 733 | -0.44 | -0.58 | -1.13 | -0.03 | day | 0.60 | -0.39 | 1.40 | 0.10 |  |
| DISC | UP8 | False | MOC_T | 5 | NEWS_202 | 554 | 0.01 | -0.14 | -0.60 | 0.30 | day | 1.04 | 0.17 | 1.93 | 0.30 |  |
| DISC | UP8 | False | MOC_T | 5 | NEWS_non202 | 179 | -1.82 | -1.96 | -3.96 | -0.17 | day | -0.78 | -3.08 | 0.99 | 0.20 |  |
| DISC | UP8 | False | MOC_T | 5 | NONEWS | 1526 | -1.03 | -1.18 | -2.01 | -0.38 | day |  |  |  | 0.00 |  |
| DISC | UP8 | False | MOC_T | 5 | AMBIG | 267 | -0.49 | -0.64 | -2.34 | 0.96 | day |  |  |  | 0.50 |  |
| DISC | UP8 | False | MOC_T | 5 | ALL | 2526 | -0.80 | -0.95 | -1.56 | -0.32 | day |  |  |  | 0.00 |  |
| DISC | UP8 | False | MOC_T | 20 | NEWS | 733 | -0.75 | -0.89 | -1.85 | 0.18 | quarter | -0.22 | -2.00 | 1.73 | 0.40 |  |
| DISC | UP8 | False | MOC_T | 20 | NEWS_202 | 554 | 0.10 | -0.04 | -0.83 | 0.81 | quarter | 0.63 | -1.25 | 2.58 | 0.60 |  |
| DISC | UP8 | False | MOC_T | 20 | NEWS_non202 | 179 | -3.37 | -3.51 | -6.46 | -0.53 | day | -2.84 | -5.97 | 0.35 | 0.40 |  |
| DISC | UP8 | False | MOC_T | 20 | NONEWS | 1526 | -0.52 | -0.67 | -2.33 | 1.05 | quarter |  |  |  | 0.20 |  |
| DISC | UP8 | False | MOC_T | 20 | AMBIG | 267 | -0.28 | -0.42 | -2.71 | 1.90 | day |  |  |  | 0.33 |  |
| DISC | UP8 | False | MOC_T | 20 | ALL | 2526 | -0.56 | -0.71 | -1.78 | 0.52 | quarter |  |  |  | 0.20 |  |
| DISC | UP8 | False | MOO_T1 | 1 | NEWS | 733 | -0.04 | -0.18 | -0.49 | 0.11 | day | 0.48 | 0.02 | 0.96 | 0.30 |  |
| DISC | UP8 | False | MOO_T1 | 1 | NEWS_202 | 554 | 0.21 | 0.06 | -0.19 | 0.30 | day | 0.73 | 0.25 | 1.21 | 0.50 |  |
| DISC | UP8 | False | MOO_T1 | 1 | NEWS_non202 | 179 | -0.80 | -0.94 | -1.73 | 0.12 | quarter | -0.28 | -1.10 | 0.78 | 0.00 |  |
| DISC | UP8 | False | MOO_T1 | 1 | NONEWS | 1526 | -0.52 | -0.66 | -1.13 | -0.23 | day |  |  |  | 0.00 |  |
| DISC | UP8 | False | MOO_T1 | 1 | AMBIG | 267 | -0.40 | -0.55 | -1.42 | 0.35 | day |  |  |  | 0.50 |  |
| DISC | UP8 | False | MOO_T1 | 1 | ALL | 2526 | -0.37 | -0.51 | -0.86 | -0.18 | day |  |  |  | 0.00 |  |
| DISC | UP8 | False | MOO_T1 | 5 | NEWS | 733 | -0.28 | -0.43 | -1.00 | 0.13 | day | 0.88 | 0.02 | 1.75 | 0.20 |  |
| DISC | UP8 | False | MOO_T1 | 5 | NEWS_202 | 554 | 0.30 | 0.15 | -0.28 | 0.59 | day | 1.46 | 0.58 | 2.33 | 0.70 |  |
| DISC | UP8 | False | MOO_T1 | 5 | NEWS_non202 | 179 | -2.08 | -2.22 | -4.19 | -0.42 | day | -0.91 | -2.79 | 0.94 | 0.00 |  |
| DISC | UP8 | False | MOO_T1 | 5 | NONEWS | 1526 | -1.16 | -1.31 | -2.10 | -0.51 | day |  |  |  | 0.00 |  |
| DISC | UP8 | False | MOO_T1 | 5 | AMBIG | 267 | -0.69 | -0.83 | -2.52 | 0.74 | day |  |  |  | 0.50 |  |
| DISC | UP8 | False | MOO_T1 | 5 | ALL | 2526 | -0.86 | -1.00 | -1.62 | -0.38 | day |  |  |  | 0.00 |  |
| DISC | UP8 | False | MOO_T1 | 20 | NEWS | 733 | -0.58 | -0.72 | -1.64 | 0.29 | quarter | 0.09 | -1.62 | 2.09 | 0.40 |  |
| DISC | UP8 | False | MOO_T1 | 20 | NEWS_202 | 554 | 0.40 | 0.25 | -0.56 | 1.10 | quarter | 1.06 | -0.79 | 3.19 | 0.60 |  |
| DISC | UP8 | False | MOO_T1 | 20 | NEWS_non202 | 179 | -3.58 | -3.72 | -6.62 | -0.77 | day | -2.91 | -6.00 | 0.15 | 0.40 |  |
| DISC | UP8 | False | MOO_T1 | 20 | NONEWS | 1526 | -0.66 | -0.81 | -2.64 | 0.99 | quarter |  |  |  | 0.20 |  |
| DISC | UP8 | False | MOO_T1 | 20 | AMBIG | 267 | -0.46 | -0.60 | -2.85 | 1.64 | day |  |  |  | 0.33 |  |
| DISC | UP8 | False | MOO_T1 | 20 | ALL | 2526 | -0.61 | -0.76 | -1.87 | 0.45 | quarter |  |  |  | 0.20 |  |
| DISC | UP8 | True | MOC_T | 1 | NEWS | 557 | -0.06 | -0.20 | -0.55 | 0.19 | day | 0.42 | -0.09 | 0.93 | 0.30 |  |
| DISC | UP8 | True | MOC_T | 1 | NEWS_202 | 441 | -0.04 | -0.19 | -0.47 | 0.12 | day | 0.43 | -0.10 | 1.03 | 0.40 |  |
| DISC | UP8 | True | MOC_T | 1 | NEWS_non202 | 116 | -0.12 | -0.26 | -1.48 | 1.38 | quarter | 0.36 | -0.96 | 1.92 | 0.25 |  |
| DISC | UP8 | True | MOC_T | 1 | NONEWS | 1224 | -0.47 | -0.62 | -1.10 | -0.15 | day |  |  |  | 0.10 |  |
| DISC | UP8 | True | MOC_T | 1 | AMBIG | 186 | 0.10 | -0.06 | -1.19 | 1.06 | day |  |  |  | 0.50 |  |
| DISC | UP8 | True | MOC_T | 1 | ALL | 1967 | -0.30 | -0.45 | -0.81 | -0.08 | day |  |  |  | 0.00 |  |
| DISC | UP8 | True | MOC_T | 5 | NEWS | 557 | -0.39 | -0.53 | -1.10 | 0.02 | day | 0.66 | -0.26 | 1.54 | 0.10 |  |
| DISC | UP8 | True | MOC_T | 5 | NEWS_202 | 441 | 0.12 | -0.02 | -0.50 | 0.50 | day | 1.18 | 0.32 | 2.11 | 0.50 |  |
| DISC | UP8 | True | MOC_T | 5 | NEWS_non202 | 116 | -2.34 | -2.48 | -4.52 | -0.54 | day | -1.29 | -3.65 | 0.42 | 0.00 |  |
| DISC | UP8 | True | MOC_T | 5 | NONEWS | 1224 | -1.05 | -1.20 | -1.98 | -0.44 | day |  |  |  | 0.10 |  |
| DISC | UP8 | True | MOC_T | 5 | AMBIG | 186 | -0.03 | -0.18 | -1.96 | 1.63 | day |  |  |  | 0.33 |  |
| DISC | UP8 | True | MOC_T | 5 | ALL | 1967 | -0.77 | -0.91 | -1.47 | -0.33 | day |  |  |  | 0.00 |  |
| DISC | UP8 | True | MOC_T | 20 | NEWS | 557 | -0.70 | -0.85 | -1.74 | 0.14 | quarter | 0.22 | -1.77 | 2.45 | 0.40 |  |
| DISC | UP8 | True | MOC_T | 20 | NEWS_202 | 441 | 0.07 | -0.08 | -0.91 | 0.83 | quarter | 1.00 | -1.14 | 3.37 | 0.60 |  |
| DISC | UP8 | True | MOC_T | 20 | NEWS_non202 | 116 | -3.64 | -3.78 | -7.00 | -0.59 | day | -2.71 | -5.97 | 0.63 | 0.25 |  |
| DISC | UP8 | True | MOC_T | 20 | NONEWS | 1224 | -0.93 | -1.07 | -2.87 | 0.76 | quarter |  |  |  | 0.20 |  |
| DISC | UP8 | True | MOC_T | 20 | AMBIG | 186 | 0.04 | -0.11 | -2.66 | 2.59 | day |  |  |  | 0.33 |  |
| DISC | UP8 | True | MOC_T | 20 | ALL | 1967 | -0.77 | -0.92 | -1.90 | 0.22 | quarter |  |  |  | 0.20 |  |
| DISC | UP8 | True | MOO_T1 | 1 | NEWS | 557 | 0.05 | -0.09 | -0.40 | 0.18 | day | 0.60 | 0.14 | 1.06 | 0.50 |  |
| DISC | UP8 | True | MOO_T1 | 1 | NEWS_202 | 441 | 0.27 | 0.13 | -0.13 | 0.40 | day | 0.82 | 0.34 | 1.35 | 0.60 |  |
| DISC | UP8 | True | MOO_T1 | 1 | NEWS_non202 | 116 | -0.80 | -0.95 | -1.86 | -0.11 | day | -0.25 | -1.17 | 0.63 | 0.00 |  |
| DISC | UP8 | True | MOO_T1 | 1 | NONEWS | 1224 | -0.55 | -0.69 | -1.14 | -0.27 | day |  |  |  | 0.00 |  |
| DISC | UP8 | True | MOO_T1 | 1 | AMBIG | 186 | 0.15 | -0.01 | -0.90 | 0.86 | day |  |  |  | 0.50 |  |
| DISC | UP8 | True | MOO_T1 | 1 | ALL | 1967 | -0.31 | -0.46 | -0.78 | -0.13 | day |  |  |  | 0.00 |  |
| DISC | UP8 | True | MOO_T1 | 5 | NEWS | 557 | -0.23 | -0.38 | -0.96 | 0.19 | day | 0.89 | 0.01 | 1.73 | 0.20 |  |
| DISC | UP8 | True | MOO_T1 | 5 | NEWS_202 | 441 | 0.44 | 0.29 | -0.17 | 0.79 | day | 1.56 | 0.69 | 2.45 | 0.80 |  |
| DISC | UP8 | True | MOO_T1 | 5 | NEWS_non202 | 116 | -2.79 | -2.94 | -5.22 | -0.91 | day | -1.67 | -3.83 | 0.35 | 0.00 |  |
| DISC | UP8 | True | MOO_T1 | 5 | NONEWS | 1224 | -1.12 | -1.27 | -2.02 | -0.53 | day |  |  |  | 0.10 |  |
| DISC | UP8 | True | MOO_T1 | 5 | AMBIG | 186 | 0.10 | -0.05 | -1.73 | 1.59 | day |  |  |  | 0.33 |  |
| DISC | UP8 | True | MOO_T1 | 5 | ALL | 1967 | -0.76 | -0.90 | -1.46 | -0.32 | day |  |  |  | 0.00 |  |
| DISC | UP8 | True | MOO_T1 | 20 | NEWS | 557 | -0.53 | -0.67 | -1.53 | 0.33 | quarter | 0.45 | -1.53 | 2.71 | 0.40 |  |
| DISC | UP8 | True | MOO_T1 | 20 | NEWS_202 | 441 | 0.38 | 0.23 | -0.59 | 1.11 | quarter | 1.35 | -0.74 | 3.82 | 0.60 |  |
| DISC | UP8 | True | MOO_T1 | 20 | NEWS_non202 | 116 | -3.96 | -4.10 | -7.45 | -0.74 | day | -2.98 | -6.29 | 0.43 | 0.25 |  |
| DISC | UP8 | True | MOO_T1 | 20 | NONEWS | 1224 | -0.98 | -1.12 | -3.05 | 0.71 | quarter |  |  |  | 0.20 |  |
| DISC | UP8 | True | MOO_T1 | 20 | AMBIG | 186 | 0.15 | 0.00 | -2.52 | 2.61 | day |  |  |  | 0.50 |  |
| DISC | UP8 | True | MOO_T1 | 20 | ALL | 1967 | -0.74 | -0.89 | -1.99 | 0.34 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | False | MOC_T | 1 | NEWS | 1835 | -0.12 | -0.26 | -0.39 | -0.13 | quarter | -0.09 | -0.31 | 0.15 | 0.00 |  |
| DISC | UP3V | False | MOC_T | 1 | NEWS_202 | 1470 | -0.07 | -0.21 | -0.35 | -0.07 | quarter | -0.04 | -0.29 | 0.19 | 0.10 |  |
| DISC | UP3V | False | MOC_T | 1 | NEWS_non202 | 365 | -0.31 | -0.45 | -0.86 | -0.05 | day | -0.28 | -0.69 | 0.14 | 0.20 |  |
| DISC | UP3V | False | MOC_T | 1 | NONEWS | 2622 | -0.03 | -0.17 | -0.38 | 0.02 | day |  |  |  | 0.20 |  |
| DISC | UP3V | False | MOC_T | 1 | AMBIG | 561 | -0.19 | -0.33 | -0.72 | 0.06 | day |  |  |  | 0.30 |  |
| DISC | UP3V | False | MOC_T | 1 | ALL | 5018 | -0.08 | -0.22 | -0.35 | -0.09 | day |  |  |  | 0.00 |  |
| DISC | UP3V | False | MOC_T | 5 | NEWS | 1835 | -0.03 | -0.17 | -0.41 | 0.08 | quarter | 0.22 | -0.16 | 0.64 | 0.30 |  |
| DISC | UP3V | False | MOC_T | 5 | NEWS_202 | 1470 | 0.04 | -0.10 | -0.33 | 0.12 | quarter | 0.28 | -0.09 | 0.66 | 0.30 |  |
| DISC | UP3V | False | MOC_T | 5 | NEWS_non202 | 365 | -0.28 | -0.43 | -1.14 | 0.35 | quarter | -0.04 | -0.87 | 0.84 | 0.30 |  |
| DISC | UP3V | False | MOC_T | 5 | NONEWS | 2622 | -0.24 | -0.39 | -0.73 | -0.05 | quarter |  |  |  | 0.30 |  |
| DISC | UP3V | False | MOC_T | 5 | AMBIG | 561 | -0.53 | -0.67 | -1.24 | -0.09 | quarter |  |  |  | 0.30 |  |
| DISC | UP3V | False | MOC_T | 5 | ALL | 5018 | -0.19 | -0.34 | -0.57 | -0.11 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | False | MOC_T | 20 | NEWS | 1834 | 0.14 | -0.00 | -0.51 | 0.49 | quarter | 0.46 | -0.27 | 1.12 | 0.70 |  |
| DISC | UP3V | False | MOC_T | 20 | NEWS_202 | 1470 | 0.23 | 0.09 | -0.38 | 0.52 | quarter | 0.55 | -0.14 | 1.19 | 0.70 |  |
| DISC | UP3V | False | MOC_T | 20 | NEWS_non202 | 364 | -0.22 | -0.37 | -1.56 | 0.85 | quarter | 0.09 | -1.19 | 1.40 | 0.50 |  |
| DISC | UP3V | False | MOC_T | 20 | NONEWS | 2620 | -0.31 | -0.46 | -1.01 | 0.10 | quarter |  |  |  | 0.30 |  |
| DISC | UP3V | False | MOC_T | 20 | AMBIG | 561 | -0.28 | -0.42 | -1.53 | 0.73 | quarter |  |  |  | 0.40 |  |
| DISC | UP3V | False | MOC_T | 20 | ALL | 5015 | -0.14 | -0.29 | -0.71 | 0.12 | quarter |  |  |  | 0.40 |  |
| DISC | UP3V | False | MOO_T1 | 1 | NEWS | 1835 | -0.01 | -0.15 | -0.26 | -0.04 | day | 0.15 | -0.03 | 0.32 | 0.10 |  |
| DISC | UP3V | False | MOO_T1 | 1 | NEWS_202 | 1470 | 0.07 | -0.07 | -0.18 | 0.05 | quarter | 0.22 | 0.04 | 0.40 | 0.30 |  |
| DISC | UP3V | False | MOO_T1 | 1 | NEWS_non202 | 365 | -0.32 | -0.47 | -0.78 | -0.17 | quarter | -0.17 | -0.54 | 0.18 | 0.20 |  |
| DISC | UP3V | False | MOO_T1 | 1 | NONEWS | 2622 | -0.15 | -0.29 | -0.44 | -0.16 | day |  |  |  | 0.00 |  |
| DISC | UP3V | False | MOO_T1 | 1 | AMBIG | 561 | -0.08 | -0.23 | -0.55 | 0.07 | day |  |  |  | 0.40 |  |
| DISC | UP3V | False | MOO_T1 | 1 | ALL | 5018 | -0.09 | -0.23 | -0.34 | -0.14 | day |  |  |  | 0.00 |  |
| DISC | UP3V | False | MOO_T1 | 5 | NEWS | 1835 | 0.09 | -0.05 | -0.29 | 0.18 | quarter | 0.44 | 0.05 | 0.86 | 0.40 |  |
| DISC | UP3V | False | MOO_T1 | 5 | NEWS_202 | 1470 | 0.19 | 0.04 | -0.18 | 0.27 | quarter | 0.54 | 0.15 | 0.93 | 0.60 |  |
| DISC | UP3V | False | MOO_T1 | 5 | NEWS_non202 | 365 | -0.31 | -0.45 | -1.08 | 0.23 | quarter | 0.04 | -0.72 | 0.86 | 0.20 |  |
| DISC | UP3V | False | MOO_T1 | 5 | NONEWS | 2622 | -0.34 | -0.49 | -0.85 | -0.14 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | False | MOO_T1 | 5 | AMBIG | 561 | -0.40 | -0.54 | -1.07 | 0.02 | day |  |  |  | 0.40 |  |
| DISC | UP3V | False | MOO_T1 | 5 | ALL | 5018 | -0.19 | -0.34 | -0.57 | -0.10 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | False | MOO_T1 | 20 | NEWS | 1834 | 0.26 | 0.12 | -0.38 | 0.59 | quarter | 0.68 | -0.02 | 1.33 | 0.80 |  |
| DISC | UP3V | False | MOO_T1 | 20 | NEWS_202 | 1470 | 0.38 | 0.24 | -0.23 | 0.67 | quarter | 0.80 | 0.12 | 1.45 | 0.80 |  |
| DISC | UP3V | False | MOO_T1 | 20 | NEWS_non202 | 364 | -0.23 | -0.37 | -1.53 | 0.76 | day | 0.19 | -1.04 | 1.40 | 0.50 |  |
| DISC | UP3V | False | MOO_T1 | 20 | NONEWS | 2620 | -0.42 | -0.57 | -1.12 | 0.00 | quarter |  |  |  | 0.10 |  |
| DISC | UP3V | False | MOO_T1 | 20 | AMBIG | 561 | -0.19 | -0.34 | -1.36 | 0.77 | quarter |  |  |  | 0.50 |  |
| DISC | UP3V | False | MOO_T1 | 20 | ALL | 5015 | -0.15 | -0.29 | -0.70 | 0.11 | quarter |  |  |  | 0.40 |  |
| DISC | UP3V | True | MOC_T | 1 | NEWS | 1513 | -0.10 | -0.24 | -0.38 | -0.11 | quarter | -0.07 | -0.29 | 0.14 | 0.10 |  |
| DISC | UP3V | True | MOC_T | 1 | NEWS_202 | 1224 | -0.06 | -0.20 | -0.35 | -0.05 | quarter | -0.03 | -0.24 | 0.19 | 0.10 |  |
| DISC | UP3V | True | MOC_T | 1 | NEWS_non202 | 289 | -0.28 | -0.43 | -0.80 | -0.05 | day | -0.25 | -0.65 | 0.14 | 0.20 |  |
| DISC | UP3V | True | MOC_T | 1 | NONEWS | 2307 | -0.03 | -0.17 | -0.36 | 0.00 | day |  |  |  | 0.30 |  |
| DISC | UP3V | True | MOC_T | 1 | AMBIG | 461 | -0.24 | -0.39 | -0.77 | -0.05 | day |  |  |  | 0.10 |  |
| DISC | UP3V | True | MOC_T | 1 | ALL | 4281 | -0.08 | -0.22 | -0.35 | -0.11 | day |  |  |  | 0.00 |  |
| DISC | UP3V | True | MOC_T | 5 | NEWS | 1513 | -0.06 | -0.20 | -0.46 | 0.07 | quarter | 0.13 | -0.28 | 0.55 | 0.20 |  |
| DISC | UP3V | True | MOC_T | 5 | NEWS_202 | 1224 | 0.03 | -0.12 | -0.39 | 0.15 | quarter | 0.22 | -0.19 | 0.62 | 0.20 |  |
| DISC | UP3V | True | MOC_T | 5 | NEWS_non202 | 289 | -0.40 | -0.55 | -1.19 | 0.05 | day | -0.22 | -0.95 | 0.52 | 0.30 |  |
| DISC | UP3V | True | MOC_T | 5 | NONEWS | 2307 | -0.19 | -0.33 | -0.62 | -0.02 | quarter |  |  |  | 0.30 |  |
| DISC | UP3V | True | MOC_T | 5 | AMBIG | 461 | -0.60 | -0.74 | -1.35 | -0.14 | quarter |  |  |  | 0.40 |  |
| DISC | UP3V | True | MOC_T | 5 | ALL | 4281 | -0.18 | -0.33 | -0.54 | -0.12 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | True | MOC_T | 20 | NEWS | 1512 | 0.00 | -0.14 | -0.60 | 0.28 | quarter | 0.46 | -0.28 | 1.12 | 0.50 |  |
| DISC | UP3V | True | MOC_T | 20 | NEWS_202 | 1224 | 0.17 | 0.03 | -0.46 | 0.46 | quarter | 0.63 | -0.14 | 1.31 | 0.60 |  |
| DISC | UP3V | True | MOC_T | 20 | NEWS_non202 | 288 | -0.73 | -0.87 | -1.95 | 0.19 | day | -0.27 | -1.43 | 0.94 | 0.30 |  |
| DISC | UP3V | True | MOC_T | 20 | NONEWS | 2305 | -0.45 | -0.60 | -1.16 | -0.05 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | True | MOC_T | 20 | AMBIG | 461 | -0.30 | -0.44 | -1.48 | 0.55 | quarter |  |  |  | 0.50 |  |
| DISC | UP3V | True | MOC_T | 20 | ALL | 4278 | -0.28 | -0.42 | -0.81 | -0.05 | quarter |  |  |  | 0.10 |  |
| DISC | UP3V | True | MOO_T1 | 1 | NEWS | 1513 | 0.01 | -0.13 | -0.24 | -0.02 | day | 0.13 | -0.04 | 0.31 | 0.20 |  |
| DISC | UP3V | True | MOO_T1 | 1 | NEWS_202 | 1224 | 0.08 | -0.06 | -0.17 | 0.07 | day | 0.21 | 0.02 | 0.40 | 0.30 |  |
| DISC | UP3V | True | MOO_T1 | 1 | NEWS_non202 | 289 | -0.30 | -0.45 | -0.73 | -0.16 | day | -0.18 | -0.48 | 0.13 | 0.20 |  |
| DISC | UP3V | True | MOO_T1 | 1 | NONEWS | 2307 | -0.12 | -0.26 | -0.40 | -0.13 | day |  |  |  | 0.00 |  |
| DISC | UP3V | True | MOO_T1 | 1 | AMBIG | 461 | 0.01 | -0.14 | -0.43 | 0.16 | quarter |  |  |  | 0.40 |  |
| DISC | UP3V | True | MOO_T1 | 1 | ALL | 4281 | -0.06 | -0.20 | -0.30 | -0.11 | day |  |  |  | 0.00 |  |
| DISC | UP3V | True | MOO_T1 | 5 | NEWS | 1513 | 0.06 | -0.08 | -0.34 | 0.18 | quarter | 0.33 | -0.11 | 0.75 | 0.30 |  |
| DISC | UP3V | True | MOO_T1 | 5 | NEWS_202 | 1224 | 0.17 | 0.03 | -0.23 | 0.30 | quarter | 0.44 | 0.03 | 0.86 | 0.40 |  |
| DISC | UP3V | True | MOO_T1 | 5 | NEWS_non202 | 289 | -0.41 | -0.56 | -1.18 | 0.03 | day | -0.15 | -0.89 | 0.56 | 0.20 |  |
| DISC | UP3V | True | MOO_T1 | 5 | NONEWS | 2307 | -0.26 | -0.41 | -0.70 | -0.08 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | True | MOO_T1 | 5 | AMBIG | 461 | -0.30 | -0.45 | -1.03 | 0.16 | quarter |  |  |  | 0.40 |  |
| DISC | UP3V | True | MOO_T1 | 5 | ALL | 4281 | -0.15 | -0.30 | -0.51 | -0.08 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | True | MOO_T1 | 20 | NEWS | 1512 | 0.12 | -0.02 | -0.47 | 0.39 | quarter | 0.64 | -0.10 | 1.31 | 0.50 |  |
| DISC | UP3V | True | MOO_T1 | 20 | NEWS_202 | 1224 | 0.31 | 0.17 | -0.30 | 0.59 | quarter | 0.84 | 0.07 | 1.52 | 0.70 |  |
| DISC | UP3V | True | MOO_T1 | 20 | NEWS_non202 | 288 | -0.71 | -0.86 | -1.96 | 0.20 | day | -0.19 | -1.36 | 1.00 | 0.20 |  |
| DISC | UP3V | True | MOO_T1 | 20 | NONEWS | 2305 | -0.52 | -0.67 | -1.23 | -0.10 | quarter |  |  |  | 0.20 |  |
| DISC | UP3V | True | MOO_T1 | 20 | AMBIG | 461 | -0.04 | -0.19 | -1.18 | 0.79 | quarter |  |  |  | 0.60 |  |
| DISC | UP3V | True | MOO_T1 | 20 | ALL | 4278 | -0.24 | -0.39 | -0.77 | -0.01 | quarter |  |  |  | 0.20 |  |
| DISC | DN8 | False | MOC_T | 1 | NEWS | 646 | -0.16 | -0.30 | -0.70 | 0.10 | day | -1.18 | -1.80 | -0.53 | 0.30 | 0.01 |
| DISC | DN8 | False | MOC_T | 1 | NEWS_202 | 473 | -0.39 | -0.53 | -0.89 | -0.19 | day | -1.41 | -2.18 | -0.62 | 0.40 | 0.25 |
| DISC | DN8 | False | MOC_T | 1 | NEWS_non202 | 173 | 0.48 | 0.33 | -0.78 | 1.48 | day | -0.55 | -1.53 | 0.48 | 0.50 | -0.62 |
| DISC | DN8 | False | MOC_T | 1 | NONEWS | 1336 | 1.02 | 0.88 | 0.22 | 1.58 | day |  |  |  | 0.60 | -1.17 |
| DISC | DN8 | False | MOC_T | 1 | AMBIG | 264 | 0.86 | 0.71 | -0.29 | 1.74 | day |  |  |  | 0.67 | -1.00 |
| DISC | DN8 | False | MOC_T | 1 | ALL | 2246 | 0.66 | 0.52 | 0.00 | 1.02 | day |  |  |  | 0.50 | -0.81 |
| DISC | DN8 | False | MOC_T | 5 | NEWS | 646 | 0.27 | 0.13 | -0.72 | 0.92 | quarter | -1.69 | -3.07 | -0.38 | 0.50 | -0.41 |
| DISC | DN8 | False | MOC_T | 5 | NEWS_202 | 473 | -0.18 | -0.32 | -1.03 | 0.33 | quarter | -2.14 | -3.55 | -0.83 | 0.40 | 0.04 |
| DISC | DN8 | False | MOC_T | 5 | NEWS_non202 | 173 | 1.52 | 1.37 | -1.04 | 3.43 | quarter | -0.45 | -2.77 | 1.80 | 0.50 | -1.66 |
| DISC | DN8 | False | MOC_T | 5 | NONEWS | 1336 | 1.96 | 1.82 | 0.49 | 3.29 | day |  |  |  | 0.50 | -2.10 |
| DISC | DN8 | False | MOC_T | 5 | AMBIG | 264 | 1.24 | 1.09 | -1.08 | 2.58 | quarter |  |  |  | 0.50 | -1.38 |
| DISC | DN8 | False | MOC_T | 5 | ALL | 2246 | 1.39 | 1.25 | 0.26 | 2.20 | day |  |  |  | 0.60 | -1.53 |
| DISC | DN8 | False | MOC_T | 20 | NEWS | 645 | 0.48 | 0.34 | -0.88 | 1.62 | day | -1.19 | -3.40 | 0.90 | 0.50 | -0.62 |
| DISC | DN8 | False | MOC_T | 20 | NEWS_202 | 473 | 0.12 | -0.02 | -1.13 | 1.13 | day | -1.55 | -3.89 | 0.67 | 0.60 | -0.26 |
| DISC | DN8 | False | MOC_T | 20 | NEWS_non202 | 172 | 1.48 | 1.33 | -1.79 | 4.57 | day | -0.20 | -3.45 | 2.83 | 0.75 | -1.62 |
| DISC | DN8 | False | MOC_T | 20 | NONEWS | 1336 | 1.68 | 1.53 | -0.99 | 3.65 | quarter |  |  |  | 0.40 | -1.82 |
| DISC | DN8 | False | MOC_T | 20 | AMBIG | 264 | 0.30 | 0.16 | -2.67 | 3.10 | day |  |  |  | 0.50 | -0.45 |
| DISC | DN8 | False | MOC_T | 20 | ALL | 2245 | 1.17 | 1.03 | -0.63 | 2.38 | quarter |  |  |  | 0.30 | -1.31 |
| DISC | DN8 | False | MOO_T1 | 1 | NEWS | 646 | -0.29 | -0.44 | -0.81 | -0.04 | day | -0.94 | -1.52 | -0.35 | 0.10 | 0.15 |
| DISC | DN8 | False | MOO_T1 | 1 | NEWS_202 | 473 | -0.50 | -0.64 | -0.97 | -0.32 | day | -1.15 | -1.80 | -0.46 | 0.10 | 0.36 |
| DISC | DN8 | False | MOO_T1 | 1 | NEWS_non202 | 173 | 0.27 | 0.12 | -0.88 | 1.10 | day | -0.38 | -1.35 | 0.57 | 0.75 | -0.41 |
| DISC | DN8 | False | MOO_T1 | 1 | NONEWS | 1336 | 0.65 | 0.51 | -0.10 | 1.09 | day |  |  |  | 0.50 | -0.79 |
| DISC | DN8 | False | MOO_T1 | 1 | AMBIG | 264 | 0.15 | 0.00 | -0.92 | 1.01 | day |  |  |  | 0.67 | -0.30 |
| DISC | DN8 | False | MOO_T1 | 1 | ALL | 2246 | 0.32 | 0.18 | -0.32 | 0.67 | day |  |  |  | 0.40 | -0.46 |
| DISC | DN8 | False | MOO_T1 | 5 | NEWS | 646 | 0.16 | 0.01 | -0.84 | 0.87 | quarter | -1.53 | -2.99 | -0.18 | 0.50 | -0.30 |
| DISC | DN8 | False | MOO_T1 | 5 | NEWS_202 | 473 | -0.29 | -0.43 | -1.15 | 0.24 | quarter | -1.98 | -3.36 | -0.60 | 0.30 | 0.15 |
| DISC | DN8 | False | MOO_T1 | 5 | NEWS_non202 | 173 | 1.38 | 1.24 | -0.95 | 3.45 | day | -0.31 | -2.67 | 2.04 | 0.75 | -1.53 |
| DISC | DN8 | False | MOO_T1 | 5 | NONEWS | 1336 | 1.69 | 1.55 | 0.21 | 2.98 | day |  |  |  | 0.60 | -1.83 |
| DISC | DN8 | False | MOO_T1 | 5 | AMBIG | 264 | 0.62 | 0.48 | -1.31 | 2.37 | day |  |  |  | 0.50 | -0.77 |
| DISC | DN8 | False | MOO_T1 | 5 | ALL | 2246 | 1.12 | 0.98 | -0.02 | 1.98 | day |  |  |  | 0.60 | -1.27 |
| DISC | DN8 | False | MOO_T1 | 20 | NEWS | 645 | 0.39 | 0.25 | -0.96 | 1.52 | day | -0.96 | -3.11 | 1.13 | 0.50 | -0.54 |
| DISC | DN8 | False | MOO_T1 | 20 | NEWS_202 | 473 | 0.03 | -0.11 | -1.22 | 1.06 | day | -1.32 | -3.64 | 0.84 | 0.60 | -0.17 |
| DISC | DN8 | False | MOO_T1 | 20 | NEWS_non202 | 172 | 1.39 | 1.25 | -1.85 | 4.41 | day | 0.03 | -3.04 | 3.05 | 0.50 | -1.54 |
| DISC | DN8 | False | MOO_T1 | 20 | NONEWS | 1336 | 1.36 | 1.22 | -1.18 | 3.52 | quarter |  |  |  | 0.40 | -1.50 |
| DISC | DN8 | False | MOO_T1 | 20 | AMBIG | 264 | -0.15 | -0.29 | -3.21 | 2.82 | day |  |  |  | 0.50 | 0.00 |
| DISC | DN8 | False | MOO_T1 | 20 | ALL | 2245 | 0.90 | 0.76 | -0.87 | 2.34 | quarter |  |  |  | 0.40 | -1.05 |

## 4. N5 exclusion effect (TEST, UP8, MOC_T): with vs without

| arm | horizon | n_all | mean_all | lo_all | hi_all | n_excl | mean_excl | lo_excl | hi_excl |
|---|---|---|---|---|---|---|---|---|---|
| NEWS | 1 | 3169.00 | 0.12 | -0.10 | 0.38 | 2553.00 | 0.03 | -0.19 | 0.25 |
| NEWS | 5 | 3164.00 | -0.22 | -0.68 | 0.22 | 2549.00 | -0.06 | -0.50 | 0.40 |
| NEWS | 20 | 3131.00 | -0.18 | -1.31 | 1.06 | 2520.00 | 0.45 | -0.70 | 1.71 |
| NEWS_202 | 1 | 2328.00 | 0.09 | -0.13 | 0.30 | 1997.00 | 0.07 | -0.14 | 0.29 |
| NEWS_202 | 5 | 2323.00 | -0.18 | -0.56 | 0.19 | 1993.00 | -0.06 | -0.46 | 0.37 |
| NEWS_202 | 20 | 2297.00 | 0.31 | -0.70 | 1.35 | 1970.00 | 0.55 | -0.40 | 1.56 |
| NEWS_non202 | 1 | 841.00 | 0.20 | -0.61 | 0.99 | 556.00 | -0.14 | -0.83 | 0.44 |
| NEWS_non202 | 5 | 841.00 | -0.32 | -1.47 | 0.80 | 556.00 | -0.08 | -1.34 | 1.19 |
| NEWS_non202 | 20 | 834.00 | -1.54 | -4.47 | 1.58 | 550.00 | 0.12 | -2.95 | 3.17 |
| NONEWS | 1 | 9262.00 | 0.00 | -0.26 | 0.28 | 7332.00 | -0.02 | -0.29 | 0.26 |
| NONEWS | 5 | 9260.00 | -0.82 | -1.49 | -0.10 | 7330.00 | -0.57 | -1.22 | 0.14 |
| NONEWS | 20 | 9124.00 | -1.59 | -3.82 | 0.70 | 7221.00 | -0.93 | -3.29 | 1.49 |

## 5. Regime: 2020-2024 vs other TEST years (MOC_T)

| row | regime | horizon | arm | n | mean | lo | hi | years_pos |
|---|---|---|---|---|---|---|---|---|
| UP8 | 2020-2024 | 5 | NEWS | 1467 | -0.09 | -0.85 | 0.57 | 0.60 |
| UP8 | 2020-2024 | 5 | NONEWS | 4982 | -0.67 | -1.64 | 0.35 | 0.20 |
| UP8 | 2020-2024 | 5 | NEWS-NONEWS | 6449 | 0.58 | -0.49 | 1.68 |  |
| UP8 | 2020-2024 | 20 | NEWS | 1453 | -0.11 | -2.05 | 1.78 | 0.60 |
| UP8 | 2020-2024 | 20 | NONEWS | 4960 | -1.40 | -4.43 | 1.10 | 0.20 |
| UP8 | 2020-2024 | 20 | NEWS-NONEWS | 6413 | 1.30 | -0.43 | 3.67 |  |
| UP8 | other | 5 | NEWS | 1697 | -0.34 | -0.83 | 0.25 | 0.57 |
| UP8 | other | 5 | NONEWS | 4278 | -0.99 | -1.97 | 0.01 | 0.29 |
| UP8 | other | 5 | NEWS-NONEWS | 5975 | 0.66 | -0.25 | 1.55 |  |
| UP8 | other | 20 | NEWS | 1678 | -0.25 | -1.51 | 1.28 | 0.71 |
| UP8 | other | 20 | NONEWS | 4164 | -1.80 | -5.00 | 2.40 | 0.14 |
| UP8 | other | 20 | NEWS-NONEWS | 5842 | 1.56 | -1.45 | 4.08 |  |
| DN8 | 2020-2024 | 5 | NEWS | 1457 | -0.04 | -0.82 | 0.74 | 0.40 |
| DN8 | 2020-2024 | 5 | NONEWS | 4124 | 0.25 | -1.44 | 2.26 | 0.20 |
| DN8 | 2020-2024 | 5 | NEWS-NONEWS | 5581 | -0.29 | -1.89 | 1.20 |  |
| DN8 | 2020-2024 | 20 | NEWS | 1438 | -1.54 | -3.54 | 0.29 | 0.20 |
| DN8 | 2020-2024 | 20 | NONEWS | 4114 | -0.32 | -4.61 | 3.23 | 0.20 |
| DN8 | 2020-2024 | 20 | NEWS-NONEWS | 5552 | -1.22 | -3.62 | 2.12 |  |
| DN8 | other | 5 | NEWS | 1839 | -0.54 | -1.25 | 0.14 | 0.29 |
| DN8 | other | 5 | NONEWS | 3606 | 0.38 | -0.62 | 1.39 | 0.71 |
| DN8 | other | 5 | NEWS-NONEWS | 5445 | -0.92 | -1.84 | 0.05 |  |
| DN8 | other | 20 | NEWS | 1814 | -0.86 | -1.73 | 0.22 | 0.43 |
| DN8 | other | 20 | NONEWS | 3514 | -0.15 | -2.96 | 3.00 | 0.29 |
| DN8 | other | 20 | NEWS-NONEWS | 5328 | -0.70 | -3.39 | 1.71 |  |

## 6. Yearly (UP8, MOC_T, net excess %)

| year | window | UP8_n | share_NONEWS | share_NEWS | NEWS_h1_n | NEWS_h1_% | NEWS_h5_n | NEWS_h5_% | NEWS_h20_n | NEWS_h20_% | NONEWS_h1_n | NONEWS_h1_% | NONEWS_h5_n | NONEWS_h5_% | NONEWS_h20_n | NONEWS_h20_% |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2005 | DISC | 67 | 0.54 | 0.36 | 24 | 0.45 | 24 | -1.19 | 24 | 2.62 | 36 | -0.35 | 36 | -2.68 | 36 | -5.10 |
| 2006 | DISC | 79 | 0.54 | 0.37 | 29 | -0.57 | 29 | 0.02 | 29 | -0.54 | 43 | -1.04 | 43 | -2.68 | 43 | -5.99 |
| 2007 | DISC | 142 | 0.53 | 0.37 | 53 | -0.50 | 53 | -1.47 | 53 | -2.77 | 75 | -0.49 | 75 | -2.22 | 75 | -4.16 |
| 2008 | DISC | 947 | 0.71 | 0.18 | 169 | 0.08 | 169 | -0.04 | 169 | -2.11 | 668 | -0.59 | 668 | -1.37 | 668 | -1.40 |
| 2009 | DISC | 526 | 0.64 | 0.23 | 123 | -0.50 | 123 | -0.53 | 123 | 1.61 | 338 | -0.46 | 338 | -0.31 | 338 | 3.91 |
| 2010 | DISC | 112 | 0.41 | 0.46 | 52 | 0.04 | 52 | -0.07 | 52 | -3.24 | 46 | -0.25 | 46 | -1.44 | 46 | -0.42 |
| 2011 | DISC | 158 | 0.47 | 0.42 | 66 | -0.60 | 66 | -0.57 | 66 | -1.71 | 74 | -0.37 | 74 | -0.77 | 74 | -2.88 |
| 2012 | DISC | 151 | 0.43 | 0.51 | 77 | -0.30 | 77 | -0.12 | 77 | 0.36 | 65 | -0.87 | 65 | -0.60 | 65 | 3.77 |
| 2013 | DISC | 155 | 0.51 | 0.45 | 69 | -0.30 | 69 | -0.67 | 69 | 0.56 | 79 | -0.15 | 79 | -0.28 | 79 | -1.94 |
| 2014 | DISC | 189 | 0.54 | 0.38 | 71 | -0.84 | 71 | -2.12 | 71 | -2.50 | 102 | -0.70 | 102 | -2.14 | 102 | -5.05 |
| 2015 | TEST | 233 | 0.58 | 0.32 | 74 | 0.30 | 74 | 0.06 | 74 | -1.23 | 136 | -0.75 | 136 | -2.06 | 136 | -3.27 |
| 2016 | TEST | 304 | 0.61 | 0.30 | 91 | 1.25 | 91 | 2.26 | 91 | 2.78 | 184 | 0.18 | 184 | 1.25 | 184 | 2.01 |
| 2017 | TEST | 202 | 0.47 | 0.43 | 87 | -0.42 | 87 | 0.74 | 87 | 0.87 | 94 | 0.71 | 94 | 0.81 | 94 | -0.64 |
| 2018 | TEST | 273 | 0.44 | 0.47 | 127 | -0.16 | 127 | -0.83 | 127 | 0.57 | 120 | -1.01 | 120 | -2.74 | 120 | -5.39 |
| 2019 | TEST | 269 | 0.43 | 0.49 | 132 | 0.23 | 132 | 0.56 | 132 | 0.77 | 116 | -0.26 | 116 | -1.32 | 116 | -3.61 |
| 2020 | TEST | 2044 | 0.71 | 0.15 | 302 | 1.14 | 302 | 0.51 | 302 | 3.16 | 1458 | 0.57 | 1458 | 0.06 | 1458 | 3.58 |
| 2021 | TEST | 1821 | 0.73 | 0.16 | 283 | -0.58 | 283 | -1.45 | 283 | -5.13 | 1336 | -0.47 | 1336 | -1.19 | 1336 | -6.79 |
| 2022 | TEST | 1296 | 0.70 | 0.20 | 255 | 0.92 | 255 | 0.59 | 255 | -0.06 | 904 | 0.06 | 904 | -1.77 | 904 | -2.65 |
| 2023 | TEST | 821 | 0.63 | 0.29 | 242 | -0.19 | 242 | -0.49 | 242 | 0.64 | 516 | 0.25 | 516 | -0.15 | 516 | -0.47 |
| 2024 | TEST | 1300 | 0.59 | 0.30 | 390 | 0.16 | 385 | 0.24 | 371 | 0.55 | 770 | -0.19 | 768 | -0.18 | 746 | -0.63 |
| 2025 | TEST | 2185 | 0.65 | 0.24 | 525 | -0.15 | 525 | -1.04 | 525 | 0.03 | 1424 | -0.11 | 1424 | -1.30 | 1424 | -1.08 |
| 2026 | TEST | 3191 | 0.69 | 0.21 | 661 | -0.14 | 661 | -0.40 | 642 | -1.30 | 2204 | 0.04 | 2204 | -0.88 | 2090 | -2.28 |

## 7. Descriptive splits within NEWS (TEST, UP8, MOC_T)

| split | horizon | n | mean | lo | hi | years_pos |
|---|---|---|---|---|---|---|
| NEWS known-at-open (pre or prior post) | 1 | 3069 | 0.13 | -0.09 | 0.40 | 0.50 |
| NEWS known-at-open (pre or prior post) | 5 | 3064 | -0.23 | -0.70 | 0.22 | 0.58 |
| NEWS known-at-open (pre or prior post) | 20 | 3031 | -0.13 | -1.27 | 1.10 | 0.75 |
| NEWS intra-only | 1 | 100 | -0.37 | -1.44 | 0.69 | 0.25 |
| NEWS intra-only | 5 | 100 | 0.16 | -1.92 | 2.11 | 1.00 |
| NEWS intra-only | 20 | 100 | -1.84 | -5.17 | 1.51 | 0.25 |
| NEWS_non202 with PR-type item (7.01/8.01/1.01/2.01/2.05/2.06/1.02/5.01/2.04/1.05) | 1 | 710 | 0.33 | -0.53 | 1.21 | 0.36 |
| NEWS_non202 with PR-type item (7.01/8.01/1.01/2.01/2.05/2.06/1.02/5.01/2.04/1.05) | 5 | 710 | -0.04 | -1.26 | 1.12 | 0.45 |
| NEWS_non202 with PR-type item (7.01/8.01/1.01/2.01/2.05/2.06/1.02/5.01/2.04/1.05) | 20 | 703 | -1.12 | -4.16 | 1.85 | 0.27 |
| NEWS_non202 routine-only items (5.02/5.07/9.01/5.03/2.03/3.03/4.01/...) | 1 | 131 | -0.52 | -1.68 | 0.62 | 0.33 |
| NEWS_non202 routine-only items (5.02/5.07/9.01/5.03/2.03/3.03/4.01/...) | 5 | 131 | -1.83 | -5.37 | 1.71 | 0.33 |
| NEWS_non202 routine-only items (5.02/5.07/9.01/5.03/2.03/3.03/4.01/...) | 20 | 131 | -3.82 | -9.13 | 2.11 | 0.17 |

**By item code present at T (overlapping categories):**

| item_at_T | n | share_of_NEWS | h1_% | h1_lo | h1_hi | h5_% | h5_lo | h5_hi | h20_% | h20_lo | h20_hi |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2.02 | 2328 | 0.73 | 0.09 | -0.13 | 0.30 | -0.18 | -0.56 | 0.19 | 0.31 | -0.70 | 1.35 |
| 7.01 | 741 | 0.23 | 0.19 | -0.32 | 0.75 | -0.20 | -1.11 | 0.77 | -0.50 | -2.21 | 1.47 |
| 8.01 | 611 | 0.19 | 0.26 | -0.40 | 1.08 | 0.72 | -0.27 | 1.71 | -1.08 | -3.47 | 1.59 |
| 1.01 | 268 | 0.08 | 0.05 | -0.89 | 0.93 | -1.01 | -2.87 | 0.84 | -1.95 | -5.84 | 2.10 |
| 2.01 | 21 | 0.01 | 0.46 | -1.94 | 3.40 | -2.82 | -8.93 | 1.88 | 2.55 | -8.42 | 12.85 |
| 5.02 | 231 | 0.07 | -0.29 | -1.07 | 0.46 | -1.92 | -3.56 | -0.15 | -1.76 | -6.31 | 2.88 |
| 3.02 | 78 | 0.02 | 0.17 | -1.40 | 1.66 | -1.41 | -4.70 | 1.88 | -7.79 | -13.47 | -2.33 |
| 5.07 | 65 | 0.02 | 1.11 | -1.10 | 3.10 | -0.31 | -3.55 | 3.17 | -5.24 | -11.58 | 0.76 |
| 5.03 | 32 | 0.01 | 1.81 | -0.99 | 5.69 | 0.88 | -4.58 | 7.05 | -0.64 | -8.95 | 8.16 |
| 2.03 | 90 | 0.03 | 1.15 | -0.46 | 2.80 | 1.23 | -2.53 | 4.98 | 1.31 | -4.04 | 6.19 |
| 3.01 | 7 | 0.00 | -2.86 | -9.39 | 4.14 | -6.61 | -16.71 | 0.96 | -3.41 | -18.71 | 8.59 |
| 4.02 | 1 | 0.00 |  |  |  |  |  |  |  |  |  |
| 1.03 | 0 | 0.00 |  |  |  |  |  |  |  |  |  |
| 9.01 only | 11 | 0.00 | -3.53 | -6.25 | -0.12 | -7.53 | -14.85 | 0.26 | -2.10 | -14.43 | 16.59 |

## 8. Liquidity tiers (TEST, UP8, MOC_T)

| size_tier | arm | horizon | n | mean | lo | hi |
|---|---|---|---|---|---|---|
| 100-500M | NEWS | 5 | 1482 | -0.61 | -1.32 | 0.03 |
| 100-500M | NEWS | 20 | 1464 | -0.59 | -2.25 | 1.05 |
| 100-500M | NONEWS | 5 | 4236 | -0.83 | -1.64 | -0.13 |
| 100-500M | NONEWS | 20 | 4178 | -1.95 | -4.62 | 0.66 |
| 50-100M | NEWS | 5 | 1171 | 0.07 | -0.55 | 0.73 |
| 50-100M | NEWS | 20 | 1163 | -0.15 | -1.48 | 1.32 |
| 50-100M | NONEWS | 5 | 3405 | -0.56 | -1.29 | 0.26 |
| 50-100M | NONEWS | 20 | 3362 | -0.78 | -2.81 | 1.10 |
| 500M-2B | NEWS | 5 | 402 | 0.24 | -1.09 | 1.64 |
| 500M-2B | NEWS | 20 | 398 | 1.24 | -0.87 | 3.52 |
| 500M-2B | NONEWS | 5 | 1265 | -1.14 | -2.34 | 0.11 |
| 500M-2B | NONEWS | 20 | 1237 | -2.27 | -5.30 | 1.19 |
| >2B | NEWS | 5 | 109 | 0.27 | -1.49 | 2.41 |
| >2B | NEWS | 20 | 106 | -0.26 | -3.50 | 3.00 |
| >2B | NONEWS | 5 | 354 | -1.92 | -3.75 | 0.21 |
| >2B | NONEWS | 20 | 347 | -2.57 | -6.00 | 2.90 |

## 9. Caveats

- Survivorship: symbol list as of 2026-09 in both the bars and the filings crawl; delisted names are absent (flatters long cells, understates reversal in the no-news arm, since no-news pumps that later delist are missing).
- 'News' = any 8-K item, including routine ones (5.02 appointments, 5.07 vote results, 9.01-only); press releases without an 8-K, 6-K filers, analyst actions and media are invisible, so part of NO-NEWS has a public catalyst this data cannot see. Item splits in section 7 are descriptive.
- Intra-day filings on T are counted as NEWS even when accepted after the move (flagged; split in section 7).
- Discovery 2005-2014 rests on 1,225 names for 2005-2013 and is descriptive.
- 2024-09-03..09-11 bars hole: events and exits touching it are dropped.
- No haircut is applied; NEWSARCH guidance is +0.25% (1-5 d) / +0.40% (20 d) against any long cell.

## 10. Sanity checks and interpretation notes

- Split artifacts: 10 of 16,465 UP8 events have a close ratio within 0.5% of an integer >= 2 or of 1.5 (a proxy for unadjusted splits); negligible. 99.9th percentile of day-0 excess in TEST is +120%, median +10.8%.
- Event rate per 10,000 eligible ticker-days: ~18-28 in calm years (2005-07, 2010-19), 149 in 2008, 92 in 2009, 177 in 2020, 131 in 2021, 97 in 2022, 62 in 2023, 95 in 2024, 99 in 2025, 137 in 2026 (to 08-31). The eligible universe per day also jumps in 2025-26 (221k/233k eligible ticker-days vs 137k in 2024), so 2025-26 supply 39% of TEST UP8 events; both arms are negative in those years (NEWS h5 -1.04%/-0.40%, NONEWS h5 -1.30%/-0.88%). The regime split (section 5) shows the same sign structure inside and outside 2020-2024: NEWS flat, NONEWS negative, difference +0.6% (h5) / +1.3-1.6% (h20) in both regimes, neither clearing zero on its own.
- Ambiguous events (8-K at T-1 or T+1 but not T): 1,508 in TEST UP8 (T+1-only 643, T-1-only 783, both 82); they behave like the no-news arm (-0.77% h5, -1.88% h20), so treating late filers as 'news' would not rescue the news arm.
- Intra-day filings are only 100 of 3,169 NEWS events (3%); the known-at-open subset carries the result.
- Within news, the item-code splits (descriptive, overlapping) say the type matters in the way NEWSARCH predicted: 3.02 dilution at T -7.8% at h20 (n 78, CI below zero); 5.02 executive change -1.9% at h5 (n 231, CI below zero); 5.07/9.01-only/routine-only filings negative; 8.01 +0.7% at h5 and 2.02 +0.3% at h20 are the only positive-pointing groups, neither clearing zero. The N5 exclusion (3.02/4.02/3.01/1.03/5.03 in the prior 60 sessions, 22% of events) lifts NEWS h20 from -0.18% to +0.45% and NEWS_202 h20 from +0.31% to +0.55%, but the CIs overlap almost entirely, so by the registered rule the exclusion is not adopted as a proven improvement (it is a defensible hygiene filter, not evidence).
- Liquidity: the no-news reversal is present in every ADV tier (h5 -0.56% to -1.92%), largest in the most liquid names, so it is not a small-cap spread artifact; NEWS is flat in every tier.
- Discovery 2005-2014 (1,225 names, descriptive): same structure, both arms negative at h5 (NEWS -0.58%, NONEWS -1.18%), difference +0.60% with CI through zero.
- Costs: 10 bp round trip at ADV >= $100M, 20 bp otherwise; the DN8 rows report long-side net; a short of news-tagged drops at h20 would net roughly +0.9% with a lower CI near -0.1% after both-way costs, i.e. not clearing zero either.
