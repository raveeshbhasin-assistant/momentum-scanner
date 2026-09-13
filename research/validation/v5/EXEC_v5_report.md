# EXEC_v5 — execution of preregistration_v5.json (run 2026-09-13)

Engine: `research/EXEC_v5_engine.py` (one vectorized numpy/pandas pass; log in `EXEC_v5_run.log`). Registration SHA-256 `6222b79d…2b1e1` (unchanged, `V5_prereg_hash.txt`). No threshold was changed.

## 0. What was and was not run

| Window | Status | Data |
|---|---|---|
| Confirmatory unseen 2004-01..2013-12 | **NOT RUN.** `bars1d_2004_2013` held 1,225 of 3,179 files at run time (< 1,500 -> reported unavailable per the executor brief; the registration's single pre-declared partial run remains available). **No forward return was computed on this window; it is still UNSEEN.** Only the registered pre-run earnings-coverage check (event flags through Close_T) was computed. File list at check time: `EXEC_v5_files_2004_2013_at_precheck.txt`. Names with data by year: 2004 927 … 2013 1,225. | — |
| Refinement on a seen window 2014-09-01..2024-08-31 (halves 2014-09..2019-12 / 2020-01..2024-08) | Run. Every result below is a **refinement on a seen window**; no pass is possible from it. | `bars1d_10y` (3,179 files, 2014-01-02..2024-08-30) |
| Discovery 2024-09-01..2026-09-12 | Run, **descriptive only**. | `bars1d_all` (2,814 files, 2024-09-12..2026-09-11) spliced after the 10y panel (per-ticker price ratio at the seam: 0 tickers off by >0.1%); no date shift needed (no Sunday labels; no overlap with 10y to correlate) |

SPY: `SPY_10y.csv` was absent; SPY 2003-06..2026-09 was fetched once via yfinance (first try) into `EXEC_v5_SPY_2004_2026.csv`; the panel calendar was restricted to SPY trading days (0 dropped). Panel: 3,185 days x 4,206 names.

Pre-run checks (executed before anything else, as registered):
- **B1 seen-window sanity check** (from `OVERNIGHT_holdout_trades.csv.gz`, E2 rows incl. E1, 63/126-day trailing mean of X1_net > 0, resolved events only): OFF share 2015-01..2019-12 = **0.809** (need >= 0.60), ON share 2020-01..2021-12 = **0.895** (need >= 0.60) -> **PASS**; V5-3/V5-4 remain in the family. Engine-recomputed B1 (tier costs) gives 0.854 / 0.869. 95.5% of days used the 63-day window, 0.5% the 126-day fallback, 4.0% OFF for < 30 events.
- **Earnings coverage of 2004-2013 E1 events** (205 E1 events on the 1,225 partial files; "resolvable" = ticker has an earnings row within 130 calendar days on both sides of T — the stricter reading): **28.8% < 60% -> V5-7c NOT promoted; Holm m = 5.**
- Earnings coverage of the seen-window events is 39.7% overall and regime-confounded: 2015 0.96, 2016 0.98, 2017 0.97, 2018 0.82, 2019 0.32, 2020 0.26, 2021 0.18, 2022 0.18, 2023 0.22, 2024 0.20 (the acquisition run was a snapshot; Nasdaq calendar only 2014-2018 done). The 7c slice is therefore dominated by the negative 2015-2018 regime.

Accounting (literal): buy Close_T x (1+c), sell Open_{T+1} x (1-c); c = 5 bp if ADV20$ >= $100M else 10 bp (universe floor $50M ADV20$, prev close >= $5, >= 250 prior rows, all through T-1); one row per ticker-day; SPY leg Open_{T+1}/Close_T - 1; C1 = 5 seeded same-day same-ADV-decile non-E2 names with |gap| < 1% and volume < 1.5x ADV (coverage 98.6%); C2 = 3 nearest same-day non-E2 names with volume < 1.5x ADV whose day return is within **both** +-1 pp (executor brief) **and** [0.75x, 1.25x] (registration) of the event's — the stricter intersection; coverage 39% overall, only 63 of 723 E1 events, so C2 for E1 is weak. CIs: day- and quarter-cluster bootstrap, 2,000 reps each, the wider one reported; one-sided p = max(P(boot <= 0), P(boot - m >= m)) from the wider distribution; Holm m = 5 across V5-1..V5-5 on the seen window.

Holdout reproduction (flat 5 bp as in `OVERNIGHT_holdout`): E1 n = 723 vs 722, +0.795% [+0.10, +1.47] vs +0.79% [+0.08, +1.48]; E2 n = 3,101 vs 3,098, +0.320% [+0.04, +0.61] vs +0.32% [+0.05, +0.62]. Reproduced within rounding.

## 1. Trading-cell family — refinement on a seen window 2014-09..2024-08 (net excess over SPY, %, tier costs)

| Cell | Subset | n | days | mean | wider 95% CI | p (1-sided) | Holm (m=5) | quarters > 0 | C1 excess [CI] | C2 excess (n) | winsor / trim-top-2% | median / win |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V5-1 | E1 all | 723 | 536 | +0.76 | [+0.07, +1.44] q | 0.017 | 0.051 | 0.385 | +0.91 [+0.21, …] | +0.86 (63) | +0.62 / **-0.08** | -0.28 / 0.41 |
| V5-2 | E2 all | 3,101 | 1,399 | +0.28 | [+0.01, +0.57] q | 0.026 | 0.051 | 0.410 | +0.41 [+0.14, …] | +0.02 (1,276) | +0.19 / **-0.26** | -0.19 / 0.45 |
| V5-3 | E1, B1 ON | 389 | — | **+1.55** | [+0.52, +2.51] | <0.001 | <0.001 | 0.652 | +1.65 [+0.61, …] | +1.03 (44) | +1.44 / +0.56 | — / 0.48 |
| V5-4 | E2, B1 ON | 1,662 | — | +0.64 | [+0.26, +1.02] | <0.001 | 0.002 | 0.640 | +0.76 [+0.39, …] | +0.04 (755) | +0.50 / **-0.05** | — / 0.49 |
| V5-5 | E1, B2 ON | 394 | — | +1.12 | [+0.07, +2.02] | 0.018 | 0.051 | 0.357 | +1.32 [+0.26, …] | **-0.65** (39) | +1.01 / +0.21 | — / 0.42 |

Halves (mandatory): V5-1 H1 2014-09..2019-12 n = 227 **-0.39 [-0.62, -0.16]**, H2 n = 496 +1.28 [+0.36, +2.21]; V5-2 H1 n = 1,007 **-0.18 [-0.30, -0.05]**, H2 +0.51 [+0.13, +0.89]. As known before registration, neither unconditional cell can meet the 2014-2019-half gate.

Gate diagnostics (seen window):

| Cell | ON share of days | mean(ON) - mean(OFF) [CI], p | OFF cell | ON >= OFF in H1 / H2 | other |
|---|---|---|---|---|---|
| V5-3 (B1) | 40.8% | **+1.71 [+0.64, +2.83], p = 0.002** | n = 334, -0.16 [-0.66, +0.38] | yes / yes | ON_H1 n = 37 -0.09; ON_H2 n = 352 +1.72 [+0.68, +2.78] |
| V5-4 (B1) | 40.8% | +0.76 [+0.37, +1.16], p = 0.001 | n = 1,439, -0.12 [-0.30, +0.07] | yes / yes | ON_H1 n = 155 -0.03; ON_H2 +0.71 [+0.29, +1.10]; K5-filled n = 1,480 +0.72 [+0.31, +1.14] |
| V5-5 (B2) | 38.8% (B2 undefined before 2017-01-30 = OFF) | +0.80 [-0.32, +2.09], **p = 0.094** | n = 329, +0.32 [-0.44, +1.15] | **no** (ON_H1 -0.51 vs OFF_H1 -0.31) / yes | quarterly Spearman(dispersion, E2 mean) rho = +0.36, one-sided p = 0.013 |

Registered gates checkable on the seen window (1 = met): V5-1 `H1_lowerCI>0=0; qpos>=0.60=0; holm<0.05=0; C1>=+0.25&lowerCI>0=1; C2>0=1; winsor&trim>0=0`. V5-2 `0;0;0;1;1;0`. **V5-3 `H1=0 (replaced by regime validation for gated cells); qpos=1; holm=1; C1=1; C2=1; winsor&trim=1; ON-OFF>=0.5&p<.05=1; ON share 25-75=1; OFF not sig. positive=1`.** V5-4 `qpos=1; holm=1; C1=1; C2=1; winsor&trim=0; ON-OFF>=0.3=1; share=1; OFF ok=1`. V5-5 `qpos=0; holm=0; C1=1; C2=0; winsor&trim=1; ON-OFF=0; share=1; OFF ok=1`.

Mechanism gates (seen window): placebo Close_{T+5} -> Open_{T+6} vs C1: E1 -0.23 [-0.53, +0.08] (not positive; |mean| slightly above the 0.20 band), E2 -0.06 [-0.19, +0.07]; vs SPY E1 -0.33 [-0.62, -0.04]. Mirror M1 (long the overnight of gap <= -8%, vol >= 3x, red close): n = 683, +0.14 [-0.24, +0.62], p = 0.30 — not significant and below 0.5 x V5-1 (0.38): **K2 not triggered**. Tiers (V5-1): >= $500M +1.41 [-0.15, +3.82] (n = 83), $100-500M +0.59, $50-100M +0.79; liquidity terciles +0.93 / +0.61 / +0.74 — not confined to the least-liquid tercier: **K4 not triggered** on this window. K1: C1 excess > 0 for both base cells (K1 not triggered). Cost sensitivity flat 10 bp/side: V5-1 +0.69 [0.00, +1.37], V5-2 +0.22 [-0.06, +0.51], V5-3 +1.48, V5-4 +0.57, V5-5 +1.06.

**Pass verdicts: none.** Every registered pass criterion requires the confirmatory window; it was not run. The seen-window ON/OFF separation of B1 is expected *by construction* (B1 was designed after seeing the 2015-2019 vs 2020-2024 pattern; the registration says so).

## 2. Discovery window 2024-09..2026-09 (descriptive; the only forward data the gates have not seen)

| Cell | n | mean | wider CI | quarters > 0 | C1 | note |
|---|---|---|---|---|---|---|
| V5-1 E1 | 432 | +0.63 | [-0.13, +1.52] | 0.67 | +0.81 | K5-filled 423: +0.65 |
| V5-2 E2 | 1,812 | +0.18 | [-0.10, +0.48] | 0.67 | +0.33 | K5-filled 1,305: **+0.39 [+0.08, +0.72]** (28% of E2 events unfilled; ranking by day return E1-first helps here) |
| V5-3 E1, B1 ON | 280 | +0.69 | [-0.03, +1.52] | 0.62 | +0.91 | OFF n = 152 +0.53; **ON - OFF = +0.16 [-1.64, +1.74], p = 0.46** — B1 does not discriminate |
| V5-4 E2, B1 ON | 1,141 | 0.00 | [-0.32, +0.34] | 0.38 | +0.18 | OFF n = 671 **+0.49 [+0.10, +1.42]**; ON - OFF = **-0.48**, p = 0.93 — wrong sign, OFF cell positive |
| V5-5 E1, B2 ON | 346 | +0.75 | [-0.15, +1.84] | 0.67 | +0.92 | OFF n = 86 +0.15; diff +0.60, p = 0.21; Spearman rho = -0.03 (p = 0.53) |

ON shares: B1 64.1%, B2 72.9% of discovery days. Mirror M1 +0.07 [-0.21, +0.36]; placebo E1 vs C1 +0.08 [-0.20, +0.36]; E2 vs C1 +0.15 [-0.01, +0.29]. Ladder pooled -0.39 [-0.95, +0.15].

Reading: in the only stretch the gate rules had not been fitted to, neither B1 nor B2 separates good from bad nights; for E2 the B1-OFF nights were the profitable ones. The 2020-2024 half remains the sole evidence for the gate, and the gate was built from it.

## 3. Yearly table (mean net excess over SPY, %, all valid events; B1/B2 = share of days ON)

```
year   E1_n  E1_mean  E2_n  E2_mean  E1|B1on_n  E1|B1on  E2|B1on_n  E2|B1on  E1|B2on_n  E1|B2on  B1_on  B2_on
2015     34   -0.49    174   -0.22        -        -          -        -          -        -      0.00   0.00
2016     34   -0.11    195   +0.01        9    +0.49         68    +0.43          -        -      0.29   0.00
2017     50   -0.22    190   -0.27       20    -0.23         62    -0.45         11    -0.52      0.30   0.20
2018     49   -0.50    197   -0.22        8    -0.41         25    -0.26         49    -0.50      0.15   0.90
2019     60   -0.54    251   -0.20        -        -          -        -         32    -0.52      0.00   0.49
2020    121   +2.91    535   +1.21       75    +3.61        388    +1.37        117    +2.58      0.77   0.92
2021    123   +1.51    463   +0.88      121    +1.55        454    +0.90         77    +1.89      0.97   0.59
2022     73   +0.36    327   -0.08       40    +1.03        173    -0.11         28    -1.57      0.51   0.25
2023     95   -0.16    419   +0.17       65    +0.38        257    +0.49         13    -0.03      0.66   0.08
2024    145   +0.58    613   +0.05       59    +1.89        255    +0.31        113    +0.79      0.51   0.66
2025    180   +0.80    721   +0.13      153    +0.88        583    -0.03        134    +0.95      0.80   0.74
2026    191   +0.69    828   +0.20      119    +0.24        538    -0.08        166    +0.78      0.62   0.76
```
(2014-09..12 has no eligible events: the 250-row history filter first clears in 2015-01, exactly as in the holdout. 2024 = Jan-Aug from the 10y panel plus Sep-Dec from the spliced panel.)

## 4. Per-capital K = 5 (E1 first, then E2, by day return; net of tier costs; idle nights = 0)

| Cell | Window | weights | total return | max DD | years > 0 | yearly % |
|---|---|---|---|---|---|---|
| V5-1 | seen | 20% each | +219.8% | -17.2% | 4/11 | 2015 -3.1, 2016 -0.6, 2017 -2.0, 2018 -4.1, 2019 -5.3, **2020 +105.9, 2021 +44.0**, 2022 +5.0, 2023 -2.7, 2024 +23.2 |
| V5-1 | seen | v5 7.5%/3.75% ATR-capped | +38.1% | -6.1% | 3/11 | 2020 +24.5, 2021 +12.5, else -2.0..+5.5 |
| V5-2 | seen | 20% each | +521.1% | -30.1% | 4/11 | 2015..2019 -2.5..-9.9 each; 2020 +233.9; 2021 +106.9; 2022 -6.3; 2023 +20.4; 2024 +11.4 |
| V5-2 | seen | v5 | +56.6% | -9.4% | 4/11 | 2020 +35.6, 2021 +21.0 |
| V5-3 | seen | 20% each | +231.7% | -8.4% | 6/11 | 2016 +1.0, 2017 -0.8, 2018 -0.3, 2020 +67.2, 2021 +44.4, 2022 +8.0, 2023 +6.9, 2024 +19.2 |
| V5-3 | seen | v5 | +43.4% | -3.4% | 6/11 | — |
| V5-4 | seen | 20% each | +695.0% | -17.9% | 5/11 | 2016 +4.6, 2017 -5.4, 2018 -0.7, 2020 +156.8, 2021 +106.6, 2022 -2.1, 2023 +27.8, 2024 +21.9 |
| V5-4 | seen | v5 | +69.1% | -3.6% | 6/11 | — |
| V5-5 | seen | 20% each | +162.4% | -12.8% | 4/11 | 2017 -1.0, 2018 -4.1, 2019 -2.9, 2020 +88.1, 2021 +32.9, 2022 -7.9, 2023 +0.5, 2024 +23.0 |
| V5-1 | discovery | 20% each | +76.4% | -12.1% | 3/3 | 2024 (Sep-Dec) +1.4, 2025 +36.3, 2026 +27.7 |
| V5-2 | discovery | 20% each | +185.8% | -16.8% | 3/3 | +40.6 / +40.1 / +45.0 |
| V5-3 | discovery | 20% each | +47.8% | -5.9% | 3/3 | +6.0 / +31.1 / +6.4 |
| V5-4 | discovery | 20% each | +38.9% | -16.8% | 3/3 | +12.6 / +14.8 / +7.4 |
| V5-5 | discovery | 20% each | +69.4% | -12.1% | 3/3 | +2.4 / +30.4 / +26.8 |

Full table incl. the v5-weight rows and SPY-excess versions: `EXEC_v5_percapital.csv`. The 20%-each series is dominated by 2020-2021 (and the un-gated series lose money every year 2015-2019); the 2024-2026 series are positive every year but this window was the decomposition's discovery data.

## 5. Research and descriptive cells

**V5-6 overnight ladder (E1; buy MOC Close_{T+k} if Close_{T+k} >= Close_T, sell MOO T+k+1), seen window:** k1 n = 380 -0.11 [-0.80, +0.72]; k2 n = 365 -0.03 [-0.44, +0.40]; k3 n = 370 +0.09 [-0.39, +0.70]; pooled n = 1,115 **-0.02 [-0.32, +0.32]**, quarters > 0 = 0.33, C1 excess +0.11 [-0.18, …]; pooled H1 -0.13 [-0.29, +0.05], H2 +0.04; B1-gated pooled n = 620 +0.14 [-0.36, +0.73]. Discovery pooled n = 658 -0.39 [-0.95, +0.15]. **Research fail on every criterion; the design stays one-night.** (C2 is undefined for ladder legs.)

**V5-7a extreme mover:** E1 >= 25% day return n = 199 **+3.00 [+0.82, +4.99]**, quarters 0.61, C1 +3.17 [+1.09, …]; E1 8-25% n = 524 **-0.09 [-0.34, +0.15]**. Discovery: +1.96 [-0.36, +4.83] vs -0.05. Expectation met: the >= 25% subset carries the whole mean. Declared slice only; v5 may not act on it. Note that "mean turns negative when the top 2% are removed" for V5-1/V5-2 is the same fact.

**V5-7b deal-pin:** 0 E1 and 0 E2 events flagged in the seen window (1 each in discovery) — the flag never co-occurs with the 3x/2x volume and green-close conditions, so the exclusion is moot (no dilution to remove).

**V5-7c earnings (resolvable subset only, regime-confounded, see coverage above):** E1 earnings n = 159 **-0.30 [-0.52, -0.08]** vs non-earnings n = 81 +0.95 [-0.48, +2.86]; E2 earnings n = 745 -0.13 [-0.23, -0.03] vs non-earnings n = 520 +0.20 [-0.21, +0.67]. Discovery: E1 earnings n = 35 +0.02 vs non-earnings n = 20 +1.10. Expectation (earnings >= non-earnings) NOT met, but the resolvable rows are ~80% from 2015-2018 (the negative regime); no inference possible until coverage is completed. "Earnings tonight" (report dated T amc or T+1) flags: E1 n = 3, E2 n = 18 (E2 -0.11 [-3.19, +3.47]).

**V5-7d tiers / terciles:** V5-3 >= $500M n = 47 +3.01 [+0.44, +7.15]; $100-500M +1.34 [+0.10, +2.95]; $50-100M +1.36 [-0.07, +2.96]; terciles +1.50 / +1.72 / +1.43. V5-4 >= $500M +0.78 [0.00, +1.61]; V5-2 most-liquid tercile +0.01 [-0.26, +0.28] vs middle +0.57. Discovery V5-1 >= $500M -0.23, most-liquid tercile -0.30 vs least-liquid +1.07 — the survivorship signature appears in the forward window for the unconditional cells.

**V5-7e risk rules:** SPY <= -3% nights: E1 n = 7 +9.79 [+2.85, +19.96], E2 n = 23 +2.57 [+0.18, +7.27] — skipping them costs 0.09 pp (E1 0.76 -> 0.67) and 0.01 pp (E2); retained as a correlation-night rule per registration. macro_eve is **approximated** (T+1 = first Friday of the month, NFP proxy; BLS calendar not on disk, CPI not flagged): E1 excluded n = 50 +0.90, remaining +0.75 (cost 0.01 pp); E2 excluded n = 217 -0.11, remaining +0.31 (improves 0.03 pp). The <= 0.10 pp adoption condition is met on the proxy, but adopt only after the real BLS calendar is applied.

**V5-7f capacity:** seen — E1 nights 536 of 2,517 days, > 5 E1 on 0.4% of nights, 2.1% of E1 events unfilled; E2 nights 1,399, mean 2.2 / max 53 per night, > 5 on 5.9% of nights, 9.8% of events unfilled. Discovery — E2 mean 4.25 / max 63 per night, 23% of nights > 5, 28% unfilled; E1 2.1% unfilled.

**V5-7g rank rule:** nights with > 5 E2 qualifiers: top-5 by day return +0.54 vs top-5 by RVOL +0.53 (diff -0.007 pp, seen); discovery +0.70 vs +0.66 (diff -0.04). Non-inferior within 0.20; day-return rank stays.

**V5-8 Part A (15:44 proxy fidelity on bars5m_inplay):** not run in this execution (5-minute engine; out of the daily-bar budget). Required before any shadow. Part B is the forward shadow.

## 6. Kill rules and verdict

K1 no (C1 excess +0.91 / +0.41 on the seen window; not evaluable on the confirmatory window). K2 no (M1 +0.14 n.s.). K3 no (placebo <= 0 vs C1 on seen; +0.08 n.s. on discovery). K4 no on the seen window (>= $500M positive in V5-1/3/4), but the discovery window shows the unconditional effect only outside the most-liquid tercile. K5 no (B1 sanity passed). K6/K7 not measured.

**Decision-tree verdict: NOT ISSUED — the confirmatory window was not run.** What can be said: (i) the engine reproduces the holdout; (ii) on the seen window the B1-gated cells clear every checkable gate, which is what a gate designed on that window must do; (iii) in the 2024-09..2026-09 forward stretch neither gate discriminates (B1 ON-OFF +0.16 for E1, -0.48 for E2 with the OFF cell positive; B2 diff +0.60 n.s., Spearman -0.03) — this is the first out-of-sample look at the gates and it is negative; (iv) the unconditional effect there is +0.63% (E1, CI includes 0) / +0.18% (E2) and again concentrated in >= 25% movers and outside the most-liquid names. The 2004-2013 run, when the download completes (or the single pre-declared partial run), remains the only legitimate pass test; no rule may be changed before it.

## 7. Ambiguities resolved (stricter reading, documented)
- B1 window "trailing 63 trading days of events whose T+1 open is <= T-1" = event days T-64..T-2 inclusive.
- C2 = intersection of the +-1 pp (brief) and [0.75x, 1.25x] (registration) bands.
- B2 requires the full 504-day history; undefined days count as OFF (2015-01..2017-01 on the seen window), which lowers the seen ON share to 38.8%.
- Earnings flag: report dated T with timing bmo/unknown, or previous trading day with amc/unknown. "Resolvable": rows within 130 days on both sides of T.
- One-sided p = the more conservative of the two bootstrap conventions.
- Cell statistics are on all qualifying events (as in the holdout); the K5-filled subset is co-reported and never differs in sign on the seen window.
- Ladder legs are clustered by the leg's own night (T+k); their B1 state is the gate on that night.
- Winsor/trim gate inherited by the gated cells (V5-4 fails it on trim).

## 8. Files
`EXEC_v5_cells.csv` (200 rows), `EXEC_v5_trades.csv.gz` (6,035 event rows with every leg, flag, control and gate), `EXEC_v5_percapital.csv`, `EXEC_v5_yearly.csv`, `EXEC_v5_daily_gates.csv` (B1/B2/companion series), `EXEC_v5_summary.json`, `EXEC_v5_SPY_2004_2026.csv`, `EXEC_v5_files_2004_2013_at_precheck.txt`, `EXEC_v5_engine.py`, `EXEC_v5_run.log`; things_tried rows appended to `things_tried.csv`.
