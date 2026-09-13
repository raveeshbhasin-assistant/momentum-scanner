# EXEC_v5_confirmatory -- the single pre-declared confirmatory run of preregistration_v5.json (run 2026-09-13T11:42:09)

**Window label: `confirmatory, unseen, partial universe (1,225 names)`.** Registration SHA-256 `6222b79d...2b1e1` unchanged; no threshold or definition changed; engine functions copied verbatim from `EXEC_v5_engine.py`. This run was executed ONCE. The 2004-2013 window is now **SEEN** (things_tried.csv).

## 0. Decision

# **RED**

Rationale (computed in code from the registered decision tree): kill rule(s) fired on the confirmatory window: ['K1_regime_artefact'].

- Gated cells passing every gate incl. Holm: none. AMBER-eligible gated cells (all gates but n / Holm): none.
- Kill rules on the confirmatory window: K1 regime artefact = True (C1<=0 for both base cells: True; best cell V5-2 half < -0.20%: True; best-cell halves -0.26 / -0.17), K2 mirror = False, K3 placebo = False, K4 survivorship signature = False, K5 (B1 sanity, pre-run) = passed.
- Partial-run clause: 1225 files < 1,500, so the run is labelled partial. Precedence used (stricter reading): RED (kill or no gated cell meets the gates) > AMBER > GREEN. GREEN additionally requires V5-8 Part A (not run).

## 1. Data and freeze

- Frozen file list: `EXEC_v5_files_2004_2013_frozen.txt`, 1225 files, sha256 `dbcb7d068e3dc42f138a6e1fe51aee5fb278aa86727b76283788d17ad3a5771d`; identical to the pre-check list: True.
- Universe = every symbol of the 2026 list that Yahoo served for 2004-2013, i.e. essentially all names that existed with >= $20M median dollar volume in early 2014 (names listed after 2014 cannot have this history). Prices are split/dividend-adjusted (yfinance): the $5 prev-close floor therefore acts on adjusted prices (see survivorship section).
- Panel: 2517 SPY trading days x 1225 names, 2004-01-02..2013-12-31; SPY days absent from the panel: 0; panel days not in the SPY calendar: 0. Names with data by year: {2004: 927, 2005: 973, 2006: 1023, 2007: 1068, 2008: 1098, 2009: 1120, 2010: 1161, 2011: 1182, 2012: 1224, 2013: 1225}. Mean eligible names per day by year: {2004: 1, 2005: 147, 2006: 178, 2007: 235, 2008: 252, 2009: 226, 2010: 253, 2011: 271, 2012: 267, 2013: 293}.
- Effective event window starts 2004-12-30 (the >= 250-row history filter clears ~2005-01; 2004 contributes no events, exactly as 2014-09..12 contributed none on the seen window). Events: E1 205, E2 1244, M1 252; valid (T+1 open exists) E1 205, E2 1243.
- Controls: C1 coverage 0.975, C2 coverage 0.268 (C2 = stricter intersection of +-1 pp and [0.75x,1.25x]; C2 for E1 n = 3).
- Earnings: resolvable share of E1 events 0.288 (< 60% -> V5-7c NOT promoted, Holm m = 5 as logged before the run).
- Gates: B1 defined from 2005-06-21 (63-day window used on 60.9% of days, 126-day on 24.4%, OFF-undefined on 14.7%); B2 defined from 2007-01-31 (undefined = OFF).

## 2. Trading-cell family on the confirmatory window (net excess over SPY, %, tier costs 5/10 bp per side)

| Cell | Subset | n | days | mean | wider 95% CI | p (1-sided) | Holm (m=5) | quarters>0 (nq) | C1 excess [lo] | C2 excess (n) | winsor / trim-top-2% | median / win | PASS |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V5-1 | E1 all | 205 | 176 | -0.38 | [-0.66, -0.09] d | 0.997 | 1.000 | 0.222 (36) | -0.25 [-0.56] | +1.17 (3) | -0.43 / -0.60 | -0.52 / 0.268 | FAIL |
| V5-2 | E2 all | 1243 | 792 | -0.21 | [-0.38, -0.01] d | 0.995 | 1.000 | 0.243 (37) | -0.06 [-0.22] | -0.20 (393) | -0.22 / -0.42 | -0.27 / 0.360 | FAIL |
| V5-3 | E1, B1 ON | 42 | 38 | -0.72 | [-1.29, -0.24] d | 0.999 | 1.000 | 0.077 (13) | -0.71 [-1.33] | n/a (2) | -0.68 / -0.81 | -0.59 / 0.262 | FAIL |
| V5-4 | E2, B1 ON | 258 | 169 | -0.24 | [-0.53, +0.08] d | 0.933 | 1.000 | 0.200 (15) | +0.01 [-0.28] | +0.21 (88) | -0.27 / -0.48 | -0.32 / 0.368 | FAIL |
| V5-5 | E1, B2 ON | 88 | 78 | -0.54 | [-0.96, -0.12] d | 0.995 | 1.000 | 0.158 (19) | -0.41 [-0.86] | n/a (1) | -0.59 / -0.72 | -0.53 / 0.273 | FAIL |

Cost sensitivity flat 10 bp/side: V5-1 -0.44 [-0.71, -0.14] d, V5-2 -0.26 [-0.44, -0.07] d, V5-3 -0.78 [-1.34, -0.30] d, V5-4 -0.29 [-0.59, +0.02] d, V5-5 -0.58 [-1.00, -0.17] d.
K5-filled subsets: V5-1 n=205 -0.38 [-0.66, -0.09] d, V5-2 n=1208 -0.27 [-0.40, -0.13] d, V5-3 n=42 -0.72 [-1.29, -0.24] d, V5-4 n=255 -0.28 [-0.58, +0.04] d, V5-5 n=88 -0.54 [-0.96, -0.12] d.
Counterfactual next-close X2 (never traded): V5-1 -0.16 [-0.63, +0.30] q, V5-2 -0.35 [-0.61, -0.05] d.

### 2a. Confirmatory halves (2004-01..2008-12 / 2009-01..2013-12)

| Cell | half | n | mean | wider CI | quarters>0 | C1 |
|---|---|---|---|---|---|---|
| V5-1 | H1_2004-01..2008-12 | 60 | -0.26 | [-0.91, +0.53] q | 0.312 | -0.21 |
| V5-1 | H2_2009-01..2013-12 | 145 | -0.43 | [-0.76, -0.16] d | 0.150 | -0.27 |
| V5-2 | H1_2004-01..2008-12 | 487 | -0.26 | [-0.62, +0.20] d | 0.294 | -0.18 |
| V5-2 | H2_2009-01..2013-12 | 756 | -0.17 | [-0.31, -0.04] d | 0.200 | +0.01 |
| V5-3 | H1_2004-01..2008-12 | 16 | -0.63 | [-1.62, -0.23] q | 0.000 | -0.88 |
| V5-3 | H2_2009-01..2013-12 | 26 | -0.78 | [-1.68, -0.09] d | 0.125 | -0.61 |
| V5-4 | H1_2004-01..2008-12 | 126 | -0.13 | [-0.45, +0.17] d | 0.167 | +0.05 |
| V5-4 | H2_2009-01..2013-12 | 132 | -0.33 | [-0.82, +0.22] d | 0.222 | -0.04 |
| V5-5 | H1_2004-01..2008-12 | 24 | -0.78 | [-1.54, +0.02] q | 0.143 | -0.81 |
| V5-5 | H2_2009-01..2013-12 | 64 | -0.45 | [-0.91, +0.03] d | 0.167 | -0.26 |

### 2b. Regime-gate validation (the only out-of-sample test of B1/B2)

| Cell | gate | ON share days (full / defined from) | mean(ON) - mean(OFF) [CI], p | OFF cell n, mean [CI], q+ | ON>=OFF H1 (ON/OFF means, n) | ON>=OFF H2 (ON/OFF means, n) | other |
|---|---|---|---|---|---|---|---|
| V5-3 | B1 [T-64,T-2] (executor, primary) | 0.184 / 0.216 from 2005-06-21 | -0.43 [-1.11, +0.18], p=0.907 | n=163, -0.29 [-0.61, +0.07], q+=0.343 | False (-0.63/-0.13, 16/44) | False (-0.78/-0.36, 26/119) |  |
| V5-4 | B1 [T-64,T-2] (executor, primary) | 0.184 / 0.216 from 2005-06-21 | -0.03 [-0.41, +0.35], p=0.581 | n=985, -0.20 [-0.40, +0.04], q+=0.324 | True (-0.13/-0.30, 126/361) | False (-0.33/-0.14, 132/624) |  |
| V5-5 | B2 | 0.312 / 0.451 from 2007-01-31 | -0.27 [-0.84, +0.29], p=0.832 | n=117, -0.27 [-0.69, +0.19], q+=0.222 | False (-0.78/+0.08, 24/36) | False (-0.45/-0.42, 64/81) | Spearman rho=0.013, one-sided p=0.469, nq=36 |

### 2c. B1 window sensitivity (verifier flag: [T-64,T-2] executor vs [T-63,T-2])

Day-state agreement 0.9897.

| Cell | B1 window | n_ON | mean ON | wider CI | quarters>0 | ON share | ON-OFF, p | OFF mean [lo] |
|---|---|---|---|---|---|---|---|---|
| V5-3 | [T-64,T-2] executor (primary) | 42 | -0.72 | [-1.29, -0.24] d | 0.077 | 0.184 | -0.43, p=0.907 | -0.29 [-0.61] |
| V5-3 | [T-63,T-2] verifier variant | 45 | -0.82 | [-1.36, -0.35] d | 0.077 | 0.187 | -0.56, p=0.970 | -0.26 [-0.57] |
| V5-4 | [T-64,T-2] executor (primary) | 258 | -0.24 | [-0.53, +0.08] d | 0.200 | 0.184 | -0.03, p=0.581 | -0.20 [-0.40] |
| V5-4 | [T-63,T-2] verifier variant | 256 | -0.32 | [-0.63, -0.01] d | 0.200 | 0.187 | -0.14, p=0.779 | -0.18 [-0.37] |

## 3. Gate-by-gate table per trading cell (1 = met)

**V5-1** (3/12 gates met; failed: holm<0.05, lowerCI>0_confirmatory, lowerCI>0_2014-09..2019-12_half, both_conf_halves>=0, quarters_pos>=0.60, n>=300, C1>=0.25&C1_lowerCI>0, winsor&trim2>0, tier500>0&not_confined_T1)

| gate | met |
|---|---|
| holm<0.05 | 0 |
| lowerCI>0_confirmatory | 0 |
| lowerCI>0_2014-09..2019-12_half | 0 |
| both_conf_halves>=0 | 0 |
| quarters_pos>=0.60 | 0 |
| n>=300 | 0 |
| C1>=0.25&C1_lowerCI>0 | 0 |
| C2>0 | 1 |
| winsor&trim2>0 | 0 |
| placebo_|mean|<0.20&n.s. | 1 |
| mirror_not_sig_pos_at_0.5x | 1 |
| tier500>0&not_confined_T1 | 0 |

**V5-2** (3/12 gates met; failed: holm<0.05, lowerCI>0_confirmatory, lowerCI>0_2014-09..2019-12_half, both_conf_halves>=0, quarters_pos>=0.60, C1>=0.15&C1_lowerCI>0, C2>0, winsor&trim2>0, placebo_|mean|<0.20&n.s.)

| gate | met |
|---|---|
| holm<0.05 | 0 |
| lowerCI>0_confirmatory | 0 |
| lowerCI>0_2014-09..2019-12_half | 0 |
| both_conf_halves>=0 | 0 |
| quarters_pos>=0.60 | 0 |
| n>=600 | 1 |
| C1>=0.15&C1_lowerCI>0 | 0 |
| C2>0 | 0 |
| winsor&trim2>0 | 0 |
| placebo_|mean|<0.20&n.s. | 0 |
| mirror_not_sig_pos_at_0.5x | 1 |
| tier500>0&not_confined_T1 | 1 |

**V5-3** (3/15 gates met; failed: holm<0.05, lowerCI>0_confirmatory, quarters_pos>=0.60, n>=300, C1>=0.25&C1_lowerCI>0, C2>0, winsor&trim2>0, tier500>0&not_confined_T1, ON-OFF>=0.5&p<0.05, OFF<=ON_H1, OFF<=ON_H2, ON_share_25-75%)

| gate | met |
|---|---|
| holm<0.05 | 0 |
| lowerCI>0_confirmatory | 0 |
| quarters_pos>=0.60 | 0 |
| n>=300 | 0 |
| C1>=0.25&C1_lowerCI>0 | 0 |
| C2>0 | 0 |
| winsor&trim2>0 | 0 |
| placebo_|mean|<0.20&n.s. | 1 |
| mirror_not_sig_pos_at_0.5x | 1 |
| tier500>0&not_confined_T1 | 0 |
| ON-OFF>=0.5&p<0.05 | 0 |
| OFF<=ON_H1 | 0 |
| OFF<=ON_H2 | 0 |
| ON_share_25-75% | 0 |
| OFF_not_sig_pos | 1 |

**V5-4** (4/15 gates met; failed: holm<0.05, lowerCI>0_confirmatory, quarters_pos>=0.60, n>=600, C1>=0.15&C1_lowerCI>0, winsor&trim2>0, placebo_|mean|<0.20&n.s., tier500>0&not_confined_T1, ON-OFF>=0.3&p<0.05, OFF<=ON_H2, ON_share_25-75%)

| gate | met |
|---|---|
| holm<0.05 | 0 |
| lowerCI>0_confirmatory | 0 |
| quarters_pos>=0.60 | 0 |
| n>=600 | 0 |
| C1>=0.15&C1_lowerCI>0 | 0 |
| C2>0 | 1 |
| winsor&trim2>0 | 0 |
| placebo_|mean|<0.20&n.s. | 0 |
| mirror_not_sig_pos_at_0.5x | 1 |
| tier500>0&not_confined_T1 | 0 |
| ON-OFF>=0.3&p<0.05 | 0 |
| OFF<=ON_H1 | 1 |
| OFF<=ON_H2 | 0 |
| ON_share_25-75% | 0 |
| OFF_not_sig_pos | 1 |

**V5-5** (4/16 gates met; failed: holm<0.05, lowerCI>0_confirmatory, quarters_pos>=0.60, n>=300, C1>=0.25&C1_lowerCI>0, C2>0, winsor&trim2>0, tier500>0&not_confined_T1, ON-OFF>=0.5&p<0.05, OFF<=ON_H1, OFF<=ON_H2, spearman_rho>0&p<0.05)

| gate | met |
|---|---|
| holm<0.05 | 0 |
| lowerCI>0_confirmatory | 0 |
| quarters_pos>=0.60 | 0 |
| n>=300 | 0 |
| C1>=0.25&C1_lowerCI>0 | 0 |
| C2>0 | 0 |
| winsor&trim2>0 | 0 |
| placebo_|mean|<0.20&n.s. | 1 |
| mirror_not_sig_pos_at_0.5x | 1 |
| tier500>0&not_confined_T1 | 0 |
| ON-OFF>=0.5&p<0.05 | 0 |
| OFF<=ON_H1 | 0 |
| OFF<=ON_H2 | 0 |
| ON_share_25-75% | 1 |
| OFF_not_sig_pos | 1 |
| spearman_rho>0&p<0.05 | 0 |

Gate conventions (stricter readings): unconditional cells need lower CI > 0 in the confirmatory window AND in the 2014-09..2019-12 half (from `EXEC_v5_cells.csv`: V5-1 -0.39 [-0.62, ...] n=227; V5-2 -0.18 [-0.30, ...] n=1007); gated cells replace that with the regime validation (ON-OFF threshold with one-sided day-cluster p, OFF <= ON in both confirmatory halves, ON share 25-75% of all confirmatory trading days with undefined gate days counted OFF, OFF cell not significantly positive; V5-5 also the quarterly Spearman). C2 > 0 and winsor/trim > 0 are applied to every trading cell (common criteria). Placebo/mirror gates are inherited from V5-1 (E2 cells additionally from V5-2). The liquidity gate is evaluated on the cell's own subset and, for gated cells, also on V5-1. "Confined to the least-liquid tercile" = T1 > 0 with T2 <= 0 and T3 <= 0. Missing values fail a gate.

## 4. Mechanism checks

- V5-1 placebo Close_T+5 -> Open_T+6 vs C1: n=204 -0.04 [-0.17, +0.10] d -> gate met; vs SPY -0.16 [-0.28, -0.03] d
- V5-2 placebo Close_T+5 -> Open_T+6 vs C1: n=1214 -0.17 [-0.36, -0.01] q -> gate NOT met; vs SPY -0.28 [-0.44, -0.15] q
- Mirror M1 (long overnight of gap<=-8%, vol>=3x, red close): n=252 +0.03 [-0.85, +0.69] q p=0.520; 0.5 x V5-1 mean = -0.19 -> K2 not triggered
- V5-1 tiers/terciles: >=500M -0.38 (n=16), 100-500M -0.25, 50-100M -0.51; terciles T1 -0.67 / T2 -0.06 / T3 -0.42; confined to T1: False
- V5-2 tiers/terciles: >=500M +0.98 (n=99), 100-500M -0.25, 50-100M -0.37; terciles T1 -0.46 / T2 -0.20 / T3 +0.03; confined to T1: False
- V5-3 tiers/terciles: >=500M n/a (n=None), 100-500M -0.73, 50-100M -0.79; terciles T1 -0.94 / T2 -0.45 / T3 -0.78; confined to T1: False
- V5-4 tiers/terciles: >=500M +1.73 (n=19), 100-500M -0.32, 50-100M -0.47; terciles T1 -0.45 / T2 -0.35 / T3 +0.09; confined to T1: False
- V5-5 tiers/terciles: >=500M -1.18 (n=3), 100-500M -0.21, 50-100M -0.76; terciles T1 -1.04 / T2 -0.44 / T3 -0.12; confined to T1: False

## 5. Yearly table (mean net excess over SPY, %; B1/B2 = share of days ON; names/eligible counts)

```
      E1_n  E1_mean%  E2_n  E2_mean%  E1|B1on_n  E1|B1on_mean%  E2|B1on_n  E2|B1on_mean%  E1|B2on_n  E1|B2on_mean%  B1_on_share  B1v_on_share  B2_on_share  names_with_data  eligible_mean
year                                                                                                                                                                                      
2004   NaN       NaN     1      0.10        NaN            NaN        NaN            NaN        NaN            NaN         0.00          0.00         0.00              927              1
2005   7.0     -0.84    72     -0.52        NaN            NaN        NaN            NaN        NaN            NaN         0.00          0.00         0.00              973            147
2006  17.0     -0.43   102     -0.35        4.0          -0.94       14.0          -0.09        NaN            NaN         0.18          0.18         0.00             1023            178
2007  18.0      0.71   120      0.01       11.0          -0.26       85.0          -0.12        6.0          -0.52         0.65          0.65         0.37             1068            235
2008  18.0     -0.86   192     -0.29        1.0          -3.43       27.0          -0.20       18.0          -0.86         0.28          0.28         1.00             1098            252
2009  31.0     -0.56   168     -0.02       14.0          -1.15       71.0          -0.31       17.0          -0.82         0.40          0.40         0.48             1120            226
2010  22.0     -0.13   140     -0.18        7.0          -0.26       34.0          -0.44        NaN            NaN         0.25          0.26         0.00             1161            253
2011  19.0     -0.49   142     -0.18        1.0          -0.36       16.0          -0.23        6.0          -0.79         0.08          0.08         0.43             1182            271
2012  37.0     -0.61   146     -0.29        4.0          -0.49       11.0          -0.31       29.0          -0.19         0.01          0.02         0.55             1224            267
2013  36.0     -0.29   160     -0.23        NaN            NaN        NaN            NaN       12.0          -0.37         0.00          0.00         0.29             1225            293
```

## 6. Per-capital K = 5 (E1 first, then E2, by day return; net of tier costs; idle nights = 0)

| Cell | weights | total return | max DD | years>0 | trades | nights | yearly % | yearly excess % |
|---|---|---|---|---|---|---|---|---|
| V5-1 | 20% each | -14.0 | -14.6 | 0.10 | 205 | 176 | {"2004": 0.0, "2005": -1.15, "2006": -1.51, "2007": 2.71, "2008": -2.56, "2009": -4.77, "2010": -0.61, "2011": -1.53, "2012": -3.97, "2013": -1.42} | {"2004": 0.0, "2005": -1.18, "2006": -1.45, "2007": 2.53, "2008": -3.07, "2009": -3.46, "2010": -0.59, "2011": -1.86, "2012": -4.45, "2013": -2.05} |
| V5-1 | v5 7.5%/3.75% ATR-capped | -5.4 | -5.7 | 0.10 | 205 | 176 | {"2004": 0.0, "2005": -0.43, "2006": -0.57, "2007": 1.02, "2008": -0.66, "2009": -1.74, "2010": -0.23, "2011": -0.58, "2012": -1.84, "2013": -0.53} | {"2004": 0.0, "2005": -0.44, "2006": -0.54, "2007": 0.96, "2008": -0.9, "2009": -1.31, "2010": -0.22, "2011": -0.7, "2012": -2.04, "2013": -0.77} |
| V5-2 | 20% each | -46.8 | -46.8 | 0.10 | 1208 | 792 | {"2004": 0.05, "2005": -6.64, "2006": -6.94, "2007": -1.45, "2008": -15.44, "2009": -3.48, "2010": -7.21, "2011": -6.57, "2012": -7.13, "2013": -5.49} | {"2004": 0.02, "2005": -7.25, "2006": -6.89, "2007": 0.12, "2008": -18.8, "2009": -2.99, "2010": -5.04, "2011": -5.84, "2012": -8.36, "2013": -6.99} |
| V5-2 | v5 7.5%/3.75% ATR-capped | -13.7 | -13.7 | 0.20 | 1208 | 792 | {"2004": 0.01, "2005": -1.48, "2006": -1.61, "2007": 0.25, "2008": -3.09, "2009": -1.84, "2010": -1.48, "2011": -1.53, "2012": -2.45, "2013": -1.31} | {"2004": 0.0, "2005": -1.61, "2006": -1.59, "2007": 0.52, "2008": -4.08, "2009": -1.58, "2010": -1.06, "2011": -1.46, "2012": -2.8, "2013": -1.73} |
| V5-3 | 20% each | -6.8 | -7.3 | 0.10 | 42 | 38 | {"2004": 0.0, "2005": 0.0, "2006": -0.78, "2007": -0.66, "2008": -0.94, "2009": -4.31, "2010": -0.27, "2011": -0.03, "2012": 0.07, "2013": 0.0} | {"2004": 0.0, "2005": 0.0, "2006": -0.75, "2007": -0.58, "2008": -0.69, "2009": -3.19, "2010": -0.36, "2011": -0.07, "2012": -0.39, "2013": 0.0} |
| V5-3 | v5 7.5%/3.75% ATR-capped | -2.3 | -2.5 | 0.10 | 42 | 38 | {"2004": 0.0, "2005": 0.0, "2006": -0.29, "2007": -0.25, "2008": -0.18, "2009": -1.51, "2010": -0.1, "2011": -0.01, "2012": 0.03, "2013": 0.0} | {"2004": 0.0, "2005": 0.0, "2006": -0.28, "2007": -0.22, "2008": -0.13, "2009": -1.11, "2010": -0.14, "2011": -0.03, "2012": -0.15, "2013": 0.0} |
| V5-4 | 20% each | -21.3 | -21.6 | 0.00 | 255 | 169 | {"2004": 0.0, "2005": 0.0, "2006": -0.27, "2007": -3.44, "2008": -3.92, "2009": -10.36, "2010": -3.9, "2011": -0.93, "2012": -0.31, "2013": 0.0} | {"2004": 0.0, "2005": 0.0, "2006": -0.26, "2007": -2.04, "2008": -1.12, "2009": -6.53, "2010": -3.05, "2011": -0.74, "2012": -0.82, "2013": 0.0} |
| V5-4 | v5 7.5%/3.75% ATR-capped | -5.2 | -5.3 | 0.00 | 255 | 169 | {"2004": 0.0, "2005": 0.0, "2006": -0.2, "2007": -0.77, "2008": -0.73, "2009": -2.64, "2010": -0.78, "2011": -0.18, "2012": -0.05, "2013": 0.0} | {"2004": 0.0, "2005": 0.0, "2006": -0.19, "2007": -0.49, "2008": -0.24, "2009": -1.71, "2010": -0.64, "2011": -0.15, "2012": -0.23, "2013": 0.0} |
| V5-5 | 20% each | -8.5 | -9.3 | 0.00 | 88 | 78 | {"2004": 0.0, "2005": 0.0, "2006": 0.0, "2007": -0.8, "2008": -2.56, "2009": -3.9, "2010": 0.0, "2011": -0.1, "2012": -0.69, "2013": -0.72} | {"2004": 0.0, "2005": 0.0, "2006": 0.0, "2007": -0.63, "2008": -3.07, "2009": -2.77, "2010": 0.0, "2011": -0.94, "2012": -1.13, "2013": -0.88} |
| V5-5 | v5 7.5%/3.75% ATR-capped | -3.2 | -3.2 | 0.00 | 88 | 78 | {"2004": 0.0, "2005": 0.0, "2006": 0.0, "2007": -0.3, "2008": -0.66, "2009": -1.41, "2010": 0.0, "2011": -0.04, "2012": -0.6, "2013": -0.27} | {"2004": 0.0, "2005": 0.0, "2006": 0.0, "2007": -0.24, "2008": -0.9, "2009": -1.05, "2010": 0.0, "2011": -0.35, "2012": -0.79, "2013": -0.33} |

## 7. Research and descriptive cells

- **V5-6 overnight ladder (E1):** k1 n=100 -0.52, k2 n=104 -0.24, k3 n=104 -0.14; pooled n=308 -0.30 [-0.43, -0.17] d q+=0.171 C1 -0.13 [-0.28]; B1-gated pooled n=54 +0.06 [-0.29, +0.46] d. Research pass requires pooled lower CI > 0 AND (2014-2019 half lower CI > 0 [known: -0.13 [-0.29,+0.05], fails] or a passing B1 gate), each k >= 0, n >= 300, q+ >= 0.60, C1 lower CI > 0 -> research FAIL; design stays one-night.
- **V5-7a extreme mover:** E1 >= 25% n=26 -1.48 [-2.42, -0.61] d vs 8-25% n=179 -0.22 [-0.50, +0.10] d (expectation ">= 25% carries the mean": NOT met).
- **V5-7b deal-pin:** E1 flagged n=0, E2 flagged n=0.
- **V5-7c earnings (descriptive only, coverage 28.8%):** E1 earnings n=40 -0.61 vs non-earnings n=19 -0.99.
- **V5-7e risk rules:** SPY <= -3% nights E1 n=0 n/a (ex: -0.38); E2 n=3 +0.18 (ex: -0.21); NFP-approx macro eve E1 n=10 +0.06 (ex -0.41).
- **V5-7f capacity:** E1 nights 176 of 2517 days, > 5 on 0.0%, unfilled 0.0%; E2 nights 792, mean 1.57 / max 16 per night, > 5 on 1.1%, unfilled 2.8%.
- **V5-7g rank rule:** RVOL top-5 minus day-return top-5 on nights > 5 E2 qualifiers: +0.73 pp.
- **Pooled 2004-2026 (context only, never a pass criterion):** V5-1 n=1360 +0.55 [+0.10, +1.03] q; V5-2 n=6156 +0.15 [-0.01, +0.31] q.

## 8. Survivorship discussion

The universe is the 2026 symbol list restricted to names Yahoo could serve for 2004-2013: 1225 names, i.e. survivors that (a) still trade in 2026 and (b) already existed and were liquid in 2014. Every 2004-2013 delisting, bankruptcy, and acquisition target is absent, and names that were small then and large later are over-represented. For a long-only continuation leg this bias is IN the rule's favour (the registration: "the confirmatory 2004-2013 window is the MOST biased in the rule's favour; a pass there is necessary, not sufficient; a fail is decisive"). C1 (five same-day, same-ADV-decile non-event survivors) cancels the universe-level component, which is why the C1 haircut (+0.25% E1 / +0.15% E2) is a gate. Two second-order effects of the adjusted prices: the $5 floor on split-adjusted prices excludes names with large later splits from the early years (works against the rule: big future winners drop out), and the ADV$ filter uses adjusted close x split-adjusted volume (approximately split-invariant, slightly deflated by dividend adjustment). The 1,225-name panel is effectively the complete 2004-2013 universe reachable from the 2026 list; the registration's completed-download re-run would add nothing material, so the partial label is formal.

## 9. Ambiguities resolved (stricter reading, documented)
- B1 primary window = executor's [T-64, T-2] (63 event-days whose T+1 open <= T-1); verifier variant [T-63, T-2] reported as sensitivity only; pass decisions use the primary.
- Gate ON share denominator = all confirmatory trading days (undefined gate days = OFF); the share over defined days is co-reported.
- "OFF <= ON in both confirmatory halves" applied to V5-5 as well as V5-3/V5-4 (task brief lists it for the gated route generally).
- C2 > 0 and winsor/trim > 0 required for every trading cell; placebo gate requires |mean| < 0.20 and a CI that includes 0; mirror gate fails if M1 lower CI > 0 and M1 mean >= 0.5 x V5-1 mean.
- Decision precedence RED > AMBER > GREEN when clauses overlap; GREEN withheld while V5-8 Part A is unrun or the run is partial.
- K1 "best cell" = family cell with the highest confirmatory mean (ON subset for gated cells); its halves are the ON-subset halves.
- K4 fires if a positive base cell's effect is confined to T1 or to the $50-100M tier while the >= $500M tier is <= 0.
- Panel reindexed to the SPY calendar so that T+1 is always the next SPY trading day (a missing stock bar = NaN, not a skipped day).

## 10. Files
`EXEC_v5_confirmatory.py`, `EXEC_v5_confirmatory_cells.csv` (124 rows), `EXEC_v5_confirmatory_trades.csv.gz` (1496 event rows), `EXEC_v5_confirmatory_yearly.csv`, `EXEC_v5_confirmatory_percapital.csv`, `EXEC_v5_confirmatory_daily_gates.csv`, `EXEC_v5_confirmatory_summary.json`, `EXEC_v5_files_2004_2013_frozen.txt`; outcome rows appended to `things_tried.csv` and 2004-2013 marked SEEN.
