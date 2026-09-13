# VERIFY_v5_confirmatory -- independent verification of the v5 CCO confirmatory run (2004-2013)

Verifier: independent recompute, 2026-09-13. Code: `VERIFY_v5_confirmatory_recompute.py` (derived from the earlier independent engine `VERIFY_v5_recompute.py`; nothing imported from `EXEC_v5_confirmatory.py` or `EXEC_v5_engine.py`). Outputs: `VERIFY_v5_confirmatory_recompute.json`, `VERIFY_v5_confirmatory_events.csv.gz`, `VERIFY_v5_confirmatory_extra.json`, `VERIFY_v5_confirmatory_files_check.txt`.

## 0. Verdict

**Decision RED -- AGREED.** Every claimed cell reproduces from raw bars (point estimates to 0.01%, CIs to bootstrap noise, n exactly). Kill rule K1 fires on both of its clauses in my recompute; no gated cell passes any substantive gate; the 40-row look-ahead audit is clean; B1 is built strictly from events resolved by T-1. The registered AMBER "partial run" clause is literally also true (1,225 < 1,500 files) but the kill-rule RED clause is unconditional ("-> RED"), the executor's RED > AMBER precedence is the stricter and intended reading, and the partial universe is the complete 2004-2013 universe reachable from the 2026 list, so the pre-declared completion re-run cannot occur or change anything.

## 1. Setup checks

| Item | Result |
|---|---|
| Registration hash | `6222b79d...2b1e1` recomputed = recorded in `V5_prereg_hash.txt` |
| Frozen file list | `EXEC_v5_files_2004_2013_frozen.txt` has 1,225 entries, set-identical to an independent `os.listdir` of `bars1d_2004_2013` (1,225 csv). sha256 of the sorted listing (`\n`-joined) = `dbcb7d06...5771d` = the value the executor reported; sha256 of the raw file on disk = `471a9d55...f1934` (CRLF line endings; content identical). I did not overwrite the executor's file; my check is in `VERIFY_v5_confirmatory_files_check.txt`. Window label "confirmatory, unseen, partial universe (1,225 names)" is appropriate. |
| Symbol list | all 1,225 names are in `us_common_symbols.csv` |
| Calendar | 2,517 SPY days 2004-01-02..2013-12-31; panel dates not in SPY calendar 0; SPY days absent from panel 0 |
| Events | E1 205, E2 1,243 (executor trades file flags 1,244 E2 rows; one lacks a T+1 open and is excluded from the cell in both engines), M1 252 -- exact set match (E2: 1 exec-only row = the unresolved one; M1: 0/0) |
| Gates | B1 primary `[T-64,T-2]` ON share 0.1843 (0.216 from first defined day 2005-06-21); B1 variant `[T-63,T-2]` 0.1867; day-state agreement primary vs variant 0.9897; B2 ON share 0.3123 (0.4509 from 2007-01-31) |

## 2. Recomputed trading-cell family (net excess over SPY, %, tier costs 5/10 bp per side; wider of day/quarter cluster 95% CI, 2,000 reps; Holm m = 5)

| Cell | n | claimed | recomputed | wider CI | q+ | Holm | halves H1/H2 | C1 excess | winsor / trim2 | K5-filled |
|---|---|---|---|---|---|---|---|---|---|---|
| V5-1 E1 | 205 | -0.382 [-0.66,-0.09] | **-0.382** | [-0.658, -0.088] d | 0.222 (36 q) | 1.00 | -0.265 / -0.431 | -0.256 [-0.55, +0.07] | -0.434 / -0.602 | 205 / -0.382 |
| V5-2 E2 | 1,243 | -0.208 [-0.38,-0.01] | **-0.208** | [-0.383, -0.016] d | 0.243 (37 q) | 1.00 | -0.260 / -0.175 | -0.063 [-0.23, +0.11] | -0.218 / -0.421 | 1,208 / -0.266 |
| V5-3 E1 & B1 | 42 | -0.722 [-1.29,-0.24] | **-0.722** | [-1.258, -0.251] d | 0.077 (13 q) | 1.00 | -0.630 / -0.779 | -0.695 [-1.32, -0.17] | -0.675 / -0.807 | -- |
| V5-4 E2 & B1 | 258 | -0.236 [-0.53,+0.08] | **-0.236** | [-0.540, +0.087] d | 0.200 (15 q) | 1.00 | -0.133 / -0.334 | +0.009 [-0.27, +0.35] | -0.273 / -0.480 | -- |
| V5-5 E1 & B2 | 88 | -0.538 [-0.96,-0.12] | **-0.538** | [-0.937, -0.132] d | 0.158 (19 q) | 1.00 | -0.778 / -0.448 | -0.405 [-0.83, +0.01] | -0.588 / -0.724 | -- |

Regime validation for the gated cells (ON-OFF one-sided day-cluster bootstrap):

| Cell | ON-OFF diff | p | OFF mean [CI] | OFF q+ | OFF vs ON H1 | OFF vs ON H2 | ON share | extra |
|---|---|---|---|---|---|---|---|---|
| V5-3 | -0.427 (claimed -0.43) | 0.909 | -0.295 [-0.617, +0.072] | 0.343 | OFF -0.132 > ON -0.630 (fail) | OFF -0.355 > ON -0.779 (fail) | 0.184 (< 25%, fail) | |
| V5-4 | -0.035 (claimed -0.03) | 0.586 | -0.201 [-0.389, +0.043] | 0.324 | OFF -0.305 <= ON -0.133 (pass) | OFF -0.141 > ON -0.334 (fail) | 0.184 (fail) | |
| V5-5 | -0.272 (claimed -0.27) | 0.829 | -0.266 [-0.710, +0.174] | 0.222 | OFF +0.078 > ON -0.778 (fail) | OFF -0.418 > ON -0.448 (fail) | 0.312 (pass) | Spearman rho 0.013, one-sided p 0.469, nq 36 (fail) |

Other gates: placebo leg (Close_{T+5} -> Open_{T+6}) vs C1: V5-1 -0.051 [-0.193, +0.094] (ok); V5-2 **-0.166 [-0.344, -0.015]** significantly negative -> the `placebo_ok` gate fails for V5-2/V5-4 exactly as claimed (K3 requires significantly *positive*, so K3 does not fire). Mirror M1: n 252, +0.030 [-0.888, +0.725] q-cluster; 0.5 x V5-1 mean = -0.19 -> K2 not triggered (my one-sided p 0.42 vs claimed 0.52 is a direction-convention difference; not significant either way). Liquidity: V5-1 >= $500M tier -0.38 (n 16), terciles low/mid/high -0.667/-0.056/-0.421; V5-2 >= $500M +0.977 (n 99), terciles -0.457/-0.199/+0.032 -- no survivorship signature (K4 off), and no positive effect anywhere to be confined. C2: V5-1 +0.001 (n 14), V5-2 -0.309 (n 520), V5-4 +0.006 (n 118); the executor's C2 n differ (3 / 393 / 88) because the matching window on day return admits few names -- immaterial. Flat 10 bp/side: -0.436 / -0.265 / -0.779 / -0.293 / -0.584.

Gate tallies: V5-1 fails 9/12, V5-2 9/12, V5-3 12/15 (claimed 12/15), V5-4 10/15 (claimed 11/15), V5-5 12/16 (claimed 13/16). The one-gate differences come from which base cell's tier gate V5-4 inherits (executor: V5-1 via "as V5-3"; I used V5-2) and how a C2 with n = 1 is counted; no verdict depends on them.

## 3. B1 window definition and one-day sensitivity

Registered text: "trailing 63 trading days ... events whose T+1 open is <= T-1". Executor primary = 63 event-days `[T-64, T-2]`; my earlier engine's reading = the 63 trading days ending T-1 minus the unresolved day, `[T-63, T-2]` (62 event-days). Both are defensible; the executor documented the choice and reports the variant. Recomputed:

| Cell | window | n | mean | wider CI | q+ | ON-OFF | p | OFF |
|---|---|---|---|---|---|---|---|---|
| V5-3 | [T-64,T-2] primary | 42 | -0.722 | [-1.258, -0.251] d | 0.077 | -0.427 | 0.909 | -0.295 |
| V5-3 | [T-63,T-2] variant | 45 | -0.819 (claimed -0.819) | [-1.426, -0.402] q (claimed [-1.36,-0.35] d) | 0.077 | -0.559 (claimed -0.56) | 0.976 | -0.260 |
| V5-4 | [T-64,T-2] primary | 258 | -0.236 | [-0.540, +0.087] d | 0.200 | -0.035 | 0.586 | -0.201 |
| V5-4 | [T-63,T-2] variant | 256 | -0.320 (claimed -0.32) | [-0.625, +0.015] d (claimed [-0.63,-0.01]) | 0.200 | -0.141 (claimed -0.14) | 0.771 | -0.179 |

The one-day shift changes the ON state on 1.0% of days and moves 3 E1 / 2 E2 events across the gate; every gated statistic is negative under both readings and the ON share is 18-19% under both (below the 25% floor). The claimed "upper CI turns negative" for the V5-4 variant is at the bootstrap boundary (+0.015 in my draw vs -0.01 in theirs); immaterial. The verifier flag from the seen window (fragile quarters-positive share) does not bite here: q+ is 0.077 / 0.200 under both windows.

## 4. Look-ahead audit (40 random rows of `EXEC_v5_confirmatory_trades.csv.gz`, seed 20260913)

Per row, from the raw bar files and the SPY file: gap = Open_T/Close_{T-1}-1; RVOL = Volume_T / mean(Volume_{T-20..T-1}); ADV20$ = mean((Close x Volume)_{T-20..T-1}) and explicitly NOT the leaky mean over T-19..T; prev close >= $5; >= 250 prior rows; my own eligibility flag; Close_T / Open_T; exit Open_{T+1} = the ticker's next bar AND that bar is the next SPY calendar day; SPY leg Open_{T+1}/Close_T-1; tier cost; net = Open_{T+1}(1-c)/(Close_T(1+c))-1; exc = net - SPY leg; B1, B1v, B2 flags equal to mine; the executor's daily `B1_mean` on that date equals the mean exc of E2 events dated in `[T-64, T-2]` and NOT the leaky `[T-63, T-1]` mean.

Result: **40/40 rows pass every look-ahead check.** 34/40 also match the executor's C1 control mean to 1e-9; the 6 misses are a control-draw convention difference, not look-ahead (section 5). Whole-series checks: executor B1 state = my primary B1 on 100.00% of 2,517 days (variant 98.97%; leaky `[T-63,T-1]` only 98.41%, i.e. the executor's series is the non-leaky one); executor `B1v` = my variant 100%; B2 100%; executor `B1_mean` = mine on all 2,148 defined days with max |diff| = 0.

## 5. C1 control discrepancy (explained, immaterial)

Across all 1,243 E2 events: same 5 controls (c1_net equal to 1e-9) for 82.5%; c1_n equal for 97.2%. The executor labels ADV20$ deciles 0-9 and assigns boundary names differently from my `ceil(pct_rank x 10)` 1-10 binning (decile labels agree in 0.6% of rows because of the offset, but the binning agrees for 82.5% of rows). Because the seed formula `crc32('YYYY-MM-DD|TICKER')` is shared, the 17.5% of events whose decile membership differs draw a different control set. Aggregate C1 excess: mine vs executor E2 -0.063 / -0.064, E1 -0.256 / -0.253. K1 clause 1 (C1 <= 0 for both base cells) under three alternative seeds: V5-1 -0.239 / -0.261 / -0.221; V5-2 -0.062 / -0.073 / -0.073 -- robust. The registration does not fix the decile-edge convention, so either implementation is compliant.

## 6. Research / falsifier cells

- V5-6 overnight ladder (E1, legs k = 1..3 held only if Close_{T+k} >= Close_T): pooled n 308, -0.297 [-0.432, -0.170], q+ 0.171; k1 n 100 -0.517, k2 n 104 -0.240, k3 n 104 -0.143; B1-gated pooled n 54 +0.064 [-0.274, +0.459]. Identical to the claim; research FAIL; design stays one-night.
- M1 mirror: as above, K2 not triggered.

## 7. Decision re-derived from the registered criteria

1. **K1 (regime artefact)** -- clause 1: confirmatory C1 excess V5-1 -0.256 <= 0 AND V5-2 -0.063 <= 0 -> TRUE. Clause 2: best family cell by mean = V5-2 (-0.208); halves -0.260 / -0.175; H1 < -0.20 -> TRUE. (If "best" were read as V5-4, halves -0.133 / -0.334 -> H2 < -0.20, still TRUE.) K1 fires. K2, K3, K4 do not fire. K5 was passed pre-run (logged).
2. **No gated cell meets the gates**: V5-3, V5-4, V5-5 each fail Holm, lower CI, q+, n, C1 haircut, ON-OFF and at least one half-ordering gate. AMBER-eligibility (all gates met except n, or raw p < 0.05 with Holm >= 0.05) -- none: raw one-sided p are 0.999 / 0.927 / 0.996.
3. **AMBER partial-run clause**: literally true (1,225 < 1,500). Registered consequence would be "shadow only, no capital; re-decision at the completed-download re-run". The download is complete for every name the 2026 list can reach (names listed after 2014 have no 2004-2013 history), so that re-run is void; and the decision tree's kill-rule clause is unconditional. RED > AMBER precedence is the stricter and intended reading.
4. **GREEN** unavailable (no gated pass; V5-8 Part A unrun; partial).

=> **RED. I agree.** Registered consequence: log and stop; no live service is written. The result is decisive in the registration's own terms: the 2004-2013 survivor panel is the window most biased in the rule's favour and the effect is still significantly negative for E1 (upper CI -0.09%) and E2 (upper CI -0.02%).

## 8. Concerns and notes for the operator

1. Registration ambiguity: the AMBER partial-run clause and the RED kill-rule clause overlap; a future registration should state precedence explicitly (the executor documented its reading, which I endorse).
2. B1 is essentially never ON in 2004-2013 (18% of days; 42 E1 ON events); the gate is structurally non-discriminating in this window, which is itself a registered fail (ON share < 25%) rather than an excuse.
3. B1 window one-day ambiguity persists in the text; immaterial here (section 3).
4. C1 decile-edge convention is unspecified; immaterial here (section 5).
5. Executor gate tallies for V5-4 / V5-5 differ from mine by one gate each (inheritance / C2 n = 1 counting); no verdict affected.
6. One-sided p convention for M1 differs (0.42 vs 0.52); not significant either way.
7. The frozen-list sha256 the executor reported is the hash of the listing string, not of the CRLF file on disk; both recorded above.
8. things_tried.csv outcome rows and the SEEN mark were not re-audited here (out of scope for the numbers; the executor report states they were appended).
