# VERIFY2 — independent recompute of the pre-registered validation (v4)

Verifier: independent script `VERIFY2_recompute.py` written from `preregistration_v4.json` + the red team's `locked_rule_table` only; orchestrator scripts (`VAL1_inplay.py`, `VAL3_swing.py`, `VAL2_control.py`) were opened **after** my numbers existed, solely to explain residual differences. Raw output: `VERIFY2_out.txt`; my trade lists: `VERIFY2_u1_trades.csv`, `VERIFY2_u3_trades.csv.gz`, `VERIFY2_u2_trades.csv`; verdicts JSON: `VERIFY2_verdicts.json`.

Tolerances applied: 0.03R (U1/U2), 0.10 pct-points (U3); a cell is CONFIRMED only if the sign of avgR and the sign of the lower CI bound are also unchanged.

## 1. U1 in-play intraday — R0/R1 x S_a/S_b (registered >=70-RTH-bar variant)

Watchlist rebuilt ex-ante from `inplay_candidates.csv` (gap_pct>=3, prev_close>=5, adv20>=2e7, top 20 by gap per date; 1,078 rows / 60 days; 55 rows dropped for missing 5m file, 7 for missing 10:00/10:05 bar). n and day counts match the orchestrator exactly in all 8 cells.

| window | cell | claimed avgR | recomputed | diff | claimed CI | my CI | verdict |
|---|---|---|---|---|---|---|---|
| TEST | R0/S_a | -0.1582 | -0.1365 | +0.022 | [-0.317, 0.014] | [-0.295, 0.036] | CONFIRMED |
| TEST | R0/S_b | -0.1959 | -0.1984 | -0.003 | [-0.340,-0.043] | [-0.349,-0.041] | CONFIRMED |
| TEST | R1/S_a | -0.1809 | -0.1731 | +0.008 | [-0.431, 0.096] | [-0.441, 0.116] | CONFIRMED |
| TEST | R1/S_b | -0.1119 | -0.1148 | -0.003 | [-0.301, 0.091] | [-0.312, 0.090] | CONFIRMED |
| DISC | R0/S_a | -0.1169 | -0.0990 | +0.018 | [-0.328, 0.117] | [-0.303, 0.141] | CONFIRMED |
| DISC | R0/S_b | -0.0141 | -0.0183 | -0.004 | [-0.317, 0.359] | [-0.326, 0.363] | CONFIRMED |
| DISC | R1/S_a | -0.2656 | -0.2465 | +0.019 | [-0.480,-0.002] | [-0.450, 0.004] | CONFIRMED |
| DISC | R1/S_b | -0.1010 | -0.0992 | +0.002 | [-0.284, 0.110] | [-0.286, 0.128] | CONFIRMED |

Excess vs R0 (same structure) TEST: R1/S_a -0.037 (claimed -0.023), R1/S_b +0.084 (claimed +0.084). No-min-bar variant (my n=488 vs their minbars2 n=489) moves nothing by more than 0.01R.

Source of the residual +0.02R on S_a cells (found after opening `VAL1_inplay.py`): the orchestrator embeds the entry cost in the fill (`fill = O*(1+cost)`), places stop/target relative to that adjusted fill and nets the exit (`ex*(1-cost)`), R = (ex_net-fill)/(fill-stop). The locked text nets the round trip off the raw-open P&L. Their convention puts the stop `cost` higher in price (more stop-outs: 46.8% vs my 43.5%) and charges roughly one side of cost on barrier exits. Both are defensible; the difference is <=0.022R and goes in the conservative direction for the claimed numbers.

## 2. U3 multi-day swing — W0 / W1 (2,700 names with >=480 rows, T+1 open -> close T+3, 10 bp/side, SPY excess on identical dates)

| cell | claimed excess | recomputed | diff | claimed n | my n | claimed quarter-CI | my quarter-CI | my day-CI | folds>0 | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| W0 | -0.151% | -0.180% | -0.029 | 17,077 | 14,903 | [-0.339, 0.053] | [-0.390, 0.009] | [-0.618, 0.188] | 2/8 (claimed 4/8) | CONFIRMED |
| W1 | -0.566% | -0.380% | +0.186 | 954 | 938 | [-1.058,-0.090] | [-0.760, 0.020] | [-0.973, 0.196] | 2/8 (claimed 3/8) | PARTIALLY_CONFIRMED |

Cause of the n gap (diagnosed by merging `VAL3_trades.csv.gz` with mine on ticker+signal_date): every one of my trades is in theirs and the returns agree to <0.003 pct-points; their 2,174 extra W0 trades and 16 extra W1 trades are **all** signals fired while the same ticker's previous position was still open (signal_date < previous exit_date). The locked table says "one open position per ticker (new signal while open ignored)"; `VAL3_swing.py` does not implement it for the per-trade metric (only the K=10 portfolio has an open-slot check). The 16 overlapping W1 trades average -11.5% excess (repeat gappers that collapsed), which is what drives the claimed W1 mean from -0.38% to -0.57% and makes the claimed quarter-CI look fully negative. Under the locked rule W1 is -0.38% with quarter-CI upper bound +0.02. Conclusion unchanged: both W0 and W1 are negative on average, fail "lower CI > 0" and fail "positive in >=6 of 8 folds" (2/8 each in my run).

## 3. U2 C0 TEST — all 3,827 live picks post-08-01 (29 days), next-bar-open entry, 5 bp/side, pick stop % with 0.5% floor, 2.5R target

| cell | claimed avgR | recomputed | diff | claimed CI | my CI | verdict |
|---|---|---|---|---|---|---|
| C0 TEST | -0.2002 | -0.1715 | +0.029 | [-0.284,-0.112] | [-0.252,-0.080] | CONFIRMED (at the tolerance edge) |

n, days, median stop % (0.712) and net pnl % per trade (-0.095 vs -0.094) all agree. Their stop rate is higher (55.0% vs 47.1%) for the same reason as U1: `VAL2_control.py` sets `stop = fill*(1-sp)` with `fill = O*(1+5bp)`, i.e. the stop sits 5 bp above mine, which with a median 0.71% stop distance is 7% of the risk and flips a number of near-miss trades into -1R. Using the pick's absolute stop price instead of its % gives -0.167 [-0.247,-0.082]. Reference arm only; strongly negative under every convention.

## 4. Look-ahead spot checks

VAL1_trades_TEST (30 random trades, seed 20260913; plus whole-file checks on 2,578 rows):
- Entry strictly after the signal bar: 30/30 and 100% of all rows (2,576 at +5 min, 2 at +10 min where a 5m bar was missing — still after the signal).
- Watchlist inputs ex-ante: 30/30 in my independently built watchlist, gap/adv identical to the ex-ante CSV fields; 100% of all 2,578 rows are in my watchlist.
- Signal bar identical in 30/30 (incl. R2 pullback and R3 ORB bars). Fill = raw open x (1+cost) in every case; stop % reproduces from signal-bar information (ATR at signal bar, day low through signal bar, OR low, pullback low) once the fill convention is applied; R differences are fully explained by that convention. No look-ahead found.
- Deviation noted: R2/S_b uses a 1% floor (`min(L_b, fill*0.99)`) where the locked table says 0.995 (0.5%). Not one of my recomputed cells; R2/S_b is deeply negative (-0.33, CI upper -0.13) so this cannot rescue it, but it is a departure from the lock.

VAL3_trades (30 random trades): entry = next trading day after signal 30/30; exit = T+h 30/30; ret = net of 10 bp/side 30/30; SPY excess on identical dates 30/30; gap ex-ante (Open_T / Close_T-1) 30/30; ADV20 dollar uses T-20..T-1 in 30/30 and never includes day T (0/30); all rule filters satisfied 30/30. No look-ahead. Deviation: one-open-position rule not applied (see section 2).

## 5. things_tried.csv

Exists, 7 rows: DISC base-rate slices, VAL1 reconciliation, the W3 fade observation, the 6-cell fade exploration, the min-bar sensitivity, the overnight decomposition and the overnight holdout run. The exploratory variants I could see referenced in the research folder are listed.

## Bottom line

All 11 checked numbers reproduce in sign and in CI conclusion; 10 within tolerance, W1 outside tolerance because the orchestrator omitted the locked one-position-per-ticker rule (its claimed -0.57% is too negative; the correct figure is -0.38%). No look-ahead was found in either trade file. **No cell passes the registered criteria** (net avgR lower CI > 0 on TEST and excess over benchmark > 0): every TEST lower bound is negative, every swing quarter-cluster lower bound is negative, and the control arm is negative with a CI entirely below zero. Survivors: none.

Concerns for the record: (a) fill-embedded cost convention differs from the locked R formula (immaterial, <=0.03R, conservative); (b) R2/S_b floor 0.99 vs locked 0.995; (c) U3 per-trade metric without the one-open-position rule; (d) the U1 >=70-RTH-bar day filter is not in the lock (sensitivity shows it is immaterial); (e) with only 8 quarter clusters the U3 quarter-cluster bootstrap CI is crude — day-cluster CIs are reported alongside and lead to the same conclusion.
