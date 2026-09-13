# VERIFY_v5 — independent verification of the EXEC_v5 claims

Verifier: independent recompute from the raw daily files (`bars1d_10y`, `bars1d_all`) and `preregistration_v5.json` only. The executor's engine was opened **after** the recompute, only to explain discrepancies. Scripts: `VERIFY_v5_recompute.py` (independent engine), `VERIFY_v5_audit.py` (40-row look-ahead audit, merge check, gate diagnostics). Outputs: `VERIFY_v5_recompute.json`, `VERIFY_v5_events.csv.gz`. The unseen 2004-2013 window was **not touched** (no return computed on `bars1d_2004_2013`).

## 1. Registration integrity

- SHA-256 of `preregistration_v5.json` = `6222b79d…2b1e1`, identical to `V5_prereg_hash.txt` (written 10:44, before the engine ran at 11:04-11:06).
- `things_tried.csv`: eight registration rows (L8-L15, one per cell, each carrying the hash) precede every outcome row; L16 B1 sanity check (pass), L17 earnings-coverage pre-check on 2004-2013 (**event flags only, no returns**, 28.8 % resolvable, V5-7c not promoted, Holm m = 5), L18 "confirmatory NOT RUN". Every outcome row is labelled `refinement on a seen window` or `discovery (seen) … descriptive`. Compliant.
- One deviation, conservative: the registration's coverage rule says "< 1,500 files → label results *partial* and re-run once on completion"; the executor **did not run** the partial pass (1,225 of 3,179 files). The window stays unseen and the single pre-declared partial run is still available. The confirmatory claims (`n = 0`) are therefore "not run", not "verified".
- Benchmark data: there is no `SPY.csv` in either bars directory; both the executor and this verifier used `EXEC_v5_SPY_2004_2026.csv` (yfinance). Spot-checked: 2020-03-16 −10.94 %, 2020-03-24 +9.06 %, 2022-11-10 +5.50 %, 2025-04-09 +10.50 % day returns; overnight-leg sd 0.70 %. Sane, but not an independent source.

## 2. Independent recompute (registered accounting: tier costs 5/10 bp per side on the trade's own notional, SPY over the identical Close_T→Open_T+1 leg, day- **and** quarter-cluster bootstrap 2,000 reps, wider interval reported)

Universe/day T from data through T−1 (prev close ≥ $5, ADV20$ ≥ $50 M, ≥ 250 prior rows), E1/E2/M1 verbatim, exit at Open_T+1 on the next SPY-calendar day, C1 = 5 same-day same-ADV-decile non-E2 names with |gap| < 1 % and RVOL < 1.5 (seed = crc32(date|ticker)).

| Cell | Window | Claimed | Recomputed | Verdict |
|---|---|---|---|---|
| V5-1 | seen 2014-09..2024-08 | n=723, +0.758 % [+0.065, +1.436], q+ 0.385, C1 +0.915 | n=722, **+0.754 % [+0.061, +1.458]** (Q), q+ 0.359 (14/39), C1 +0.91 [+0.22, +1.59], flat-10 bp +0.69, K5-filled 707 / +0.78 | CONFIRMED |
| V5-2 | seen | n=3101, +0.285 % [+0.007, +0.573], q+ 0.41, C1 +0.414 | n=3098, **+0.288 % [−0.006, +0.576]** (Q), q+ 0.41, C1 +0.42 [+0.13, +0.69], flat-10 bp +0.22 | PARTIALLY (lower CI sits on zero; sign of the bound is bootstrap noise) |
| V5-3 | seen, B1 ON | n=389, +1.549 % [+0.519, +2.513], q+ 0.652, C1 +1.653 | n=386, **+1.559 % [+0.422, +2.732]** (Q), **q+ 0.565 (13/23)**, C1 +1.68 [+0.62, +2.82], ON−OFF +1.73 % p=0.002 | PARTIALLY (estimate/CI/C1 reproduced; q+ ≥ 0.60 NOT reproduced) |
| V5-4 | seen, B1 ON | n=1662, +0.638 % [+0.262, +1.023], q+ 0.64, C1 +0.762 | n=1651, **+0.639 % [+0.233, +1.044]** (Q), **q+ 0.56 (14/25)**, C1 +0.75 [+0.36, +1.13], ON−OFF +0.75 % p<0.001 | PARTIALLY (same pattern) |
| V5-5 | seen, B2 ON | n=394, +1.122 % [+0.069, +2.021], q+ 0.357, C1 +1.305 | n=393, **+1.117 % [+0.007, +2.028]** (Q), q+ 0.321, C1 +1.28 [+0.24, +2.20], ON−OFF +0.80 % p=0.08 | CONFIRMED (lower bound ≈ 0 in both) |
| V5-6 | seen, ladder | n=1115, −0.02 % | not recomputed (research cell, outside the four-cell budget) | UNVERIFIABLE here |
| M1 | seen, mirror | n=683, +0.137 % [−0.245, +0.615] | n=682, **+0.138 % [−0.231, +0.633]**, C1 +0.17 | CONFIRMED |
| V5-1 | disc 2024-09..2026-09 | n=432, +0.63 % [−0.13, +1.52], q+ 0.67, C1 +0.81 | n=430, +0.631 % [−0.131, +1.547], q+ 0.625, C1 +0.78 | CONFIRMED |
| V5-2 | disc | n=1812, +0.18 % [−0.10, +0.48] | n=1782, +0.183 % [−0.103, +0.486] | CONFIRMED |
| V5-3 | disc, B1 ON | n=280, +0.69 % [−0.03, +1.52] | n=285, +0.574 % [−0.240, +1.372]; ON−OFF −0.17 % p=0.57 | PARTIALLY (within gate-definition noise; ON−OFF ≈ 0 in both) |
| V5-4 | disc, B1 ON | n=1141, +0.00 % [−0.32, +0.34] | n=1168, −0.059 % [−0.362, +0.276]; **ON−OFF −0.70 % [−1.43, −0.05], p=0.98** | CONFIRMED (and the gate has the wrong sign) |
| V5-5 | disc, B2 ON | n=346, +0.75 % | n=0: with a strict SPY calendar, B2 is undefined after the 2024-09 data hole (see §4) | UNVERIFIABLE (not refuted) |
| V5-1..V5-5 | confirmatory 2004-2013 | n=0 | not run by executor; not run by verifier | UNVERIFIABLE (nothing computed) |

Holm (m = 5, seen window, one-sided wider-cluster p): V5-1 0.051, V5-2 0.051, V5-3 0.012, V5-4 0.010, V5-5 0.051 — matches the executor's 0.051 / 0.051 / 0.000 / 0.002 / 0.051 in ordering and conclusion.

### Mandatory regime halves (recomputed)

| Cell | 2014-09..2019-12 | 2020-01..2024-08 |
|---|---|---|
| V5-1 | n=227, **−0.389 % [−0.615, −0.148]**, q+ 0.10 | n=495, +1.278 % [+0.40, +2.19], q+ 0.63 |
| V5-2 | n=1007, **−0.179 % [−0.298, −0.044]**, q+ 0.20 | n=2091, +0.513 % [+0.12, +0.91] |
| V5-3 (B1 ON) | n=37, −0.093 % [−0.50, +0.36]; ON share of days 13.8 %; ON−OFF +0.35 % p=0.08 | n=349, +1.734 % [+0.57, +3.13]; ON share 71 % |
| V5-4 (B1 ON) | n=156, −0.035 % [−0.73, +0.45]; ON−OFF +0.17 % p=0.24 | n=1495, +0.709 % [+0.31, +1.15] |
| V5-5 (B2 ON) | n=92, −0.509 % [−1.01, +0.04]; ON−OFF −0.20 % | n=301, +1.613 % [+0.54, +2.91] |

Executor's half rows agree (V5-1 H1 −0.389 [−0.619, −0.163]; V5-3 H1 n=37 −0.093; V5-4 H1 n=155 −0.032). No cell has a positive 2014-2019 half; the entire seen-window effect is 2020-2024. In the 2014-2019 half the B1 gate is OFF 86 % of the time and its ON subset (n=37 E1, SE ≈ 1.3 %) carries no measurable effect — the gate "works" there only by abstaining.

## 3. Per-event accounting — exact reproduction

Outer merge of the executor's E2 rows with the independently generated events: 4,880 common, 0 events only in mine, 33 only in the executor's (all dated 2024-09-12..2024-10-07, explained in §4). On the 4,880 common events `gap, rvol, adv_d, close_T, open_T, open_T1, net, exc, spy_leg` agree to ≤ 4e-6 absolute (float noise); `isE1` agrees 100 %; executor `B1` agrees with the registered B1 on 97.7 % of events, `B2` 100 % through 2023.

## 4. Look-ahead audit (40 random rows of `EXEC_v5_trades.csv.gz`, seed 7, recomputed from the raw ticker CSVs and the SPY calendar)

All 40 rows pass every check: gap from Close_T−1; RVOL and ADV20$ from rows T−20..T−1 only; prev close ≥ $5, ADV ≥ $50 M, ≥ 250 prior rows; `close_T`/`open_T` equal the raw prints; cost tier from ADV20$ through T−1; E1/E2/M1 flags re-derived; exit date is the **next SPY trading day** and `open_T1` equals that raw open; `net` = Open_T+1(1−c)/(Close_T(1+c))−1; `spy_leg` from the same two SPY rows; `exc` = net − spy_leg; `exc` does not use Close_T+1; executor `B1` equals the verifier's B1 on all 40 dates. One `earnings` flag differs (DDS 2022-05-12) — a logged descriptive flag, used only in the V5-7c slice, never in a trading cell.

Engine review after the fact (`EXEC_v5_engine.py`):
- **B1** (`b1_series`): uses only E2 events with date ≤ T−2, i.e. resolved at Open_T−1 — matches the registration's "T+1 open ≤ T−1"; no look-ahead. Window is 63 event-days ending T−2 (`[T−64, T−2]`) versus my `[T−63, T−2]`; this one-day difference is the whole source of the 2.3 % gate disagreement and of the q+ gap in V5-3/V5-4.
- **B2**: `csd` of eligible names (≥ 50 names), 21-day mean shifted one day, ≥ trailing-504-day median shifted one day. Matches registration; no look-ahead.
- **C1 controls**: same day, same ADV$ decile within the eligible universe, not E2 on T, |gap| < 1 %, RVOL < 1.5, next open exists; 5 draws without replacement, seed crc32(date|ticker); tier cost on the control's own ADV. Matches registration. C2 is the stricter intersection (±1 pp AND [0.75×, 1.25×]) → 39 % coverage; documented in things_tried.
- **Universe / features** (`features()`): identical formulas to the registration and to mine.

Data-panel finding (not a look-ahead, but must be on record): the two directories leave a **hole of 7 trading days, 2024-09-03..2024-09-11, with zero names having data** (10y ends 2024-08-30; `bars1d_all` starts 2024-09-12). The executor's panel index is the union of ticker dates, so it silently skips the hole: (a) 3 E2 / 1 E1 events dated 2024-08-30 are exited at the 2024-09-12 open (a 13-calendar-day "overnight"); (b) ADV20/RVOL for the 20 days after the hole bridge it (the 33 executor-only events); (c) the executor's B2 series bridges it, whereas a strict SPY-calendar implementation makes B2 undefined for the following 504 days — hence V5-5 discovery cannot be reproduced without adopting the executor's convention. None of this touches the seen window; on the discovery window it moves n by ≤ 2 % and the means by < 0.05 pp for V5-1..V5-4.

## 5. Concerns

1. **No cell passes, and the executor says so.** Every pass criterion needs the confirmatory window; it was not run. The seen-window results for V5-3/V5-4 are "expected by construction": B1 was designed after seeing that 2015-2019 was negative and 2020-2024 positive, and on the seen window it does exactly that.
2. **The q+ ≥ 0.60 flag on V5-3/V5-4 is fragile**: 0.652/0.640 in the executor's run, 0.565/0.560 with a one-day-different B1 window; four ON quarters have ≤ 5 events (2017Q4 n=1, 2022Q1 n=5, 2022Q2 n=4, 2022Q4 n=4). Do not cite "clears every checkable gate" without this caveat.
3. **B1 fails its only quasi-out-of-sample check.** The discovery window (2024-09..2026-09) was not used to design B1. There, ON−OFF is ≈ 0 for E1 (mine −0.17 %, executor +0.16 %, p ≈ 0.5) and **negative for E2** (mine −0.70 % [−1.43, −0.05]; executor −0.48 %). A trailing-P&L brake that does not separate ON from OFF on the first period it did not see is weak evidence for a structural regime detector; the confirmatory window remains the decisive test, and this result lowers the prior for it.
4. V5-2's lower bound is zero to within bootstrap noise (+0.007 claimed, −0.006 recomputed); V5-5's is +0.007 recomputed. Neither should be described as "excluding zero".
5. Labelling nit: the registered V5-1/V5-2 cell is K=5/night; the executor's "primary" rows are all events (723/3101) with the K5-filled subset (708/2796) reported as a variant. Numbers differ by ≤ 0.05 pp, but the primary should be the registered one.
6. SPY benchmark comes from a single yfinance file shared by executor and verifier (spot-checked correct on four known extreme days).
7. Power: the 2014-2019 ON subsets (n=37 E1, n=156 E2) have SE ≈ 1.3 % / 0.3 % — they cannot show a positive gated effect even if one existed; they can only fail to contradict the "gate abstains" reading.

## 6. Verdict summary

- Reproduced (numbers survive): V5-1 seen, V5-5 seen, M1 seen, V5-1 disc, V5-2 disc, V5-4 disc; V5-3 / V5-4 seen estimates and CIs reproduced, their quarters-positive shares not; V5-2 seen point estimate reproduced with a zero-straddling lower bound; V5-3 disc consistent within gate-definition noise.
- Not reproducible with the verifier's strict calendar: V5-5 disc (data-hole convention).
- Not verifiable: confirmatory cells (not run), V5-6 (not recomputed).
- **Cells surviving verification as a tradable pass: none.** The registration itself allows no pass without the confirmatory window; the seen-window halves are ≤ 0 for every cell in 2014-2019; and the gate that carries V5-3/V5-4 fails its quasi-OOS check on 2024-2026.
