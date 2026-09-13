# DESIGN_synthesis — judge's verdict and the v5 registration

Written 2026-09-13 by the JUDGE agent. Inputs: five independent proposals (EVENTQ, RM, EXR, SK, OP), the holdout report (`OVERNIGHT_holdout_report.md`), the original registration (`preregistration_overnight.json`), and the on-disk state (bars1d_2004_2013 at 982/3,179 files; `earnings_dates.csv` present, 1.6 MB; `SPY_10y.csv` absent from `research/`).

Registration file: `C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad/research/preregistration_v5.json`

---

## 1. One-paragraph verdict

All five proposals converge on the same trade because the evidence leaves exactly one leg standing: buy the closing auction of a big-catalyst day (E1: gap >= 8%, volume >= 3x ADV20, green close; E2: gap >= 3%, volume >= 2x, held), sell the next opening auction, market-on-close in and market-on-open out, no intraday exit, no hold past the open. They differ on sizing (5% to 20% per slot), on which regime gate to register, on how hard the pass criteria bite, and on how much operational scaffolding to build before the confirmatory test. The synthesis takes the trade shape and exchange-aware execution from EXR, the population-based zero-threshold gate and OFF-cell falsification from RM, the placebo/mirror/matched-control/survivorship-haircut/GREEN-AMBER-RED machinery from SK, the earnings tag, deal-pin exclusion and slice discipline from EVENTQ, and the shadow log schema, NO-TRADE records and counterfactual-exit columns from OP. It rejects 20% slots, pooled seen+unseen pass criteria, fitted thresholds as gates, building the product before the confirmatory run, and any price or technical exit.

Two facts confirmed while judging shape everything below: E2 was negative in 4 of 5 years 2015-2019 (not only E1), so **neither unconditional cell can pass the mandated 2014-2019-half criterion**; the only route to capital under the brief is a **regime gate validated out of sample on 2004-2013**. That is what v5 is built to test.

---

## 2. Scores (1-5)

| Lens | Evidence fit | Tradeability | Validation rigor | Originality | Notes |
|---|---|---|---|---|---|
| EVENTQ (event quant) | 5 | 4 | 3 | 4 | Best rule hygiene (verbatim E1/E2, rejects E3/E4, deal-pin, EDGAR earnings source). Rigor docked: unseen pass = point estimate >= 0 only, leans on a pooled 2004-2026 test that mixes the seen window; brake R5 evaluated on the full series including seen data; 7 cells. |
| RM (regime microstructure) | 4 | 3 | 5 | 5 | Only proposal that makes the regime claim falsifiable: seen-window sanity check before touching the unseen window, OFF cell, quarterly rank correlations with Holm. Tradeability docked: 20%/slot, 100% gross. Overnight ladder is the one new leg family not already ruled out. |
| EXR (execution and risk) | 5 | 5 | 4 | 4 | Most useful practitioner content: per-event distribution (median -0.22%, top-5% carry, sd by year), friction ledger, exchange-aware cutoffs, 7.5% x 3 with drawdown ladder, capacity. Rigor docked: 90% CIs, dispersion threshold fitted on the seen split, >= 25% slice registered from a seen slice. |
| SK (statistician skeptic) | 5 | 3 | 5 | 4 | Highest evidential standard: C1/C2 controls, +0.25/+0.15% survivorship haircut, placebo leg, mirror event, halves, winsorized/trimmed, frozen file list, verdict computed in code, CUSUM with ARLs. Tradeability docked: thin operations; 5% x 5 is fine. |
| OP (operator product) | 4 | 4 | 3 | 3 | Best day-to-day loop (statuses, MISSED_CUTOFF, fill form, Sunday report, counterfactual columns). Rigor docked: no Holm, controls only "reported alongside", brake pass = braked >= unbraked, product built on days 2-8 before the confirmatory run, 20% slots and a 20% sleeve drawdown kill. |

---

## 3. The synthesized design (what v5 registers)

**Trade.** E1/E2 verbatim from the holdout. MOC buy at the official close of T (NYSE tickets by 15:49, Nasdaq by 15:54; missed cutoff = no trade; no LOC, no chase). MOO sell at the official open of T+1 (tickets 08:30-09:20; halted names rest to the reopen). Max 5 positions, E1 first then E2, ranked by day return (the fill order the holdout K5 series already priced). No stops, no targets, no pre-market, no hold past 09:30 in v5.

**Live decision at 15:44 ET.** D1: gap >= 8%, cumulative volume >= 2.7x ADV20sh, last >= open x 1.005. D2: gap >= 3%, >= 1.8x, last >= open. No volume projection. Fidelity measured on bars5m_inplay before shadow (V5-8): coverage >= 70%, conversion >= 85%, flip <= 15%. Conversion 70-85% -> 15:58:30 marketable limit with +10 bp; < 70% kills the close-entry design.

**Costs.** 5 bp/side (ADV20 >= $100M), 10 bp/side ($50-100M), on the trade's own notional; flat 10 bp reported as sensitivity; shadow must show median |fill - print| <= 10 bp/side.

**Benchmarks.** SPY over the identical leg AND C1 = 5 random same-day, same-ADV-decile non-event names (|gap| < 1%, volume < 1.5x), fixed seed; C2 = 3 nearest same-day non-event names by day return (diagnostic: does the catalyst volume matter or any big up day?).

**Regime gates (the heart of v5).**
- **B1** (primary): ON iff the trailing-63-trading-day mean net excess of ALL resolved E2 events (E1 included, traded or not) > 0; 126 days if < 30 events. Zero threshold, population-based (75-125 events per window, SE ~0.3-0.4%), computable every morning. Must pass a seen-window sanity check BEFORE the unseen run (OFF >= 60% of days 2015-2019, ON >= 60% of days 2020-2021) or the B1 cells are void.
- **B2** (mechanism): ON iff trailing-21-day universe cross-sectional dispersion >= its trailing-504-day median. Hypothesis-driven, independent of the event rule and of the strategy's own P&L, no calibration. Validated by ON-OFF difference and a quarterly Spearman on 2004-2013.
- Live composition pre-declared: B1 if a B1 cell passes; B2 (E1 only) if only V5-5 passes; both passing -> full/half/zero by count of gates ON.

**Sizing.** Fixed dollar sleeve S. E1 7.5% of S, E2 3.75%, max 5 positions (37.5% gross), per-name cap w x 3 x ATR20% <= 2% of S. Pilot at half weights for 100 events. Drawdown ladder -6% halve / -10% zero and review. Risk rule: skip the night if SPY <= -3% on T. Worst case stated: -30% name = -2.25% of S; five names on a Mar-2020 overnight ~ -4.2% of S.

**Cells (8).** V5-1 E1 unconditional (base; cannot pass, known), V5-2 E2 unconditional (base; cannot pass, known), V5-3 E1|B1, V5-4 E2|B1, V5-5 E1|B2, V5-6 overnight ladder (research, not traded), V5-7 pre-declared slices (extreme mover, deal-pin, earnings, tiers, risk-rule costs, capacity, rank non-inferiority), V5-8 execution fidelity + shadow. Holm family = the five trading cells (m = 5; m = 6 if the earnings slice is promoted by the coverage rule run BEFORE the confirmatory run; m shrinks if the B1 sanity check voids V5-3/V5-4).

**Pass (trading cells).** Holm one-sided p < 0.05; wider-of-day/quarter-cluster 95% lower CI > 0 in the confirmatory window AND in the 2014-09..2019-12 half, OR (gated cells) ON-OFF >= +0.50% (E2: +0.30%) with p < 0.05, OFF <= ON in both confirmatory halves, ON share 25-75%, OFF not significantly positive, and for B2 a positive quarterly Spearman; quarters positive >= 60%; n >= 300 (E1) / 600 (E2) in the confirmatory window; C1 excess >= +0.25%/+0.15% with lower CI > 0; placebo, mirror, liquidity-tercile gates. All-gates-but-n = AMBER, never a pass.

**Decision tree computed in code.** GREEN -> standalone shadow module (no shared code with scanner or themes) -> 60-day shadow -> pilot -> full. AMBER -> shadow only, re-decide on the completed download or after 12 months of shadow pooled with the confirmatory events. RED -> stop; the fresh-start search has exhausted intraday, multi-day and overnight continuation.

**Kill rules K1-K9** (regime artefact, wrong mechanism, name characteristic, survivorship signature, gate does not capture the regime, untradeable at the close, cannot execute, live monitors incl. CUSUM and drawdown ladder, design integrity) are in the JSON.

---

## 4. Conflicts and how they were resolved

| Conflict | Options on the table | Resolution and why |
|---|---|---|
| Position size | 20% x 5 (RM, OP), 10%/5% x 5 (EVENTQ), 7.5% x 3 (EXR), 5% x 5 (SK) | 7.5% E1 / 3.75% E2 x 5 with ATR cap and drawdown ladder. Per-trade sd 7.9% and a -26% observed worst trade make 20% slots a -5% single-name night; 5 slots rather than 3 because E2 fill-in makes slots bind on cluster nights and 90% of E1 events fit within 5. |
| Which gate to register | Trailing-100-event own P&L (EVENTQ), trailing-63-day E2 population (RM), realized sd >= 5% (EXR), activity rate >= seen median (SK), trailing-40 pooled + SPY -3% (OP) | B1 = RM's population gate (zero threshold, best powered, has a pre-declared sanity check and an OFF cell). B2 = universe dispersion at its own median (hypothesis-driven, no calibration). EXR's fitted 5% threshold and SK's seen-median theta are reported as diagnostics, not gates, because a fitted threshold confirmed on one unseen decade is weaker than a zero/median threshold. OP's SPY -3% rule kept as a risk rule, not alpha. |
| Ranking within a night | Day return (RM, EXR, SK) vs RVOL (EVENTQ, OP) | Day return: it is what the holdout K5 series priced. RVOL is a V5-7g non-inferiority diagnostic; a v6 may switch. |
| Pass window | Pooled 2004-2026 (EVENTQ) vs unseen-only (SK, RM, EXR, OP) | Unseen-only for pass; pooled reported for context. Pooling mixes the seen window into the verdict. |
| CI level | 90% (EXR) vs 95% (others) | 95%, wider of day/quarter cluster, as the brief mandates. |
| Earnings | Split and trade differently (EVENTQ), exclude own-earnings-tonight (EXR), diagnostic (SK, OP) | `earnings_dates.csv` exists, so: slice always; promoted to a sixth trading cell only if coverage >= 60% of 2004-2013 E1 events, decided BEFORE the run so m is fixed. EXR's own-earnings-tonight exclusion is folded into the earnings tag (report dated T-1 AMC = the catalyst is the earnings; a report dated T+1 BMO would be a new binary and is logged). |
| Build before or after the confirmatory run | OP builds the product on days 2-8; others test first | Test first. The only code written before the confirmatory verdict is the engine, the fidelity study and the SPY fetch. The shadow module is built only on GREEN/AMBER. |
| Not-daily-out | RM's overnight ladder vs everyone else's strict one-night | Ladder registered as research cell V5-6, not traded in v5; it is the only multi-day shape the evidence does not already rule out (evidence 2 measured holds from the T+1 open, not repeated close-to-open legs). |
| Price/technical/fixed-return exits (the operator's words) | EVENTQ's shadow +3% pre-market limit; OP's counterfactual columns | Counterfactual columns only (cf_next_close, cf_1000, cf_tp5_touch, cf_hold_if_green); pre-registered expectation that none beats the open; a v6 forward test needs > 0.30% with lower CI > 0 after 120 events. The pre-market limit variant is dropped: daily bars cannot test it and thin pre-market books cost more than the auction on average. |

---

## 5. Rejected ideas and why

- **20% per slot / 100% gross** (RM, OP): incompatible with a 7.9% per-trade sd, a -26% observed worst trade and no overnight stop.
- **Pooled 2004-2026 as a pass criterion** (EVENTQ R1): the 2014-2024 component is seen; a pooled pass would be a partially in-sample pass.
- **Unseen pass = point estimate >= 0** (EVENTQ): below the brief's standard; cannot distinguish a real +0.8% from noise at n = 200 (SE 0.56%).
- **Fitted regime thresholds as gates** (EXR's 5% realized sd, SK's seen-median activity rate): reported as companion variables; a zero/median threshold that needs no calibration is what gets registered.
- **Track-record brake evaluated by non-inferiority on the full series** (EVENTQ R5): the full series includes the seen window; replaced by an OFF cell and an ON-OFF difference on the unseen window.
- **90% confidence intervals** (EXR): brief mandates 95%.
- **Building the Railway product before the confirmatory run** (OP days 2-8): sunk cost pressure on a RED verdict; the product is built only on GREEN/AMBER.
- **Pre-market +3% limit-sell variant** (EVENTQ type A): untestable on daily bars, thin books, and it caps exactly the right tail that carries the mean.
- **$20-50M ADV tier** (EXR-2): outside the registered universe; 10 bp/side and auction impact erode a median trade of -0.22%; a v6 cell may register it.
- **Earnings tagging via EDGAR** (EVENTQ): superseded by the delivered `earnings_dates.csv`; kept as the fallback source if coverage of 2004-2013 is poor.
- **Extreme-mover (>= 25%) selection as a rule** (EXR-5): origin is a seen slice; expected n in the confirmatory window < 300; registered as a declared slice so a v6 may act on it.
- **Any intraday technical exit, price target, or hold from the T+1 open**: measured negative in this project; K9 makes adding one a registration-voiding event.
- **Repurposing app.py / the scanner service for the live module** (OP): the live module is a separate process; CLAUDE.md invariants and the brief's 'no shared code' both point that way.

---

## 6. Build plan (in order; nothing computes a return on 2004-2013 before step 3 is complete)

1. **Freeze.** `sha256sum preregistration_v5.json` -> `research/V5_prereg_hash.txt`; append the 8 cell rows to `research/things_tried.csv` (who = V5, status pre-registered, hash in note).
2. **Pre-run checks (seen data only).** `V5_prechecks.py`: (a) B1 walk-forward on `OVERNIGHT_holdout_trades.csv.gz` -> yearly ON share; sanity gate OFF >= 60% 2015-2019, ON >= 60% 2020-2021; (b) `earnings_dates.csv` coverage of 2004-2013 E1 events by date range and ticker match -> promote V5-7c or not; (c) log both and the resulting Holm m to things_tried.csv. Also locate SPY 2014-2024 (`bars1d_10y/SPY.csv`, else refetch to `research/V5_SPY_2014_2024.csv`) and fetch SPY 2004-2013 via yfinance with 5 retries into `research/V5_SPY_2004_2013.csv`.
3. **Engine.** `V5_engine.py` on daily bars: universe with T-1 data only; E1/E2/M1 flags; earnings, deal-pin, extreme-mover, tier, macro_eve flags; B1 and B2 daily series (plus catalyst rate, attention concentration, realized sd); K=5 E1-first day-return fill; tier costs; SPY, C1 (seeded), C2 legs; placebo leg; ladder legs; day- and quarter-cluster bootstraps (2,000 reps); Holm; yearly/quarterly/halves/winsorized/trimmed/tier tables; K5 per-capital series at 7.5%/3.75%; verdict computed in code. Unit-test on synthetic bars and reproduce the holdout's E1/E2 cells on 2014-2024 to within rounding before the unseen run.
4. **Fidelity (V5-8 Part A).** `V5_proxy_fidelity.py` on `bars5m_inplay` + `bars1d_all`: coverage, conversion, flip rate, drift, overnight leg of D1-but-not-E1 names -> `V5_proxy_report.md`; decide MOC vs 15:58:30 fallback.
5. **Confirmatory run, once.** When the download completes or stalls: freeze the file list, run `V5_engine.py` on 2004-2013 (label 'confirmatory, unseen' or 'partial'), then on 2014-2024 (label 'refinement on a seen window') and 2024-2026 (descriptive). Outputs `V5_cells.csv`, `V5_trades.csv.gz`, `V5_report.md` with GREEN/AMBER/RED computed. Append outcome rows; mark 2004-2013 SEEN. If 'partial', the single pre-declared re-run happens on completion.
6. **On GREEN or AMBER: shadow module.** Separate process (not app.py, no imports from scanner or themes): 07:00 universe build; 15:35 watch snapshot; 15:44 decision + gate + [ON-ORDERS]/[ON-NOTRADE] record by 15:45:30; 16:10 reconciliation; 08:30 exit record; 09:40 settlement vs official prints; append-only log with the V5-8 schema backed up to origin/data-backups; Sunday summary. Runs on Railway because FMP real-time is production-only. Deploy only with the operator's explicit OK (release hygiene applies if anything in the repo changes).
7. **Shadow >= 60 trading days** against V5-8 Part B; fix plumbing only; no rule changes.
8. **On GREEN + shadow pass: pilot** at half weights for 100 events (operator places MOC/MOO tickets manually), then full weights under the drawdown ladder, CUSUM and K8 monitors. AMBER: shadow continues to the re-decision point. RED: stop and report.

---

## 7. Questions for the operator

1. **Sleeve size.** What fixed dollar amount is the sleeve S? Sizing is the only stop; the worst-case statements are in % of S and the 7.5% weight assumes a $50k-per-name ceiling before opening-auction impact matters.
2. **Broker mechanics.** Please confirm your broker accepts MOC on both NYSE and Nasdaq names up to the 15:50/15:55 cutoffs, MOO up to 09:28, and how it handles a halted name at the open. If MOC is restricted the 15:58:30 fallback becomes the default and costs rise ~10 bp.
3. **Availability at 15:44-15:49 ET.** The design has a five-minute manual window every event day (roughly 21% of days for E1, 56% including E2). Is that acceptable, or should v6 register a broker-API auto-submit path?
4. **Acceptance of an overlay.** Average exposure is 5-15% of S and the book is idle most nights; expected value is low-to-mid single digits of S per year in a good regime and roughly zero to slightly negative in a bad one. Is that worth running as a standalone sleeve?
5. **Regime-dependence tolerance.** If the confirmatory verdict is AMBER (gate works but n is short), are you willing to run 12 months of shadow with no capital before deciding?
6. **Earnings file.** The acquisition agent delivered `earnings_dates.csv`; do you know its date range and source? Coverage of 2004-2013 decides whether the earnings cell is promoted.
7. **Overnight ladder.** If V5-6 passes as a research cell, do you want a v6 registered for it (it is the only shape that honours 'not necessarily daily-out'), or keep the product strictly one-night?
8. **Deployment.** The live/shadow module is a new separate process; confirm you want it on Railway (needed for FMP real-time) and that a repo change to add it will go through the usual explicit-OK-before-push rule.

---

## 8. Post-registration notes (facts checked after the files were written; no rule changed)

- `preregistration_v5.json` validated (8 cells); SHA-256 `6222b79dd312090e7bab4d980675d93fac8a11f2abca94daf62c7f3bece2b1e1` written to `research/V5_prereg_hash.txt`. Build step 1 is therefore already done; the things_tried.csv rows remain to be appended by whoever runs step 2.
- `earnings_dates.csv`: 107,367 rows, columns `ticker,date,timing,source`, 3,025 tickers, dates 2004-07-31..2026-09-11. Rows per year: ~700 in 2004, ~2,550-3,400 in 2005-2013 (roughly 650-850 tickers x 4), ~10,000-11,300 in 2014-2018, ~3,500 in 2019-2026. The sampled `timing` values are `unknown`, so the registered "ambiguous timing counts either T or T-1" rule will do most of the work. Coverage of 2004-2013 E1 events is therefore uncertain and the pre-run promotion check (>= 60% resolvable -> Holm m = 6) is genuinely undecided; it must be run and logged before the confirmatory run, exactly as registered.
- bars1d_2004_2013 stood at 982 of 3,179 files at registration; the 'partial' label and the single pre-declared re-run apply unless the download reaches 1,500 files before the run.
