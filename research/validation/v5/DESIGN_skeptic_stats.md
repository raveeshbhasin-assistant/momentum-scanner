# DESIGN — Statistician-Skeptic lens (key prefix `SK_`)

Date: 2026-09-13. Author: designer 3 of 5. No data touched beyond the files named in the brief; nothing fitted.

## 0. Position

The project has already falsified intraday continuation (753k 5-min bars, 100 live days, -0.23R/trade) and multi-day continuation at T+1 open. The only survivor is a one-leg bet: buy the closing auction of a catalyst day, sell the next opening auction. The pre-registered holdout produced +0.79% (E1) and +0.32% (E2) net vs SPY, but with 41-44% of quarters positive, Holm p 0.07-0.10, negative in every year 2015-2019 and positive in four of five years 2020-2024, and trade-level win share of 42% (median trade loses; the mean is carried by the right tail). Events per year doubled in 2020 (34-60 -> 73-123).

Null hypothesis I hold until the evidence below overturns it: **the overnight catalyst effect is a post-2019 regime artefact (retail/zero-commission/options-flow era) sitting on top of a survivorship-inflated tail**, not a structural feature of US equities. Under that null the correct action is shadow-only, no capital.

The design below is the smallest set of pre-registered tests that could move me off that null, with explicit outcomes mapped to actions. It deliberately does NOT invent new events or exits; changing the rule now would convert a confirmatory test into a new discovery.

## 1. What is seen, what is not, and how each may be used

| Window | Status | Permitted use |
|---|---|---|
| 2024-09..2026-09 (bars1d_all, 2,814 names) | DISCOVERY (decomposition seen) | Nothing evidential. Plumbing only (5-min snapshot check uses 2026 5-min bars). |
| 2014-09..2024-08 (bars1d_10y, 3,179 names) | PARTIALLY SEEN (8 cells, yearly means) | (a) calibrate ONE regime threshold (theta) and log it before the unseen run; (b) descriptive re-benchmark of the 8 seen cells against matched controls (tells us whether 2015-2019 was "effect absent" or "effect reversed"); (c) NOTHING tested here counts toward pass/fail. Every artefact from this window is labelled `refinement-on-seen-window`. |
| 2004-01..2013-12 (bars1d_2004_2013, 982/3,179 names downloaded at time of writing) | UNSEEN | Confirmatory. Run once, after `SK_prereg.json` is hashed and logged in things_tried.csv. Partial download is acceptable only if the file set is frozen before the run and the frozen list is logged; no top-ups after the run. |

Regime halves are fixed now: seen window H1 = 2014-09..2019-12, H2 = 2020-01..2024-08; unseen window U1 = 2004-01..2008-12 (pre-crisis + crisis), U2 = 2009-01..2013-12 (ZIRP recovery). A structural effect should be >= 0 in both U1 and U2; a regime artefact will not be.

## 2. Survivorship in the confirmatory window and how to bound it

The symbol list is as of 2026-09, so a name in the 2004 data survived 22 years. Universe-level survivor premium is real and grows with lookback. Two mitigations, both pre-specified:

1. **Same-universe matched controls cancel the universe-level bias.** Benchmark C1 = 5 random non-event names drawn on the same day T from the same dollar-ADV20 decile (all survivors too). The event-minus-C1 difference is free of the "survivors drift up" component and of the generic close-to-open premium. C1 does not cancel *event-selection* bias (catalyst days in names that later failed are missing; those are plausibly the worst overnight outcomes).
2. **A pre-specified haircut for event-selection bias.** Keeping the registered $50M ADV floor (nominal; stricter in 2004 dollars, which is the direction we want) restricts to names with low negative-delisting hazard (~1%/yr => ~18% of 2004 liquid names removed for negative reasons by 2026; mergers remove more but with benign prior returns). If the missing 18% of catalyst events had overnight mean -1%, the observed mean overstates truth by ~0.2%. Therefore the pass criterion demands **point estimate >= +0.25% (E1) / +0.15% (E2) net vs C1**, not merely > 0. Coverage diagnostic to report: number of names with data per year in 2004-2013 vs the 2014 count, and the fraction of eligible-universe-days covered.

Also reported (descriptive): effect by liquidity tercile within the eligible universe. If the effect lives only in the least liquid tercile, survivorship and cost assumptions dominate and the result is graded AMBER regardless of the CI.

## 3. Event clustering, the unit of analysis, and power

- E1 had 722 events on 535 days (1.35/day); E2 3,098 on 1,398 days (2.2/day). Overnight returns are correlated across names on macro nights, so the **day is the primary cluster** and the quarter the secondary cluster. Both CIs are reported; the pass rule requires both to exclude zero.
- The traded object is a **day-level basket**: at most K=5 names per day (registered fill order: E1 first, then by day return). The primary series is the day-level basket net excess; per-trade means are reported alongside for comparability with the seen window.
- Power (sd per overnight trade 3.5%, day design effect ~1.1 for E1, ~1.3 for E2; one-sided alpha 0.05/3 for the smallest Holm p in a family of 3; 80% power):
  - MDE = 2.9 x 3.5% x sqrt(DEFF) / sqrt(n).
  - E1 in 2004-2013: expected n ~150-300 (34-60/yr in the pre-2020 seen years on 3,179 names, fewer names and a stricter nominal floor pre-2014). n=200 -> SE 0.26%, MDE ~0.75%. **E1 is powered to detect a 2020-2024-sized effect (+1.2%) but not a +0.4% effect.** If n_E1 < 200 and the CI includes zero, the verdict is INCONCLUSIVE, not FAIL.
  - E2 in 2004-2013: expected n ~800-1,200 -> SE ~0.12%, MDE ~0.35%. **E2 is the better-powered confirmatory cell** despite the smaller effect; it is the one most likely to deliver a clean answer.
  - Regime-conditioned E1 (SK-3): n ~half of E1 -> MDE ~1.0%; can only pass if the conditional effect is large. Registered anyway because it is the one cell that could convert "artefact" into "regime-conditional rule".
- Shadow/live power is far worse: 60 trading days yields ~15 E1 and ~70 E2 events -> SE 0.9% / 0.4%. **Shadow cannot confirm or refute the edge; it validates plumbing only.** The historical confirmatory test is the only evidential step before capital.

## 4. The registered family (three cells; everything else is diagnostic)

Definitions are copied verbatim from `preregistration_overnight.json` so that 2004-2013 is a true confirmation, not a new cell. Universe on day T uses data through T-1: prev close >= $5, dollar ADV20 >= $50M, >= 250 rows history. Costs: 10 bp/side (double the registered 5 bp; the 5 bp figure is reported as sensitivity). Entry buy at Close_T, exit sell at Open_{T+1}. K=5 per day.

| id | event | test window | Holm family |
|---|---|---|---|
| SK-1 | E1_big_catalyst: gap >= 8%, Volume_T >= 3x ADV20sh, Close_T > Open_T | 2004-2013 (unseen) | yes |
| SK-2 | E2_gapup_held: gap >= 3%, Close_T >= Open_T, Volume_T >= 2x ADV20sh | 2004-2013 (unseen) | yes |
| SK-3 | E1 traded only when activity state A_T >= theta, where A_T = (E1 events in the eligible universe over T-63..T-1) / (mean eligible-universe size over T-63..T-1) x 1000; theta = median of A_T over E1 event days in 2014-09..2024-08, computed once and logged BEFORE the unseen run | 2004-2013 (unseen); theta is `refinement-on-seen-window` | yes |

Diagnostics (pre-specified, reported, NOT counted in Holm, but several are gates):

| id | what | prediction if the effect is real momentum |
|---|---|---|
| SK-C1 | matched random control: 5 non-event names, same day, same dollar-ADV decile, fixed seed = hash(T, symbol); excess = event overnight - mean control overnight | excess > 0 (gate) |
| SK-C2 | move-matched control: 3 nearest non-event names by Close_T/Close_{T-1} within [0.75x, 1.25x] of the event's day return AND Volume_T < 1.5x ADV20 | excess > 0 (gate; if ~0 the driver is "big move", not "catalyst", and E3/E4 already say big moves carry nothing) |
| SK-P1 | placebo leg: same events, Close_{T+5} -> Open_{T+6} vs C1 | |mean| < 0.2%, n.s. (gate; a positive placebo means a persistent name characteristic, e.g. high-beta overnight premium) |
| SK-P2 | mirror event: gap <= -8%, Volume_T >= 3x, Close_T < Open_T; same leg vs C1 | <= 0. If significantly positive with magnitude similar to SK-1, the mechanism is a high-volume overnight premium, not continuation (gate: fail if P2 lower CI > 0 and P2 mean >= 0.5 x SK-1 mean) |
| SK-H | halves U1/U2, yearly table, quarters-positive share, winsorized (1%/99%) mean, leave-out-top-2%-of-events mean | all >= 0 (gates) |
| SK-L | liquidity terciles within the eligible universe | not "only the least liquid tercile" (gate) |
| SK-S | 15:45 snapshot vs close-definition mismatch on bars5m_inplay (929 gappers, 60 days): live rule = gap >= 8%, vol_15:45 >= 2.7x ADV20, last_15:45 >= Open x 1.005. Report agreement rate and overnight return for {both, close-only, snapshot-only} | agreement >= 85% and snapshot-only events not worse than both-events by > 0.3% (else haircut the historical estimate by the difference) |
| SK-R | refinement-on-seen-window: the 8 seen cells re-benchmarked vs C1/C2 by half H1/H2 | descriptive only; answers whether 2015-2019 was "absent" (vs C1 ~0) or "reversed" (vs C1 < 0) |
| SK-EARN | if `earnings_dates.csv` arrives: split SK-1 by earnings-day vs non-earnings catalyst | descriptive only |

Not allowed after the unseen run: any threshold change, any new event/exit, any re-selection of the 2004-2013 file set, any second look at 2004-2013 with a different rule (that window becomes seen the moment SK_confirm runs).

## 5. Decision tree (fixed now)

Holm across {SK-1, SK-2, SK-3} at alpha 0.05, one-sided, primary statistic = per-trade mean net excess vs SPY over the identical leg (comparability with the seen window), with **both** day-cluster and quarter-cluster bootstrap 95% CIs (>= 2,000 reps).

**GREEN — start 60-day shadow, then live at 5% of book per position.** At least one of SK-1/SK-2/SK-3 has Holm p < 0.05 AND, for that cell: both cluster CIs (vs SPY) exclude zero; net excess vs C1 point estimate >= the haircut (0.25% E1 / 0.15% E2) with day-cluster CI > 0; C2 excess > 0; both halves U1, U2 >= 0; >= 50% of quarters positive (weaker than the original 0.60 — set after seeing the 42% win share of the seen window; the 0.60 outcome is also reported and flagged); winsorized and leave-out-top-2% means > 0; SK-P1 null; SK-P2 not "as positive"; SK-L not illiquid-only; SK-S agreement >= 85%; n >= 200 (SK-1/SK-3) or >= 600 (SK-2).

**AMBER — shadow only, no capital; re-decide after 12 months of shadow (~300 E2 events, SE ~0.22%) with the same gates.** Point estimates positive but a gate fails, or the CI includes zero with n below threshold.

**RED — abandon the overnight family, log in things_tried.csv.** Point estimate vs C1 <= 0 for both SK-1 and SK-2 in the unseen window; or either half < -0.2% for the best cell; or SK-P2 mechanism refuted; or SK-S shows the effect disappears when defined at 15:45.

My prior over outcomes: RED/AMBER ~60%, GREEN ~40%. Stated so that a GREEN is read as surprising evidence and not as confirmation of a hope.

## 6. If GREEN: the trading rule, sizing, monitoring and kill rules

Rule (unchanged from registration, made live-implementable):
- 15:40-15:45 ET scan of the eligible universe (us_common_symbols.csv filtered by prev close >= $5, dollar ADV20 >= $50M). Event flags per Section 4 SK-S live definitions (E2 live analogue: gap >= 3%, vol_15:45 >= 1.8x ADV20, last >= Open).
- Submit MOC buys by 15:48 ET (NYSE MOC cutoff 15:50, Nasdaq 15:55). At most 5 names per day, E1 first then by day return; no averaging, no intraday adds.
- Submit market-on-open sells the same evening for Open_{T+1}. No profit target, no stop, no hold past the open (X2 next-close was worse in every registered cell; overnight stops do not exist). The trade is auction-to-auction, fixed duration.
- If SK-3 was the passing cell and SK-1 was not, trade only when A_T >= theta.

Sizing: 5% of book per position, max 5 overnight positions (25% overnight gross) for the first 150 live trades; step to 10% / 50% only if the CUSUM below has not alarmed and cumulative mean net excess > 0. Never above 10% per name: a single -20% overnight gap must cost <= 2% of book.

Monitoring (parameters fixed before shadow starts; ARLs to be verified by simulation on the holdout trade distribution, winsorizing z at +/-3 because tails are fat):
- Lower CUSUM on standardized per-trade net excess z_t = (x_t - mu0)/sigma with mu0 = +0.35%, sigma = 3.5% (fixed from the holdout): S_t = max(0, S_{t-1} - z_t - k), k = 0.05, alarm at S_t > h with h ~ 25. Siegmund approximation: ARL ~2,000 trades if the true mean stays +0.35%, ~330 trades if the true mean is 0. **Honest statement: the per-trade signal is ~0.1 sigma, so no monitor can detect decay to zero in fewer than ~300 trades (~1 year). Sizing must make a year of zero-edge trading cheap (at 5% sizing and 10 bp/side, ~-3% of book).**
- Hard stops that need no calibration: after >= 150 live trades, kill if cumulative mean net excess < 0; after >= 300 live trades, kill if the 95% day-cluster CI still includes 0; kill if strategy drawdown > 6% of book at 5% sizing (12% at 10%). A kill returns the rule to shadow; re-entry requires 60 shadow trades with mean net excess > 0 AND no change to the rule.
- Fill audit: median |fill - assumed price| must stay <= 10 bp/side on each leg over any 40-trade window; if breached, cost assumption is raised and the pass criteria re-evaluated on the historical result.

## 7. Expected economics (skeptic's central case, if GREEN)

- Frequency: E2 (superset of E1) ~300-400 trades/yr under the $50M floor, ~1.3/day, capped at 5/day. E1 ~60-90/yr.
- Per-trade net excess after a 50% haircut on the holdout point estimates and 10 bp/side: E1 ~+0.4%, non-E1 E2 ~+0.15%; blended ~+0.2%.
- Book level at 5% sizing: ~+3-4%/yr excess over SPY with annual sd ~3.5-4% (0.175% per trade x sqrt(350) x sqrt(1.3)); Sharpe ~0.8-1.0 if the effect sits at the haircut level; ~+7%/yr at 10% sizing. At 2015-2019 levels (-0.2%/trade net) the same book loses ~3%/yr. Range: -3% to +7% of book per year. This is a small overlay with lottery-shaped payoffs, not a business, and it uses capital only overnight.

## 8. Biggest risks

1. Regime dependence: five consecutive negative years already observed; the kill rule needs ~1 year to notice a flip.
2. Survivorship: the confirmatory window is the most biased one we have; the haircut is a bound from delisting hazards, not a measurement.
3. Right-tail dependence: 42% win share; the mean lives in ~10% of trades. Position caps prevent lottery sizing but a few missed tail events (early kill, day cap) erase a year.
4. Look-ahead in the daily-bar definition: green close and full-day volume are unknown at the 15:50 MOC cutoff. SK-S bounds this on 60 days only.
5. Opening-auction execution on catalyst names: wide indicative ranges; yfinance Open may be first print not auction print for some names.
6. Multiplicity creep: three cells plus gates already; any fourth cell added later must join the family and re-Holm.

## 9. What would kill it

Unseen-window E1 and E2 both <= 0 vs matched controls, or the mirror event SK-P2 equally positive (mechanism is not continuation), or the effect vanishes when the event is defined at 15:45 rather than at the close, or live opening-auction fills run > 20 bp worse than the daily-bar Open. Any one of these is decisive; none requires a second look at the data.

## 10. Build steps (all under research/, `SK_` prefix; nothing inside C:/dev/Trader-v3)

1. `SK_prereg.json` — verbatim copy of the registered universe/events/costs, plus Sections 4-6 of this document (family, controls, gates, decision tree, CUSUM parameters). Compute sha256, append a row to `things_tried.csv` (`SK,prereg,2004-2013 pending,...`). Freeze the list of files present in `bars1d_2004_2013` at that moment into `SK_frozen_files.txt`.
2. `SK_calibrate_theta.py` — on 2014-09..2024-08 compute A_T on E1 event days, theta = median; also run SK-R (8 seen cells vs C1/C2 by H1/H2). Write `SK_refinement_seen.md`, log both rows as `refinement-on-seen-window`.
3. `SK_snapshot_mismatch.py` — bars5m_inplay: close-definition vs 15:45-definition agreement and overnight returns by group. Write `SK_snapshot.md`. Log.
4. Fetch SPY 2004-2013 via yfinance with retries (3 attempts, exponential backoff); if unavailable, C1 becomes the sole benchmark and this is recorded.
5. `SK_confirm_2004_2013.py` — single run: build eligible universe per day (T-1 data), E1/E2/mirror events, K=5 day baskets, C1 (seeded) and C2 controls, P1 placebo leg; per-trade and day-basket statistics; day- and quarter-cluster bootstrap CIs (2,000 reps); Holm across SK-1/2/3; gates; halves, yearly, quarters, winsorized, trimmed, liquidity terciles, coverage table. Output `SK_confirm_cells.csv`, `SK_confirm_trades.csv.gz`, `SK_report.md` with the GREEN/AMBER/RED verdict computed by code from the pre-registered rules, not by reading the table.
6. Log the run in `things_tried.csv`; mark 2004-2013 as SEEN in the log.
7. If GREEN: `SK_cusum_sim.py` to verify ARLs on the holdout trade distribution; build the 15:45 scanner as a standalone script (FMP quotes in prod), shadow log schema (timestamp, flags at 15:45, assumed vs realized fills), 60-trading-day shadow; then live at 5% sizing with the Section 6 monitors.
8. If AMBER: steps 7's scanner and shadow log only; re-decision at 12 months with the same gates.
9. If RED: log, stop, and report that the fresh-start search has exhausted intraday, multi-day and overnight continuation in this data.
