# v5 Plan: Catalyst-Close-to-Open (CCO), a Fresh Start Built on Where Continuation Actually Occurs

**Date:** 2026-09-13. Operator brief: start afresh; pick up trades early while momentum is building; sell above a threshold, on a technical, or at a fixed return; sustainable entries and exits; not day-bound but not a long hold.
**Provenance:** exploratory decomposition → registered overnight holdout (2014–2024) → five independent design proposals → judge synthesis and registration (`preregistration_v5.json`, SHA-256 in `research/validation/v5/V5_prereg_hash.txt`) → executor run on seen windows → independent verification → single confirmatory run on the unseen 2004–2013 decade → independent verification (§6). All files in `research/validation/v5/`.

---

## 1. What the evidence says about the brief

Three tests in this project, on different data, agree that **intraday** continuation in US stocks is negative after costs: the 100-day live scanner (−0.23R per trade), 753k random bars of large caps (−0.07R before costs), and the day's biggest gap-ups under momentum rules (negative in both windows). A two-year multi-day continuation family entered at the next open also underperforms SPY. So "buy strength during the session and exit on a technical" is on the wrong side of a well-documented pattern: individual-stock momentum accrues **overnight**, and the following session reverts.

**Decomposition (2024-09 → 2026-09, 2,700 names, SPY-adjusted, gross):** after a big-catalyst day (gap ≥ 8%, volume ≥ 3× the 20-day average, green close) the close-to-next-open leg is +0.64% [+0.05, +1.24] (n = 504); the next session is +0.02%; five days later −1.25%. Gap-ups held into the close: overnight +0.19%, next session −0.22%. This reframes the brief: "early" is the closing auction of the first day the catalyst is observable; "threshold" is the next opening auction, a time threshold rather than a price one, because every price or technical exit tested here has been negative.

## 2. Registered overnight holdout (rules fixed, run once, 2014-09 → 2024-08, 3,179 names)

| Event at the close | n | Net excess over SPY, close → next open | 95% CI | Quarters positive | Pass |
|---|---|---|---|---|---|
| E1 big catalyst (gap ≥ 8%, vol ≥ 3×, green) | 722 | **+0.79%** | [+0.08, +1.48] | 41% | no (Holm p 0.105) |
| E2 gap-up held (gap ≥ 3%, vol ≥ 2×) | 3,098 | +0.32% | [+0.05, +0.62] | 44% | no (Holm p 0.072) |
| E3 strong close on volume | 4,750 | −0.02% | [−0.18, +0.14] | 36% | no |
| E4 top-20 movers on volume | 31,066 | −0.04% | [−0.11, +0.04] | 31% | no |

Holding to the next close is worse in every cell. **Yearly pattern:** the catalyst effect was negative every year 2015–2019 (−0.07% to −0.50%) and positive every year 2020–2024 (+2.9%, +1.5%, +0.4%, −0.1%, +1.1%). Real in the mean, regime-dependent, not structural. This is why v5 is built as a *gated* strategy and why the only route to capital is a gate validated on data the gate never saw.

## 3. The design (judge synthesis of five independent proposals)

Proposals came from five lenses: event-driven quant, market microstructure and regime, execution and risk, statistician-skeptic, and one-person operations. They converged on the trade and differed on sizing, the regime gate and how hard the pass criteria should bite; the synthesis and rejected ideas are in `DESIGN_synthesis.md`.

**Trade.** Events E1 and E2 exactly as in the holdout. **Buy market-on-close** on the event day T (NYSE tickets by 15:49, Nasdaq by 15:54; missed cutoff = no trade). **Sell market-on-open** on T+1. No stops, no targets, no pre-market, no hold past 09:30. Live decision at 15:44 ET from a batch snapshot: gap ≥ 8%, cumulative volume ≥ 2.7× the 20-day average, last ≥ open × 1.005 (E1 proxy); gap ≥ 3%, ≥ 1.8×, last ≥ open (E2 proxy). Proxy fidelity must be measured on 5-minute bars before any shadow (coverage ≥ 70%, conversion ≥ 85%, flip ≤ 15%).

**Universe.** US common stocks, prior close ≥ $5, 20-day dollar volume ≥ $50M, all filters through T−1. Costs 5 bp/side above $100M ADV, 10 bp/side for $50–100M, on the trade's own notional.

**Regime gates (the heart of v5).** B1: on if the trailing 63-trading-day mean net excess of *all* resolved E2 events is above zero (population-based, zero threshold, computable every morning; 126 days if fewer than 30 events). B2: on if the trailing 21-day cross-sectional dispersion of the universe is at or above its trailing 504-day median (mechanism-driven, no calibration). Pre-declared composition: B1 if a B1 cell passes; B2 for E1 only if only that cell passes; both → full, half, or zero size by the count of gates on.

**Sizing and risk.** Fixed dollar sleeve S; E1 7.5% of S, E2 3.75%, at most 5 positions (37.5% gross), per-name cap so that 3 × ATR20 ≤ 2% of S; pilot at half weights for the first 100 events; drawdown ladder halves at −6% of S and stops at −10% for review; skip the night if SPY closes ≤ −3%. Per-trade overnight standard deviation is about 7.9% with a fat left tail (a −26% overnight was observed), which is why 20% slots were rejected.

**Benchmarks.** SPY over the identical leg, and C1 = five random same-day non-event names in the same liquidity decile (fixed seed), C2 = nearest non-event names by day return.

**Registered cells (8).** V5-1 E1 unconditional; V5-2 E2 unconditional (both already known to fail the 2014–2019 half); V5-3 E1 gated by B1; V5-4 E2 gated by B1; V5-5 E1 gated by B2; V5-6 overnight ladder (research only, the one multi-day shape not already ruled out); V5-7 pre-declared slices (≥ 25% movers, deal-pin exclusion, earnings vs non-earnings, liquidity tiers, the SPY −3% rule, capacity, rank rule); V5-8 execution fidelity and shadow criteria.

**Pass criteria (trading cells).** Holm one-sided p < 0.05 across the five trading cells; wider-of-day/quarter-cluster 95% lower CI > 0 in the confirmatory window *and* in the 2014–2019 half, or, for gated cells, ON−OFF ≥ +0.50% (E2: +0.30%) with p < 0.05, OFF ≤ ON in both confirmatory halves, ON share 25–75%, OFF not significantly positive, and a positive quarterly rank correlation for B2; ≥ 60% of quarters positive; n ≥ 300 (E1) / 600 (E2) in the confirmatory window; matched-control excess ≥ the survivorship haircut (+0.25% / +0.15%) with lower CI > 0; placebo, mirror-event and liquidity-tercile checks. All gates but n → AMBER, never a pass. **Decision computed in code:** GREEN → standalone shadow module → 60-day shadow → half-weight pilot for 100 events → full weights under the drawdown ladder and CUSUM monitors. AMBER → shadow only, re-decide after 12 months pooled with the confirmatory events. RED → stop; the fresh-start search has then exhausted intraday, multi-day and overnight continuation.

## 4. Seen-window results (refinement, 2014-09 → 2024-08; expected by construction for the gated cells)

Net excess over SPY, tier costs, wider of day/quarter cluster CI. Numbers independently reproduced to bootstrap noise by a separate agent with its own engine; 40-row audit found no look-ahead.

| Cell | n | Excess | 95% CI | 2014–2019 half | Quarters positive | Matched control C1 | Note |
|---|---|---|---|---|---|---|---|
| V5-1 E1 unconditional | 723 | +0.76% | [+0.07, +1.44] | −0.39% [−0.62, −0.16] | 39% | +0.92% | fails the half criterion |
| V5-2 E2 unconditional | 3,101 | +0.28% | [+0.01, +0.57] | −0.18% | 41% | +0.41% | lower CI ≈ 0; fails the half |
| V5-3 E1 & B1 on | 389 | +1.55% | [+0.52, +2.51] | no effect (n = 37) | 65% (57% under a one-day window shift) | +1.65% | ON−OFF +1.71%, p 0.002; OFF −0.16% n.s. |
| V5-4 E2 & B1 on | 1,662 | +0.64% | [+0.26, +1.02] | ~0 (n = 156) | 64% (56%) | +0.76% | ON−OFF +0.76%, p 0.001; fails the trimmed-mean gate |
| V5-5 E1 & B2 on | 394 | +1.12% | [+0.07, +2.02] | −0.51% | 36% | +1.32% | ON−OFF +0.80%, p 0.09 (fails) |

B1's seen-window sanity check passed (off 81% of days in 2015–2019, on 90% in 2020–2021), but that separation is what B1 was designed on and carries no evidential weight. The instructive fact is the **2024-09 → 2026-09 stretch, which B1 never saw:** E1 ON−OFF is ~0 (+0.16%, p 0.46; verifier −0.17%) and for E2 the gate has the **wrong sign** (ON−OFF −0.48% to −0.70%, OFF significantly positive). B2 does not discriminate there either (Spearman −0.03). The ladder (V5-6) is flat to negative, so the design stays strictly one night. Slices: the entire payoff is carried by movers ≥ 25% (+3.0% vs −0.1% for 8–25%); the SPY ≤ −3% nights were the best nights (n = 7); earnings-tagged E1 events look worse than non-earnings ones but coverage is regime-confounded. Per-capital at the registered weights on the seen window: V5-3 +43% over ten years with a −3.4% maximum drawdown, all of it from 2020 onward.

Verifier's material concerns: the quarters-positive gate is fragile to a one-day ambiguity in the B1 window; V5-2 and V5-5 lower bounds are zero within noise; the seen-window gate results are expected by construction; a seven-trading-day hole (2024-09-03 to 09-11) in the spliced panel affects only descriptive numbers.

## 5. Data acquired for this track

Two years (2,814 names), ten years 2014–2024 (3,179 names) and 2004–2013 (1,225 names; every symbol on the 2026 list that existed with liquid history in 2014 that Yahoo would serve) of daily bars; SPY 2004–2026; an earnings-date file assembled from the Nasdaq calendar and SEC 8-K Item 2.02 filings (107,367 rows, 3,025 tickers, 2004–2026; coverage of 2004–2013 E1 events 29%, so the earnings cell was not promoted and the Holm family stayed at five). Survivorship: all lists are as of 2026-09; delisted names are absent, which favours long continuation rules; the registered matched-control haircut is the mitigation.

## 6. Confirmatory run on the unseen 2004–2013 decade (single pre-declared run)

Frozen universe: 1,225 names, every symbol on the 2026 list that Yahoo served for 2004–2013, which after two throttle-safe passes is effectively every listed name that existed with liquid history in early 2014 (names listed after 2014 cannot have this history). Eligible names per day after the $50M filter: 147–293. Net excess over SPY, tier costs, wider of day/quarter cluster CI. Engine functions copied verbatim from the seen-window engine; registration hash unchanged; run once.

| Cell | n | Excess | 95% CI | Quarters positive | ON share | ON−OFF | Matched control C1 | Holm p | Pass |
|---|---|---|---|---|---|---|---|---|---|
| V5-1 E1 unconditional | 205 | **−0.38%** | [−0.66, −0.09] | 22% | — | — | −0.25% | 1.00 | no |
| V5-2 E2 unconditional | 1,243 | **−0.21%** | [−0.38, −0.01] | 24% | — | — | −0.06% | 1.00 | no |
| V5-3 E1 & B1 on | 42 | −0.72% | [−1.29, −0.24] | 8% | 18% | −0.43% (p 0.91) | −0.71% | 1.00 | no |
| V5-4 E2 & B1 on | 258 | −0.24% | [−0.53, +0.08] | 20% | 18% | −0.03% (p 0.58) | +0.01% | 1.00 | no |
| V5-5 E1 & B2 on | 88 | −0.54% | [−0.96, −0.12] | 16% | 31% | −0.27% (p 0.83) | −0.41% | 1.00 | no |

Both confirmatory halves (2004–2008, 2009–2013) are negative for every cell; the effect is negative in every year 2005–2013 except 2007. The B1 gate was on for only 18% of days because the trailing population mean was negative almost throughout, and its ON events were *worse* than OFF; B2's dispersion rank correlation (+0.36 on the seen window) is 0.01 here. The overnight ladder is −0.30% [−0.43, −0.17]; the ≥ 25% movers that carried the seen-window payoff are −1.5% here. Per-capital at the registered weights loses money in nine of ten years for every cell (E1 sleeve −5.4%, E2 sleeve −13.7%; at 20% slots −14% and −47%). The one-day ambiguity in the B1 window flips 1% of days and moves every gated statistic further negative.

**Decision computed in code: RED.** Kill rule K1 (regime artefact) fires on both clauses: the matched-control excess is ≤ 0 for both base cells, and the best cell's 2004–2008 half is below −0.20%. Independently of K1, no gated cell meets any substantive gate and none is AMBER-eligible (V5-3 fails 12 of 15 gates, V5-4 11 of 15, V5-5 13 of 16). The partial-run AMBER clause does not rescue the verdict: a kill rule is unconditional, and the panel is the complete reachable universe, so the pre-declared completed-download re-run cannot change it. This universe is also the one most biased in the rule's favour (2026 survivors), and the effect is still significantly negative.

**Independent verification (separate agent, own engine from the raw files):** every claimed cell reproduced (n exactly, point estimates to 0.001%, CIs to bootstrap noise, ON shares, ON−OFF differences, Holm = 1.00); K1 fires in the verifier's numbers too and is robust to three alternative control seeds; 40 of 40 audited trades pass every look-ahead check (filters and averages through T−1, entry at the close, exit at the next opening print, B1 built only from events resolved by T−1); the B1 window ambiguity is immaterial. **Decision agreed: RED.** Minor notes: the matched-control decile edge convention is unspecified (17.5% of events draw different controls; aggregates agree to 0.003%); the ladder and mirror cells reproduce exactly.

**Reading.** 2004–2013 looks like 2015–2019, not like 2020–2024: the overnight-after-catalyst return that was worth +0.8% per trade in the retail-flow era of 2020–2021 does not exist in either earlier decade, and neither a strategy-performance gate nor a dispersion gate can identify the good regime out of sample. The registered consequence is to log and stop: with intraday, multi-day and overnight continuation all tested and failed, the fresh-start search space of price-and-volume continuation in US equities is exhausted at retail costs.

## 6a. What is genuinely left, and what is not

- **Not left:** any long continuation rule on price and volume alone, at any horizon from minutes to a week, on any of the three universes tested. Re-parameterising it would be the same mistake the scanner made five times.
- **Left, each requiring new information and its own registration:** (i) *news content* per catalyst (earnings beat, guidance, deal, dilution), because "gap plus volume" mixes events whose overnight hazards differ in sign; (ii) a proper *earnings calendar with timing* to isolate post-earnings drift from generic gaps and to avoid holding into a release; (iii) *order-flow or short-interest* data if a squeeze mechanism is the real driver of the 2020–2024 window; (iv) a *mean-reversion* product, since the one consistent finding across every test is reversal after extension, but the exploratory fade on 2026 data was sign-unstable across months and would need the same discipline. None of these can be evaluated with the data on disk today.
- **Cheapest honest next step:** log the daily catalyst watchlist and the event payload from now on (no capital, no notifications), so that in six to twelve months a regime-aware test has lived data instead of survivor histories.

## 7. Operator decisions the design needs

1. **Sleeve size S.** Sizing is the only stop; worst-case statements are in percent of S (one −30% name = −2.25% of S; five names on a March-2020 night ≈ −4.2%).
2. **Broker mechanics.** Confirm market-on-close is accepted on NYSE and Nasdaq names to the 15:50 / 15:55 cutoffs and market-on-open to 09:28, and how a halted name at the open is handled. If MOC is restricted, a 15:58:30 marketable limit becomes the default and costs rise about 10 bp.
3. **Availability 15:44–15:49 ET** on event days (roughly 21% of days for E1, 56% including E2), or a broker-API auto-submit path in a later version.
4. **Acceptance of an overlay:** average exposure 5–15% of S, idle most nights, expected value low-to-mid single digits of S per year in a favourable regime and about zero in an unfavourable one.
5. **Regime tolerance:** if the verdict is AMBER, 12 months of shadow without capital before deciding.
6. **Deployment:** the shadow module is a separate process (no shared code with the scanner or themes), on Railway because real-time data is production-only, deployed only with explicit approval.

## 8. What this track did not do, on purpose

No intraday technical exits, price targets or holds from the next open (all measured negative here); no fitted regime thresholds as gates; no pooling of seen and unseen windows into a pass; no product code before the confirmatory verdict; no reuse of the scanner's scoring, universe or tiers.
