# WF1: Does following insider purchases and off-exchange flow, conditioned on trend and news context, earn an excess return? (pre-registration v3)

**Status: LOCKED 2026-10-04**, before any insider or flow signal was joined to any real return. The operator confirmed the 63-session primary holding period and asked for the lock and the discovery run on 2026-10-04. Options flow is not part of this registration; if it is pursued it gets its own.

History: v1 written 2026-10-03; v2 folded in the pre-lock red team (2 blockers, 5 major, 19 minor; `redteam_wf1.txt`); v3 added two amendments after the self-test and the event counts. All changes are listed at the end. The v3 code passed `--selftest`, and `--counts` confirmed every sample-size gate is reachable (`things_tried.csv`). SHA-256 of this file, the code and the data files is in `PREREG_sha256.txt`, written before `wf1_run.py --window DISC` ran. Rationale and priors are in `research_plan_v6_whale_flow.md`.

The engine `wf1_run.py` is the definition of every rule below; where prose and code differ, the code as hashed governs.

## Data (point in time)

- **Insider trades:** SEC Insider Transactions Data Sets 2006Q1–2026Q1. Original Form 4 only (4/A ignored), non-derivative table, code P, acquired. Usable from the first session after the SEC filing date.
- **Off-exchange flow:** FINRA Reg SHO daily short-sale volume files, 2009-08-03 onward, summed over reporting facilities. Usable from the first session after the trade date.
- **Prices:** Yahoo daily bars 2004–2026 for the 5,245-symbol 2026 common-stock list, with split events and dividend-adjusted close. For issuers no longer on that list, FMP history for the symbol typed on the form, over the dates the symbol was used.
- **Issuer → series:** by issuer CIK to the 2026 ticker; otherwise `{CIK}_{symbol on form}`. A CIK-matched series is trusted. A symbol-matched (delisted) series may belong to a later user of the symbol, so it must also pass a **point-in-time trust rule:** of the issuer's Form 4 purchase and sale prices on forms filed on or before the event's filing date, ≥ 70% lie within 5% of that day's as-traded low–high range (with fewer than 5 such trades it is accepted). In every case the event's own trades must pass the open-market price check below.
- **8-K record:** `news8k_events.csv` (dated by ET acceptance date) plus the same SEC source for delisted issuers (dated by SEC filing date).
- **Returns:** dividend-adjusted, entry at the open, exit at the close; SPY over the identical span. If a series ends inside the holding period the position is closed at its last bar and SPY is measured to that same bar.

## Universe, costs, timing

- Signal day `s` = the last session on or before the filing date. All filters use data through the close of `s`. Entry = open of `s+1`.
- Eligible: as-traded close ≥ $5, 20-session average dollar volume ≥ $10M, ≥ 200 sessions of history.
- Costs per side: 5 bp (ADV ≥ $100M), 10 bp ($50–100M), 25 bp ($10–50M); charged twice.
- Holding period: **63 sessions** (primary), 21 (secondary, descriptive). Dark-flow cell: 21 sessions.

## Event definitions

**Qualifying purchase:** officer or director; value ≥ $25k; ≥ 90% of its value priced within 5% of the day's as-traded range (open-market, not an offering); shares ≤ 5× the day's volume; filed 0–10 calendar days after the last trade; not routine. No upper value cap.

- *Routine:* the same person bought in the same calendar month in each of the three prior years, on forms already filed before this filing.
- *One trade on two forms* (a person and their trust or fund) counts once: identical trade date, shares and value, with at least one form reporting indirect ownership; the earliest filing is kept. Two insiders buying identical lots directly are two purchases.
- *No trusted series:* the open-market and volume checks cannot be run; the filing still counts toward event construction so that events lost to missing prices can be counted (they are never traded).

**Events**

- **Cluster buy:** ≥ 2 distinct insiders with qualifying purchases, combined ≥ $250k, filed within 30 calendar days; the event is the filing that completes the cluster. All joint filings (several reporting owners on one form) together count as one insider.
- **Conviction buy:** a qualifying purchase by a CEO, CFO or chair (titles containing "former", "emeritus", "retired", and "vice"/"committee" chairs excluded) of ≥ $500k, or of ≥ $100k that raises their holding by ≥ 10%. The holding increase is measured only when every purchase line on the form is the same ownership line; a zero prior balance counts as a new holder and qualifies.
- **Insider event set U:** cluster and conviction events together.
- One event per issuer per 63 sessions in each set.

**Trend state at `s`:** UP = close > 200-session average and ≥ 85% of the 252-session high close. DOWN = close < 200-session average and ≤ 75% of that high. Otherwise NEUTRAL.

**Sharp drop at `s`:** close fell ≥ 15% over the prior 20 sessions. Its news split (an earnings 8-K, any 8-K, or none dated from session `s−20` to the Form 4 filing date) is descriptive only.

**Dark-flow z at `s`:** 5-session off-exchange short volume ÷ 5-session off-exchange total volume, z-scored against the stock's own trailing 252 sessions (≥ 200 required). Valid only if off-exchange volume was 5–120% of the bar's as-traded volume on ≥ 90% of those sessions. 2026-listed symbols only.

**Matched controls:** for each event, five names drawn (fixed seed) from eligible 2026-listed stocks on the same day, in the same ADV decile, the same trend state and the same side of the −15% drop line, excluding any name inside the 63-session holding period of its own insider event, the event's own symbol, and the 2026 symbol matching a delisted event's symbol. Control-adjusted excess = the event's net excess over SPY minus the controls' mean net excess over SPY.

## Primary cells (one Holm family of six)

| Cell | Sample | Tested statistic | Side |
|---|---|---|---|
| WF-1 | Cluster buys | mean 63-session control-adjusted excess | > 0 |
| WF-2 | Conviction buys | same | > 0 |
| WF-3 | U, trend UP vs DOWN | difference in control-adjusted excess | two-sided |
| WF-4 | U, after a sharp drop vs not | difference in control-adjusted excess | > 0 |
| WF-5 | All eligible stocks, every 5th session | top-decile minus bottom-decile dark-flow z, 21-session excess | two-sided |
| WF-6 | U, supportive vs unsupportive dark flow | difference in control-adjusted excess | > 0 |

"Supportive" in WF-6 means the sign of z that WF-5 favours on the discovery window; the discovery run writes it to `out/WF1_wf5_sign.json`, which is hashed at the confirmatory lock, and confirmatory runs refuse to start without it.

Every cell is tested on control-adjusted excess, so that the return of a typical surviving stock of the same size and state against SPY cannot pass as an insider effect. Net excess over SPY is reported for every cell and gated (a signal must also make money against SPY).

## Windows

- **Discovery:** entries 2014-01-01 → 2019-09-30. May be examined. At most one amendment afterwards, logged in `things_tried.csv`, before the confirmatory lock.
- **Confirmatory A:** entries 2020-01-01 → 2026-04-01. Run once (the engine refuses a second run).
- **Confirmatory B:** entries 2006-02-01 → 2013-09-30 (dark-flow cells from mid-2010). Run once. Sign agreement only.
- Discovery and Confirmatory B stop one holding period before the next window opens, so no return day is shared.

## Inference

- Cluster bootstrap of the mean, B = 10,000. Because a 63-session hold spills into the following quarter, three schemes are run (entry day, entry quarter, entry half-year) and the **widest interval and largest p** are reported. Difference cells and WF-5 use quarter and half-year clusters.
- WF-5: mean of the per-date spread; Newey–West (4 lags) t reported alongside.
- Holm adjustment across the six primary p-values within each window.

## Gates (Confirmatory A unless stated) and verdict, computed by `wf1_run.py --verdict`

- **Level cell (WF-1, WF-2) passes:** Holm p < 0.05; control-adjusted lower bound > 0; SPY-excess lower bound > 0; ≥ 60% of calendar years (n ≥ 10) with positive control-adjusted mean; with the five largest-contributing names removed the control-adjusted lower bound stays > 0; n ≥ 300; control-adjusted mean also positive in Confirmatory B.
- **Difference cell (WF-3, 4, 6) passes:** Holm p < 0.05; interval excludes 0 in the stated direction; ≥ 150 events per arm; same sign in B; **same sign when delisted events are removed** (events and controls then carry the same survivor conditioning); and the favoured arm on its own has lower bounds > 0 for control-adjusted and SPY excess.
- **WF-5 passes:** Holm p < 0.05; interval excludes 0; ≥ 60% of years share the sign; same sign in B; and the favoured decile net of costs beats the eligible universe held without trading, lower bound > 0.
- **GREEN:** at least one cell passes → shadow page, no capital. **AMBER:** none passes, but at least one has Holm p < 0.10 with the same sign in B (level cells: control-adjusted mean positive in both; difference cells: favoured arm's mean positive) → log forward only. **RED:** otherwise → stop.

## Accounting, placebos and descriptive cells (never a pass)

- **Event funnel**, per event set and year, written by every run: registered / no price series / series fails the trust rule / ineligible at `s` / no entry bar / tradeable.
- **Survivorship sensitivity:** for events with no usable series and a Form 4 price ≥ $5, the mean excess they would have needed for the cell's SPY excess to be zero (`lost_breakeven_exc`). For holds cut short by the series ending, the SPY-excess mean with the remaining value written down by 30% and by 100%.
- **Routine purchases** run as events: expected ≈ 0.
- **`--selftest`** (run before the lock; v2 code passed on 2026-10-03): each event's issuer replaced by a random eligible name on the same day, every stock given another stock's flow series, and 200 random name-days checked for agreement between the panel path and the single-series path used for delisted issuers. Criterion: every level cell's control-adjusted interval contains 0 and every primary Holm p > 0.10. The level cells' SPY-excess mean in the selftest is the survivor-versus-SPY baseline and is reported.
- **`--counts WINDOW`** (run before the lock): funnel and arm sizes without writing any return, to confirm the n gates are reachable.
- Reported: 21-session versions, trend arms, sharp-drop events split by news (earnings 8-K / no earnings 8-K / no 8-K), liquidity split, delisted-only and survivor-only events.

## Known limits

- Control stocks are 2026 survivors only. For level cells this is conservative; for difference cells it is not neutral, hence the survivors-only gate.
- Event-side delisted coverage depends on FMP; a wrong later split record makes an event's own trade fail the price check and the event is dropped as "not open-market". The funnel reports the totals but cannot separate that cause.
- Form 4 relationship flags are OR-ed across joint filers, so a fund filing jointly with its board designee counts as a director purchase.
- FINRA symbols are point in time while bars carry 2026 symbols; the volume-share sanity rule is the only guard in WF-5/WF-6. The consolidated FINRA file starts 2018-08; earlier days sum three facility files, one nearly empty in 2013 and 2015–16.
- 10b5-1 plan flags are not used (not available before 2023). An 8-K accepted after hours on the Form 4 filing day is not seen for delisted issuers.
- No portfolio simulation here; a K-slot ledger with K, tie-break and start date swept is required before any product if a cell passes.

## Changes from v1 (red team, all before any outcome)

1. Filings with no price series are kept and counted; funnel and survivorship sensitivity implemented in code (blocker 1).
2. Engine fixed to run under pandas 3 (read-only arrays); selftest and counts are required before the lock (blocker 2).
3. WF-1/WF-2 are tested on control-adjusted excess; SPY excess becomes a gate; AMBER needs a positive control-adjusted mean in both windows (major 3).
4. Half-year clusters added; widest of three schemes reported (major 4).
5. Difference cells must keep their sign on survivor-only events (major 5).
6. Series trust rule made point in time (major 6).
7. Concentration gate changed from "top five ≤ 30% of the net sum" to "lower bound > 0 without the top five names" (major 7).
8. Minors: controls blocked only during another event's holding period; routine flag keyed to filing dates; holding increase per ownership line; joint-filer and duplicate rules stated; title rules tightened; refractory in sessions; embargo between windows; WF-5 cost comparison; sign file required and hashed; confirmatory re-runs refused; panel cache keyed to the bar files; FMP empty results retried; truncation sensitivity; 8-K dating; selftest criterion; self-control via reused symbol blocked.

## Changes from v2 (after the self-test and event counts; no real return computed)

9. **WF-4 redefined.** "Dropped ≥ 15% with no 8-K at all" has 10 / 43 / 23 tradeable events in the three windows against a gate of 150 per arm, because most issuers file some 8-K in any 20-session span; "no earnings 8-K" has 51 / 138 / 67. WF-4 is now the sharp-drop contrast (333 / 612 / 305 events in the drop arm), with the news split reported descriptively. The news-specific question is under-powered with 8-K data and is stated as such.
10. **Trust rule limited to symbol-matched series.** 639 CIK-matched issuers failed the point-in-time rule at some date, but for 127 of the 150 largest the median Form 4 price ÷ as-traded close is 0.9–1.1: their prices are right and the failures are small early samples and plan purchases reported at average prices. A CIK match cannot be a reused symbol, so the rule now applies only where it guards against something. The event's own open-market check is unchanged.
