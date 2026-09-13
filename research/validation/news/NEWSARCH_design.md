# NEWSARCH — Real-time news for momentum: thesis, sources, classification, tests, pipeline

**Date:** 2026-09-13. **Role:** real-time news architect (design only; no data was crunched for this memo).
**Operator question:** "Is it basically down to news, and is tracking real-time news the best way to find momentum in stocks?"
**Inputs read:** `C:/dev/Trader-v3/research_plan_v5_overnight_catalyst.md` §1, §2, §6; the 8-K Item 2.02 archive (`research/earn/sec_sub_raw_0..4.jsonl`, 196,281 filings, 5,213 CIKs, 2004–2026; co-items 9.01 189k, 7.01 34k, 8.01 13k, 5.02 4.6k, 1.01 2.5k, 3.02 425, 4.02 333); `things_tried.csv` (72 rows); `news.py` (Finnhub `company-news` already wired).

---

## 1. Thesis: is news the answer?

**Short answer.** News is why stocks move, but "tracking real-time news" is three different games and only one of them is open to a retail operator on a Railway poller.

**(a) Racing headlines (seconds to ~30 minutes).** Lost before it starts. Machine-readable feeds (Bloomberg event-driven, Dow Jones elementized, RavenPack) deliver parsed releases to co-located systems in milliseconds; the first prints after a headline are made against wide spreads by people who already know the number. A Python poller sees EDGAR 10–60 s after acceptance, and the 8-K itself is filed 1–30 minutes *after* the wire release. This project's own three intraday tests (100 live days −0.23R/trade; 753k random bars −0.07R gross; biggest gap-ups negative in both windows) are the empirical version of that statement: by the time a retail system can act intraday, the continuation is gone and what remains is the reversal. Nothing in this design touches intraday entries or exits.

**(b) Classifying the TYPE of catalyst (hours to weeks).** This is the open game, and it is exactly what the project has not tested. The literature's prior is specific: stock moves accompanied by public news continue while no-news moves revert (Chan 2003; Tetlock 2010); post-earnings-announcement drift is the most replicated short-horizon continuation effect, measured from day 1 to day 60, concentrated in small and mid caps, and much weaker in large caps since roughly 2010; guidance changes are the strongest sub-component of earnings news; analyst recommendation changes drift for days to weeks in small caps; equity offerings and restatements are followed by underperformance; index adds pop into the effective date and revert after; FDA and M&A outcomes are binary and mostly priced at the print. The common thread is that the *type* fixes the sign of the hazard over the next days, and "gap ≥ 8% on 3× volume" mixes types whose hazards have opposite signs. That is a testable claim, the data to test the earnings half of it is already on disk (timestamps and item codes), and a once-a-day decision at the closing or opening auction is not latency-disadvantaged. Honest expectation: the effect is small per event (tens of basis points per week, net), largest where spreads are widest, and has been shrinking; the most valuable output may be *exclusions* (no-news moves, dilution, restatements) rather than a standalone long. The v5 memo's own note that earnings-tagged E1 events looked worse than non-earnings ones on the overnight leg is a warning that the overnight leg is not where earnings-type continuation lives; PEAD is a multi-day object.

**(c) No-news reversal.** Chan (2003): large moves without a public catalyst revert over the following weeks. For this operator that is a filter, not a trade: never enter a continuation position on a move that has no 8-K, no press release and no calendar event within [T−1, T]. It is also the cleanest first test, because it needs only the 8-K timestamps against the daily bars already on disk, and it is the test of "is it the news or the move?".

**So:** real-time news is not the best way to *find* momentum in minutes; it is the only way left in this project to *condition* the overnight/multi-day trade on a mechanism. The decisive experiment costs nothing and is in §5.

## 2. Who wins at each horizon

| Horizon | Who wins | Retail position | This project's evidence |
|---|---|---|---|
| 0–60 s after the headline | HFT, news-feed readers on machine-readable feeds | Cannot compete; spread + latency | n/a (never tried, correctly) |
| 1–30 min | Pro desks, prop algos reading the release | Loses: first prints revert; intraday continuation negative | 100 live days −0.23R; 753k bars −0.07R gross |
| Rest of session | Nobody reliably; reversal dominates | Do not trade | Gap-ups held: next session −0.22%; biggest gap-ups negative |
| Close → next open | Overnight flow; regime-dependent | Auction fills OK; effect existed only 2020–2024 | E1 +0.79% seen; −0.38% in 2004–2013; RED |
| Day 1 → day 5–60 (PEAD, guidance, analyst drift) | Patient, cost-disciplined capital; small-cap specialists | The only horizon where a Railway poller has no structural disadvantage; capacity is small but irrelevant | **Untested here** (multi-day family tested only unconditionally) |
| Weeks–months | Fundamental/thematic investors | Already the Themes service | out of scope |

## 3. Source inventory

| Source | What it gives | Latency | Cost | Coverage | Backtestable history | Access |
|---|---|---|---|---|---|---|
| **SEC EDGAR latest-filings feed** (`browse-edgar?action=getcurrent&type=8-K&output=atom`), plus per-filing index for items and EX-99.1 text | Every 8-K with item codes; the earnings press release is inside as EX-99.1 | Seconds to ~1 min after acceptance during 06:00–22:00 ET. **Filings accepted after 17:30 ET are not disseminated until 06:00 ET the next business day** (confirmed: SEC filer manual and the edgartools note). Company files the 8-K 1–30 min after the wire release. | Free; ≤10 req/s with a declared User-Agent (poll every 20–30 s) | All SEC registrants (foreign issuers file 6-K, not 8-K) | Complete 2004→ (item codes from Aug 2004); acceptance timestamps exact; press-release text free. No source-side survivorship. | Already have the submissions fetcher (`sec_sub_fetch.py`, 0.45 s/req); re-run keeping *all* items (~40 min). Full-text search `efts.sec.gov/LATEST/search-index` for historical phrase labels ("complete response letter", "registered direct offering"). |
| **EDGAR submissions API** (`data.sec.gov/submissions/CIK….json`) | Per-CIK filing list with items and acceptance time | Minutes to hours behind acceptance (not a real-time source) | Free | Same | Same | Already used for the archive |
| **Press-release wires RSS**: GlobeNewswire, PR Newswire, Business Wire, Accesswire | Headline + body link within a minute of release; the true first public timestamp for most corporate news | ~1 min | Free RSS (firehose/API is paid; check each wire's terms for polling) | GlobeNewswire = small/mid caps; Business Wire = large caps; PRN broad; Accesswire = microcaps (promotional) | **None usable**: RSS holds the last few dozen–hundred items. Historical archives are scrape-hostile. Use EDGAR EX-99.1 and FMP for history. | Poll every 60 s; store body text at first sight |
| **FMP (production key)**: `/stable/news/press-releases-latest`, `/stable/news/press-releases?symbols=`, `/stable/news/stock-latest`, `/stable/news/stock?symbols=`, `/stable/earnings-calendar`, `/stable/grades-latest-news` (analyst actions) | Aggregated PRs and articles with symbol tags; earnings calendar with actuals | Minutes to tens of minutes (aggregator); timestamps sometimes ingestion time, sometimes date-only | Included in the plan; rate limit plan-dependent | US-listed, broad | Multi-year for surviving symbols; **survivorship**: delisted symbols drop out; timestamp precision unreliable for a backtest clock | Simple GET; already used in the repo for prices |
| **Finnhub `company-news`** (in `news.py`) | Headlines + summary from many secondary sources (Yahoo, SA, MarketWatch), unix time | Minutes; noisy duplicates | Free tier: 60 calls/min | Broad | **1 year** on the free tier (confirmed); heavy duplication; counts carry no signal (July memo) | Already wired; use as a cheap headline text source for classification, never for latency or history |
| **Exchange halt feeds**: Nasdaq Trader RSS `rss.aspx?feed=tradehalts` (also `&haltdate=mmddyyyy`), NYSE `nyse.com/trade-halt-current` | T1 news pending, T2 news released, T12 more info requested, LUDP (LULD pause), H10 SEC suspension, with times | ~1 min | Free | Nasdaq feed covers Nasdaq-listed plus UTP-reported halts; NYSE page for NYSE names | Nasdaq halt search by date is queryable → a daily halt table can be rebuilt for the backtest hazard | Poll every 30 s; T1 at a decision time = no trade |
| **Earnings calendars with timing**: Nasdaq calendar (already fetched, `nasdaq_raw.jsonl`), Finnhub `/calendar/earnings` (hour bmo/amc/dmh), FMP earnings-calendar | Expected date and session | Daily | Free / included | Broad; timing wrong for a meaningful minority of small caps | Ex-post truth = 8-K 2.02 acceptance time; the calendars are only for the *ahead* flag | Fetch 05:30 ET daily |
| Not adopted now | Machine-readable news feeds (Bloomberg/DJ/RavenPack), Benzinga Pro API, Alpha Vantage NEWS_SENTIMENT (25 req/day free, 2022→) | — | $100s–$10,000s/mo | — | Benzinga has history but only for a paid plan | Revisit only if a registered test passes and latency proves to matter (it should not, by design) |

**Backtest clock rule.** The only timestamp allowed to define "known by" in any backtest is the EDGAR acceptance time (`acc`, converted to ET with DST). Aggregator timestamps (FMP, Finnhub) are never used as the clock, because they can post-date the move (look-ahead the wrong way) or pre-date the true publication (look-ahead in our favour). Acceptance ≥ 17:30 ET keeps `fd` = next day but `acc` is the truth; the wire had it at once, so day 0 = next session either way.

**Session mapping.** `acc` in ET: before 09:30 → same session is day 0 (BMO); 09:30–16:00 → intraday release, flag `intraday=1` and treat the *next* session as day 0 for any test (rare for 2.02, ~3%); after 16:00 → next trading session is day 0 (AMC). Trading calendar from SPY dates.

## 4. Classification design (LLM)

**Two stages, one frozen schema.**
- Stage 1 (bulk, smallest model): headline + first ~1,500 characters. Runs on every deduped item for universe names.
- Stage 2 (full text, larger model): only for names that will reach a decision time (rule candidates at 15:44 or overnight events for 09:28) — the full EX-99.1 or PR body, plus supplied context (consensus EPS/revenue from FMP estimates, prior guidance if we have it, market cap, ADV, next scheduled earnings date). The model never has to know consensus from memory; it is given the numbers.

**Schema (JSON, enum values frozen before any test; a change = new version):**
```
event_id, ticker, cik, doc_kind: 8-K | PR | headline | halt
event_type: earnings_results | guidance_only | preannouncement | analyst_action |
            m_and_a_target | m_and_a_acquirer | deal_terminated | equity_offering |
            convertible_or_debt | buyback_or_dividend | fda_or_clinical | contract_award |
            index_change | management_change | restatement_nonreliance |
            legal_regulatory | going_concern_or_delisting_notice | reverse_split |
            activist_or_13d | insider_transaction | macro_or_sector | other | none
direction: -2..+2            (model's read of the news for the stock, not the price)
magnitude: small | medium | large   (expected |move| bucket: <3%, 3–10%, >10%)
novelty: scheduled | unscheduled ; already_preannounced: bool
guidance_changed: raised | lowered | maintained | initiated | withdrawn | none
eps_vs_consensus / revenue_vs_consensus: beat | miss | inline | unknown  (from supplied numbers)
dilution_flag: bool ; dilution_pct_of_mktcap: number|null ; offering_kind: registered | direct | atm | pipe | convertible | null
binary_event_ahead: bool ; binary_event_kind ; binary_event_date (≤30 d)
promotional_flag: bool      (microcap PR fluff, no numbers, no counterparty named)
confidence: 0–1 ; evidence_quote: ≤25 words verbatim ; rationale: one sentence
```
**Few-shot policy.** 10–14 fixed exemplars, one or two per type, including hard negatives: "record revenue" with lowered guidance (net negative); "exploring strategic alternatives" (other, direction 0, binary ahead); "registered direct offering priced at a discount" (equity_offering, dilution); "at-the-market program" (equity_offering, small); a beat that was preannounced (already_preannounced); a Reg FD 7.01 investor-day deck (guidance_only). Temperature 0, structured output enforced, prompt text and exemplars hashed into `classifier_version`. No prompt edits between the freeze and the end of a test window; changes ship as a new version and are re-validated on the golden set.

**Validation, in order.**
1. **Item-code agreement (coarse ground truth, free):** 2.02 ↔ earnings_results/guidance; 1.01 ↔ deal/contract/M&A; 3.02 ↔ equity_offering; 5.02 ↔ management_change; 4.02 ↔ restatement; 5.03 ↔ reverse_split/charter; 1.03 ↔ bankruptcy; 3.01 ↔ delisting notice; 7.01 ↔ guidance/investor-day. Target ≥ 90% agreement where the mapping is unambiguous; disagreements audited by hand.
2. **Numeric fidelity:** eps/revenue fields against XBRL/FMP actuals; guidance fields against a 200-document hand set. Any hallucinated number → the field is dropped from the trading rule, not patched.
3. **Stability:** re-classify 500 documents; label agreement ≥ 95%; direction within ±1 in ≥ 98%.
4. **Time split:** exemplars and prompt frozen using 2024-09 → 2025-08 documents; all validation statistics reported on 2025-09 → 2026-09.
5. **Forward-return validation (the only one that matters for trading):** the classifier's marginal value is what it adds *given* the day-0 return, which the operator already knows at decision time. So the test is forward excess return by (type, direction, guidance_changed) *within* day-0 return buckets, against matched no-news names — never "does direction predict the day-0 sign" (it will; that is not tradeable).

**Volume and cost (tokens are the stable unit; confirm dollar prices on the current Anthropic pricing page before budgeting).** Normal day: 300–600 8-Ks across all registrants, ~150–300 after the universe filter, plus 2–5k aggregator headlines collapsing to a few hundred after dedupe. Stage 1 at ~1k tokens/doc ≈ 0.3–0.8M input tokens/day; Stage 2 on 20–60 docs at ~6–12k tokens ≈ 0.3–0.7M. Peak earnings days ×4. Order of magnitude: low single-digit dollars per day at the smallest model tier, tens of dollars on peak days if Stage 2 uses a large model. Historical labelling for the test in §5 step 3: ~20k EX-99.1 documents (2024-09 → 2026-09, universe names) at ~8k tokens ≈ 160M input tokens; restricting to |day-0 excess| ≥ 5% cuts it to ~4–6k documents (≈ 40M tokens), which is the version to run first.

## 5. Tradeable shapes and how each is pre-registered

Constraints inherited from the evidence: no intraday technical entries or exits; entries only at the close (MOC) or next open (MOO); exits by time only, fixed horizons declared in advance; no price targets or stops unless a registered cell shows them positive (none has). Costs 5 bp/side (ADV20 ≥ $100M) else 10 bp; universe prior close ≥ $5, ADV20 ≥ $50M through T−1; benchmarks SPY over the identical leg **and** matched non-event names with the same day-0 excess return (C2, nearest 5 in the same liquidity decile, fixed seed); day- and quarter-cluster bootstrap, wider CI reported; Holm within each family; n ≥ 300 per cell (E1-type) / 600 (broad); survivorship haircut +0.25% (multi-day cells +0.40% because delisting risk grows with the hold); every extra variant logged to `things_tried.csv` with `who=NEWSARCH`.

**Windows.** Discovery: 2014-09 → 2021-12 (10-year panel, first 7.3 years). Confirmatory, run once with frozen thresholds: (i) 2022-01 → 2026-09 (splice of the 10-year and 2-year panels; the 7-day hole is descriptive only) and (ii) 2004-01 → 2013-12 (1,225 names; item codes exist from 2004-08; expect lower earnings coverage). A cell passes only if both confirmatory halves have lower CI > 0 after costs and the haircut, matched-control excess > 0 with lower CI > 0, ≥ 60% of quarters positive. Both, not either: the v5 lesson is that 2020–2024 alone proves nothing.

| Cell | Event (all through the EDGAR acceptance clock) | Entry | Exit | Prior | Pass threshold (net, vs SPY and vs C2) |
|---|---|---|---|---|---|
| **N1 Chan test** (the decisive one) | Day-0 excess return ≥ +8% (and ≥ +3%/vol ≥ 2× as a second row); split by *has a 2.02 filing whose day 0 is T* vs *no 8-K of any item in [T−1, T]* (after step 2 of the plan; until then, "no 2.02") | MOC T and MOO T+1 (two sub-cells) | Close T+1, T+5, T+20 | News continues, no-news reverts | News − no-news difference ≥ +0.50% at T+5 and ≥ +1.0% at T+20 with lower CI > 0 in both confirmatory windows |
| **N2 PEAD long-only** | Earnings day 0 (2.02), day-0 excess in the top decile of that day's earnings names (min +5%), ADV ≥ $50M | MOO T+1 and MOC T+1 (sub-cells; the project's evidence says the T+1 session reverts, so MOC T+1 is the favoured one) | Close T+5, T+10, T+20, T+40 | +0.5–1.5% over 20 d in mid caps if it still exists | Excess ≥ +0.75% at T+20 with lower CI > +0.40% (haircut) and C2 excess lower CI > 0 |
| **N3 Guidance-raised** (needs step 3 labels; 2024-09 → 2026-09 only, so it is a discovery cell until a 2027+ live window exists) | N2 event ∧ `guidance_changed = raised` ∧ `already_preannounced = false` | as N2 | as N2 | Strongest sub-component of PEAD in the literature | Reported, not promoted, until out-of-sample live data exists |
| **N4 Earnings-overnight** | E1/E2 of v5 ∧ 2.02 day 0 = T, vs E1/E2 ∧ no 8-K | MOC T | MOO T+1 | Weak (v5 slice hinted earnings E1 is worse) | Same as v5 (registered here for completeness; expect fail) |
| **N5 Negative-type exclusions** (research, then filter) | 3.02 / 424B5 (offering), 4.02 (restatement), 5.03 reverse split, 3.01 delisting notice, 1.03, going-concern language | — | — | Underperformance over 20–60 d | Applied as a 60-day exclusion to every long cell if excluding them raises the cell's excess with a non-overlapping CI; otherwise dropped |
| **N6 Analyst actions, index adds** | FMP `grades-historical`, index announcements | MOC T | Close T+5/T+20 | Drift in small caps; index effect shrinking | Deferred: no timestamped history on disk; survivorship in FMP grades; only after N1/N2 resolve |

Sizing if anything passes: fixed sleeve S; per-position 5% of S; ≤ 6 concurrent; per-name cap 3×ATR20 ≤ 2% of S; skip entries when the SPY ≤ −3% on T; no entry if a scheduled binary event (earnings, PDUFA, court date, shareholder vote) falls inside the hold horizon; half weights for the first 100 events. Same drawdown ladder as v5 (halve at −6% of S, stop at −10%).

## 6. Backtest design (steps in order; no LLM until step 3)

1. **Event table.** From the five jsonl files: one row per (cik, ticker, accession) with `acc_et`, `session_bucket` (BMO/intraday/AMC), `day0` (trading date from the SPY calendar), item set. Map CIK→ticker with `company_tickers.json` (first-listed ticker; log multi-ticker CIKs). Merge onto the daily panels by (ticker, day0). Report coverage per year and per panel (the 2004–2013 coverage figure of 29% in v5 must be decomposed: true non-earnings share vs archive gaps).
2. **Full 8-K item history.** Re-run `sec_sub_fetch.py` keeping every 8-K/8-K/A regardless of items (same request count; ~40 min at 0.45 s). This gives the "no 8-K in [T−1, T]" arm of N1 and the N5 tables. Add 424B5 and 6-K later if needed.
3. **Discovery run (2014-09 → 2021-12):** N1, N2, N4 with all sub-cells; matched controls C2 built from the same day's non-event names; day- and quarter-cluster bootstrap. Fix thresholds. Write `NEWSARCH_prereg.json` with SHA-256 before touching a confirmatory window.
4. **Confirmatory run, once:** 2022-01 → 2026-09 and 2004 → 2013. Holm across the family {N1×3 horizons×2 entries, N2×4 horizons×2 entries, N4×2} (declare the exact count in the prereg). Independent verifier re-derives n and point estimates from the raw files, as in v5.
5. **Labels (step 3 of the build plan):** download EX-99.1 for 2.02 filings in 2024-09 → 2026-09 with |day-0 excess| ≥ 5% (≈4–6k documents), classify with the frozen schema, run N3 as a discovery cell; validate the classifier per §4. The N3 result is a prior for a live test, not a pass.
6. **Placebos:** shift the event date by +1 and −1 sessions (should destroy N1/N2); mirror events (day-0 ≤ −8% with news vs without) reported for the sign structure; liquidity terciles; earnings-season vs off-season.

## 7. Real-time pipeline (separate service; no shared code with the scanner or Themes)

```
pollers ──▶ normalize ──▶ dedupe ──▶ classify(1) ──▶ enrich ──▶ candidates ──▶ classify(2) ──▶ rank ──▶ decision times ──▶ tickets ──▶ log
```
- **Ingest.** EDGAR getcurrent Atom every 30 s (06:00–22:00 ET; remember the 17:30 dissemination wall, so the 06:00 burst is expected); four wire RSS feeds every 60 s; FMP `press-releases-latest` and `stock-latest` every 5 min; Finnhub `company-news` only for names already flagged (60/min budget); Nasdaq/NYSE halt feeds every 30 s; earnings calendar and FMP estimates at 05:30 ET; prices: 15:44 batch snapshot (existing FMP path) and end-of-day bars. Persistent cursors on the Railway volume so a restart never replays or skips.
- **Normalize.** Ticker via CIK map for EDGAR; symbol tags for FMP/Finnhub; wire items mapped by exchange:ticker parsing of the release, else by company-name fuzzy match with a confidence field; universe filter (prior close ≥ $5, ADV20 ≥ $50M); foreign issuers dropped.
- **Dedupe.** Canonical key = 8-K accession when present; otherwise `sha1(ticker + normalized headline)` within a 12-hour window; a PR and its later 8-K are linked (same ticker, title similarity ≥ 0.8, 8-K within 4 h) and the *earliest* public timestamp is kept as `source_time`, EDGAR acceptance as `edgar_time`.
- **Classify (1)** on every deduped item; **enrich** with day-0 return so far, gap, RVOL, ADV, market cap, halt state, days to next scheduled earnings; **candidates** = items that meet a registered cell's event definition; **classify (2)** on full text for candidates only.
- **Rank.** Only the registered rule's own ordering (e.g., day-0 excess return descending within the cell); no composite score, no discretion. Cap by the concurrency limit.
- **Decision times.** 09:20 ET: build the MOO list from overnight/06:00 events (tickets by 09:28). 15:44 ET: build the MOC list from the batch snapshot (tickets by 15:49 NYSE / 15:54 Nasdaq). Exits are time-based and generated the night before. Nothing else fires. A name halted (T1/T12/LUDP) at a decision time is skipped; a name that halts while held is exited at the first scheduled decision time after the resumption.
- **Tickets.** Phase 1: a message with the exact ticket (side, symbol, qty, MOC/MOO, cutoff); the operator submits. Phase 2 (only after the pilot): broker API auto-submit with a kill switch.
- **Log.** Every event, candidate, decision and non-decision, per §8, written before the ticket is sent.

**Operator loop.** 08:30 ET (5 min): read the overnight event list and Stage-2 outputs; mark any classifier error (this becomes golden-set data). 09:20–09:28: submit MOO tickets if any. 15:40–15:49: MOC tickets if any. Daily after the close: automatic backfill of fills and returns; a one-line summary. Weekly (20 min): 20 random classifications audited; dedupe misses; CUSUM of realized vs registered expectation per cell; latency histogram (source_time → first_seen). Monthly: registered statistics recomputed on the live log only; prompt/model version decisions (never silent). Quarterly: re-decide continue / halve / stop per the ladder.

## 8. What to log per event from day one

Identity and timing: `event_id`, ticker, CIK, exchange, `doc_kind`, source, URL/accession, item codes, `source_time`, `edgar_time`, `first_seen` (our poller), `classified_at` (both stages), `decision_time`, dissemination flag (after-17:30 wall), earnings-calendar expectation vs actual session.
Content: stored raw text (or EX-99.1 path) and its hash, headline, full Stage-1 and Stage-2 JSON, `classifier_version`, model id, prompt hash, token counts, cost.
Market context (all as of the decision time, then backfilled): prior close, open, 15:44 last, close, next open, next close, closes at +5/+10/+20/+40, SPY for the same points, ADV20, market cap, gap %, RVOL at 15:44 and at the close, halt codes and times, days to next scheduled earnings, any scheduled binary event inside 40 days, spread at the auction if the broker reports it.
Decision: which cells fired (all registered cells evaluated in parallel as shadow columns, so every cell accumulates live evidence at once), rank, the ticket that would be/was sent, submitted yes/no and why not (cutoff missed, halt, concurrency cap, operator veto), fill price and slippage vs the official auction print, realized excess per horizon.
Null events: every universe name with |day-0 excess| ≥ 5% and *no* linked news, logged with the same context; this is the no-news arm and it is free.

## 9. Failure modes

- **Latency arbitrage.** Anything whose payoff depends on being first is not this operator's; the design allows only auction-time decisions. If live latency from source_time to first_seen ever seems to matter for a cell, that cell is measuring the racing game and should be dropped.
- **Classification drift.** Model or prompt changes silently shift labels; mitigation is a pinned model id, a hashed prompt, a 300-document golden set run on every version change, and no change during a test window.
- **Survivorship in news archives.** FMP/Finnhub histories exist only for surviving tickers; the ticker list itself is 2026-09. EDGAR does not forget delisted filers, so the CIK-based event table is the least biased part; the daily bars are the biased part, hence the haircut and the CIK→ticker log of names with filings but no bars (a direct measure of the missing population).
- **Costs in small caps.** PEAD lives where spreads are 30–100 bp; MOC/MOO fills reduce but do not remove this; the $50M ADV floor and the 10 bp tier are already generous to the strategy. Report liquidity terciles; if the effect exists only below $100M ADV, it is probably not net positive.
- **Halts.** T1 at the open means no MOO fill or a fill at the reopening price; LULD pauses after news; SEC suspensions (H10) can last days. Rule: halted at decision time = skip; halted while held = exit at the next decision time after resumption; log every case.
- **Holding into binary events.** Earnings inside a 20–40 day hold, PDUFA dates, court rulings, votes. The `binary_event_ahead` flag plus calendar exclusion; the backtest must apply the same exclusion or its returns overstate.
- **Timestamp lies and dedupe misses.** Aggregator times, date-only items, ticker collisions (parent/subsidiary, SPAC renames, class shares). Backtest clock = EDGAR acceptance only; live logs keep every timestamp so the difference is measurable.
- **LLM numeric hallucination.** Supplied consensus numbers, mandatory evidence quotes, numeric fields cross-checked against XBRL/FMP actuals; a field that fails the check is excluded from any rule.
- **Regime dependence.** The 2020–2024 anomaly will show up again in any news-conditioned cell; both confirmatory windows are required, and a cell that passes only 2022–2026 is AMBER at best.
- **Schema drift as multiple testing.** Every new event_type or threshold after the freeze is a new test; log it to `things_tried.csv` and count it in the Holm family.
- **Operational.** EDGAR rate limits (10 req/s, User-Agent required), wire RSS terms, Railway restarts (persistent cursors), the 17:30 dissemination wall (do not mistake the 06:00 burst for fresh news), and the operator trading an exciting headline off-rule; the log records vetoes and off-rule trades separately.

## 10. Build plan (cheapest decisive experiment first)

1. **N1 Chan test and N2 PEAD on data already on disk** (free, 1–2 sessions of compute; no LLM): event table from the 2.02 archive → discovery 2014-09 → 2021-12 → freeze → confirmatory 2022–2026 and 2004–2013 → verifier. Decision: if news-conditioned movers do not beat matched no-news movers net at T+5 or T+20 in both confirmatory windows, the answer to the operator's question is "news explains the moves but does not make them tradeable at retail costs on this data", and the only remaining path is live logging.
2. **Full 8-K item history** (~40 min fetch) → the no-8-K arm of N1 done properly and the N5 exclusion tables; re-run N1/N2 with exclusions (declared in the prereg as a single pre-planned variant).
3. **Historical LLM labels** on 2024-09 → 2026-09 EX-99.1 for |day-0| ≥ 5% earnings movers (≈4–6k documents, ≈40M input tokens) → classifier validation per §4 → N3 as a discovery cell. Skip this step if step 1 is RED on both N1 and N2.
4. **Logging-only real-time service on Railway** (start now regardless of steps 1–3, because it costs nothing but a poller and makes a 2027 regime-aware test possible with lived data): ingest, dedupe, Stage-1 classification, halt tracking, null events, all shadow cells. No notifications, no capital. 60 days minimum; report latency, dedupe rate, classifier agreement with item codes, live vs backtest event frequency.
5. **Shadow decisions** for any GREEN cell from step 1 (or AMBER after 12 months of step 4 data), then half-weight pilot for 100 events, then full weights under the ladder — the v5 promotion path unchanged.
6. **Only then:** analyst actions and index adds (N6), broker-API auto-submit, Stage-2 on a larger model, paid news feeds if and only if a passed cell shows latency sensitivity (it should not).

**Bottom line for the operator.** It is "down to news" in the sense that news is the mechanism behind the moves; it is not down to *tracking* news in real time, which is the game retail loses. The one thing this project has not tested is whether the *kind* of news fixes the sign of the next few days, and the earnings half of that test is sitting on disk with exact timestamps. Run it before building anything that polls.
