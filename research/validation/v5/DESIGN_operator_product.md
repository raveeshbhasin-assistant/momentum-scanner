# DESIGN — Operator product: "Catalyst Close-to-Open" (lens: product & operations for one person)

Designer key: `OP_`. Date: 2026-09-13. Status: proposal only; no data touched, no code written.

## 0. One-paragraph thesis

Every intraday continuation idea this project has tested is negative after costs, and every multi-day continuation entered at T+1 open loses to SPY. The single leg with positive evidence is the **overnight** leg after a big-catalyst day (close T -> open T+1): +0.79% net excess over SPY, n=722, on a ten-year window that was run once — but it fails its own pass criteria (41% of quarters positive, Holm p 0.105, negative 2015-2019, positive 2020-2024). The product therefore has to be built as a **shadow-first, kill-switch-first operation**: a fixed 15:45 ET decision that buys one to five catalyst names in the closing auction (MOC) and sells all of them in the next opening auction (MOO), emits an explicit NO-TRADE record on most days, and writes one log line per event that carries every counterfactual the operator is curious about (next close, 10:00, fixed +5%). "Pick them up early" is re-read as *the first close after the catalyst* — the earliest point with any positive expectation. "Sell above a threshold / technical / fixed return" is re-read as *the opening auction is the threshold*: it is a time threshold, not a price one, because holding past the open has been worse in every cell tested. The whole thing must fit in one person's day: ~12 minutes at 15:35-15:50 and ~5 minutes at 08:45-09:30, and be shadow-runnable inside two weeks by adding five APScheduler jobs, one notifier path, one page and one JSON log to the existing scanner service.

## 1. Algorithm (rule-based; every threshold is fixed here, before any new test)

### 1.1 Universe (computed each morning with data through T-1 only)
- US common stocks from `us_common_symbols.csv` (no ETFs/ADR filtering beyond what that list already does).
- `Close_{T-1} >= $5`; `ADV20_dollar (T-20..T-1) >= $50M`; `>= 250` daily rows of history.
- Identical to the registered holdout universe so the live product stays comparable with the tested cells. Costs: 5 bp/side assumed for ADV >= $100M, 10 bp/side for $50-100M (auction fills do not cross a spread, so this is conservative for MOC/MOO).

### 1.2 Event at decision time (15:44 ET snapshot; a conservative subset of the registered E1/E2)
Let `gap = Open_T / Close_{T-1} - 1`, `V1544 = cumulative volume at 15:44`, `ADV20sh = mean share volume T-20..T-1`, `L1544 = last trade at 15:44`.
- **D1 (catalyst)**: `gap >= 0.08` AND `V1544 >= 3.0 x ADV20sh` AND `L1544 > Open_T`.
- **D2 (gap-up held)**: `gap >= 0.03` AND `V1544 >= 2.0 x ADV20sh` AND `L1544 >= Open_T`.
- No volume projection to the close: requiring the multiple to be met *already* at 15:44 is strictly more conservative than the registered close-based rule. Names that hit 3x only in the last 16 minutes are missed on purpose (measured, not guessed: rule OP3 below).
- At 16:10 the system re-evaluates each traded name with the official Close_T/Volume_T and stamps `close_confirmed = {E1, E2, none}` so the log separates "decision-time signal that held into the close" from "flipped".
- Registered E3 (strong close) and E4 (top-20 movers) are **not** traded: both <= 0 overnight in the holdout.

### 1.3 Ranking and capacity
- D1 events first, ranked by `V1544 / ADV20sh` descending; then D2 events by the same ratio. Take the top **5**. (Ranking only binds on the rare nights with >5 events; it is fixed here so it can never be tuned later.)
- Expected frequency from the holdout: E1 ~0.3 events/day on ~21% of days; E2 ~1.2/day on ~56% of days. Most nights: 0-2 positions. Roughly 40-45% of trading days produce **no trade**.

### 1.4 Entry (timing + order type)
- **MOC (market-on-close)** for each selected name, entered 15:45-15:49 ET. NYSE MOC cutoff is 15:50, Nasdaq 15:55 — the decision email must land by 15:45:30 or the day is a NO-TRADE (recorded as `MISSED_CUTOFF`, never a late chase).
- Fill = official closing auction print, which is exactly what the backtest priced. Slippage = `fill / Close_T - 1`, logged.
- If the broker rejects MOC on a symbol, the only permitted fallback is a marketable limit at 15:58 at ask + 0.10%, logged as `entry_type = LMT_LATE`; no other discretionary entry.

### 1.5 Exit
- **MOO (market-on-open)** for 100% of each position, entered 08:45-09:27 ET the next morning. Fill = official opening auction print (= the backtest's Open_{T+1}).
- There is **no overnight stop**: none can exist. Position size is the stop (1.6). A gap-down still exits at the open.
- Nothing is held past 09:30 in Phase 1 (shadow and first live tranche). The operator's fixed-return and technical exits are captured as **logged counterfactual columns**, not live rules: `cf_next_close` (Close_{T+1}), `cf_1000` (10:00 price), `cf_tp5_touch` (did the pre-market/opening 30-min high touch entry x 1.05), `cf_hold_if_green` (hold to 10:00 only if Open_{T+1} > entry and the 09:30-09:45 bar closes above its open). Pre-registered expectation: none of them beats the plain open exit (evidence items 1, 4). If after >= 120 logged events one beats X1 by > 0.30% with a day-cluster lower CI > 0, it becomes a candidate for a *new* forward test — it is never adopted retroactively.
- Halted name at the open: leave the MOO in; it fills at the reopen. Logged `exit_type = REOPEN`.

### 1.6 Position sizing and risk
- Sleeve `S` = a fixed dollar amount the operator assigns to this strategy (suggested ladder once live: 10% of trading capital -> 25% -> 50%, each step gated by the rules in section 3).
- Each position = **S / 5 = 20% of the sleeve**, equal weight, regardless of tier. A one-event night deploys 20%; never scale up to fill idle slots (this is what the K5 series priced; it also caps single-night risk).
- Risk model to state out loud: single-name overnight tail -15% x 20% = -3% of S; five names on a -3% SPY gap with ~2x overnight beta = -6% of S. Worst historical K5 drawdowns in the seen window: -16.8% (E1) / -25.1% (E2).
- Kill switch: sleeve drawdown from peak > 20% -> back to shadow (orders off) until the weekly review explicitly re-enables.
- Costs charged to every trade on the trade's own notional: 5 or 10 bp/side by ADV bucket, plus the measured auction slippage.

### 1.7 Regime / brake (two rules, both fixed now)
- **Risk brake (not alpha)**: skip the night entirely if `SPY Close_T / Close_{T-1} - 1 <= -3.0%`. Expected to fire on ~1% of days; it exists to avoid holding five gap-prone longs into crash nights.
- **Equity-curve brake**: from the 41st logged event onward (shadow + live, chronological, E1+E2 pooled), live orders are allowed only when the trailing-40-event mean net excess > 0; otherwise the system runs shadow-only. It is honest about what the yearly table showed (five negative years then five positive) without claiming to know *why*. A calendar or "retail boom" regime switch is deliberately **not** proposed — it would be fitted to the seen pattern.
- Both brakes are tested as **refinements on a seen window** (2014-2024) and confirmed only on 2004-2013 (rule OP4).

## 2. The operator's day, end to end

All times ET, Mon-Fri. The machine does the pulls and ranking; the operator only enters orders and fills.

### 2.1 07:00 — universe refresh (machine)
`on_universe_0700`: rebuild ADV20sh, ADV20$ and Close_{T-1} for the ~3,000-name universe from daily bars (FMP -> yfinance fallback, same wrapper as `fmp_data.py`). Writes `data/overnight_universe.json`. Also pulls SPY Close_{T-1}. No email.

### 2.2 08:45 — morning card (machine -> operator, only on mornings with positions)
`on_morning_0845` emails **[ON-EXIT]**: per held name — entry fill, shares, pre-market indicative, SPY pre-market, and the single instruction "Enter MOO sell for ALL listed positions by 09:27". Operator action: enter MOO orders (2-3 minutes). No decisions.

### 2.3 09:30-09:40 — opening auction, settle (machine + 1 operator input)
Fills happen in the auction. `on_settle_0940` pulls the official Open_{T+1} for each name and SPY, computes `ret_gross, ret_net, spy_leg, excess_net`, and emails a two-field prompt per position: actual exit fill (and, if not yet recorded, actual entry fill). Operator enters them on `/overnight` (a two-column form); until entered, the log carries the official prints with `fill_source = OFFICIAL`. Slippage bp is computed the moment a real fill is entered.
The counterfactual columns (`cf_1000`, `cf_tp5_touch`, `cf_hold_if_green`, later `cf_next_close`) are back-filled by the 16:10 job the same day.

### 2.4 09:40-15:30 — nothing
The existing 15-minute scanner may keep running (it is independent); the overnight product needs no attention here. This is the point of the design: the operator is not screen-bound.

### 2.5 15:35 — preliminary watchlist (machine -> operator)
`on_prelim_1535`: real-time snapshot (FMP quotes — the production key; yfinance's 15-minute delay is unusable here) for the universe; lists names already meeting the gap and volume legs with the price leg marked tentative. Emails **[ON-WATCH]** only if the list is non-empty. Operator action: open the broker, pre-stage MOC tickets for the listed names, sized at S/5 each. Nothing is sent yet.

### 2.6 15:44-15:45 — the decision (machine); 15:45-15:49 — order entry (operator)
`on_decide_1544` re-snapshots, applies D1/D2, ranks, caps at 5, applies the SPY risk brake and the equity-curve brake, and emails **[ON-ORDERS]** by 15:45:30: an ordered table `ticker | tier | gap% | RVOL@15:44 | last | shares | notional`, plus the brake state and the literal instruction "Enter MOC BUY for these N names by 15:49 (NYSE cutoff 15:50)". Or **[ON-NOTRADE]** with the top-3 near-misses (two of three conditions met — logged, never traded). Operator action: send the pre-staged MOC tickets; cancel any pre-staged ticket not on the final list. If the operator is not at a screen, the night is `MISSED_OPERATOR` — a distinct status so shadow statistics are not polluted by non-executions.

### 2.7 16:10 — confirmation (machine)
`on_confirm_1610`: official Close_T and Volume_T for the night's names -> `close_confirmed`, `flip_at_close` (last > open at 15:44 but close <= open), decision-vs-close price drift, and the entry-fill prompt. Emails **[ON-CONFIRM]** only when something differs from the 15:45 email.

### 2.8 Days with no events
The 15:45 email still goes out (**[ON-NOTRADE]**), every day. A daily touchpoint is what keeps the operator ready on the ~21% of days when a D1 shows up; a system that is silent for a week gets ignored on the day it matters. The NO-TRADE record also counts toward coverage and brake bookkeeping.

### 2.9 Sunday 18:00 — weekly review (machine -> operator; 10 minutes to read)
`on_weekly_sun_1800` renders `/overnight?week=` and emails **[ON-WEEK]** with:
1. n events (by tier), n nights, n NO-TRADE, n MISSED_*, n SKIPPED_BRAKE.
2. Mean net excess per trade with a day-cluster 95% CI; running cumulative mean plotted against the pre-registered expectation band (+0.30% .. +0.80%) and against 0.
3. Hit rate, median, worst overnight, best overnight.
4. Execution: mean/95th-pct slippage bp at entry and exit vs the 5/10 bp assumption; share of MOC rejects; time-stamp of the 15:44 quote vs 15:44:00 (latency).
5. Coverage: decision-time D1/D2 count vs post-hoc E1/E2 count from official bars (target >= 70%); flip rate (target <= 15%).
6. Sleeve equity, drawdown from peak, brake state and the trailing-40 mean.
7. Counterfactual table: X1 (open) vs `cf_next_close`, `cf_1000`, `cf_tp5_touch`, `cf_hold_if_green`.
8. Phase gate status (section 3): how many shadow days/events remain before the next tranche decision.

### 2.10 Log record — one line per event per night (`data/overnight_log.json`, append-only)
`date_T, ticker, tier(D1/D2), rank, status{SHADOW,LIVE,MISSED_CUTOFF,MISSED_OPERATOR,SKIPPED_BRAKE_SPY,SKIPPED_BRAKE_EQ,NOTRADE}, gap_pct, rvol_1544, last_1544, open_T, close_prev, adv20_dollar, adv20sh, quote_ts_1544, close_T, volume_T, close_confirmed{E1,E2,none}, flip_at_close, entry_type{MOC,LMT_LATE}, entry_fill, entry_slip_bp, shares, notional, sleeve_pct, spy_close_T, spy_close_prev, spy_ret_T, open_T1, exit_type{MOO,REOPEN}, exit_fill, exit_slip_bp, fill_source{OFFICIAL,OPERATOR}, ret_gross, cost_bp, ret_net, spy_leg, excess_net, cf_next_close, cf_1000, cf_tp5_touch, cf_hold_if_green, brake_state, trailing40_mean, near_misses(json, NOTRADE rows only), notes`.
The file is added to the existing `data_backup.py` push to `origin/data-backups` so the live record survives Railway volume resets.

## 3. Pre-registered rules and phase gates (thresholds fixed here; nothing is tuned after)

Windows: 2024-09..2026-09 = DISCOVERY (already spent). 2014-09..2024-08 = SEEN (yearly aggregates for E1-E4 have been looked at); any new cell there is labelled *refinement on a seen window*. 2004-01..2013-12 = CONFIRMATORY (unseen; download at 708/3,179 files as of writing, may be partial — the run waits for completion or is labelled partial with the file count). Shadow = forward, >= 60 trading days.

| id | definition | exit | test window | pass criterion |
|---|---|---|---|---|
| OP1 | E1 exactly as registered (gap>=8%, Vol_T>=3x ADV20sh, Close_T>Open_T; universe 1.1) | X1: buy Close_T x1.0005, sell Open_{T+1} x0.9995 | CONFIRMATORY 2004-2013, run once | quarter-cluster lower CI of net excess over SPY > 0; quarters positive >= 0.60; n >= 300 (fewer survivors that far back); matched non-event control (same day, same ADV bucket, no gap) excess reported |
| OP2 | E2 exactly as registered (gap>=3%, Close>=Open, Vol_T>=2x) | X1 | CONFIRMATORY 2004-2013, run once | same as OP1 |
| OP3 | decision-time D1/D2 (15:44 cumulative volume, last vs open) vs official E1/E2 | measurement only, no return claim | `bars5m_inplay` 60 days (DISC) + shadow | coverage >= 70% of E1 nights captured at 15:44; flip rate <= 15%; 15:44 quote latency <= 60 s in production |
| OP4 | brakes: (a) skip if SPY ret_T <= -3.0%; (b) live only if trailing-40-event pooled mean net excess > 0 | X1 | SEEN 2014-2024 (labelled refinement) then CONFIRMATORY 2004-2013 | on CONFIRMATORY: braked series mean net excess >= unbraked AND K5 max drawdown not worse; if it fails, brakes stay as *risk* rules only and are reported as such |
| OP5 | shadow run of the full product (emails, orders simulated at official prints, operator enters paper fills) | X1 | >= 60 trading days forward, from first Railway deploy | n >= 40 events; mean net excess > 0 (day-cluster CI reported; not required > 0 because power is insufficient); mean slippage <= 10 bp/side; coverage >= 70%; MISSED_* <= 20% of event nights; OP1 or OP2 passed. All five -> live tranche 1 (S = 10% of trading capital) |
| OP6 | live tranche 1 -> tranche 2 (25%) -> tranche 3 (50%) | X1 | each tranche >= 60 trading days | trailing-60-day mean net excess > 0 with realised fills; sleeve drawdown < 20%; no brake breach; slippage assumption still holds |
| OP7 | counterfactual exits (`cf_next_close`, `cf_1000`, `cf_tp5_touch`, `cf_hold_if_green`) | as named | logged during OP5/OP6 | expectation: none beats X1. A candidate is created only if one beats X1 by > 0.30% with day-cluster lower CI > 0 after >= 120 events; it then gets its own forward test |

Power statement: per-trade net excess sd is ~4% (E1 closer to 5-6%). n=225 gives SE ~0.27%; 60 shadow days yield roughly 20 E1 + 70 E2 events, SE ~0.45% — shadow cannot confirm the edge statistically. Shadow is for operations (latency, cutoffs, slippage, discipline); the statistical decision rests on OP1/OP2 on the unseen window plus the seen-window result already in hand.

Every run (OP1-OP4) appends a row to `things_tried.csv` with `who=OP`, the window label and n.

## 4. Expected economics (stated on the seen window, with the discount that implies)

- Per trade, seen window, net of 5 bp/side: E1 +0.79% [+0.08, +1.48] excess over SPY (raw +0.99%), win share 42% — a fat-right-tail trade; E2 +0.32% [+0.05, +0.62], win 46%.
- Sleeve-level, K5 equal-weight 20% slots, seen window: E1 +236% over 535 deployed nights (max DD -16.8%); E2 +685% over 1,398 nights (max DD -25.1%) — about +13%/yr and +23%/yr CAGR *including* five negative years, on a survivorship-favoured sample. 2015-2019 alone would have lost roughly 3-8% of the sleeve per year after costs.
- Costs dominate: ~370 trades/yr at 20% of the sleeve and ~10 bp round trip = ~7% of the sleeve per year in friction, already inside the numbers above. Auction fills are the only reason the assumed costs are attainable at retail; anything that crosses a spread breaks the arithmetic.
- Honest forward prior given regime dependence, survivorship and the failed pass criteria: **0 to +10%/yr on the sleeve, max drawdown ~20%, with a real chance (the 2015-2019 case) of a slow -5%/yr bleed.** Go-live is worth it only if OP1/OP2 pass on 2004-2013; otherwise the product remains a shadow log.
- Operator time: ~15 minutes/day, ~10 minutes Sunday.

## 5. Biggest risks
1. Regime dependence: the effect was negative every year 2015-2019. The equity-curve brake reacts after ~40 events (two to five months) — it limits, it does not prevent, a bleed.
2. Survivorship grows with lookback, so the 2004-2013 confirmatory window is *more* biased in the rule's favour than 2014-2024; a pass there is necessary, not sufficient; a fail is decisive.
3. Overnight tail risk with no stop: a -30% single-name gap (guidance cut, offering, fraud) costs 6% of the sleeve; five correlated longs on a crash night cost more. Sizing is the only defence.
4. Execution/latency: the whole design hinges on a real-time snapshot at 15:44 and orders in by 15:49-15:50. FMP real-time exists only in production, so shadow must run on Railway; email delivery jitter of even 3 minutes converts trades into MISSED_CUTOFF.
5. Event clustering: E1 nights cluster in earnings seasons; months can pass with zero D1 events, then four nights in a row with five positions each. Behavioural risk: the operator stops watching, or overrides the cap.
6. Broker mechanics: some retail brokers restrict MOC on Nasdaq names or after 15:45, partial-fill MOO on thin opens, or queue MOO for halted names differently.
7. Decision-vs-close mismatch: the 15:44 last price and volume are not Close_T/Volume_T; OP3 measures the gap, but the live cell is not literally the tested cell.
8. Multiple looks: this is the fourth family tested in the project; Holm across the eight registered cells already failed. Adding OP-cells increases the family; the confirmatory window is the only clean look left.

## 6. What would kill it
- OP1 and OP2 both fail on 2004-2013 (lower CI <= 0 or quarters positive < 0.60): no structural effect; the product is a regime bet and does not go live. The shadow log continues only as a free data collection.
- OP3 coverage < 50% or flip rate > 25%: the tradable cell is materially different from the tested cell.
- Shadow slippage > 15 bp/side or MISSED_* > 30% of event nights: the operator cannot execute the design as specified.
- A single overnight loss > 20% of the sleeve, or two consecutive brake breaches, at any tranche: stop and re-register before resuming.

## 7. Build steps (sized for a two-week shadow start; all new research files under `research/OP_*`; repo work in `C:\dev\Trader-v3` only after the operator's explicit OK)
1. Day 1: copy `OVERNIGHT_holdout.py` to `OP_confirmatory_2004_2013.py` with the window changed and nothing else; add the matched non-event control and the brake evaluation as separate, clearly labelled sections; fetch SPY 2004-2013 via yfinance with retries (note: `SPY_10y.csv` is not at the stated research path today — locate or refetch before OP1/OP2). Do not run until `bars1d_2004_2013` is complete or the run is stamped partial with the file count.
2. Day 1: `OP_coverage_1544.py` on `bars5m_inplay` (60 days): decision-time D1/D2 vs official E1/E2 -> coverage, flip rate, decision-vs-close drift (OP3 measurement). Append `things_tried.csv`.
3. Days 2-4: `overnight.py` in the scanner service (no imports from `themes/`): universe builder, snapshot, D1/D2 detection, ranking, brakes, log writer, counterfactual back-fill. Pure functions over pandas frames so `tests/test_overnight.py` can drive them with synthetic bars (event detection, ranking tie-break, cap of 5, brake state machine, log schema, cutoff handling).
4. Day 5: `app.py`: add jobs `on_universe_0700`, `on_prelim_1535`, `on_decide_1544`, `on_confirm_1610`, `on_morning_0845`, `on_settle_0940`, `on_weekly_sun_1800` (all `max_instances=1`, `misfire_grace_time=60` for the 15:44 job so a late fire becomes MISSED_CUTOFF rather than a late order). Env flags `OVERNIGHT_ENABLED`, `OVERNIGHT_MODE=shadow|live`, `OVERNIGHT_SLEEVE_USD`. Existing scanner jobs untouched.
5. Day 5: `notifier.py`: add `send_overnight(subject, html)` that bypasses the STRONG/ELITE gates, controlled by `NOTIFY_OVERNIGHT=1`; subjects `[ON-WATCH] [ON-ORDERS] [ON-NOTRADE] [ON-CONFIRM] [ON-EXIT] [ON-WEEK]`.
6. Day 6: `templates/overnight.html` + `GET /overnight` (today's card, log table, weekly metrics, phase-gate status) + `POST /api/overnight/fill` (operator fill entry). Reuse `_head/_nav/_footer.html`. Add `overnight_log.json` and `overnight_universe.json` to `data_backup.py`.
7. Day 7: local dry run against `bars1d_all` daily replay (decision at official close, since local data is delayed) to validate the log end-to-end; Jinja-parse the template; `python -m pytest tests/ -q`.
8. Day 8: release hygiene (bump `config.APP_VERSION`, `templates/logic.html` release entry), commit and push **only with the operator's OK**; Railway env: `OVERNIGHT_ENABLED=1`, `OVERNIGHT_MODE=shadow`, `NOTIFY_OVERNIGHT=1`.
9. Days 9-14: shadow operation. Verify: 15:44 quote timestamps, email arrival time vs 15:50, MOC/MOO handling at the operator's broker on one paper ticket, fill-entry form, Sunday report. Fix operational bugs only; no rule changes.
10. Day 15 onward: 60-trading-day shadow clock runs (to roughly mid-December 2026). OP1/OP2/OP4 run once when the 2004-2013 download completes. Go/no-go for tranche 1 on the OP5 criteria at the first Sunday review after both are satisfied.
