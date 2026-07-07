# Live 52-Day Analysis — ELITE Precision & Entry Timing (Verified)

**Date:** 2026-07-06
**Window:** 2026-04-20 → 2026-07-06 (52 trading days) via `GET /api/performance/range` on the deployed app.
**Picks:** 5,830 total → **4,399 truly-live first-appearance picks** (date ≥ 2026-05-12, when `strong_signal` began being logged live; earlier data is backfill-contaminated and used only for context). All metrics below are on the live, first-appearance set unless noted.
**Bar-level layer:** all **941 live STRONG picks** were re-enriched with 5-min bars (yfinance, prepost) — entry-bar shape, look-ahead-safe range position, and forward timing sim. The sim reproduces the API's recorded WIN/LOSS on **99%** of picks, so the bar-matching is sound.
**Verification:** every headline claim was independently recomputed from raw data by 4 adversarial verifier agents + 1 completeness critic (separate scripts, no shared code with the analysis). Verdicts are noted inline. Numbers below are the *verified* ones.

**Metric conventions:** `hit2.5R` = `result=="WIN"` (target before stop, before 15:55); `WR` = pnl>0; `avgR` = mean r_realized (LOSS = −1R).

---

## 1. State of the union — where the P&L actually is

| Cohort (live era) | n | hit2.5R | WR | avgR | totR |
|---|---|---|---|---|---|
| ALL first-appearance | 4,399 | 13.3% | 38.9% | **+0.010** | +44 |
| **STRONG** | 941 | 19.8% | 41.6% | **+0.154** | **+145** |
| non-STRONG | 3,458 | 11.5% | 38.2% | **−0.029** | **−100** |
| re-entries (appearance 2) | 343 | 10.8% | — | −0.009 | −3 |

**The single biggest fact in the dataset: STRONG carries 100% of the system's positive expectancy, and the 3,458 non-STRONG picks burn −100R against it.** Every precision discussion below is a refinement *within* STRONG; the largest untouched lever is simply not treating non-STRONG output as tradeable signal. *(Verified — critic agent flagged this as the #1 under-emphasized lever.)*

### Temporal decay (VERDICT: CONFIRMED, reproduced to the decimal)

| Tier | 5/12–5/22 | 5/26–6/12 | 6/15–7/06 |
|---|---|---|---|
| ALL avgR | +0.173 | +0.045 | **−0.113** |
| Legacy ELITE (catD+rvol≥2) avgR | +0.504 | +0.266 | **+0.067** |
| **v3.7.5 ELITE avgR** | +0.626 | +0.470 | **+0.492** |
| v3.7.5 WR | 50.0% | 48.1% | 48.5% |

- The overall system and the legacy ELITE **demonstrably decayed** to break-even/negative (large n, monotone; OLS slope −0.007 R/day for legacy).
- The **v3.7.5 tightening worked**: flat-to-slightly-rising avgR (+0.003 R/day slope), ~48–50% WR held. The config's pessimistic "decayed to +0.07" note describes the *legacy* rule, not v3.7.5.
- ⚠️ Honest qualifier from verification: v3.7.5's per-window n is 16/27/33 and bootstrap CIs on avgR all cross zero. Correct statement: **"showed no decay in a sample too small to prove either way."** Promising, under-powered, keep collecting.

---

## 2. What technical signals would raise ELITE precision

### 2a. Verified and actionable

**(i) The scanner's #1 structural bias: it rewards extension, and extension is exhaustion.**
Within live STRONG, on look-ahead-safe bar features (verifier audited the look-ahead safety and CLEARED it):

| Feature at signal bar | Good side | Bad side |
|---|---|---|
| `range_pos_safe` (position in running day range) | 0.5–0.75: **34.9% hit, +0.55R** (n=146) | 0.9–1.0: **14.9%, +0.06R** (n=537) |
| `consec_green` *within 09:30–10:00 window* | ≤2: 26.7–33.3% hit, +0.30/+0.60R | **≥3: 12.5% hit, −0.09R** (n=56) |
| above opening-range high (entries ≥9:45 only) | inside OR: +0.39R (n=39, outlier-driven) | **above ORB: −0.06R** (n=417) |

**57% of live STRONG picks fire in the top 10% of the stock's running range** — the scanner systematically arrives at the top. The composite (EMA-stack + new-HOD + VWAP-distance) is mechanically maximized at maximum extension; the winners are the *least-extended* STRONG picks.
*(VERDICT: PARTIALLY_CONFIRMED — direction real and look-ahead-clean; the raw combo-gate lift is ~confounded with the time window (see 2c), so the residual independent effect inside the catD+window control is modest: ~+1.3 to +6 hit-points depending on the cut. Ship as a shadow tier, not a hard gate.)*

**(ii) RSI floor: leave at 68 — do NOT raise to 70.**
My initial read was that 68→70 was the robust lever (test-month 40.9% hit / +0.73R). The adversarial verifier broke this: rsi≥70 is *slightly worse* than the 68 floor in the train window and only wins in one 11-day test window (n=22; z=0.57, not significant); the genuinely dead band is **[65,68) (−0.079 avgR)** — which the current ELITE_MIN_RSI=68 already excludes — while [68,70) is mildly positive. *(VERDICT: PARTIALLY_CONFIRMED → recommendation withdrawn.)*

**(iii) Confirmed dead weight the current rule already handles well:** cat A/B/C STRONGs are all negative (LEADER −0.09R, LAGGARD −0.42R live — the "confirmed sector leader" is beta, not alpha, exactly as the 19-day diagnostic suspected); the 10:00–11:00 hour is the worst on the board (−0.123R, and only 6.8% hit).

### 2b. Tested and REJECTED (would have been shipped without OOS discipline)

| Candidate gate | In-sample | Held-out month | Verdict |
|---|---|---|---|
| `rvol ≥ 3` inside ELITE | 46% hit, **+0.94R** | 20% hit, **+0.12R** | **Mirage — do not ship** *(confirmed)* |
| `spy_above_vwap==0` (weak-tape filter) | +0.52R vs +0.23R | 23.8% vs 23.6% within window+catD control | Proxy, no residual OOS edge |
| tighter stop-distance bands | mixed | no stable band | noise |

The 5–6 RVOL "blow-off" cap (ELITE_MAX_RVOL=5) remains directionally supported — RVOL ≥5 STRONGs drop to 18.3% hit / +0.04R vs 32.3%/+0.41R for 3–5.

### 2c. The proposed rule (for shadow tracking, not silent replacement)

The rvol/stop gates in v3.7.5 are its weakest members. The stack that dominated every cut:

```
PROPOSED_ELITE = STRONG AND catD AND entry 09:30–10:00
                 AND consec_green(5m) <= 2 at signal bar
                 [optionally AND range_pos_safe <= 0.9]
```

| Rule | n (37d) | /day | hit2.5R | WR | avgR | ~$/day @ $100 risk |
|---|---|---|---|---|---|---|
| Current v3.7.5 ELITE | 76 | 2.1 | 32.9% | 48.7% | +0.512 | +$105 |
| + rsi≥70 + cg≤2 variant | 154 | 4.2 | 35.7% | 52% | +0.608 | +$253 |
| **window+catD+cg≤2 (balanced)** | 474 | 12.8 | 27.6% | — | +0.352 | +$451 |
| window+catD (STRONG opening-hour) | 574 | 15.5 | 26.0% | 43.9% | +0.289 | +$448 |

Robustness: the tight variant survives removing its top-5 R-contributing tickers (still 30.9% hit / +0.43R), so it is not a five-name artifact — though the critic's warning stands: **top-5 tickers = 42% of all STRONG-live profit**, so per-ticker daily caps are prudent regardless. Do **not** stack all three of {v3.7.5 rvol/stop gates, rsi≥70, anti-ext} — the intersection collapses to n=25 and *degrades* (+0.13R).

**Caveat that applies to everything in 2c:** `consec_green`/`range_pos_safe` cuts were selected from ~9 candidates on the same window (multiple-testing risk), and the residual effect inside the proper control is modest. That is exactly the epistemic situation STRONG itself was in circa v3.7.0 — the correct move is the same one used then: **persist the fields, badge the picks, gate only after ~4 weeks of live confirmation.**

---

## 3. Were picks made at the right time? (VERDICT: CONFIRMED in full)

The question decomposes into "right bar?" and "right hour?" — the data answers them oppositely:

**Right bar? Essentially yes — entry-bar micro-timing is NOT the problem.**
- Only **9.6%** (47/488) of stopped-out live STRONG losers ever printed their target later the same day. A stopped STRONG pick is a wrong *selection* 90% of the time, not a mistimed entry. (The old 3-day study's "46% premature" does not replicate on live data — different era, definition, and trivial n.)
- Winners confirm this: median **60 minutes** to +2.5R, median MAE **0.34R**, 68.8% never more than 0.5R underwater. When the pick is right, it works almost immediately and cleanly. There is no systematic "buy the pick 15 minutes later" improvement available — alt-entry tweaking would rescue ~5 losers/month while risking the fast winners.

**Right hour? No — and this is where timing genuinely lives.**
- 09:30–10:00 STRONG: **25.8% hit, +0.285R** (n=577) → the entire STRONG edge.
- 10:00–11:00 STRONG: **6.8% hit, −0.123R** (n=221) → actively money-losing, worse than the lunch hours.
- The v3.7.5 window gate is therefore confirmed as the correct call; the marginal improvement is recognizing that 10:00–11:00 is not "less good" but *bad*, which matters for any future loosening.

**The deeper synthesis — "right time" really means "right position in the move":** the scanner tends to fire only after the breakout is fully extended (57% of STRONG picks in the top decile of the running range, 3+ green bars already printed). The ideal moment was 1–3 bars earlier, when the same conjunction was forming rather than mature. Since only 9.6% of losers recover, *waiting* is not the fix — **selecting the less-extended fires is** (§2a). Practically: two simultaneous STRONG picks at 09:40 → take the one at 0.6 of its running range with 1–2 green bars, skip the one at 1.0 with 4.

**Follow-up worth simulating (not yet done):** with 68.8% of winners never exceeding 0.5R adverse, a tighter initial stop (e.g. 1.2–1.5×ATR instead of 2×) could raise realized R-multiples materially — but it re-denominates R for losers too, so it needs a full bar-level re-sim before touching `ATR_STOP_MULTIPLIER`.

---

## 4. Recommendations (priority order)

1. **P0 — Treat non-STRONG as research-only.** −100R live across 3,458 picks. Whether via `NOTIFY` scope, dashboard demotion, or emission gating: the composite-score-only tier should not be tradeable by default. Largest, highest-confidence lever in this report.
2. **P0 — Keep v3.7.5 ELITE as-is** (incl. RSI floor 68 — the 70 bump did not verify). It shows no live decay; its problem is volume (2.1/day), not precision.
3. **P1 — Persist the anti-extension fields on every pick** (`consec_green_5m`, `range_pos_running`, `above_orb_high` where OR is closed, plus `sentiment_score`/`premarket_boost` disaggregated — still missing from the payload). Zero-risk, unlocks the next iteration.
4. **P1 — Shadow-badge `PROPOSED_ELITE`** (§2c) alongside current ELITE for ~4 weeks; promote if hit ≥30% and avgR ≥ +0.4 hold. Expected ~4–13 picks/day vs today's 2.
5. **P2 — Per-ticker exposure cap** (e.g. max 2 ELITE trades/ticker/week): 42% profit concentration in 5 names is a fragility even though the rules survive leave-one-out.
6. **P2 — Re-sim tighter initial stop** (1.2–1.5×ATR) on the enriched bar cache before considering a config change.
7. **Watchlist, not action:** Wednesday is the one structurally negative weekday live (−0.134 avgR, all picks); SPY-below-VWAP looked strong raw but has no residual OOS edge within the window control — track, don't gate.

## 5. Caveats

- 37 live trading days ≈ one regime (May–Jul 2026, drifting-down tape). No cross-regime validation exists yet for the anti-extension effects.
- ELITE-tier inference sits on n=76–154; bootstrap CIs on avgR cross zero for the v3.7.5 tier in every sub-window.
- `elite` flag is not persisted per-pick in the API payload — it was reconstructed from config v3.7.5 constants (exact-match verified against field definitions, but persisting `signal["elite"]` into the performance log would remove this reconstruction risk for future analysis).
- Multiple-testing pressure on the bar-level features is real (~9 candidates tested); treat §2a magnitudes as upper bounds until shadow-tracked.

## 6. Provenance

- Data: deployed `/api/performance/range?start=2026-04-20&end=2026-07-06` (5,830 picks) + yfinance 5m bars (May 12–Jul 6, all 941 STRONG live picks; entry-bar match error median 0.05%).
- Sim validation: 99% agreement (485/488 LOSS, 185/186 WIN) between bar-level forward sim and API-recorded outcomes.
- Adversarial verification: 4 independent recomputation agents (decay: CONFIRMED; timing: CONFIRMED; rsi: PARTIALLY — recommendation withdrawn; anti-ext: PARTIALLY — confound quantified) + completeness critic (surfaced: non-STRONG bleed, anti-ext⊂window nesting, ticker concentration, Wednesday effect). Scratch scripts in the session scratchpad (`harness.py`, `analysis_*.py`, `verify_*.py`).
