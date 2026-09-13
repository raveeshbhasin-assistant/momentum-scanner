# Is It Down to News? Post-Earnings Drift and News-vs-No-News, Tested on 22 Years of SEC Filings

**Date:** 2026-09-13. Operator question: "Are you saying it's basically down to news, and tracking real-time news might be the best way to find momentum in stocks? Let's think about how we can solve that."
**Method:** the free, complete, timestamped record of public catalysts is SEC EDGAR. We built it: every 8-K filing for the universe since 2004 (701,481 filings, 4,384 tickers, item codes, acceptance time converted to Eastern with DST, mapped to the first tradeable session), plus a 196k-filing earnings history from Item 2.02. Two pre-registered tests were run once each on the 2015 → 2026-08 test window with discovery on 2005 → 2014, and each was independently recomputed by a separate agent from the raw files. A third agent designed the real-time architecture. Files: `research/validation/news/`; datasets in `C:\dev\Trader-v3-data\research\` (not in git; 109 MB).
**Conventions:** returns are net of costs (5 bp/side above $100M ADV, 10 bp/side for $50–100M) and in excess of SPY over the identical window; CIs are 95%, the wider of day- and quarter-clustered bootstraps; universe filters (prior close ≥ $5, ADV20 ≥ $50M) use data through the prior day. Survivorship: the symbol list is as of 2026-09; delisted names are absent, which flatters every long result and understates reversals.

---

## 1. Post-earnings-announcement drift: does not pass

24,784 earnings reactions (reaction session mapped from the 8-K acceptance time; 52% after the close, 46% before the open), entry at the next open.

| Cell (test window, long unless stated) | n | 5 sessions | 20 sessions | Matched no-news movers (20d) |
|---|---|---|---|---|
| Top-quintile earnings reaction | ~5,000 | −0.17% [−0.44, +0.09] | +0.10% [−0.59, +0.79] | −0.22% |
| Big-beat proxy: reaction ≥ +8%, volume ≥ 3× | ~1,580 | −0.10% [−0.52, +0.32] | +0.75% [−0.44, +1.81] | +1.13% |
| Top quintile, ADV $50–500M | 4,207 | — | +0.00% [−0.71, +0.71] | −0.27% |
| Top quintile, most-liquid tercile | 1,645 | — | +0.76% [−0.23, +1.76] | −1.05% |
| Bottom-quintile reaction, short side | ~4,900 | +0.40% [−0.02, +0.80] | +0.66% [−0.01, +1.36] | drifts the same |

Zero of eight registered cells pass (Holm-corrected p ≥ 0.21 everywhere). There is no small-cap gradient; if anything mega-caps drift more, on 143 events. Post-close announcers drift more than pre-open ones (+0.39% vs −0.31% at 20 days) but neither clears zero. The only thing that looked alive, shorting the worst reactions, is matched by non-event stocks that fell the same amount: the move, not the news. Every long cell is negative in 2025 and 2026. Per-capital at ten positions underperforms holding SPY. Verifier: all cells reproduced to rounding, 40 of 40 audited trades free of look-ahead.

## 2. News versus no news, same size of move: the decisive result

For every ticker-day with a move of +8% or more over SPY (13,939 in the test window), was there a public catalyst? An 8-K of any item type whose tradeable session is that day counts as NEWS; no 8-K in the surrounding sessions counts as NO-NEWS.

**Two thirds of these moves have no filing at all** (9,262 of 13,939). Only 23% carry an 8-K on the day (2,328 earnings, 841 other items).

| Arm, entry at the close of the move day | n | T+1 | T+5 | T+20 | Years positive (T+5) |
|---|---|---|---|---|---|
| NEWS (any 8-K that session) | 3,169 | +0.12% [−0.10, +0.38] | −0.22% [−0.68, +0.22] | −0.18% [−1.31, +1.06] | 58% |
| NO-NEWS | 9,262 | +0.00% [−0.27, +0.28] | **−0.82% [−1.49, −0.10]** | −1.59% [−3.82, +0.70] | 25% |
| NEWS minus NO-NEWS | | +0.12% | +0.60% [−0.12, +1.25] | +1.40% [−0.07, +3.09] | |

The classic result replicates in sign: **moves without a public catalyst revert** (whole interval below zero at five sessions, negative in nine of twelve years, same sign in the 2005–2014 discovery decade and in both regime halves, robust to the verifier's stricter implementable definition: −0.85% [−1.51, −0.15]). **Moves with a catalyst are flat, not positive.** The entire news-minus-no-news gap is the no-news reversal. Zero of six family cells pass as a long. Non-earnings 8-K movers (−1.54% at 20 days) behave like no-news movers; earnings movers are the only positive-pointing sub-arm (+0.31% at 20 days, +0.55% after excluding recent dilution and restatement filers) and are nowhere near clearing zero, consistent with §1.

**By item type (descriptive, 20-day, net over SPY):** dilution filed on the move day −7.8% [−13.5, −2.3] (n = 78); shareholder-vote results −5.2%; executive changes −1.9% at five days; material agreements −2.0%; Reg FD −0.5%; earnings +0.3%. The type fixes the sign the way the architecture review predicted, and no type is a positive long after costs.

**Down-side mirror:** 8-K-tagged drops of 8% or more keep falling (−0.37% the next session, −1.16% [−2.15, −0.18] over 20 days); no-news drops are flat, no bounce. A short of news-driven drops nets about +0.9% over 20 days before borrow, lower interval near −0.1%: suggestive, not established.

Verifier: the event set reproduced exactly (13,939 events; 0 mismatches on arm, cost tier or returns to 1e-16), all fourteen claimed cells confirmed, no look-ahead; concerns noted that "no 8-K" also contains foreign 6-K filers, analyst actions, index changes and press releases that never became filings, so the true catalyst-free share is below 66% and the reversion is, if anything, understated.

## 3. What this answers

- **"Is it down to news?"** In the mechanistic sense, yes: the moves that revert are the ones with no public reason, and that is two thirds of the big moves the scanner was chasing. In the tradeable sense, no: knowing that a move had a filed catalyst, when it was filed, what type it was and how the price reacted does not produce a continuation that survives 10–20 bp of costs on this universe, at one, five or twenty sessions, in either decade.
- **"Is tracking real-time news the best way to find momentum?"** Racing headlines is a game for co-located machines and this project's three intraday tests are the empirical version of that fact. Catalyst *type* is genuinely informative about the sign of what follows, but the information has so far shown up as **what to avoid** (no-news moves; dilution, restatement, delisting and executive-departure filings; holding through the session after a move) rather than as a long to buy. The one lever not yet tested is the **content** of the release: beat size, guidance change, whether it was pre-announced. That needs the release text (EX-99.1 exhibits, ~4–6k documents for the last two years' large reactions) classified by an LLM against a fixed schema, and it is the only remaining experiment on this line worth funding.

## 4. The real-time architecture, if content classification ever passes

Designed in `research/validation/news/NEWSARCH_design.md`. Sources with latency, cost and backtestable history: EDGAR live filing feed (seconds to a minute after acceptance; complete history since 2004 with item codes and exact timestamps; free); press-release wire RSS (about a minute; no usable archive); FMP press-release and news endpoints (minutes; survivor-only history; production key); Finnhub company news (minutes; one year of history; duplicates; counts carry nothing); exchange halt feeds; earnings calendars with timing. Decision times are the closing and opening auctions only; nothing intraday. Classification schema: event type, direction, magnitude, novelty, guidance changed, dilution flag, binary event ahead; pinned model and prompt, a golden regression set, no changes inside a test window. Pipeline: ingest → dedupe → classify → rank → decision window → orders → append-only log. Build order: the on-disk history tests first (done: both negative), exclusions second, the EX-99.1 content study third and only if warranted, a logging-only poller from day one regardless (it costs nothing and builds the lived dataset), shadow decisions only for a cell that has passed.

## 5. Recommendations

1. **Adopt the exclusions as standing hygiene for anything traded from here on:** no continuation position in a move that has no public filing; no long in a name with a dilution, restatement, delisting-notice, bankruptcy or reverse-split filing in the prior 60 sessions; no holding through the session after a no-news move. These are the only findings in the news track with intervals that exclude zero.
2. **Do not build a real-time news trading pipeline now.** There is no passed cell to act on; a poller that only logs is the right scale until one exists.
3. **The one experiment left on this line:** register a content-classification study on the last two years of earnings releases (EX-99.1 text, LLM labels for guidance and beat size, validated against 8-K item codes and forward returns), discovery on 2024-09 → 2025-08, test on 2025-09 → 2026-08. Expect a small effect at best; the value is in settling the question.
4. **The short side of news-driven drops** is the only sign-stable positive-expectancy pattern seen today across tracks (news drops continue; extended large caps fade). It is unregistered and needs borrow costs, halts and a proper test; if the operator can short, it deserves a registration before anything long does.

## 6. Provenance

Registrations: `preregistration_pead.json` (SHA e836e79f…), `preregistration_news_vs_nonews.json` (SHA 9a61eb6a…). Engines and reports: `PEAD_run.py`, `PEAD_report.md`, `N1_run.py`, `N1_report.md`, verifier reports `VPEAD_report.md`, `VN1_report.md`. Dataset builders: `NEWS8K_sec_8k_all_fetch.py`, `NEWS8K_flatten.py`, `news8k_README.md` (item map, counts by year). Things tried: 100+ rows appended to `things_tried.csv` across the two tests, all descriptive variants labelled as such.
