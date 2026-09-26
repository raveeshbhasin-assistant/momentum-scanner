# themes_web — Release Log

_One entry per user-visible change, newest first. Convention: bump
`themes_web/version.py` → add an entry here (what / why / verified / rollback)
→ the footer and `/releases` page update automatically. This mirrors the
scanner's `config.APP_VERSION` + `logic.html` release-hygiene convention._

---

## v1.6.0 — 2026-09-26 **(current)**

**What** Ignition Watch becomes a position ledger with a sell signal and a
permanent run log.
- `ignition/scan.py` replays every fire since 2025-01-02 as a position
  (bought at the next open). Statuses: NEW / HOLD / REFIRED /
  EDGE_EXPIRED / SELL_PENDING / CLOSED. A position sells on **ignition
  failed** (a close below the pre-ignition base) or ages out after 252
  sessions without a re-fire. The ledger is rebuilt from prices each run,
  so skipped runs can't lose events. Anything since the previous run is
  reported as new.
- New `.github/workflows/ignition-daily.yml` is now the scan of record. It
  runs weekdays at 21:30 UTC and commits every run (`latest.json`,
  `runs.jsonl`, `history/`) to the `ignition-data` branch. That branch is
  not main, so there's no redeploy. themes_web now **syncs** from the
  branch every 30 minutes and at boot, replacing the 17:00 ET in-app scan.
  Local scan remains as a fallback only, via
  `POST /api/refresh_ignition?local=1`.
- `/ignition` page redone: alerts since the previous run, KPIs, open
  positions with each one's sell line and room to it, closed positions,
  run log, near misses and RS leaders.
- Research: `research/ignition_exits/` — 28 exit rules on 748 trades,
  train/holdout/live splits.

**Why** Operator: remember every run's outcome, and say when a breakout is
unlikely to continue so the stock should be sold. The operator also asked
whether dropping off the list (CNH) means sell. It doesn't: that was the
worst of 28 rules. Stocks that were off the list the next day still
returned +11.6% over 3 months, vs +4.6% for all stocks. The pre-ignition
base close was the only exit near the top in all three splits.

**Verified** `pytest` green (43, including 6 new ledger and render tests
on synthetic prices). Full scan run end to end on live Yahoo data: 107
positions since 2025, CNH = HOLD with its sell line at $11.83. `/ignition`
and `/api/ignition` render from real output. The workflow is confirmed on
its first run after the push.

**Rollback** Revert the commit. The `ignition-data` branch can stay
(inert) or be deleted.

---

## v1.5.0 — 2026-09-26

**What** New `/ignition` page: Ignition Watch. A daily scan of the S&P 500
+ 400 (903 stocks) for the momentum-ignition signal: a +12% week on 1.5×
volume with the 50-DMA above the 200-DMA. The page shows a 90-session fire
log with NEW and +40% badges, near misses, and RS leaders.
- New standalone `ignition/` module (`scan.py`, pinned `universe.txt`,
  README with the full backtest). Runs by subprocess only, which keeps the
  no-cross-import rule.
- Scheduler: new weekday 17:00 ET job, plus a one-off boot scan when
  `ignition/data/latest.json` is missing (the disk is wiped on redeploy).
  Manual trigger: `POST /api/refresh_ignition`. JSON: `GET /api/ignition`.
- "Ignition" nav link next to "3X Screen".

**Why** Operator research: find Dell/Micron-style explosive moves early.
Across 628 episodes of +80% in 63 days (2015–2026), the one catchable tell
was this ignition week. After a fire, 26.9% traded ≥ +40% within 63
sessions vs 4.6% baseline, and the signal beat baseline in 10 of 11 years.
The 3X Screen is a 2–3y thesis view; this is its weeks-scale counterpart.

**Verified** `pytest` green. Full scan run end to end against live Yahoo
data (903 tickers, ~1 min). TestClient checks: `/ignition` renders 200 with
and without data, `/api/ignition` returns 404 then 200. Nav renders on
existing pages. Scheduler registers the new jobs.

**Rollback** Revert the commit. The new module and page are additive;
existing pages and jobs are untouched.

---

## v1.4.0 — 2026-07-21

**What** `/referrals` app v2 — "Early indicators" lens for newer/pivoting
companies the 3y flywheel gate structurally excludes:
- Eligibility: listed ≤ 5 years (real IPO date — statement count would
  misclassify mega-caps as young) OR re-accelerating (<10% growth two FYs
  ago → ≥15% now) with revenue < $10B (size test uses revenue, not market
  cap, preserving the no-price-data purity rule).
- Gates (latest data only): growth ≥ 15% · latest magic ≥ 1 · engine
  improving (S&M% fell or magic rose YoY).
- Universe +15 recent IPOs (FIG, CHYM, CRCL, KLAR, KVYO, IOT, RBRK, CART,
  ONON, BIRK, SN, SG, BROS, TEM, OSCR → 127 companies scored).
- App: early-picks strip, EARLY badge, filter, per-gate detail; snapshots
  now record the early flag for forward tracking. No themes_web code
  changes — referral_moat module + regenerated static page only.

**Why** Operator: flywheel picks skew to forever-incumbents (GOOGL et al);
wants a lens for companies without 3y history due to time-in-market or
business pivot.

**Verified** pytest green; TestClient `/referrals` 200; GOOGL/META/AMZN
correctly NOT early-eligible after the listing-age fix; 9 early picks all
listed ≤5y or genuine re-accelerations; UI checks in browser.

**Rollback** Revert the commit; prior scorecards/site restore on deploy.

---

## v1.3.0 — 2026-07-21

**What**
- New `/referrals` page: the Referral-Moat research app (new standalone
  `referral_moat/` module — ~111 companies across 12 industry groups scored
  on the financial fingerprints of word-of-mouth customer acquisition, with
  a three-gate "flywheel" pick list: growing + efficient acquisition +
  sales intensity not rising). Self-contained static page; scores never see
  price data (returns are attached afterwards as an output-only evaluation).
- Monthly scheduler job (1st of month, 19:00 ET) rebuilds scorecards + site
  via subprocess (`referral_moat/build.py` → `make_site.py`), preserving the
  no-cross-import rule. Manual trigger: `POST /api/refresh_referrals`.
- `referral_moat/data/` (scorecards + dated snapshots) is committed as the
  deploy-time seed, same philosophy as `themes/_benchmarks.json`; root
  `.gitignore` narrowed from `data/` to `/data/` to allow it.

**Why** Operator research thesis: high-customer-referral companies should
outperform; wants it deployed, queryable, refreshed monthly, with picks
chosen on theory metrics only (returns as output, never input).

**Verified** `pytest` green; TestClient renders `/referrals` (200) with
embedded data; flywheel gates spot-checked against per-year statements;
scheduler registers both jobs.

**Rollback** Revert the commit — `/referrals` disappears, themes pages and
scanner unaffected (module is fully standalone).

---

## v1.2.0 — 2026-07-09

**What**
- Version convention introduced: `themes_web/version.py` (`THEMES_WEB_VERSION`),
  wired into `FastAPI(version=…)`, a Jinja global, a site footer on every page,
  and this release log rendered at `/releases`.
- Performance-data fix: the 9 themes locked 2026-07-06 showed `+0.00%` on the
  Portfolio page because their `candidates.json` was still the lock-day seed —
  the very snapshot their tracker entry prices came from (`now == entry` by
  construction). All 9 refreshed with current prices.
- `themes/_benchmarks.json` is now committed as a **deploy-time seed baseline**
  (it was untracked, so every fresh deploy had no SPY reference until the
  nightly 18:00 ET job ran — that's why "vs SPY" read `+0.00 pp`). The nightly
  refresh still overwrites it at runtime; the committed copy just guarantees a
  sane baseline immediately after deploy, same philosophy as the scanner's
  `data_seed/`.

**Why** Operator report: Portfolio cards for the new themes all read
`+0.00% / +0.00 pp vs SPY`.

**Verified** `pytest` green; TestClient renders `/`, `/portfolio`, `/moonshots`,
`/releases` (200); Portfolio deltas non-zero after refresh; footer version
matches `version.py` on every page type.

**Rollback** Revert the commit; the app runs fine without `version.py`'s
consumers (footer falls back to no version display) and with stale seeds.

---

## v1.1.0 — 2026-07-08

**What** New `/moonshots` "3X Screen" page + nav link: the Portfolio view
filtered to the 12 names most likely to 3X in 2–3 years, from the
10X/3X multibagger research (`research_findings_3x_growth.md`). Tier-grouped
cards with live prices, Δ since anchor, 3X-target progress, bull/bear cases,
odds, tracker-pick vs universe-only badges, notable exclusions.
`/api/moonshots` serves the JSON. Data: `themes/_moonshots_3x.json` joined
with each theme's nightly `candidates.json`.

**Why** Operator asked which picks across all 14 themes carry 3X convexity and
for a dedicated filtered page.

**Verified** TestClient 200s on `/moonshots` + `/api/moonshots` + regression on
`/portfolio` and tracker pages; both templates Jinja-parse.

**Rollback** Remove the route + template + nav link; config JSON is inert.

_(Released untagged — this entry back-fills it; the version convention arrived
in v1.2.0.)_

---

## v1.0.0 — 2026-05-16

**What** Initial themes_web app: 5 page types per theme (tracker / thesis /
supply-chain / candidates / scoring), theme dropdown with Active/On Deck/Backlog
optgroups, cross-theme `/portfolio` view, live-quote overlay, 13F + news +
earnings overlays, nightly APScheduler refresh (18:00 ET weekdays) + manual
`POST /api/refresh/{slug}`.

_(Back-filled entry — predates the version convention.)_
