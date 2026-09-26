# themes_web — Release Log

_One entry per user-visible change, newest first. Convention: bump
`themes_web/version.py` → add an entry here (what / why / verified / rollback)
→ the footer and `/releases` page update automatically. This mirrors the
scanner's `config.APP_VERSION` + `logic.html` release-hygiene convention._

---

## v1.9.0 — 2026-09-26 **(current)**

**What** Ignition Watch history is now append-only.
- **Carry-forward:** each run carries forward every position the previous run
  recorded (`scan.carry_forward`).
  - A closed trade keeps its recorded exit, even if revised prices would now
    say otherwise (`kept`, shown as "(recorded)").
  - An open position the price rebuild no longer produces is kept with its
    last values, a reason, and a grey "kept · <date>" tag. If the data returns,
    the fresh position replaces it.
- **Download guard:** failed Yahoo batches are retried three times, and a
  scan where under 97% of tickers have a last close is refused
  (`check_coverage`; normal coverage is 100%). The Actions run fails without
  committing, so the page keeps the last good data.
- **Run log:** each line now records `carried`.

**Why** Operator: history must survive every run and redeploy. Redeploys were
already safe, since every run is committed to the `ignition-data` branch and
Railway re-syncs at boot. But the ledger is rebuilt from prices each run, so
three things could still silently erase a position: refreshing
`universe.txt` (which the README tells you to do), a failed Yahoo batch (it
was skipped silently), or an adjusted-price revision.

**Verified** `pytest` green (58; 7 new tests cover carried/kept positions,
fire-date shifts, the refusal of partial downloads, and page rendering).
End-to-end on real prices: rerunning with BE and MRVL removed from the
universe used to drop them (107 to 105 positions, losing BE's +723%). It now
keeps both, flagged, with every other position and stat identical.

**Rollback** Revert the commit. Positions already carried in `latest.json`
stay in the data branch history.

---

## v1.8.2 — 2026-09-26

**What** The Ignition Watch open-positions table fits without a horizontal
scrollbar. At 1440px it was 1205px wide in a 1022px box.
- Status tags stack vertically.
- Headers shortened: "Room to sell line" is now "To sell line" and "Sessions
  since last fire" is now "Since fire". The full wording is in tooltips.
- "68.0% above" is now "68.0%".
- The stop bar is narrower (64 to 48px) and cell padding went from 12 to 10px.
- Below 1000px the Entry, Peak and Re-fires columns hide.

**Why** Operator: the table was cut off with a scroller at the bottom.

**Verified** Measured in-browser on live data. Natural table width against
available width: 1440px viewport 1022 of 1022 (was 1205); 1024px 916 of 951;
960px 696 of 887. Phones still scroll inside the table, as expected for a
dense table. `pytest` green (51).

**Rollback** Revert the commit.

---

## v1.8.1 — 2026-09-26

**What** Fix: `/ignition` returned HTTP 500 after the v1.8.0 deploy. The
container still held `latest.json` from the previous scan, which has no
`cap_trim` field.
- **Cause:** in Jinja a missing key is Undefined, not None, so
  `p.cap_trim is not none` was true and formatting `p.weight_x` raised an error.
- **Fix:** the pill now checks `is defined` first.
- **Recovery:** production was restored within minutes by syncing the new
  scan (`POST /api/refresh_ignition`), before this fix shipped.

**Why** A deploy can land before the next scan, so the page must render data
written by an older `scan.py`.

**Verified** `pytest` green (51). A new test renders the page from a
`latest.json` with every v1.7/v1.8 field removed; it fails on the v1.8.0
template. The page was also rendered locally from the real pre-v1.8
`ignition-data` scan (e6dcbce).

**Rollback** Revert the commit.

---

## v1.8.0 — 2026-09-26

**What** Ignition Watch adds a **2× cap review**. A held position whose
weight has reached 2× or more of an equal share gets a "2× cap · trim N%"
pill, where N% is the part to sell to get back to 2×. The Open positions tile
lists the flagged names.
- **How weight is estimated:** the ledger has no real sizes, so it assumes
  equal dollars at each entry: `(1 + ret) / mean(1 + ret)` over held positions.
- **On 2026-09-25 data:** BE is at 4.5× (trim ~56%) and LITE at 3.5× (trim ~43%).
- Flag only: no status, sell or stat changes.

**Why** Operator asked to surface the 2× cap for review. In the trimming study
(`research/ignition_exits/profit_protection/`), a hard 2× cap cut the worst
drawdown at nearly every portfolio size for ~0.5 pp CAGR. It is risk control,
not a return edge.

**Verified** `pytest` green (50; new tests cover the weight maths, only held
positions counting, and well-formed pill markup). A real scan leaves the
ledger unchanged at 34 open / 73 closed.

**Rollback** Revert the commit.

---

## v1.7.1 — 2026-09-26

**What** Fix: the "6-mo review" pill printed a stray `">` before its label.
Its hover title used the `pct()` macro, which emits a `<span class="...">`,
and the quotes closed the attribute early. The title now uses plain text.

**Why** Found on the live page right after the v1.7.0 deploy.

**Verified** `pytest` green (48). The page test now checks that the pill
markup is well-formed, and it fails on v1.7.0's template.

**Rollback** Revert the commit.

---

## v1.7.0 — 2026-09-26

**What**
- **6-month review** on Ignition Watch. A position 126+ sessions after entry
  that is up, but by less than +30%, gets a "6-mo review" pill and a
  `CHECKPOINT` event, and appears in a new run-log column. It is a flag only:
  statuses, sells and the existing stats are unchanged.
- The "All positions" tile adds a what-if line: the same ledger scored as if
  every flag had been sold at the next open. On data through 2026-09-25:
  avg +22.1% vs +23.8% live, median −10.1% vs −11.6%, 42.1% vs 37.4% up.
- The run-log column "Sell signals" is now "Exits" and is no longer red,
  matching the v1.6.4 tile.

**Why** Operator: "we only sell when the gains are gone." True: the sell
line sits ~14% below entry, so every base sell is a loss. A pre-registered
study (`research/ignition_exits/profit_protection/`) tested 60 profit-lock
rules and none passed. The 6-month review was the most stable near-miss:
about half the round-trips and a better median, for ~6 pp of mean. So it is
shown, not enforced.

**Verified** `pytest` green (48; new tests cover the flag, big winners
running unflagged, and the page rendering old run-log lines). A real scan on
2026-09-25 data gives an unchanged ledger (34 open / 73 closed), with 8 open
positions flagged.

**Rollback** Revert the commit. The next scan rewrites `latest.json` without
the new fields, and the template guards on their absence.

---

## v1.6.4 — 2026-09-26

**What** The Ignition Watch KPI tile "Sell signals, last 20 sessions" is
now **"Exits, last 20 sessions"**, and it is no longer coloured red. It
counts every close: failed ignitions *and* 12-month age-outs (e.g. RMBS on
2026-09-23 aged out at +0.1%; TWLO aged out at +109%), so "sell signal"
and red were misleading.

**Why** Operator review of the live page.

**Verified** `pytest` green; template parses; page renders locally.

**Rollback** Revert the commit.

---

## v1.6.3 — 2026-09-26

**What**
- Live quotes on the tracker read `FMP_API_KEY` straight from the
  environment instead of importing the scanner's `config` module. Same key,
  same behaviour; themes_web no longer loads scanner config at request time.
- CLAUDE.md now describes the repo as it is: scanner + themes_web, plus
  the standalone `ignition/` module. The import rule is spelled out
  (themes may use `config`/`fmp_data`, never `app.py`/`scanner.py`;
  ignition is never imported). Ignition data goes only to `ignition-data`.
  themes_web releases bump `version.py` + this file.

**Why** Operator: align the repo docs after Ignition Watch moved in.

**Verified** `pytest` green; `themes_web.app` imports cleanly.

**Rollback** Revert the commit.

---

## v1.6.2 — 2026-09-26

**What**
- The Ignition Watch research moved into this repo from the operator's
  Test1 repo, as `research/ignition_discovery/`: the 628-episode discovery
  study, its pipeline, `results.json`, and the standalone `report.html`.
  All references now point here. Left out: the applied v1.5.0 deploy
  patch, and `recent_fires.py`, which `ignition/scan.py` replaced. Both
  remain in Test1's git history.
- Fix: a second scan on the same data date (a manual re-run, or a code
  push on a weekend) re-reported that day's fires and sells as "new". It
  now reports nothing new.
- The workflow's push trigger is narrowed to `ignition/scan.py`,
  `ignition/universe.txt` and the workflow file, so doc edits don't start
  a scan.

**Why** Operator: keep all the momentum work in the trader repo. The
re-run bug turned up while reviewing the workflow trigger.

**Verified** `pytest` green (45; the new re-run test fails on the old
line). Moved scripts and `results.json` parse. No remaining Test1
references except the provenance note.

**Rollback** Revert the commit.

---

## v1.6.1 — 2026-09-26

**What** Fix: the Ignition Watch boot sync now runs immediately after a
deploy.

**Why** v1.6.0 scheduled the first sync with a naive `datetime.now()`.
APScheduler reads that in the scheduler's America/New_York zone, so on
Railway's UTC host the boot sync was queued about 4 hours in the future.
After the v1.6.0 deploy, `/ignition` showed "Loading" until a manual
`POST /api/refresh_ignition`.

**Verified** New regression test (fails on the old line, passes on the
fix); `pytest` green (44). On production, the manual sync loaded
`source=github`, 107 positions and the first run-log entry.

**Rollback** Revert the commit (restores v1.6.0 behavior).

---

## v1.6.0 — 2026-09-26

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
