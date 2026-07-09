# themes_web — Release Log

_One entry per user-visible change, newest first. Convention: bump
`themes_web/version.py` → add an entry here (what / why / verified / rollback)
→ the footer and `/releases` page update automatically. This mirrors the
scanner's `config.APP_VERSION` + `logic.html` release-hygiene convention._

---

## v1.2.0 — 2026-07-09 **(current)**

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
