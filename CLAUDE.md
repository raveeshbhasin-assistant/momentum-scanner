# Trader v3 — Project Guide

Personal quant equity system: **two strictly independent services** in one repo (scanner + themes_web), both deployed on Railway, sharing FMP + Finnhub API keys but **no shared code**. A third standalone module, **`ignition/`**, is surfaced by themes_web (subprocess/data only — never imported). Research (`research/`) is offline-only.

## ⚠️ Canonical location & sync — read this first every session

- **Canonical local repo: `C:\dev\Trader-v3`.** The old OneDrive copy (`…\OneDrive\Documents\Claude\Projects\Trader v3`) is **DEPRECATED — do not edit it.** OneDrive was truncating files and leaving stale `.git/*.lock` files; the repo was moved out on 2026-07-06. The OneDrive folder is kept only as a cold backup.
- **The deployed code is the source of truth, and it lives on GitHub (`origin/main`).** Railway auto-deploys `origin/main`.
- **At session start:** `git fetch origin && git status -sb`. If `behind`, `git reset --hard origin/main`.
- **After changes:** commit + push so local/GitHub/prod stay identical — **but only with the operator's explicit OK.**

## Service 1 — Momentum Scanner (`app.py`, FastAPI)

Intraday day-trading signals. Run: `python app.py`. Scans ~200 tickers every 15 min (9:35a–4:05p ET) via APScheduler.
- **PAUSED since 2026-09-26 (v3.9.0)** — `config.SCANNER_PAUSED` defaults on (no edge per the 100-day review): no scheduled jobs, no emails, `/api/scan` refuses, pages still serve. Resume: `SCANNER_PAUSED=0` on Railway + restart.
- Composite score = `technical×0.65 + sentiment×0.25 + volume×0.10` + additive boosts (sector/premarket/leadership/earnings). Weak floor 40, strong gate 60.
- **STRONG** = 4-way AND on the last *closed* RTH bar: `bar_green ∧ above_vwap ∧ new_hod ∧ pm_high_hold`.
- **ELITE** = `config.is_elite()` — the single source of truth. STRONG + cat D + RVOL∈[2,5) + RSI≥68 + entry 09:30–10:00 ET + stop≥0.9%.
- Persistence: picks → `data/{date}.json`; post-market P&L (4:15p) → `data/performance_log.json`. `data_seed/` bootstraps the Railway volume. (`data/` is gitignored — local runtime only.)
- Email: `notifier.py` via Resend API. **Off by default since v3.8.4** (`NOTIFY_ENABLED` defaults to false; the 100-day review found no edge). When enabled, gated ELITE-only (`NOTIFY_ELITE_ONLY=1`).
- Config lives in `config.py`; every threshold cites a backtest window — preserve/extend the citation when changing one.

## Service 2 — Themes Research (`themes/` + `themes_web/`, separate FastAPI)

Human-owned, Claude-assisted long-term thesis investing. Run: `uvicorn themes_web.app:app`.
- Per theme: `thesis.md` → `supply_chain.md` → `candidates.md/json` → `scoring` → `tracker.json/md` (top ~5–7 holdings).
- Active themes: AI Data Center, Space Economy, Modern Defense, Robotics, GLP-1.
- `themes/refresh_data.py` refreshes prices/fundamentals nightly (FMP→yfinance) into `candidates.json` + shared `themes/_benchmarks.json`.
- `/ignition` (v1.9.1): standalone `ignition/` module — momentum-ignition signal (+12% week, 1.5× volume, 50>200 DMA) replayed as a position ledger with a sell rule (close below the pre-ignition base, which sits ~14% under entry) plus flag-only reviews: a 6-month take-profit review and a 2× weight cap (research/ignition_exits/profit_protection/: 0/60 profit-lock rules passed; the review is shown as a what-if, never closes positions). The scan of record runs in GitHub Actions (`.github/workflows/ignition-daily.yml`, weekdays 21:30 UTC) and commits each run to the `ignition-data` branch; themes_web only syncs from that branch. Never push data to main (it would redeploy). History is append-only: `scan.py` carries forward every position a previous run recorded (closed trades keep their recorded exit) and refuses to publish a scan covering <97% of the universe, so refreshing `universe.txt` or a Yahoo gap can't erase history. Method: `ignition/README.md`; discovery study: `research/ignition_discovery/`; exits: `research/ignition_exits/README.md`.
- `/pullback` (v1.10.0): standalone `pullback/` module — one rule on eight index funds (SPY QQQ IWM DIA MDY IJR VTI RSP): 5-session return ≤ −3% while above the 200-session average → buy next open, sell at the close five sessions after the signal. Came out of the blank-slate search (`research_findings_blank_slate.md`, `research/blank_slate/`); holdout 66.8% hit vs 55.9% any-day base but **not confirmed** — the page is a forward tracker, not a recommendation. Same plumbing as Ignition: scan of record in GitHub Actions (`.github/workflows/pullback-daily.yml`, weekdays 21:40 UTC) → `pullback-data` branch → themes_web syncs. History is append-only (`scan.carry_forward`); the scan refuses to publish on missing or stale bars. Method: `pullback/README.md`.
- `/signals` (v1.11.0; `/pullback` is the same page): the pullback rule plus **Nasdaq Leaders**, standalone `nasdaq10/` module — two monthly lists of ten Nasdaq-100 stocks (ten largest by market value; ten strongest by 12-1 month return), ranked at the month's last close, traded at the next close, equal weight. From the top-N studies (`research_findings_top_n.md`, `research/top_n/`): both led QQQ in both halves of 2007–2026 (19.5% / 18.8% vs 16.5%) but are **not confirmed**, and the same rules showed nothing on the S&P 500. Daily scan `.github/workflows/nasdaq10-daily.yml` → `nasdaq10-data` branch; ledger starts with the 2026-10-01 rebalance.

## Release hygiene — every push is a deploy

A `git push` to main auto-deploys via Railway, so **every push is a release**. A PreToolUse hook (`.claude/hooks/pre_push_hygiene.py`, registered in `.claude/settings.json`) injects the full checklist whenever a Bash command contains `git push` — follow it. The short version:
- Bump **`config.APP_VERSION`** (single source — FastAPI version, page footers, and CSS cache-bust all derive from it; never hand-edit version strings in templates).
- Add a **release entry to `templates/logic.html`** (new `release current` div + Current pill; demote the previous one).
- Run **`python -m pytest tests/ -q`** after the last edit; Jinja-parse touched templates.
- **themes_web / ignition / pullback / nasdaq10 changes version separately:** bump `themes_web/version.py` (`THEMES_WEB_VERSION`) and add an entry to `themes_web/RELEASES.md` — not `config.APP_VERSION` / `logic.html`.

## Invariants — do not break

- **No imports between scanner, themes and `ignition/`** (enforced in `themes/README.md`, `ignition/README.md`). themes_web runs ignition only by subprocess and reads its JSON. Only allowed direction: themes scripts may import the shared data helpers `config` / `fmp_data` (never `app.py` / `scanner.py`).
- **Ignition data goes to the `ignition-data` branch only** (written by GitHub Actions) — never commit ignition data to main.
- **Pullback data goes to the `pullback-data` branch only**, same rule. `pullback/` imports nothing and is imported by nothing; the rule in `pullback/scan.py` is frozen — a changed threshold is a new rule and needs its own test.
- **Nasdaq Leaders data goes to the `nasdaq10-data` branch only**, same rule. `nasdaq10/` imports nothing and is imported by nothing; the two rules in `nasdaq10/scan.py` are frozen and a month's recorded picks are never re-picked.
- **Never edit a locked `thesis.md`** — write a dated addendum.
- `config.is_elite()` is the single source of truth for the ELITE flag.
- **JSON is source of truth; `.md` files are one-way derived views** (regenerated by the refresh scripts).
- **No commits/pushes without explicit operator confirmation.**
