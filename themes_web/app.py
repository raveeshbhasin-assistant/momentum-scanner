"""
themes_web — FastAPI app for the long-term theme research dashboard.

Completely separate from the deployed momentum scanner. Reads markdown +
JSON from `../themes/` and renders 5 page types per theme: tracker, thesis,
supply chain, candidates, scoring.

Run locally:
    uvicorn themes_web.app:app --port 8001 --reload

Hard rule: nothing in `themes/` is allowed to be imported by `app.py` /
`scanner.py`. This app reads `themes/` files but is itself a sibling app —
the deployed scanner doesn't see it.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from themes_web.render import (
    aggregate_moonshots,
    aggregate_portfolio,
    benchmark_price,
    discover_themes_full,
    load_tracker_json,
    load_tracker_live_json,
    load_candidates_json,
    load_scoring_json,
    read_markdown_file,
    render_markdown,
    theme_dir,
)
from themes_web.scheduler import start_scheduler, stop_scheduler, trigger_manual_refresh

logger = logging.getLogger(__name__)

_HERE = Path(__file__).parent
_TEMPLATES_DIR = _HERE / "templates"
_STATIC_DIR = _HERE / "static"

app = FastAPI(title="Themes Web — Long-term thesis tracker")
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
# Static mount is optional — only mount if the directory exists and has files.
# Empty dirs don't get committed to git, so this avoids crashing on Railway
# when themes_web/static/ wasn't tracked. Add files (and .gitkeep) when we start
# serving CSS/JS that doesn't live inline in the templates.
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.on_event("startup")
def _on_startup():
    try:
        start_scheduler()
    except Exception as e:
        logger.exception(f"Failed to start scheduler: {e}")


@app.on_event("shutdown")
def _on_shutdown():
    try:
        stop_scheduler()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Shared context — every page needs the topbar + dropdown + tabs
# ─────────────────────────────────────────────────────────────

def _base_ctx(slug: str, active_page: str) -> dict:
    """Common template context. Loads tracker.json + all themes list."""
    return {
        "active_slug": slug,
        "active_page": active_page,
        "tracker": load_tracker_json(slug),
        "all_themes": discover_themes_full(),
    }


def _require_theme(slug: str) -> dict:
    """Load tracker.json; 404 if theme doesn't exist."""
    t = load_tracker_json(slug)
    if t is None:
        raise HTTPException(status_code=404, detail=f"Theme not found: {slug}")
    return t


# ─────────────────────────────────────────────────────────────
# Tracker page (with aggregate + holdings overlay)
# ─────────────────────────────────────────────────────────────

def _compute_tracker_view(slug: str) -> Optional[dict]:
    """Merge tracker.json (anchor) + candidates.json (current prices) + tracker_live.json (13F/earnings/news)."""
    tracker = load_tracker_json(slug)
    if tracker is None:
        return None
    cands_doc = load_candidates_json(slug) or {}
    candidates_map = {c["ticker"]: c for c in cands_doc.get("candidates", [])}
    live_doc = load_tracker_live_json(slug) or {}
    live_map = live_doc.get("per_ticker") or {}

    bench_anchor = tracker.get("benchmarks_at_init") or {}
    spy_lock = float(bench_anchor.get("spy_at_theme_lock") or 0)
    # Live SPY from themes/_benchmarks.json (refreshed daily). Falls back to
    # spy_lock when the file is missing — keeps the page rendering and
    # surfaces the staleness as 0% instead of fabricating a vs-SPY value.
    live_spy = benchmark_price("SPY")
    spy_now = float(live_spy) if live_spy else spy_lock
    spy_chg_pct = ((spy_now / spy_lock - 1) * 100) if spy_lock > 0 else 0

    holdings = []
    for h in tracker.get("holdings") or []:
        tk = h["ticker"]
        entry = float(h.get("entry_price") or 0)
        cand = candidates_map.get(tk) or {}
        live = live_map.get(tk) or {}
        now = float(cand.get("price") or entry)
        chg_pct = ((now / entry - 1) * 100) if entry > 0 else 0
        chg_vs_spy = chg_pct - spy_chg_pct
        holdings.append({
            **h,
            "current_price": round(now, 2),
            "chg_pct": round(chg_pct, 2),
            "chg_vs_spy_pp": round(chg_vs_spy, 2),
            # Live data overlay (empty lists if tracker_live.json not yet refreshed)
            "f13_holders": live.get("13f_holders", []),
            "upcoming_earnings": live.get("upcoming_earnings", []),
            "recent_news": live.get("recent_news", []),
        })

    if holdings:
        avg_chg = sum(h["chg_pct"] for h in holdings) / len(holdings)
        tracker_vs_spy_pp = avg_chg - spy_chg_pct
    else:
        avg_chg = 0
        tracker_vs_spy_pp = 0

    days_since_lock = None
    if tracker.get("theme_locked_at"):
        try:
            d_lock = datetime.fromisoformat(tracker["theme_locked_at"]).date()
            days_since_lock = (datetime.now().date() - d_lock).days
        except Exception:
            pass

    return {
        "tracker": tracker,
        "holdings": holdings,
        "aggregate": {
            "tracker_chg_pct": round(avg_chg, 2),
            "spy_chg_pct": round(spy_chg_pct, 2),
            "tracker_vs_spy_pp": round(tracker_vs_spy_pp, 2),
            "holdings_count": len(holdings),
            "active_falsifiers": 0,
            "total_falsifiers": 4,
            "days_since_lock": days_since_lock,
        },
        "live_meta": {
            "generated_at": live_doc.get("generated_at"),
            "trade_date": live_doc.get("trade_date"),
            "data_sources": live_doc.get("data_sources", {}),
        },
    }


@app.get("/", response_class=HTMLResponse)
def root():
    """Redirect to the first active theme's tracker."""
    themes = [t for t in discover_themes_full() if t.get("has_tracker")]
    if not themes:
        return HTMLResponse("<h1>No themes found</h1><p>Add a theme directory under <code>themes/</code> with a <code>tracker.json</code>.</p>", status_code=404)
    return RedirectResponse(url=f"/themes/{themes[0]['slug']}/tracker")


@app.get("/portfolio", response_class=HTMLResponse)
def portfolio_page(request: Request):
    """Top-level cross-theme portfolio view. Aggregates every Active theme."""
    portfolio = aggregate_portfolio()
    return templates.TemplateResponse(
        request=request,
        name="portfolio.html",
        context={
            "active_slug": None,
            "active_page": "portfolio",
            "tracker": None,
            "all_themes": discover_themes_full(),
            "portfolio": portfolio,
        },
    )


@app.get("/api/portfolio", response_class=JSONResponse)
def api_portfolio():
    return aggregate_portfolio()


@app.get("/moonshots", response_class=HTMLResponse)
def moonshots_page(request: Request):
    """Cross-theme 3X screen — the portfolio view filtered to the names most
    likely to 3X in 2-3 years (see themes/_moonshots_3x.json + research doc)."""
    moonshots = aggregate_moonshots()
    if moonshots is None:
        return HTMLResponse("<h1>No 3X screen found</h1><p>Missing <code>themes/_moonshots_3x.json</code>.</p>", status_code=404)
    return templates.TemplateResponse(
        request=request,
        name="moonshots.html",
        context={
            "active_slug": None,
            "active_page": "moonshots",
            "tracker": None,
            "all_themes": discover_themes_full(),
            "moonshots": moonshots,
        },
    )


@app.get("/api/moonshots", response_class=JSONResponse)
def api_moonshots():
    m = aggregate_moonshots()
    return m if m is not None else JSONResponse({"error": "missing _moonshots_3x.json"}, status_code=404)


@app.get("/how-it-works", response_class=HTMLResponse)
def how_it_works(request: Request):
    """Standalone methodology page. Theme-agnostic."""
    md_path = _HERE / "HOW_IT_WORKS.md"
    if not md_path.exists():
        raise HTTPException(status_code=404, detail="HOW_IT_WORKS.md not found")
    body_html, toc_html = render_markdown(md_path.read_text(encoding="utf-8"))
    return templates.TemplateResponse(
        request=request,
        name="how_it_works.html",
        context={
            "active_slug": None,
            "active_page": "how-it-works",
            "tracker": None,  # no per-theme header
            "all_themes": discover_themes_full(),
            "body_html": body_html,
            "toc_html": toc_html,
        },
    )


@app.get("/themes/{slug}/tracker", response_class=HTMLResponse)
def tracker_page(request: Request, slug: str):
    view = _compute_tracker_view(slug)
    if view is None:
        raise HTTPException(status_code=404, detail=f"Theme not found: {slug}")
    ctx = _base_ctx(slug, "tracker")
    ctx.update(view)
    return templates.TemplateResponse(request=request, name="tracker.html", context=ctx)


# ─────────────────────────────────────────────────────────────
# Thesis page (markdown article)
# ─────────────────────────────────────────────────────────────

@app.get("/themes/{slug}/thesis", response_class=HTMLResponse)
def thesis_page(request: Request, slug: str):
    _require_theme(slug)
    md_path = theme_dir(slug) / "thesis.md"
    md_text = read_markdown_file(md_path)
    if not md_text:
        raise HTTPException(status_code=404, detail="thesis.md not found")
    body_html, toc_html = render_markdown(md_text)
    ctx = _base_ctx(slug, "thesis")
    ctx.update({"body_html": body_html, "toc_html": toc_html})
    return templates.TemplateResponse(request=request, name="thesis.html", context=ctx)


# ─────────────────────────────────────────────────────────────
# Supply chain page (markdown + Mermaid)
# ─────────────────────────────────────────────────────────────

@app.get("/themes/{slug}/supply-chain", response_class=HTMLResponse)
def supply_chain_page(request: Request, slug: str):
    _require_theme(slug)
    md_path = theme_dir(slug) / "supply_chain.md"
    md_text = read_markdown_file(md_path)
    if not md_text:
        raise HTTPException(status_code=404, detail="supply_chain.md not found")
    body_html, toc_html = render_markdown(md_text)
    ctx = _base_ctx(slug, "supply-chain")
    ctx.update({"body_html": body_html, "toc_html": toc_html})
    return templates.TemplateResponse(request=request, name="supply_chain.html", context=ctx)


# ─────────────────────────────────────────────────────────────
# Candidates page (interactive table from JSON)
# ─────────────────────────────────────────────────────────────

@app.get("/themes/{slug}/candidates", response_class=HTMLResponse)
def candidates_page(request: Request, slug: str):
    tracker = _require_theme(slug)
    cands_doc = load_candidates_json(slug)
    if cands_doc is None:
        raise HTTPException(status_code=404, detail="candidates.json not found")
    promoted_tickers = [h["ticker"] for h in (tracker.get("holdings") or [])]
    # Sample data_source from any candidate that recorded it (yfinance only in sandbox)
    cands = cands_doc.get("candidates", [])
    data_source = cands[0].get("_data_source", "FMP + yfinance") if cands else "—"
    ctx = _base_ctx(slug, "candidates")
    ctx.update({
        "candidates": cands,
        "candidates_meta": {
            "trade_date": cands_doc.get("trade_date"),
            "universe_size": cands_doc.get("universe_size"),
            "data_source": data_source,
        },
        "promoted_tickers": promoted_tickers,
        "promoted_count": len(promoted_tickers),
    })
    return templates.TemplateResponse(request=request, name="candidates.html", context=ctx)


# ─────────────────────────────────────────────────────────────
# Scoring page (rubric + ranked table + per-pick detail)
# ─────────────────────────────────────────────────────────────

@app.get("/themes/{slug}/scoring", response_class=HTMLResponse)
def scoring_page(request: Request, slug: str):
    tracker = _require_theme(slug)
    scoring = load_scoring_json(slug)
    if scoring is None:
        raise HTTPException(status_code=404, detail="scoring_log.json not found")
    # Flatten + sort results
    results = scoring.get("results") or {}
    ranked = []
    for ticker, r in results.items():
        ranked.append({
            "ticker": ticker,
            **r,
        })
    ranked.sort(key=lambda r: -(r.get("raw_total") or 0))
    for i, r in enumerate(ranked, 1):
        r["rank"] = i

    promoted_count = len(tracker.get("holdings") or [])
    # The order of criteria for table columns and per-pick detail
    scoring_criteria_order = list((scoring.get("weights") or {}).keys())

    ctx = _base_ctx(slug, "scoring")
    ctx.update({
        "scoring": scoring,
        "ranked": ranked,
        "top_n": promoted_count,
        "promoted_count": promoted_count,
        "scoring_criteria_order": scoring_criteria_order,
    })
    return templates.TemplateResponse(request=request, name="scoring.html", context=ctx)


# ─────────────────────────────────────────────────────────────
# JSON APIs (for future JS overlay / external consumers)
# ─────────────────────────────────────────────────────────────

@app.get("/api/themes", response_class=JSONResponse)
def api_themes():
    return discover_themes_full()


@app.get("/api/themes/{slug}/tracker", response_class=JSONResponse)
def api_tracker(slug: str):
    view = _compute_tracker_view(slug)
    if view is None:
        raise HTTPException(status_code=404, detail=f"Theme not found: {slug}")
    return view


@app.get("/api/themes/{slug}/candidates", response_class=JSONResponse)
def api_candidates(slug: str):
    doc = load_candidates_json(slug)
    if doc is None:
        raise HTTPException(status_code=404, detail="candidates.json not found")
    return doc


@app.get("/api/themes/{slug}/scoring", response_class=JSONResponse)
def api_scoring(slug: str):
    doc = load_scoring_json(slug)
    if doc is None:
        raise HTTPException(status_code=404, detail="scoring_log.json not found")
    return doc


@app.post("/api/refresh", response_class=JSONResponse)
@app.post("/api/refresh/{slug}", response_class=JSONResponse)
def api_refresh(slug: Optional[str] = None):
    """
    Manually trigger refresh. Blocks until done — can take ~5 minutes per theme.
    Without slug: refreshes every active theme.
    """
    try:
        result = trigger_manual_refresh(slug)
        return {"ok": True, "result": result}
    except Exception as e:
        logger.exception("Manual refresh failed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/rescore/{slug}", response_class=JSONResponse)
def api_rescore(slug: str):
    """
    Run the theme's _score_run.py to regenerate scoring_log.json from the
    current candidates.json. Safe to call after a refresh to see updated scores.
    """
    import subprocess
    from pathlib import Path
    script = Path(__file__).parent.parent / "themes" / slug / "_score_run.py"
    if not script.exists():
        raise HTTPException(status_code=404, detail=f"_score_run.py not found for theme {slug}")
    try:
        r = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(script.parent.parent.parent),
            capture_output=True, text=True, timeout=120,
        )
        return {
            "ok": r.returncode == 0,
            "returncode": r.returncode,
            "stdout_tail": (r.stdout or "")[-1000:],
            "stderr_tail": (r.stderr or "")[-500:],
        }
    except Exception as e:
        logger.exception(f"Rescore failed for {slug}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/themes/{slug}/quotes", response_class=JSONResponse)
def api_quotes(slug: str):
    """
    Live quote fetch for the theme's tracker tickers, plus SPY for comparison.
    Used by the tracker page JS for the live-price overlay (more recent than
    the daily candidates.json refresh).

    Falls back gracefully if FMP_API_KEY is absent — returns the same prices
    that are already in candidates.json, so the page just shows the cached value.
    """
    tracker = load_tracker_json(slug)
    if tracker is None:
        raise HTTPException(status_code=404, detail=f"Theme not found: {slug}")
    tickers = [h["ticker"] for h in (tracker.get("holdings") or [])]
    if not tickers:
        return {"quotes": {}, "spy": None, "fetched_at": datetime.utcnow().isoformat() + "Z", "source": "empty"}

    # Try FMP first (real-time)
    quotes = {}
    spy_price = None
    source = "stale"
    try:
        import config
        if config.FMP_API_KEY:
            import httpx
            with httpx.Client(timeout=10) as client:
                for tk in tickers + ["SPY"]:
                    try:
                        r = client.get(
                            "https://financialmodelingprep.com/stable/quote",
                            params={"symbol": tk, "apikey": config.FMP_API_KEY},
                        )
                        r.raise_for_status()
                        data = r.json()
                        if isinstance(data, list) and data:
                            q = data[0]
                            if tk == "SPY":
                                spy_price = q.get("price")
                            else:
                                quotes[tk] = {
                                    "price": q.get("price"),
                                    "change_pct": q.get("changesPercentage") or q.get("changePercentage"),
                                    "volume": q.get("volume"),
                                }
                    except Exception as e:
                        logger.debug(f"FMP live quote failed for {tk}: {e}")
            source = "fmp" if quotes else "stale"
    except Exception as e:
        logger.debug(f"FMP path failed: {e}")

    # Fallback: read latest candidates.json values
    if not quotes:
        cands_doc = load_candidates_json(slug) or {}
        for c in cands_doc.get("candidates", []):
            if c["ticker"] in tickers:
                quotes[c["ticker"]] = {
                    "price": c.get("price"),
                    "change_pct": c.get("change_pct"),
                    "volume": c.get("volume"),
                }

    return {
        "quotes": quotes,
        "spy": spy_price,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": source,
    }
