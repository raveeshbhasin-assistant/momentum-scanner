"""
Momentum Scanner — Web Application
───────────────────────────────────
FastAPI server that runs scheduled scans and serves
a professional trading dashboard.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler

import config
from scanner import run_scan
from demo_data import generate_demo_signals
from history import add_signals_to_daily, load_daily_finds, get_history_days, cleanup_old_files

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── App Setup ─────────────────────────────────────────────────
app = FastAPI(title="Momentum Scanner", version="3.0")

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# ── State ─────────────────────────────────────────────────────
scan_results: list[dict] = []
last_scan_time: str = "Never"
scan_history: list[dict] = []  # History of past scans
is_scanning: bool = False


# ═══════════════════════════════════════════════════════════════
#  SCANNER JOB
# ═══════════════════════════════════════════════════════════════

def scheduled_scan():
    """Run by APScheduler every 30 min during market hours (Mon-Fri 9 AM – 4:30 PM ET)."""
    global scan_results, last_scan_time, is_scanning

    now = datetime.now(config.ET)
    logger.info("Running scheduled scan...")
    is_scanning = True

    try:
        try:
            results = run_scan()
        except Exception as scan_err:
            logger.warning(f"Live scan error: {scan_err}")
            results = []

        if not results:
            logger.warning("Live scan returned no results — no demo fallback during market hours")

        scan_results = results
        last_scan_time = now.strftime("%Y-%m-%d %I:%M:%S %p ET")

        # Persist to daily cumulative finds (for /today and /history pages)
        if results:
            add_signals_to_daily(results)

        # Store in history
        scan_history.append({
            "time": last_scan_time,
            "signal_count": len(results),
            "top_ticker": results[0]["ticker"] if results else "—",
            "top_score": results[0]["composite_score"] if results else 0,
        })
        # Keep last 20 scans
        if len(scan_history) > 20:
            scan_history.pop(0)

        logger.info(f"Scan complete: {len(results)} signals")
    except Exception as e:
        logger.error(f"Scan failed: {e}")
    finally:
        is_scanning = False


# ── Scheduler Setup ───────────────────────────────────────────
scheduler = BackgroundScheduler(timezone=config.ET)
scheduler.add_job(
    scheduled_scan,
    "cron",
    day_of_week="mon-fri",
    hour="9-16",
    minute="0,30",
    timezone=config.ET,
    id="momentum_scan",
    max_instances=1,
    misfire_grace_time=300,  # Allow 5-min grace if a run is slightly late
)


@app.on_event("startup")
async def startup():
    scheduler.start()
    cleanup_old_files()
    logger.info(
        f"Scheduler started: scanning every 30 min, "
        f"Mon-Fri 9:00 AM – 4:30 PM ET"
    )
    logger.info(f"Dashboard running at http://localhost:{config.PORT}")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ═══════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the main dashboard."""
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "signals": scan_results,
            "last_scan": last_scan_time,
            "scan_interval": config.SCAN_INTERVAL_MINUTES,
            "is_scanning": is_scanning,
            "has_finnhub_key": bool(config.FINNHUB_API_KEY),
            "data_source": "FMP Real-Time" if config.FMP_API_KEY else "yfinance (15m delay)",
        },
    )


@app.get("/api/signals", response_class=JSONResponse)
async def api_signals():
    """JSON API for current signals."""
    return {
        "signals": scan_results,
        "last_scan": last_scan_time,
        "is_scanning": is_scanning,
        "signal_count": len(scan_results),
    }


@app.get("/api/history", response_class=JSONResponse)
async def api_history():
    """JSON API for scan history."""
    return {"history": scan_history}


@app.post("/api/scan", response_class=JSONResponse)
async def trigger_scan():
    """Manually trigger a scan."""
    global is_scanning
    if is_scanning:
        return {"status": "already_running"}

    # Run in background
    import threading
    t = threading.Thread(target=scheduled_scan)
    t.start()
    return {"status": "started"}


@app.get("/api/config", response_class=JSONResponse)
async def get_config():
    """Return current scanner configuration."""
    return {
        "scan_interval_minutes": config.SCAN_INTERVAL_MINUTES,
        "min_rvol": config.MIN_RVOL,
        "min_composite_score": config.MIN_COMPOSITE_SCORE,
        "risk_reward_ratio": config.RISK_REWARD_RATIO,
        "atr_stop_multiplier": config.ATR_STOP_MULTIPLIER,
        "risk_per_trade_pct": config.RISK_PER_TRADE_PCT,
        "technical_weight": config.TECHNICAL_WEIGHT,
        "sentiment_weight": config.SENTIMENT_WEIGHT,
        "volume_weight": config.VOLUME_WEIGHT,
        "ticker_count": len(config.SP500_LIQUID),
    }


@app.get("/today", response_class=HTMLResponse)
async def today_page(request: Request):
    """Serve today's cumulative finds table."""
    finds = load_daily_finds()
    return templates.TemplateResponse(
        request=request,
        name="today.html",
        context={
            "finds": finds,
            "find_count": len(finds),
            "unique_tickers": len(set(f["ticker"] for f in finds)),
        },
    )


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    """Serve multi-day history page."""
    days = get_history_days()
    return templates.TemplateResponse(
        request=request,
        name="history.html",
        context={"days": days},
    )


@app.get("/api/today", response_class=JSONResponse)
async def api_today():
    """JSON API for today's cumulative finds."""
    finds = load_daily_finds()
    return {"finds": finds, "count": len(finds)}


@app.get("/logic", response_class=HTMLResponse)
async def logic_page(request: Request):
    """Serve the algorithm logic explanation page."""
    return templates.TemplateResponse(
        request=request,
        name="logic.html",
    )


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.HOST, port=config.PORT)
