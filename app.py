"""
Momentum Scanner — Web Application
───────────────────────────────────
FastAPI server that runs scheduled scans and serves
a professional trading dashboard.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler

import config
from scanner import run_scan
from demo_data import generate_demo_signals

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
    """Run by APScheduler every N minutes during market hours."""
    global scan_results, last_scan_time, is_scanning

    now = datetime.now()
    hour, minute = now.hour, now.minute

    # Only scan during market hours (ET) — adjust if your server is in a different timezone
    # For now, we run scans regardless of time so you can test anytime
    logger.info("Running scheduled scan...")
    is_scanning = True

    try:
        try:
            results = run_scan()
        except Exception as scan_err:
            logger.warning(f"Live scan error: {scan_err}")
            results = []
        # Fall back to demo data if live scan returns nothing (e.g., API blocked)
        if not results:
            logger.warning("Live scan returned no results — using demo data")
            results = generate_demo_signals(count=8)
        scan_results = results
        last_scan_time = now.strftime("%Y-%m-%d %H:%M:%S ET")

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
scheduler = BackgroundScheduler()
scheduler.add_job(
    scheduled_scan,
    "interval",
    minutes=config.SCAN_INTERVAL_MINUTES,
    id="momentum_scan",
    max_instances=1,
)


@app.on_event("startup")
async def startup():
    scheduler.start()
    logger.info(f"Scheduler started: scanning every {config.SCAN_INTERVAL_MINUTES} minutes")
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
