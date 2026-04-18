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
from sector_rotation import detect_sector_rotation, get_sector_priority_tickers
from premarket import run_premarket_scan, reset_daily as reset_premarket, get_premarket_flags
from daily_analysis import analyze_day
from market_regime import get_regime
from earnings import refresh_earnings_cache
from backtest import (
    run_backtest as _run_backtest,
    Filters as _BacktestFilters,
    save_result as _save_backtest,
    load_result as _load_backtest,
    list_results as _list_backtests,
)

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── App Setup ─────────────────────────────────────────────────
app = FastAPI(title="Momentum Scanner", version="3.3")

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# ── State ─────────────────────────────────────────────────────
scan_results: list[dict] = []
last_scan_time: str = "Never"
scan_history: list[dict] = []  # History of past scans
is_scanning: bool = False
_today_tickers_seen: set = set()  # For re-entry suppression
_last_reset_date: str = ""        # Track daily reset
_sector_priority: list[str] = []  # Current hot-sector tickers


# ═══════════════════════════════════════════════════════════════
#  SCANNER JOBS
# ═══════════════════════════════════════════════════════════════

def _reset_daily_state():
    """Reset daily tracking state at start of each trading day."""
    global _today_tickers_seen, _last_reset_date
    today = datetime.now(config.ET).strftime("%Y-%m-%d")
    if today != _last_reset_date:
        _today_tickers_seen = set()
        _last_reset_date = today
        reset_premarket()
        logger.info(f"Daily state reset for {today}")


def premarket_scan_job():
    """Run pre-market scan at 8:00 and 9:00 AM ET, plus refresh earnings cache."""
    _reset_daily_state()
    logger.info("Running pre-market catalyst scan...")
    try:
        tickers = config.get_full_universe()
        flagged = run_premarket_scan(tickers)
        logger.info(f"Pre-market scan: {len(flagged)} tickers flagged")
    except Exception as e:
        logger.error(f"Pre-market scan failed: {e}")

    # ── Refresh earnings cache (v3.3) ──
    try:
        cache = refresh_earnings_cache(force=False)
        logger.info(f"Earnings cache: {len(cache)} tickers with upcoming prints")
    except Exception as e:
        logger.error(f"Earnings refresh failed: {e}")


def sector_rotation_job():
    """Detect sector rotation at 9:30 and periodically during the day."""
    global _sector_priority
    logger.info("Running sector rotation detection...")
    try:
        top_sectors = detect_sector_rotation(top_n=config.SECTOR_TOP_N)
        _sector_priority = get_sector_priority_tickers(top_sectors)
        sector_names = [s["sector"] for s in top_sectors]
        logger.info(
            f"Sector rotation: {sector_names} → "
            f"{len(_sector_priority)} priority tickers"
        )
    except Exception as e:
        logger.error(f"Sector rotation detection failed: {e}")


def post_market_analysis_job():
    """
    Run daily analysis after market close (5:00 PM ET).
    Fetches Yahoo Finance 5m bars, computes WIN/LOSS/EOD for all
    picks from today, and appends results to scanner_trade_log.xlsx.
    The performance page reads from this file on each page load.
    """
    today = datetime.now(config.ET).strftime("%Y-%m-%d")
    logger.info(f"Running post-market analysis for {today}...")
    try:
        analyze_day(today)
        logger.info(f"Post-market analysis complete for {today}")
    except Exception as e:
        logger.error(f"Post-market analysis failed: {e}")


def scheduled_scan():
    """Run by APScheduler every 30 min during market hours (Mon-Fri 9 AM – 4:30 PM ET)."""
    global scan_results, last_scan_time, is_scanning

    now = datetime.now(config.ET)
    _reset_daily_state()

    # ── DEAD ZONE FILTER: Skip lunch batches (0W/21L over 3 days) ──
    batch_key = now.strftime("%H:%M")
    # Map scheduler times to batch keys
    dead_zone_times = {"11:30", "12:00"}  # scheduler fires at :00/:30
    if batch_key in dead_zone_times:
        logger.info(f"SKIP: {batch_key} is in the lunch dead zone (0W/21L). Skipping scan.")
        return

    logger.info("Running scheduled scan...")
    is_scanning = True

    try:
        try:
            results = run_scan(sector_priority=_sector_priority)
        except Exception as scan_err:
            logger.warning(f"Live scan error: {scan_err}")
            results = []

        # ── RE-ENTRY SUPPRESSION: Mark and filter re-entries ──
        if config.SUPPRESS_REENTRIES and results:
            filtered = []
            for signal in results:
                ticker = signal["ticker"]
                if ticker in _today_tickers_seen:
                    signal["is_reentry"] = True
                    logger.info(f"RE-ENTRY suppressed: {ticker} (already signaled today)")
                    # Still record it but don't show in top signals
                    continue
                else:
                    signal["is_reentry"] = False
                    _today_tickers_seen.add(ticker)
                    filtered.append(signal)
            # Log what was filtered
            suppressed = len(results) - len(filtered)
            if suppressed > 0:
                logger.info(f"Re-entry filter: {suppressed} signals suppressed, {len(filtered)} kept")
            results = filtered

        if not results:
            logger.warning("Live scan returned no results after filters")

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

# Main momentum scan: every 30 min during market hours
scheduler.add_job(
    scheduled_scan,
    "cron",
    day_of_week="mon-fri",
    hour="9-16",
    minute="0,30",
    timezone=config.ET,
    id="momentum_scan",
    max_instances=1,
    misfire_grace_time=300,
)

# Pre-market catalyst scan: 8:00 AM and 9:00 AM ET
scheduler.add_job(
    premarket_scan_job,
    "cron",
    day_of_week="mon-fri",
    hour="8,9",
    minute="0",
    timezone=config.ET,
    id="premarket_scan",
    max_instances=1,
)

# Sector rotation: 9:30 AM then every hour
scheduler.add_job(
    sector_rotation_job,
    "cron",
    day_of_week="mon-fri",
    hour="9-15",
    minute="30",
    timezone=config.ET,
    id="sector_rotation",
    max_instances=1,
)

# Post-market daily analysis: 5:00 PM ET (after Yahoo Finance finalizes data)
scheduler.add_job(
    post_market_analysis_job,
    "cron",
    day_of_week="mon-fri",
    hour="17",
    minute="0",
    timezone=config.ET,
    id="post_market_analysis",
    max_instances=1,
    misfire_grace_time=600,
)


@app.on_event("startup")
async def startup():
    scheduler.start()
    cleanup_old_files()
    logger.info(
        f"Scheduler started: v3.2 with sector rotation, pre-market scan, "
        f"dead zone filter, re-entry suppression, and auto post-market analysis. "
        f"Mon-Fri 8:00 AM – 5:00 PM ET"
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
    regime = get_regime() if config.MARKET_REGIME_ENABLED else None
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
            "regime": regime,
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
    regime = get_regime() if config.MARKET_REGIME_ENABLED else None
    return templates.TemplateResponse(
        request=request,
        name="today.html",
        context={
            "finds": finds,
            "find_count": len(finds),
            "unique_tickers": len(set(f["ticker"] for f in finds)),
            "regime": regime,
        },
    )


@app.get("/api/regime", response_class=JSONResponse)
async def api_regime():
    """Current market regime (VIX-based)."""
    return get_regime() if config.MARKET_REGIME_ENABLED else {"label": "DISABLED"}


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


@app.get("/performance", response_class=HTMLResponse)
async def performance_page(request: Request):
    """Serve the performance tracker dashboard."""
    trades, daily_summary = _load_trade_log()
    return templates.TemplateResponse(
        request=request,
        name="performance.html",
        context={
            "trades": trades,
            "daily_summary": daily_summary,
        },
    )


def _load_trade_log() -> tuple[list[dict], list[dict]]:
    """Read scanner_trade_log.xlsx and return (trades, daily_summary) for the dashboard."""
    xlsx_path = BASE_DIR / "scanner_trade_log.xlsx"
    if not xlsx_path.exists():
        return [], []

    try:
        import openpyxl
        wb = openpyxl.load_workbook(xlsx_path, data_only=True)
        ws = wb["Trade Log"]

        trades = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row[0]:  # skip empty rows
                continue
            trades.append({
                "date": str(row[0]),
                "batch_time": str(row[1]) if row[1] else "",
                "ticker": str(row[2]) if row[2] else "",
                "entry_price": float(row[3]) if row[3] else 0,
                "target": float(row[4]) if row[4] else 0,
                "stop": float(row[5]) if row[5] else 0,
                "rvol": float(row[6]) if row[6] else 0,
                "rsi": float(row[7]) if row[7] else 0,
                "score": float(row[8]) if row[8] else 0,
                "result": str(row[9]) if row[9] else "",
                "exit_price": float(row[10]) if row[10] else 0,
                "pnl_dollar": float(row[11]) if row[11] else 0,
                "pnl_pct": float(row[12]) if row[12] else 0,
                "appearance": int(row[13]) if row[13] else 1,
            })

        # Build daily summary
        from collections import defaultdict
        by_day = defaultdict(list)
        for t in trades:
            by_day[t["date"]].append(t)

        daily_summary = []
        for day in sorted(by_day.keys()):
            day_trades = by_day[day]
            w = sum(1 for t in day_trades if t["result"] == "WIN")
            l = sum(1 for t in day_trades if t["result"] == "LOSS")
            e = sum(1 for t in day_trades if t["result"] == "EOD")
            dec = w + l
            daily_summary.append({
                "date": day,
                "total": len(day_trades),
                "wins": w,
                "losses": l,
                "eods": e,
                "win_rate": round(w / dec * 100, 1) if dec > 0 else 0,
                "net_pnl": round(sum(t["pnl_dollar"] for t in day_trades), 2),
            })

        wb.close()
        return trades, daily_summary

    except Exception as e:
        logger.error(f"Failed to load trade log: {e}")
        return [], []


@app.get("/logic", response_class=HTMLResponse)
async def logic_page(request: Request):
    """Serve the algorithm logic explanation page."""
    return templates.TemplateResponse(
        request=request,
        name="logic.html",
    )


# ═══════════════════════════════════════════════════════════════
#  BACKTEST (v3.3)
# ═══════════════════════════════════════════════════════════════

@app.get("/backtest", response_class=HTMLResponse)
async def backtest_page(request: Request):
    """Serve the backtest UI — loads the latest saved run, if any."""
    latest = _load_backtest("latest")
    runs = _list_backtests()
    return templates.TemplateResponse(
        request=request,
        name="backtest.html",
        context={
            "latest": latest,
            "runs": runs,
        },
    )


@app.post("/api/backtest/run", response_class=JSONResponse)
async def api_backtest_run(payload: dict):
    """Kick off a backtest. Expects JSON body:
      {
        "start_date": "YYYY-MM-DD",   (optional — defaults to 10 days back)
        "end_date":   "YYYY-MM-DD",   (optional — defaults to today)
        "filters":    { ... filters ... },  (optional)
        "max_tickers": int (optional, for quick runs)
      }
    Blocking call — returns the full result JSON when done.
    """
    from datetime import date as _d, datetime as _dt, timedelta as _td

    try:
        end_s = payload.get("end_date")
        start_s = payload.get("start_date")
        end_date = _dt.fromisoformat(end_s).date() if end_s else _d.today()
        start_date = _dt.fromisoformat(start_s).date() if start_s else end_date - _td(days=10)

        filters = _BacktestFilters.from_dict(payload.get("filters"))
        max_tickers = payload.get("max_tickers")

        result = _run_backtest(
            start_date=start_date,
            end_date=end_date,
            filters=filters,
            max_tickers=max_tickers,
        )
        _save_backtest(result, name="latest")
        return JSONResponse(result)
    except Exception as e:
        logger.exception("Backtest run failed")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/backtest/latest", response_class=JSONResponse)
async def api_backtest_latest():
    """Return the most recently saved backtest result, if any."""
    latest = _load_backtest("latest")
    if latest is None:
        return JSONResponse({"error": "No backtest has been run yet."}, status_code=404)
    return JSONResponse(latest)


@app.get("/api/backtest/runs", response_class=JSONResponse)
async def api_backtest_runs():
    """List all saved backtest runs."""
    return JSONResponse({"runs": _list_backtests()})


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.HOST, port=config.PORT)
