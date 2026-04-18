"""
History & Daily Finds Persistence
──────────────────────────────────
Stores scan results to JSON files so they survive restarts.
- Daily finds: all unique signals found during the trading day
- History: last N days of daily finds for review
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import config

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
HISTORY_DAYS = 3  # Keep this many days of history


def _ensure_data_dir():
    """Create data directory if it doesn't exist."""
    DATA_DIR.mkdir(exist_ok=True)


def _daily_file(date_str: str) -> Path:
    """Return the path to a day's JSON file."""
    return DATA_DIR / f"{date_str}.json"


def _today_str() -> str:
    """Today's date string in ET."""
    return datetime.now(config.ET).strftime("%Y-%m-%d")


def _signal_key(signal: dict) -> str:
    """Unique key for deduplication: ticker + scan time (rounded to minute)."""
    return f"{signal['ticker']}_{signal.get('found_time', signal.get('timestamp', ''))}"


def load_daily_finds(date_str: str = None) -> list[dict]:
    """Load all finds for a given day (default: today)."""
    _ensure_data_dir()
    if date_str is None:
        date_str = _today_str()

    path = _daily_file(date_str)
    if not path.exists():
        return []

    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load {path}: {e}")
        return []


def save_daily_finds(finds: list[dict], date_str: str = None):
    """Save the full list of daily finds for a given day."""
    _ensure_data_dir()
    if date_str is None:
        date_str = _today_str()

    path = _daily_file(date_str)
    try:
        with open(path, "w") as f:
            json.dump(finds, f, indent=2, default=str)
    except IOError as e:
        logger.error(f"Failed to save {path}: {e}")


def add_signals_to_daily(signals: list[dict]):
    """
    Add new scan results to today's cumulative finds.
    Each signal gets a 'found_time' stamp. If the same ticker is found again
    later, we keep BOTH entries (different times = different opportunities).
    """
    if not signals:
        return

    now = datetime.now(config.ET)
    found_time = now.strftime("%I:%M %p ET")
    date_str = now.strftime("%Y-%m-%d")

    existing = load_daily_finds(date_str)

    for signal in signals:
        # Create a slim record for the daily table (no chart_data to save space)
        record = {
            "ticker": signal["ticker"],
            "found_time": found_time,
            "found_timestamp": now.isoformat(),
            "price": signal["price"],
            "composite_score": signal["composite_score"],
            "technical_score": signal.get("technical_score", 0),
            "sentiment_score": signal.get("sentiment_score", 0),
            "rvol": signal.get("rvol", 0),
            "entry": signal.get("trade", {}).get("entry", 0),
            "atr_target": signal.get("trade", {}).get("target", 0),
            "resistance_target": signal.get("trade", {}).get("resistance_target", 0),
            "resistance_level": signal.get("trade", {}).get("resistance_level", ""),
            "stop_loss": signal.get("trade", {}).get("stop_loss", 0),
            "risk_reward_ratio": signal.get("trade", {}).get("risk_reward_ratio", 0),
            "rsi": signal.get("indicators", {}).get("rsi"),
            # v3.3 context
            "leadership": signal.get("leadership"),
            "earnings": signal.get("earnings"),
            "regime_label": signal.get("regime_label"),
            "leader_adjustment": signal.get("leader_adjustment", 0),
            "earnings_adjustment": signal.get("earnings_adjustment", 0),
        }
        existing.append(record)

    save_daily_finds(existing, date_str)
    logger.info(f"Added {len(signals)} signals to daily finds ({date_str}), total: {len(existing)}")


def get_history_days() -> list[dict]:
    """
    Return the last HISTORY_DAYS days of data.
    Each entry: {"date": "2026-04-12", "date_display": "Sat Apr 12", "finds": [...]}
    """
    _ensure_data_dir()
    today = datetime.now(config.ET).date()
    days = []

    for i in range(HISTORY_DAYS):
        d = today - timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        date_display = d.strftime("%a %b %d")
        finds = load_daily_finds(date_str)
        days.append({
            "date": date_str,
            "date_display": date_display,
            "finds": finds,
            "signal_count": len(finds),
            "unique_tickers": len(set(f["ticker"] for f in finds)),
        })

    return days


def cleanup_old_files():
    """Remove history files older than HISTORY_DAYS."""
    _ensure_data_dir()
    today = datetime.now(config.ET).date()
    cutoff = today - timedelta(days=HISTORY_DAYS + 1)

    for path in DATA_DIR.glob("*.json"):
        try:
            file_date_str = path.stem  # e.g. "2026-04-10"
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
            if file_date < cutoff:
                path.unlink()
                logger.info(f"Cleaned up old history file: {path.name}")
        except (ValueError, OSError):
            continue
