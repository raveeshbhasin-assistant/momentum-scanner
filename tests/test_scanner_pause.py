"""v3.9.0 scanner pause (config.SCANNER_PAUSED): nothing runs, pages still serve."""
import asyncio
import importlib

import pytest

import app as scanner_app
import config


@pytest.fixture
def paused(monkeypatch):
    monkeypatch.setattr(config, "SCANNER_PAUSED", True)
    return scanner_app


def test_paused_by_default_and_env_resumes(monkeypatch):
    try:
        monkeypatch.delenv("SCANNER_PAUSED", raising=False)
        assert importlib.reload(config).SCANNER_PAUSED is True
        for off in ("0", "false", "no", "off"):
            monkeypatch.setenv("SCANNER_PAUSED", off)
            assert importlib.reload(config).SCANNER_PAUSED is False
    finally:
        monkeypatch.delenv("SCANNER_PAUSED", raising=False)
        importlib.reload(config)


def test_startup_starts_nothing_and_keeps_history_files(paused, monkeypatch):
    calls = []
    monkeypatch.setattr(paused, "_seed_data_volume", lambda: calls.append("seed"))
    monkeypatch.setattr(paused, "cleanup_old_files", lambda: calls.append("cleanup"))
    monkeypatch.setattr(paused.scheduler, "start", lambda *a, **k: calls.append("scheduler.start"))
    monkeypatch.setattr(paused, "sector_rotation_job", lambda: calls.append("sector"))
    asyncio.run(paused.startup())
    assert calls == ["seed"]                      # the volume is still seeded; nothing else runs
    asyncio.run(paused.shutdown())                # must not raise when the scheduler never started


def test_scheduled_scan_never_scans_while_paused(paused, monkeypatch):
    monkeypatch.setattr(paused, "run_scan", lambda **k: pytest.fail("scan ran while paused"))
    paused.scheduled_scan()


def test_manual_scan_endpoint_refuses_while_paused(paused, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(paused, "scheduled_scan", lambda: pytest.fail("scan thread started while paused"))
    r = TestClient(paused.app).post("/api/scan")
    assert r.status_code == 200 and r.json()["status"] == "paused"


def test_every_page_shows_the_paused_banner(paused, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setitem(paused.templates.env.globals, "SCANNER_PAUSED", True)
    monkeypatch.setattr(config, "MARKET_REGIME_ENABLED", False)          # no network on /
    client = TestClient(paused.app)
    for path in ("/", "/today", "/history", "/performance", "/logic", "/backtest",
                 "/theme-scanner", "/theme-backtest"):
        page = client.get(path)
        assert page.status_code == 200, path
        assert "Momentum scanner paused since" in page.text and "SCANNER_PAUSED=0" in page.text, path


def test_dashboard_offers_no_scan_button_while_paused(paused, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setitem(paused.templates.env.globals, "SCANNER_PAUSED", True)
    monkeypatch.setattr(config, "MARKET_REGIME_ENABLED", False)
    monkeypatch.setattr(paused, "scan_results", [])
    page = TestClient(paused.app).get("/").text
    assert "Run First Scan" not in page and 'onclick="triggerScan()"' not in page
    assert "Scanner paused</h2>" in page and 'id="scanBtn" disabled' in page


def test_test_email_endpoint_refuses_while_paused(paused, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(paused, "send_test_email", lambda: pytest.fail("email sent while paused"))
    r = TestClient(paused.app).post("/api/notify/test")
    assert r.json()["status"] == "paused"


def test_logic_header_shows_the_current_version(paused):
    from fastapi.testclient import TestClient

    page = TestClient(paused.app).get("/logic").text
    assert f'<span class="version-tag">v{config.APP_VERSION}</span>' in page
    assert f"Momentum Scanner v{config.APP_VERSION}</title>" in page
