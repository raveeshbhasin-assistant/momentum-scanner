"""The Ignition page's "Run scan now" button (themes_web v1.14.0): the route
dispatches the GitHub workflow with the token from the environment, says so
without one, and schedules follow-up syncs. No network."""
import json

import pytest


@pytest.fixture
def client(monkeypatch):
    import themes_web.app as web
    import themes_web.scheduler as sch
    from fastapi.testclient import TestClient
    monkeypatch.setattr(web, "start_scheduler", lambda: None)
    monkeypatch.setattr(sch, "_scheduler", None)          # no running scheduler: no follow-ups
    return TestClient(web.app), sch


def test_without_a_token_the_button_explains_instead_of_running(client, monkeypatch):
    c, sch = client
    monkeypatch.delenv("GITHUB_DISPATCH_TOKEN", raising=False)
    monkeypatch.setattr(sch.urllib.request, "urlopen", lambda *a, **k: pytest.fail("must not call GitHub"))
    r = c.post("/api/ignition/run")
    assert r.status_code == 503
    assert "GITHUB_DISPATCH_TOKEN" in r.json()["error"]


def test_with_a_token_it_dispatches_the_workflow_on_main(client, monkeypatch):
    c, sch = client
    monkeypatch.setenv("GITHUB_DISPATCH_TOKEN", "ghp_test")
    seen = {}

    class Resp:
        status = 204
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(req, timeout):
        seen["url"], seen["body"] = req.full_url, json.loads(req.data)
        seen["auth"], seen["method"] = req.get_header("Authorization"), req.get_method()
        return Resp()

    monkeypatch.setattr(sch.urllib.request, "urlopen", fake_urlopen)
    r = c.post("/api/ignition/run")
    assert r.status_code == 200 and r.json()["ok"] is True and r.json()["github_status"] == 204
    assert seen["url"].endswith("/repos/raveeshbhasin-assistant/momentum-scanner/actions/workflows/ignition-daily.yml/dispatches")
    assert seen["body"] == {"ref": "main"} and seen["auth"] == "Bearer ghp_test" and seen["method"] == "POST"
    assert r.json()["followup_syncs_min"] == []              # scheduler not running in tests


def test_a_refusal_from_github_is_reported_not_raised(client, monkeypatch):
    import urllib.error
    c, sch = client
    monkeypatch.setenv("GITHUB_DISPATCH_TOKEN", "ghp_bad")

    def refuse(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {}, __import__("io").BytesIO(b'{"message":"Resource not accessible"}'))

    monkeypatch.setattr(sch.urllib.request, "urlopen", refuse)
    r = c.post("/api/ignition/run")
    assert r.status_code == 502 and "HTTP 403" in r.json()["error"]


def test_followup_syncs_are_scheduled_on_a_running_scheduler(monkeypatch):
    import themes_web.scheduler as sch
    added = []

    class FakeScheduler:
        running = True
        def add_job(self, func, trigger, **kw):
            added.append((func.__name__, kw["id"]))

    monkeypatch.setattr(sch, "_scheduler", FakeScheduler())
    assert sch.schedule_ignition_followup_syncs() == [3, 6, 12, 25]
    assert added == [("refresh_ignition", f"ignition_sync_followup_{m}") for m in (3, 6, 12, 25)]


def test_page_shows_the_buttons(client, monkeypatch, tmp_path):
    import themes_web.app as web
    c, _ = client
    monkeypatch.setattr(web, "_IGNITION_DIR", tmp_path)        # no data: buttons only show with a ledger
    assert c.get("/ignition").status_code == 200
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location("ig", Path(__file__).resolve().parents[1] / "ignition" / "scan.py")
    ig = importlib.util.module_from_spec(spec); spec.loader.exec_module(ig)
    latest = {"version": 2, "asof": "2026-10-19", "scanned_at": "x", "universe": 1, "ledger_start": "2025-01-02",
              "rule": {}, "backtest": ig.BACKTEST, "exit_research": ig.EXIT_RESEARCH, "positions": [], "events": [],
              "leaders": [], "near_misses": [], "summary": ig.summarize([], [], "2026-10-16", "2026-10-19", "2026-09-20")}
    (tmp_path / "latest.json").write_text(json.dumps(latest))
    page = c.get("/ignition")
    assert page.status_code == 200
    assert 'id="run-now"' in page.text and 'id="sync-now"' in page.text and "/api/ignition/run" in page.text
    assert "3am and 7am ET" in page.text
