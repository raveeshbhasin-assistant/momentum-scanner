"""ignition/notify.py — which runs mail, to whom, and what the mail says. No network."""
import importlib.util
import json
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "ignition_notify", Path(__file__).resolve().parents[1] / "ignition" / "notify.py")
nt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nt)


def _latest(new_events, positions=None, since="2026-10-08", asof="2026-10-09", **summary):
    return {
        "asof": asof, "scanned_at": "2026-10-09 21:40 ET",
        "summary": {"since": since, "new_events": new_events, "open": 3, "closed": 7,
                    "new_sells": [], "new_expired": [], "new_checkpoints": [], **summary},
        "positions": positions or [],
    }


FIRE = {"date": "2026-10-09", "ticker": "PTC", "type": "FIRE", "detail": "+38.0% week on 1.85x volume"}
REFIRE = {"date": "2026-10-09", "ticker": "BURL", "type": "REFIRE", "detail": "+14.0% week on 2.1x volume"}
SELL = {"date": "2026-10-09", "ticker": "CNH", "type": "SELL", "detail": "closed 11.39 below pre-ignition base 11.83"}
POS = [
    {"ticker": "PTC", "fired": "2026-10-09", "status": "NEW", "fire_close": 194.45, "base": 140.3,
     "entry": None, "entry_date": None, "price": 194.45, "to_stop": -27.8, "gap": True, "pinned": True},
    {"ticker": "BURL", "fired": "2026-09-22", "last_refire": "2026-10-09", "status": "REFIRED", "base": 227.28,
     "entry": 255.66, "entry_date": "2026-09-23", "price": 276.97, "to_stop": -17.9, "ret": 8.3, "gap": False},
    {"ticker": "BURL", "fired": "2025-03-03", "status": "CLOSED", "base": 100.0, "entry": 110.0,
     "entry_date": "2025-03-04", "exit": 99.0, "ret": -10.0},
]


def test_only_fires_and_refires_trigger_a_mail():
    assert nt.collect(_latest([SELL])) == []
    items = nt.collect(_latest([SELL, REFIRE, FIRE], POS))
    assert [(i["event"]["type"], i["event"]["ticker"]) for i in items] == [("FIRE", "PTC"), ("REFIRE", "BURL")]
    assert items[0]["position"]["fired"] == "2026-10-09"
    assert items[1]["position"]["status"] == "REFIRED"           # the open row, not the 2025 closed one


def test_same_day_rerun_has_nothing_new():
    """scan.py reports nothing new when since == asof; the mail follows."""
    assert nt.collect(_latest([], POS, since="2026-10-09")) == []


def test_subject_and_body_name_the_tickers_and_the_rule():
    latest = _latest([FIRE, REFIRE, SELL], POS, new_sells=["CNH"])
    items = nt.collect(latest)
    assert nt.subject(latest, items) == "Ignition Watch 2026-10-09: new fire PTC · re-fire BURL"
    text = nt.render_text(latest, items, "https://x/ignition")
    assert "NEW FIRE  PTC" in text and "[gap · pinned]" in text and "buy at the next open" in text
    assert "RE-FIRE  BURL" in text and "held since 2026-09-23 at 255.66; +8.3% so far" in text
    assert "pre-ignition base 140.30 (-27.8% away)" in text
    assert "Sells this run: CNH" in text and "https://x/ignition" in text
    html = nt.render_html(latest, items, "https://x/ignition")
    assert "PTC" in html and "BURL" in html and 'href="https://x/ignition"' in html


def test_default_recipients_and_override(monkeypatch):
    monkeypatch.delenv("IGNITION_NOTIFY_EMAIL", raising=False)
    assert nt.recipients() == ["Raveesh.bhasin@gmail.com", "Meetarao@gmail.com", "shalomthomas@gmail.com"]
    monkeypatch.setenv("IGNITION_NOTIFY_EMAIL", " a@x.com, b@y.com ,")
    assert nt.recipients() == ["a@x.com", "b@y.com"]
    monkeypatch.setenv("IGNITION_NOTIFY_EMAIL", "")                 # an empty Actions variable = default
    assert nt.recipients() == nt.RECIPIENTS


def test_main_sends_one_mail_per_recipient(tmp_path, monkeypatch):
    path = tmp_path / "latest.json"
    path.write_text(json.dumps(_latest([FIRE, REFIRE], POS)))
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setenv("IGNITION_NOTIFY_FROM", "Ignition <ignition@example.com>")
    monkeypatch.delenv("IGNITION_NOTIFY_EMAIL", raising=False)
    sent = []

    def fake_send(api_key, sender, to, subj, text, body_html):
        sent.append((api_key, sender, to, subj))
        return {"id": f"id-{len(sent)}"}

    monkeypatch.setattr(nt, "send", fake_send)
    assert nt.main(path) == 0
    assert [s[2] for s in sent] == nt.RECIPIENTS
    assert all(s[0] == "re_test" and s[1] == "Ignition <ignition@example.com>" for s in sent)
    assert all(s[3] == "Ignition Watch 2026-10-09: new fire PTC · re-fire BURL" for s in sent)


def test_main_skips_without_key_or_news(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    monkeypatch.setattr(nt, "send", lambda *a, **k: pytest.fail("must not send"))
    path = tmp_path / "latest.json"
    path.write_text(json.dumps(_latest([FIRE], POS)))
    assert nt.main(path) == 0                                         # not configured: print, exit 0
    assert "RESEND_API_KEY not set" in capsys.readouterr().out
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    path.write_text(json.dumps(_latest([SELL], POS)))
    assert nt.main(path) == 0                                         # nothing new: exit 0, no send
    assert nt.main(tmp_path / "missing.json") == 0


def test_a_rejected_send_fails_the_step_but_tries_everyone(tmp_path, monkeypatch):
    path = tmp_path / "latest.json"
    path.write_text(json.dumps(_latest([FIRE], POS)))
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.delenv("IGNITION_NOTIFY_EMAIL", raising=False)
    tried = []

    def flaky(api_key, sender, to, subj, text, body_html):
        tried.append(to)
        if to == nt.RECIPIENTS[1]:
            raise RuntimeError("validation_error: sandbox sender")
        return {"id": "ok"}

    monkeypatch.setattr(nt, "send", flaky)
    assert nt.main(path) == 1
    assert tried == nt.RECIPIENTS


def test_send_posts_resend_payload(monkeypatch):
    captured = {}

    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b'{"id":"abc"}'

    def fake_urlopen(req, timeout):
        captured["url"], captured["body"] = req.full_url, json.loads(req.data)
        captured["auth"] = req.get_header("Authorization")
        return Resp()

    monkeypatch.setattr(nt.urllib.request, "urlopen", fake_urlopen)
    assert nt.send("re_k", "A <a@b.c>", "to@x.com", "subj", "txt", "<p>h</p>") == {"id": "abc"}
    assert captured["url"] == nt.RESEND_API_URL and captured["auth"] == "Bearer re_k"
    assert captured["body"] == {"from": "A <a@b.c>", "to": ["to@x.com"], "subject": "subj",
                                "html": "<p>h</p>", "text": "txt"}
