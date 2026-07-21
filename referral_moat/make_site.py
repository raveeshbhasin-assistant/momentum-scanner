"""Inject scorecards.json + qualitative notes into the site template.

Run after build.py:  python referral_moat/make_site.py
Out: referral_moat/site/index.html (self-contained, no external requests)
"""
import json
from pathlib import Path

from qualitative import QUALITATIVE

HERE = Path(__file__).parent
data = json.loads((HERE / "data" / "scorecards.json").read_text(encoding="utf-8"))
tpl = (HERE / "site_template.html").read_text(encoding="utf-8")

html = (tpl
        .replace("__ASOF__", data["as_of"])
        .replace("__DATA__", json.dumps(data, separators=(",", ":")))
        .replace("__QUAL__", json.dumps(QUALITATIVE, separators=(",", ":"))))

out = HERE / "site"
out.mkdir(exist_ok=True)
(out / "index.html").write_text(html, encoding="utf-8")
print(f"Wrote {out / 'index.html'} ({len(html)//1024} KB)")
