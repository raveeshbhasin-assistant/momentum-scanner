"""Write PREREG_sha256.txt: SHA-256 of the registration, the code and the data it reads.

  python research/whale_flow/lock.py            first lock (refuses to overwrite)
  python research/whale_flow/lock.py --check    compare the current files with the lock
  python research/whale_flow/lock.py --add out/WF1_wf5_sign.json   append one file (confirmatory lock)
"""
import hashlib
import json
import sys
from pathlib import Path

from wf_common import WF

HERE = Path(__file__).parent
LOCK = HERE / "PREREG_sha256.txt"
CODE = ["PREREG_WF1.md", "wf_common.py", "build_tables.py", "build_events.py", "wf1_run.py"]
DATA = ["insider_trans.pkl", "insider_purchases.pkl", "darkflow.pkl", "news8k_all.pkl", "delisted_meta.jsonl"]
DIRS = ["bars", "bars_delisted"]


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha_dir(d):
    """One digest over every file's name and content, in name order."""
    h = hashlib.sha256()
    files = sorted(d.glob("*.csv"))
    for p in files:
        h.update(p.name.encode())
        h.update(sha(p).encode())
    return f"{h.hexdigest()} ({len(files)} files)"


def current():
    out = {n: sha(HERE / n) for n in CODE}
    out.update({f"data/{n}": sha(WF / n) for n in DATA})
    out.update({f"data/{d}/": sha_dir(WF / d) for d in DIRS})
    return out


if __name__ == "__main__":
    if "--check" in sys.argv:
        old, new = json.loads(LOCK.read_text()), current()
        bad = [k for k in new if old.get(k) != new[k]]
        print("lock intact" if not bad else f"CHANGED since lock: {bad}")
        sys.exit(1 if bad else 0)
    if "--add" in sys.argv:
        rel = sys.argv[sys.argv.index("--add") + 1]
        old = json.loads(LOCK.read_text())
        key = f"data/{rel}"
        if key in old:
            sys.exit(f"{key} already locked")
        old[key] = sha(WF / rel)
        LOCK.write_text(json.dumps(old, indent=1))
        print("added", key, old[key])
        sys.exit(0)
    if LOCK.exists():
        sys.exit("PREREG_sha256.txt exists: the registration is already locked")
    LOCK.write_text(json.dumps(current(), indent=1))
    print(LOCK.read_text())
