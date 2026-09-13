"""NEWS8K: resumable SEC submissions-API fetcher. For every universe CIK pull EVERY 8-K* filing (all items)
from data.sec.gov/submissions (recent block + all archive pages covering 2004+). Sharded: argv = nshards shard [dry]."""
import requests, json, time, os, sys, glob
import pandas as pd
RES = "C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad/research"
EARN = RES + "/earn"; OUTD = RES + "/news8k"
NSH = int(sys.argv[1]) if len(sys.argv) > 1 else 1; SH = int(sys.argv[2]) if len(sys.argv) > 2 else 0
DRY = len(sys.argv) > 3 and sys.argv[3] == "dry"
OUT = f"{OUTD}/sec_8k_all_raw_{SH}.jsonl"; FAIL = f"{OUTD}/failed_{SH}.txt"
H = {"User-Agent": "Raveesh Singh raveeshsingh@gmail.com", "Accept-Encoding": "gzip, deflate"}
MIN_GAP = 0.5
def norm(s): return str(s).strip().upper().replace(".", "-").replace("/", "-") if s else ""
UNI = {norm(t) for t in pd.read_csv(RES + "/us_common_symbols.csv")["yf"].dropna()}
s = requests.Session(); s.headers.update(H)
last = 0.0; nreq = 0
def get(url):
    global last, nreq
    for attempt in range(10):
        gap = MIN_GAP - (time.time() - last)
        if gap > 0: time.sleep(gap)
        try:
            r = s.get(url, timeout=60); last = time.time(); nreq += 1
            if r.status_code == 200: return r.json()
            if r.status_code == 404: return {}
            print(f"  HTTP {r.status_code} attempt {attempt} {url}", flush=True); time.sleep(30 + 30*attempt)
        except Exception as e:
            print(f"  EXC {type(e).__name__}: {str(e)[:80]}", flush=True); time.sleep(5 + 5*attempt)
    return None
ct = json.load(open(EARN + "/company_tickers.json"))
cik2t = {}
for v in ct.values():
    t = norm(v["ticker"])
    if t in UNI: cik2t.setdefault(int(v["cik_str"]), set()).add(t)
ciks = sorted(cik2t)
done = set()
for p in glob.glob(f"{OUTD}/sec_8k_all_raw_*.jsonl"):
    for line in open(p, encoding="utf-8"):
        try: done.add(json.loads(line)["cik"])
        except Exception: pass
todo = [c for i, c in enumerate(ciks) if i % NSH == SH and c not in done]
print(f"shard {SH}/{NSH}: universe {len(UNI)} mapped ciks {len(ciks)} done {len(done)} todo {len(todo)}", flush=True)
if DRY: sys.exit(0)
def extract(block, out):
    if not block: return
    forms = block.get("form", []); fd = block.get("filingDate", []); rd = block.get("reportDate", []); acc = block.get("acceptanceDateTime", []); it = block.get("items", []); an = block.get("accessionNumber", [])
    for i in range(len(forms)):
        if str(forms[i]).startswith("8-K") and fd[i] >= "2004-01-01":
            out.append({"a": an[i], "form": forms[i], "fd": fd[i], "rd": rd[i], "acc": acc[i], "it": it[i]})
t0 = time.time(); n = 0; nf = 0
for cik in todo:
    j = get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json")
    if j is None: open(FAIL, "a").write(f"{cik}\n"); continue
    fl = []; filings = j.get("filings", {}) if j else {}
    extract(filings.get("recent"), fl); ok = True; narch = 0
    for f in filings.get("files", []) or []:
        if f.get("filingTo", "9999") >= "2004-01-01":
            j2 = get(f"https://data.sec.gov/submissions/{f['name']}")
            if j2 is None: ok = False; break
            extract(j2, fl); narch += 1
    if not ok: open(FAIL, "a").write(f"{cik}\n"); continue
    rec = {"cik": cik, "tickers": sorted(cik2t[cik]), "sec_tickers": j.get("tickers") if j else None, "name": j.get("name") if j else None,
           "sic": j.get("sic") if j else None, "exch": j.get("exchanges") if j else None, "narch": narch, "n": len(fl), "f": fl}
    with open(OUT, "a", encoding="utf-8") as fo: fo.write(json.dumps(rec) + "\n")
    n += 1; nf += len(fl)
    if n % 50 == 0 or n <= 2:
        el = time.time() - t0
        print(f"  {n}/{len(todo)} ciks, {nreq} req, {nf} 8-K filings, {el/60:.1f} min, {nreq/el:.2f} req/s, eta {(len(todo)-n)*el/n/60:.0f} min, last={cik} {rec['tickers']} n={len(fl)}", flush=True)
print(f"FINISHED ciks={n} req={nreq} filings={nf} {(time.time()-t0)/60:.1f} min", flush=True)
