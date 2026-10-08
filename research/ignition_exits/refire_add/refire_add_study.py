"""
Re-fire add study — does buying MORE on a re-fire beat putting the same money
into a fresh fire elsewhere?

Every ignition fire 2016-2026 (S&P 500+400, ignition/universe.txt) is replayed
with the live ledger rule (ignition/scan.py): buy at the next open, sell at the
open after a close below the pre-ignition base, or after 252 sessions without a
re-fire. Each RE-FIRE (a fresh signal after >= 5 sessions off) becomes an "add"
tranche bought at the next open and held to the same exit as the position.

Compared on the same exits:
  - first tranche of every fire   = the alternative use of the money (a new fire)
  - ADD at 1st / any re-fire      = the add tranche's own return, forward
                                    63-session excess vs the universe, and the
                                    share that drew down >= 20% within 63 sessions
Splits as in exit_study.py: train = fires before 2022, holdout = complete
trades from 2022, live = still open / censored.

Exploratory, not pre-registered. Run: python refire_add_study.py  (needs
Yahoo; downloads into ./px). Results: README.md next to this file.
"""
import os, glob, time, datetime as dt
import numpy as np, pandas as pd, yfinance as yf

T0 = time.time()
HERE = os.path.dirname(os.path.abspath(__file__))
tk = open(os.path.join(HERE, "..", "..", "..", "ignition", "universe.txt")).read().split()
tk = [t for t in tk if not t.startswith("#")]
os.makedirs("px", exist_ok=True)
B = 300
for i in range(0, len(tk), B):
    f = f"px/c{i//B}.pkl"
    if os.path.exists(f):
        continue
    d = yf.download(tk[i:i+B], start="2015-01-01", auto_adjust=True, progress=False, threads=True)
    d[["Open", "Close", "Volume"]].astype("float32").to_pickle(f)
    print(f"chunk {i//B} saved at {time.time()-T0:.0f}s", flush=True)
parts = [pd.read_pickle(f) for f in sorted(glob.glob("px/c*.pkl"))]


def J(fld):
    df = pd.concat([p[fld] for p in parts], axis=1)
    return df.loc[:, ~df.columns.duplicated()].sort_index()


O, C, V = J("Open"), J("Close"), J("Volume")
if C.index[-1].date() >= dt.date.today():          # never score a live bar
    O, C, V = O.iloc[:-1], C.iloc[:-1], V.iloc[:-1]
print(f"universe {C.shape[1]} tickers, {C.index[0].date()}..{C.index[-1].date()}", flush=True)
r5 = C.pct_change(5, fill_method=None)
r252 = C.pct_change(252, fill_method=None)
volr = V.rolling(21).mean() / V.rolling(126).mean()
golden = C.rolling(50).mean() > C.rolling(200).mean()
dv = (C * V).rolling(21).mean()
valid = (C > 3) & (dv > 5e6) & r252.notna()
sig = ((r5 > 0.12) & (volr > 1.5) & golden & valid).values
fwd = C.shift(-63) / C - 1
xs = fwd.sub(fwd.where(valid).mean(axis=1), axis=0).values      # forward excess vs universe
Cv, Ov = C.values, O.values
n, m = Cv.shape
dates = C.index
A, Bt = [], []
for j in range(m):
    last = -10_000
    for t in np.flatnonzero(sig[:, j]):
        new = t - last > 20
        last = t
        if not new or t < 5 or t + 1 >= n or not np.isfinite(Ov[t+1, j]):
            continue
        e, entry, base = t + 1, Ov[t+1, j], Cv[t-5, j]
        refs, lastfire, d, reason = [], t, t + 1, None
        while d < n:
            c = Cv[d, j]
            if sig[d, j]:
                if not sig[max(0, d-5):d, j].any():
                    refs.append(d)
                lastfire = d
            if np.isfinite(c) and c < base:
                reason = "BASE"
                break
            if d - lastfire >= 252:
                reason = "TIME"
                break
            d += 1
        if reason and d + 1 < n and np.isfinite(Ov[d+1, j]):
            xi, xp, done = d + 1, Ov[d+1, j], True
        else:
            xi, xp, done = min(d, n-1), Cv[min(d, n-1), j], False
        if not np.isfinite(xp):
            xp = pd.Series(Cv[e:xi+1, j]).ffill().iloc[-1]
        fire_date = dates[t]
        split = "train" if fire_date < pd.Timestamp("2022-01-01") else ("holdout" if done and t + 252 + 63 < n else "live")

        def dd63(p, i0):
            w = Cv[i0:i0+64, j]
            w = w[np.isfinite(w)]
            return (w.min() / p - 1) if len(w) else np.nan

        A.append(dict(split=split, ret=xp/entry-1, xs=xs[e, j], dd=dd63(entry, e), held=xi-e, nref=len(refs), done=done))
        for k, dr in enumerate(refs):
            if dr >= d or dr + 1 >= n or not np.isfinite(Ov[dr+1, j]):
                continue
            p = Ov[dr+1, j]
            Bt.append(dict(split=split, ret=xp/p-1, xs=xs[dr+1, j], dd=dd63(p, dr+1), held=xi-(dr+1),
                           k=k+1, since=dr-t, gain_at=Cv[dr, j]/entry-1, ret_A=xp/entry-1))
A, Bt = pd.DataFrame(A), pd.DataFrame(Bt)


def S(df):
    return dict(n=len(df), mean=round(100*df.ret.mean(), 1), median=round(100*df.ret.median(), 1),
                win=round(100*(df.ret > 0).mean()), xs63=round(100*df["xs"].mean(), 1),
                xs63_win=round(100*(df["xs"] > 0).mean()), dd20=round(100*(df.dd <= -0.2).mean()),
                held=round(df.held.mean()))


print(f"\ntrades {len(A)}  re-fire adds {len(Bt)}  ({time.time()-T0:.0f}s)")
for sp in ("train", "holdout", "live"):
    a, b = A[A.split == sp], Bt[Bt.split == sp]
    print(f"\n== {sp.upper()} ==")
    print(" first tranche (every fire)   ", S(a))
    print(" first tranche, trades w/ refire", S(a[a.nref > 0]))
    print(" ADD at 1st re-fire           ", S(b[b.k == 1]) if len(b) else "-")
    print(" ADD at any re-fire           ", S(b) if len(b) else "-")
    if len(b):
        b1 = b[b.k == 1]
        print(f" same trades: A alone {100*b1.ret_A.mean():.1f}%  vs equal-$ A+add {100*((b1.ret_A+b1.ret)/2).mean():.1f}%  (add tranche {100*b1.ret.mean():.1f}%)")
        for lo, hi in ((0, 20), (20, 63), (63, 10_000)):
            s = b[(b.since >= lo) & (b.since < hi)]
            if len(s):
                print(f"   re-fire {lo}-{hi} sessions after fire: n={len(s)} add ret {100*s.ret.mean():.1f}% xs63 {100*s['xs'].mean():.1f}% win {100*(s.ret>0).mean():.0f}%")
        for lo, hi in ((-1, 0.1), (0.1, 0.3), (0.3, 10)):
            s = b[(b.gain_at > lo) & (b.gain_at <= hi)]
            if len(s):
                print(f"   position up {lo:+.0%}..{hi:+.0%} at re-fire: n={len(s)} add ret {100*s.ret.mean():.1f}% xs63 {100*s['xs'].mean():.1f}% win {100*(s.ret>0).mean():.0f}%")
print(f"\nfraction of trades with >=1 re-fire: {100*(A.nref>0).mean():.0f}%   ({time.time()-T0:.0f}s)")
