#!/usr/bin/env python
"""PEAD executor. Registered design: preregistration_pead.json (KEY PEAD). Single run.
Stage 1 builds the panel, events, trades and matched controls (cached); stage 2 computes statistics and writes outputs.
"""
import os, sys, json, time, warnings
import numpy as np, pandas as pd
from datetime import datetime, date

warnings.filterwarnings("ignore")
BASE = "C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad"
RES = BASE + "/research"
VARIANT = os.environ.get("PEAD_CTRL", "registered")
PFX = RES + ("/PEAD_" if VARIANT == "registered" else f"/PEAD_sensCtrl_{VARIANT}_")
CACHE = PFX + "cache_stage1.pkl"
T0 = time.time()
def log(*a): print(f"[{time.time()-T0:7.1f}s]", *a, flush=True)

HORIZONS = [1, 5, 20, 60]
REPS = 2000
SEED = 20260913
DISC = (pd.Timestamp('2005-01-01'), pd.Timestamp('2014-12-31'))
TEST = (pd.Timestamp('2015-01-01'), pd.Timestamp('2026-08-31'))
FAMILY = [('C1', 5), ('C1', 20), ('C2', 5), ('C2', 20), ('C3', 5), ('C3', 20), ('C4', 20), ('C5', 20)]

# ----------------------------------------------------------------------------- ET conversion
def _first_sunday(y, m):
    d1 = date(y, m, 1)
    return date(y, m, 1 + (6 - d1.weekday()) % 7)

def _last_sunday(y, m, last_day):
    dl = date(y, m, last_day)
    return date(y, m, last_day - (dl.weekday() + 1) % 7)

_dst_cache = {}
def dst_bounds_utc(y):
    if y not in _dst_cache:
        if y >= 2007:
            s = _first_sunday(y, 3); s = date(y, 3, s.day + 7)
            e = _first_sunday(y, 11)
        else:
            s = _first_sunday(y, 4)
            e = _last_sunday(y, 10, 31)
        _dst_cache[y] = (datetime(s.year, s.month, s.day, 7, 0), datetime(e.year, e.month, e.day, 6, 0))
    return _dst_cache[y]

def utc_to_et(dt):
    s, e = dst_bounds_utc(dt.year)
    off = 4 if (s <= dt < e) else 5
    return dt - pd.Timedelta(hours=off)

# ----------------------------------------------------------------------------- stage 1
def stage1():
    spy = pd.read_csv(RES + "/EXEC_v5_SPY_2004_2026.csv", parse_dates=['date']).set_index('date').sort_index()
    spy = spy[spy.index >= '2004-01-01']
    spy = spy[~spy.index.duplicated()]
    cal = spy.index
    nd = len(cal)
    log("calendar", cal[0].date(), cal[-1].date(), nd)

    uni = pd.read_csv(RES + "/us_common_symbols.csv", dtype=str)
    sym2yf = dict(zip(uni['symbol'], uni['yf']))

    dirs = [BASE + "/bars1d_2004_2013", BASE + "/bars1d_10y", BASE + "/bars1d_all"]
    files = {}
    for d in dirs:
        for f in os.listdir(d):
            if f.endswith('.csv'):
                files.setdefault(f[:-4], []).append(os.path.join(d, f))
    log("tickers with bars:", len(files))
    O, C, V = {}, {}, {}
    for i, (t, fl) in enumerate(files.items()):
        parts = []
        for p in fl:
            try:
                df = pd.read_csv(p, usecols=['date', 'Open', 'Close', 'Volume'])
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                parts.append(df)
            except Exception:
                continue
        if not parts:
            continue
        df = pd.concat(parts).drop_duplicates('date', keep='last').set_index('date').sort_index()
        df = df[(df['Close'] > 0) & (df['Open'] > 0)]
        df = df.reindex(cal)
        O[t] = df['Open'].to_numpy(dtype=float); C[t] = df['Close'].to_numpy(dtype=float); V[t] = df['Volume'].to_numpy(dtype=float)
        if i % 1000 == 0:
            log("loaded", i)
    tickers = list(O.keys())
    O = pd.DataFrame(O, index=cal); C = pd.DataFrame(C, index=cal); V = pd.DataFrame(V, index=cal)
    log("panel", O.shape)

    prevC = C.shift(1)
    adv_d = (C * V).rolling(20, min_periods=15).mean().shift(1)
    adv_sh = V.rolling(20, min_periods=15).mean().shift(1)
    ret = C / prevC - 1
    spy_ret = spy['Close'].pct_change()
    exc = ret.sub(spy_ret, axis=0)
    elig = (prevC >= 5) & (adv_d >= 50e6) & C.notna() & O.notna() & V.notna()
    gap = O / prevC - 1
    volr = V / adv_sh
    On = O.to_numpy(); Cn = C.to_numpy(); excn = exc.to_numpy(); advn = adv_d.to_numpy(); elign = elig.to_numpy()
    gapn = gap.to_numpy(); volrn = volr.to_numpy()
    spyO = spy['Open'].to_numpy(); spyC = spy['Close'].to_numpy()
    col = {t: i for i, t in enumerate(tickers)}
    del O, C, V, prevC, adv_sh, ret, exc, gap, volr
    log("derived matrices done")

    # ---- events
    rows = []
    n_raw = n_amend = n_nomap = n_badts = 0
    for k in range(5):
        with open(RES + f"/earn/sec_sub_raw_{k}.jsonl", encoding='utf-8') as fh:
            for line in fh:
                rec = json.loads(line)
                tk = None
                for t in rec.get('tickers') or []:
                    t = str(t).strip().upper()
                    for cand in (t, sym2yf.get(t), t.replace('.', '-')):
                        if cand in col:
                            tk = cand; break
                    if tk: break
                if tk is None:
                    n_nomap += len(rec.get('f') or []); continue
                for f in rec.get('f') or []:
                    n_raw += 1
                    if f.get('form') != '8-K':
                        n_amend += 1; continue
                    items = [x.strip() for x in str(f.get('it', '')).split(',')]
                    if '2.02' not in items:
                        continue
                    acc = f.get('acc')
                    try:
                        dt = datetime.strptime(acc[:19], "%Y-%m-%dT%H:%M:%S")
                    except Exception:
                        n_badts += 1; continue
                    if dt.year < 2004:
                        continue
                    rows.append((rec['cik'], tk, f['a'], dt))
    ev = pd.DataFrame(rows, columns=['cik', 'ticker', 'acc_no', 'acc_utc'])
    log(f"filings raw={n_raw} amendments_excluded={n_amend} unmapped_filings={n_nomap} bad_ts={n_badts} usable={len(ev)}")
    et = ev['acc_utc'].map(utc_to_et)
    ev['acc_et'] = et
    etd = et.dt.normalize()
    mins = et.dt.hour * 60 + et.dt.minute
    timing = np.where(mins < 570, 'pre', np.where(mins < 960, 'intra', 'post'))
    pos = cal.searchsorted(etd.to_numpy())  # first session >= filing date
    pos = np.minimum(pos, nd - 1)
    is_td = (cal.to_numpy()[pos] == etd.to_numpy().astype(cal.dtype))
    r_pos = pos.copy()
    # post on a trading day -> next session; intra/pre on a non-trading day -> next session (already pos), retag as 'pre'
    r_pos = np.where((timing == 'post') & is_td, pos + 1, pos)
    timing = np.where((~is_td) & (timing != 'post'), 'pre', timing)
    timing = np.where((~is_td) & (timing == 'post'), 'post', timing)
    ev['timing'] = timing
    ev['r_pos'] = r_pos
    ev = ev[ev['r_pos'] < nd - 1].copy()
    ev['R'] = cal[ev['r_pos'].to_numpy()]
    ev['tk_idx'] = ev['ticker'].map(col).astype(int)
    ev = ev.sort_values(['ticker', 'r_pos', 'acc_utc']).reset_index(drop=True)
    # greedy dedupe within ticker: keep first, drop any with r_pos within 5 sessions after the last kept
    keep = np.ones(len(ev), dtype=bool)
    last_tk = None; last_r = -10**9
    tks = ev['ticker'].to_numpy(); rps = ev['r_pos'].to_numpy()
    for i in range(len(ev)):
        if tks[i] != last_tk:
            last_tk = tks[i]; last_r = rps[i]; continue
        if rps[i] - last_r <= 5:
            keep[i] = False
        else:
            last_r = rps[i]
    log(f"events after ET/R mapping={len(ev)}, dropped as duplicates within 5 sessions={int((~keep).sum())}")
    ev = ev[keep].reset_index(drop=True)
    # event mask for the control 'no 8-K within +-5 sessions' rule (all deduped events, before eligibility)
    evm = np.zeros((nd, len(tickers)), dtype=np.int32)
    np.add.at(evm, (ev['r_pos'].to_numpy(), ev['tk_idx'].to_numpy()), 1)
    cs = np.vstack([np.zeros((1, evm.shape[1]), dtype=np.int32), np.cumsum(evm, axis=0, dtype=np.int32)])
    idx_hi = np.minimum(np.arange(nd) + 6, nd); idx_lo = np.maximum(np.arange(nd) - 5, 0)
    evwin = (cs[idx_hi] - cs[idx_lo]) > 0
    del evm, cs

    # ---- eligibility + signal
    rp = ev['r_pos'].to_numpy(); ti = ev['tk_idx'].to_numpy()
    ev['elig'] = elign[rp, ti]
    ev['reaction'] = excn[rp, ti]
    ev['gap'] = gapn[rp, ti]
    ev['volratio'] = volrn[rp, ti]
    ev['adv_d'] = advn[rp, ti]
    ev['close_R'] = Cn[rp, ti]
    ev['open_R1'] = On[rp + 1, ti]
    ev['spy_ret_R'] = spyC[rp] / spyC[rp - 1] - 1
    ev = ev[(ev['R'] >= DISC[0]) & (ev['R'] <= TEST[1])]
    log(f"events in 2005-01..2026-08 = {len(ev)}; eligible = {int(ev['elig'].sum())}")
    ev = ev[ev['elig'] & ev['reaction'].notna() & ev['open_R1'].notna()].copy().reset_index(drop=True)
    ev['window'] = np.where(ev['R'] <= DISC[1], 'DISC', 'TEST')
    ev['tier_cost'] = np.where(ev['adv_d'] >= 100e6, 0.0010, 0.0020)
    rp = ev['r_pos'].to_numpy(); ti = ev['tk_idx'].to_numpy(); cost = ev['tier_cost'].to_numpy()
    for h in HORIZONS:
        xp = rp + h
        ok = xp < nd
        xpc = np.minimum(xp, nd - 1)
        exitC = np.where(ok, Cn[xpc, ti], np.nan)
        spy_x = np.where(ok, spyC[xpc], np.nan)
        ev[f'ret_o_{h}'] = exitC / ev['open_R1'].to_numpy() - 1
        ev[f'spy_o_{h}'] = spy_x / spyO[rp + 1] - 1
        ev[f'net_o_{h}'] = ev[f'ret_o_{h}'] - ev[f'spy_o_{h}'] - cost
        ev[f'net_c_{h}'] = (exitC / ev['close_R'].to_numpy() - 1) - (spy_x / spyC[rp] - 1) - cost
    log("event returns done")

    # ---- matched controls
    ev = ev.sort_values(['r_pos', 'ticker']).reset_index(drop=True)
    rng = np.random.default_rng(SEED)
    n_ev = len(ev)
    ctrl = {h: np.full(n_ev, np.nan) for h in HORIZONS}
    n_ctrl = np.zeros(n_ev, dtype=int)
    rp = ev['r_pos'].to_numpy(); ti = ev['tk_idx'].to_numpy(); rx = ev['reaction'].to_numpy()
    starts = np.flatnonzero(np.r_[True, rp[1:] != rp[:-1]])
    ends = np.r_[starts[1:], n_ev]
    for s, e in zip(starts, ends):
        i = rp[s]
        el = elign[i]
        if el.sum() < 10:
            continue
        adv_row = advn[i]
        dec = np.full(el.shape[0], -1)
        eidx = np.flatnonzero(el)
        order = np.argsort(adv_row[eidx], kind='stable')
        ranks = np.empty(len(eidx), dtype=int); ranks[order] = np.arange(len(eidx))
        dec[eidx] = (ranks * 10) // len(eidx)
        base = el & (~evwin[i])
        exc_row = excn[i]
        cost_row = np.where(adv_row >= 100e6, 0.0010, 0.0020)
        for j in range(s, e):
            m = (base & (np.abs(exc_row - rx[j]) <= 0.01)) if VARIANT == 'anydecile' else (base & (dec == dec[ti[j]]) & (np.abs(exc_row - rx[j]) <= 0.01))
            m[ti[j]] = False
            cidx = np.flatnonzero(m)
            if len(cidx) == 0:
                continue
            if len(cidx) > 5:
                cidx = rng.choice(cidx, 5, replace=False)
            entry = On[i + 1, cidx]
            good = ~np.isnan(entry)
            if not good.any():
                continue
            cidx = cidx[good]; entry = entry[good]
            n_ctrl[j] = len(cidx)
            for h in HORIZONS:
                xp = i + h
                if xp >= nd:
                    continue
                r = Cn[xp, cidx] / entry - 1 - (spyC[xp] / spyO[i + 1] - 1) - cost_row[cidx]
                if np.isnan(r).all():
                    continue
                ctrl[h][j] = np.nanmean(r)
        if (s % 20000) == 0:
            log("controls progress", s, "/", n_ev)
    ev['n_ctrl'] = n_ctrl
    for h in HORIZONS:
        ev[f'ctrl_net_{h}'] = ctrl[h]
        ev[f'ce_{h}'] = ev[f'net_o_{h}'] - ev[f'ctrl_net_{h}']
    log("controls done; share of events with >=1 control:", round((n_ctrl > 0).mean(), 3))

    # ---- cells
    ev['month'] = ev['R'].dt.to_period('M').astype(str)
    g = ev.groupby('month')['reaction']
    q = g.rank(pct=True); nmo = g.transform('count')
    ev['q_month'] = q; ev['n_month'] = nmo
    top = (q > 0.8) & (nmo >= 10)
    bot = (q <= 0.2) & (nmo >= 10)
    ev['C1'] = top
    ev['C2'] = (ev['reaction'] >= 0.08) & (ev['volratio'] >= 3.0)
    ev['C3'] = bot
    ev['C4'] = top & (ev['adv_d'] >= 50e6) & (ev['adv_d'] < 500e6)
    lq = ev.groupby('month')['adv_d'].rank(pct=True)
    ev['C5'] = top & (lq > 2.0 / 3.0)
    thr = ev['reaction'].shift(1).rolling(250, min_periods=100).quantile(0.8)
    ev['C1_trail'] = ev['reaction'] > thr
    ev['year'] = ev['R'].dt.year
    ev['quarter'] = ev['R'].dt.to_period('Q').astype(str)
    ev['size_tier'] = pd.cut(ev['adv_d'], [50e6, 100e6, 500e6, 2000e6, np.inf], right=False,
                             labels=['50-100M', '100-500M', '500M-2B', '>2B']).astype(str)
    # keep panel pieces needed for per-capital
    pd.to_pickle({'ev': ev, 'cal': cal, 'spyC': spyC, 'spyO': spyO}, CACHE)
    np.save(PFX + "cache_Cn.npy", Cn); np.save(PFX + "cache_On.npy", On)
    log("stage 1 cached")
    return ev, cal, spyC, spyO, Cn, On

# ----------------------------------------------------------------------------- stats
def boot(x, cl, reps=REPS, seed=SEED, direction=1):
    x = np.asarray(x, dtype=float) * direction
    codes, _ = pd.factorize(pd.Series(cl))
    sums = np.bincount(codes, weights=x); cnts = np.bincount(codes).astype(float)
    m = len(sums)
    if m < 3 or len(x) < 3:
        return np.nan, np.nan, np.nan
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, m, size=(reps, m))
    means = sums[idx].sum(1) / cnts[idx].sum(1)
    lo, hi = np.percentile(means, [2.5, 97.5])
    p = max(float((means <= 0).mean()), 0.5 / reps)
    return float(lo), float(hi), p

def wider(x, days, qtrs, direction=1):
    lo_d, hi_d, p_d = boot(x, days, direction=direction)
    lo_q, hi_q, p_q = boot(x, qtrs, direction=direction)
    if np.isnan(lo_q) or (not np.isnan(lo_d) and (hi_d - lo_d) >= (hi_q - lo_q)):
        lo, hi, which = lo_d, hi_d, 'day'
    else:
        lo, hi, which = lo_q, hi_q, 'quarter'
    p = np.nanmax([p_d, p_q])
    return lo, hi, p, which, (lo_d, hi_d), (lo_q, hi_q)

def cell_stats(sub, h, direction=1, col_prefix='net_o_'):
    col = f'{col_prefix}{h}'
    s = sub[sub[col].notna()]
    out = {'n': int(len(s))}
    if len(s) < 3:
        return out
    x = s[col].to_numpy() * direction
    out['mean'] = float(x.mean()) * 100
    out['median'] = float(np.median(x)) * 100
    out['win'] = float((x > 0).mean())
    lo, hi, p, which, dd, qq = wider(s[col].to_numpy(), s['R'].to_numpy(), s['quarter'].to_numpy(), direction)
    out.update({'lo': lo * 100, 'hi': hi * 100, 'p': p, 'ci_cluster': which,
                'lo_day': dd[0] * 100, 'hi_day': dd[1] * 100, 'lo_q': qq[0] * 100, 'hi_q': qq[1] * 100})
    sc = s[s[f'ce_{h}'].notna()]
    out['n_ctrl_events'] = int(len(sc)); out['ctrl_share'] = float(len(sc) / len(s))
    if len(sc) >= 3:
        out['ctrl_mean'] = float(sc[f'ctrl_net_{h}'].mean()) * 100 * direction
        out['event_mean_on_ctrl_subset'] = float(sc[col].mean()) * 100 * direction
        clo, chi, cp, cwhich, _, _ = wider(sc[f'ce_{h}'].to_numpy(), sc['R'].to_numpy(), sc['quarter'].to_numpy(), direction)
        out.update({'ce_mean': float(sc[f'ce_{h}'].mean()) * 100 * direction, 'ce_lo': clo * 100, 'ce_hi': chi * 100, 'ce_p': cp})
    yr = s.groupby('year')[col].agg(['mean', 'count'])
    yr = yr[yr['count'] >= 10]
    out['years_n'] = int(len(yr)); out['years_pos'] = float(((yr['mean'] * direction) > 0).mean()) if len(yr) else np.nan
    return out

def holm(pvals):
    p = np.asarray(pvals, dtype=float); m = len(p)
    order = np.argsort(p); adj = np.empty(m)
    run = 0.0
    for k, i in enumerate(order):
        v = min(1.0, (m - k) * p[i]); run = max(run, v); adj[i] = run
    return adj

def per_capital(tr, Cn, On, cal, spyC, K, h, t0, t1):
    nd = len(cal)
    tr = tr.sort_values(['r_pos', 'reaction'], ascending=[True, False])
    slot_free = np.zeros(K, dtype=int)
    port = np.zeros(nd); taken = skipped = 0; expo = np.zeros(nd)
    for row in tr.itertuples():
        e = row.r_pos + 1; x = row.r_pos + h
        if x >= nd:
            continue
        s = int(np.argmin(slot_free))
        if slot_free[s] > e:
            skipped += 1; continue
        slot_free[s] = x + 1
        c = Cn[e:x + 1, row.tk_idx]; o = On[e, row.tk_idx]
        prev = np.concatenate([[o], c[:-1]])
        r = c / prev - 1
        r = np.nan_to_num(r, nan=0.0)
        r[-1] -= row.tier_cost
        port[e:x + 1] += r / K; expo[e:x + 1] += 1.0 / K
        taken += 1
    i0 = cal.searchsorted(t0); i1 = min(cal.searchsorted(t1, side='right'), nd)
    pr = port[i0:i1]; eq = np.cumprod(1 + pr)
    dd = eq / np.maximum.accumulate(eq) - 1
    yrs = len(pr) / 252
    spy_eq = spyC[i0:i1] / spyC[i0]
    spy_dd = spy_eq / np.maximum.accumulate(spy_eq) - 1
    eq_s = pd.Series(eq, index=cal[i0:i1])
    yearly = eq_s.groupby(eq_s.index.year).last()
    yearly = (yearly / yearly.shift(1).fillna(1.0) - 1)
    return {'taken': taken, 'skipped': skipped, 'final': float(eq[-1]), 'cagr': float(eq[-1] ** (1 / yrs) - 1), 'maxdd': float(dd.min()),
            'avg_exposure': float(expo[i0:i1].mean()), 'spy_final': float(spy_eq[-1]), 'spy_cagr': float(spy_eq[-1] ** (1 / yrs) - 1),
            'spy_maxdd': float(spy_dd.min()), 'ann_vol': float(pr.std() * np.sqrt(252)), 'yearly': {int(k): round(float(v) * 100, 1) for k, v in yearly.items()}}

def md_table(df, floatfmt="{:.2f}"):
    cols = list(df.columns)
    lines = ["| " + " | ".join(str(c) for c in cols) + " |", "|" + "|".join(["---"] * len(cols)) + "|"]
    for _, r in df.iterrows():
        cells = []
        for c in cols:
            v = r[c]
            if isinstance(v, (float, np.floating)):
                cells.append("" if np.isnan(v) else floatfmt.format(v))
            else:
                cells.append(str(v))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)

# ----------------------------------------------------------------------------- stage 2
def stage2(ev, cal, spyC, spyO, Cn, On):
    cells_rows = []
    cell_defs = {'C1': ('C1', 1, 'top-quintile reaction (month), long'), 'C2': ('C2', 1, 'reaction>=+8% & vol>=3x, long'),
                 'C3': ('C3', -1, 'bottom-quintile reaction (month), SHORT'), 'C4': ('C4', 1, 'top quintile, ADV 50-500M, long'),
                 'C5': ('C5', 1, 'top quintile, most-liquid tercile, long'), 'C1_trail': ('C1_trail', 1, 'trailing-250 80th pct (sensitivity), long')}
    for win in ['TEST', 'DISC']:
        sub_w = ev[ev['window'] == win]
        for cname, (flag, direction, desc) in cell_defs.items():
            sub = sub_w[sub_w[flag]]
            for h in HORIZONS:
                for entry, pfx in [('open_R+1', 'net_o_'), ('close_R', 'net_c_')]:
                    st = cell_stats(sub, h, direction, pfx)
                    row = {'cell': cname, 'desc': desc, 'window': win, 'horizon': h, 'entry': entry, 'direction': 'short' if direction < 0 else 'long',
                           'in_family': (win == 'TEST' and entry == 'open_R+1' and (cname, h) in FAMILY)}
                    row.update(st); cells_rows.append(row)
    cells = pd.DataFrame(cells_rows)
    fam = cells[cells['in_family']].copy()
    fam['p_holm'] = holm(fam['p'].fillna(1.0).to_numpy())
    fam['pass_ci'] = fam['lo'] > 0
    fam['pass_ctrl'] = (fam['ce_mean'] > 0) & (fam['ce_lo'] > 0)
    fam['pass_years'] = fam['years_pos'] >= 0.60
    fam['pass_n'] = fam['n'] >= 500
    fam['pass_holm'] = fam['p_holm'] < 0.05
    fam['PASS'] = fam[['pass_ci', 'pass_ctrl', 'pass_years', 'pass_n', 'pass_holm']].all(axis=1)
    cells = cells.merge(fam[['cell', 'window', 'horizon', 'entry', 'p_holm', 'pass_ci', 'pass_ctrl', 'pass_years', 'pass_n', 'pass_holm', 'PASS']],
                        on=['cell', 'window', 'horizon', 'entry'], how='left')
    cells.to_csv(PFX + "cells.csv", index=False)
    log("cells written")

    tst = ev[ev['window'] == 'TEST']
    # ---- splits (TEST, entry open R+1)
    def split_table(sub, by, h, direction=1):
        rows = []
        for k, g in sub.groupby(by):
            st = cell_stats(g, h, direction)
            rows.append({by: k, 'n': st.get('n'), 'mean_%': st.get('mean'), 'lo': st.get('lo'), 'hi': st.get('hi'), 'ctrl_excess_%': st.get('ce_mean'), 'years_pos': st.get('years_pos')})
        return pd.DataFrame(rows)
    timing_tabs = {}
    for cname, direction in [('C1', 1), ('C2', 1), ('C3', -1)]:
        for h in [5, 20]:
            timing_tabs[(cname, h)] = split_table(tst[tst[cname]], 'timing', h, direction)
    size_tabs = {h: split_table(tst[tst['C1']], 'size_tier', h) for h in [5, 20]}
    tst = tst.assign(regime=np.where(tst['year'].between(2020, 2024), '2020-2024', 'other'))
    regime_rows = []
    for cname, h in FAMILY:
        direction = -1 if cname == 'C3' else 1
        for reg, g in tst[tst[cname]].groupby('regime'):
            st = cell_stats(g, h, direction)
            regime_rows.append({'cell': f'{cname}_h{h}', 'regime': reg, 'n': st.get('n'), 'mean_%': st.get('mean'), 'lo': st.get('lo'), 'hi': st.get('hi'), 'ctrl_excess_%': st.get('ce_mean')})
    regime = pd.DataFrame(regime_rows)
    # yearly
    yearly_rows = []
    for y, g in ev.groupby('year'):
        r = {'year': y, 'window': g['window'].iloc[0], 'events': len(g)}
        for cname, h, direction in [('C1', 5, 1), ('C1', 20, 1), ('C2', 20, 1), ('C3', 20, -1), ('C4', 20, 1), ('C5', 20, 1)]:
            s = g[g[cname] & g[f'net_o_{h}'].notna()]
            r[f'{cname}_h{h}_n'] = len(s); r[f'{cname}_h{h}_%'] = float(s[f'net_o_{h}'].mean() * 100 * direction) if len(s) else np.nan
        yearly_rows.append(r)
    yearly = pd.DataFrame(yearly_rows)
    yearly.to_csv(PFX + "yearly.csv", index=False)
    # per-capital
    pc = {}
    for cname in ['C1', 'C2']:
        pc[cname] = per_capital(tst[tst[cname] & tst['net_o_20'].notna()], Cn, On, cal, spyC, 10, 20, TEST[0], TEST[1])
    # news-or-move decomposition for C1 h20 and h5
    nom = {}
    for h in [5, 20]:
        s = tst[tst['C1'] & tst[f'ce_{h}'].notna()]
        e_all = float(tst[tst['C1'] & tst[f'net_o_{h}'].notna()][f'net_o_{h}'].mean() * 100)
        e = float(s[f'net_o_{h}'].mean() * 100); c = float(s[f'ctrl_net_{h}'].mean() * 100); d = float(s[f'ce_{h}'].mean() * 100)
        nom[h] = {'event_all': e_all, 'event_on_ctrl_subset': e, 'control': c, 'diff': d, 'n': len(s), 'share_explained_by_move': (c / e if e > 0 else np.nan)}
    # trades file
    keep_cols = ['cik', 'ticker', 'acc_no', 'acc_utc', 'acc_et', 'timing', 'R', 'window', 'year', 'quarter', 'reaction', 'gap', 'volratio', 'adv_d', 'size_tier', 'tier_cost',
                 'close_R', 'open_R1', 'q_month', 'n_month', 'C1', 'C2', 'C3', 'C4', 'C5', 'C1_trail', 'n_ctrl'] + \
                [f'{p}{h}' for h in HORIZONS for p in ['ret_o_', 'spy_o_', 'net_o_', 'net_c_', 'ctrl_net_', 'ce_']]
    ev[keep_cols].to_csv(PFX + "trades.csv.gz", index=False, compression='gzip')
    log("trades written")

    # ---- things_tried append
    tt = []
    for _, r in fam.iterrows():
        tt.append(('PEAD', f"{r['cell']}_h{int(r['horizon'])} ({r['desc']}), entry open R+1, tier costs", 'TEST 2015-01..2026-08', int(r['n']), round(r['mean'], 3),
                   f"registered family cell; net excess over SPY %, CI[{r['lo']:.2f},{r['hi']:.2f}] {r['ci_cluster']}-cluster; ctrl excess {r.get('ce_mean', np.nan):.2f} [{r.get('ce_lo', np.nan):.2f},{r.get('ce_hi', np.nan):.2f}]; years_pos {r['years_pos']:.2f}; Holm p {r['p_holm']:.3f}; PASS={bool(r['PASS'])}"))
    for _, r in cells[~cells['in_family'] & cells['n'].ge(30)].iterrows():
        tt.append(('PEAD', f"{r['cell']}_h{int(r['horizon'])} entry {r['entry']}", f"{r['window']}", int(r['n']), round(r['mean'], 3) if not np.isnan(r.get('mean', np.nan)) else '',
                   f"descriptive variant, not in family; CI[{r.get('lo', np.nan):.2f},{r.get('hi', np.nan):.2f}]; ctrl excess {r.get('ce_mean', np.nan):.2f}"))
    for cname in ['C1', 'C2']:
        p = pc[cname]
        tt.append(('PEAD', f"per-capital K=10 {cname}_h20 1/K weights", 'TEST', p['taken'], round(p['cagr'] * 100, 2), f"CAGR %; maxDD {p['maxdd']*100:.1f}%; SPY CAGR {p['spy_cagr']*100:.2f}% maxDD {p['spy_maxdd']*100:.1f}%; skipped {p['skipped']}; avg exposure {p['avg_exposure']:.2f}"))
    ttdf = pd.DataFrame(tt, columns=['who', 'variant', 'window', 'n', 'avgR_net', 'note'])
    ttdf['note'] = ttdf['note'].str.replace('\n', ' ')
    if VARIANT != 'registered':
        ttdf = ttdf.iloc[:len(fam)].copy(); ttdf['variant'] = 'CTRL-MATCH SENSITIVITY (' + VARIANT + '): ' + ttdf['variant']; ttdf['note'] = 'control-matching sensitivity, not registered, pass flags void; ' + ttdf['note']
    ttdf.to_csv(RES + "/things_tried.csv", mode='a', header=False, index=False)
    log("things_tried appended", len(ttdf))

    # ---- report
    L = []
    L.append("# PEAD: post-earnings-announcement drift from SEC 8-K Item 2.02 filings (KEY PEAD)\n")
    L.append(f"Registered: `preregistration_pead.json` (sha256 in `PEAD_prereg_sha256.txt`). Single run, {datetime.now():%Y-%m-%d %H:%M}. Units: percent net excess over SPY per trade after tier costs; CI = 95% wider-of-day/quarter cluster bootstrap ({REPS} reps).\n")
    n_t = int((ev['window'] == 'TEST').sum()); n_d = int((ev['window'] == 'DISC').sum())
    L.append(f"Events: {len(ev):,} eligible earnings reactions ({n_d:,} discovery 2005-2014, {n_t:,} TEST 2015-01..2026-08), {ev['ticker'].nunique():,} tickers. Timing mix (TEST): " +
             ", ".join(f"{k} {v:.0%}" for k, v in tst['timing'].value_counts(normalize=True).items()) + f". Events with >=1 matched control: {(ev['n_ctrl']>0).mean():.0%}.\n")
    L.append("## 1. Registered family (TEST window, entry Open_{R+1})\n")
    show = fam[['cell', 'horizon', 'direction', 'n', 'mean', 'lo', 'hi', 'ci_cluster', 'ce_mean', 'ce_lo', 'ce_hi', 'ctrl_share', 'years_pos', 'years_n', 'p', 'p_holm', 'PASS']].copy()
    show.columns = ['cell', 'h', 'dir', 'n', 'net excess %', 'lo', 'hi', 'cluster', 'ctrl excess %', 'ce lo', 'ce hi', 'ctrl cov', 'years+', 'yrs', 'p', 'Holm p', 'PASS']
    L.append(md_table(show, "{:.3f}"))
    L.append("\nPass requires: lower CI > 0, matched-control excess > 0 with lower CI > 0, >= 60% of years positive, n >= 500, Holm p < 0.05. C3 is evaluated as a short (values are short-side).\n")
    L.append("## 2. All horizons, both entries, both windows\n")
    show2 = cells[['cell', 'window', 'horizon', 'entry', 'direction', 'n', 'mean', 'lo', 'hi', 'ce_mean', 'ce_lo', 'ce_hi', 'years_pos']].copy()
    show2.columns = ['cell', 'window', 'h', 'entry', 'dir', 'n', 'net excess %', 'lo', 'hi', 'ctrl excess %', 'ce lo', 'ce hi', 'years+']
    L.append(md_table(show2, "{:.2f}"))
    L.append("\n## 3. News or move? (C1, TEST)\n")
    for h in [5, 20]:
        m = nom[h]
        L.append(f"- h={h}: C1 event net excess {m['event_all']:+.2f}% (all), {m['event_on_ctrl_subset']:+.2f}% on the {m['n']:,} events with controls; matched non-event names with the same day-0 move, ADV decile and date: {m['control']:+.2f}%; difference (the part attributable to the earnings news rather than the move) {m['diff']:+.2f}%; share explained by the move alone: {m['share_explained_by_move']:.0%}." if not np.isnan(m['share_explained_by_move']) else
                 f"- h={h}: event {m['event_on_ctrl_subset']:+.2f}%, controls {m['control']:+.2f}%, difference {m['diff']:+.2f}% (event mean not positive, share undefined).")
    L.append("\n## 4. Yearly (net excess %, entry open R+1; C3 short-side)\n")
    L.append(md_table(yearly, "{:.2f}"))
    L.append("\n## 5. By timing (TEST)\n")
    for (cname, h), t in timing_tabs.items():
        L.append(f"\n**{cname} h={h}**\n\n" + md_table(t, "{:.2f}"))
    L.append("\n## 6. By size tier (C1, TEST)\n")
    for h, t in size_tabs.items():
        L.append(f"\n**h={h}**\n\n" + md_table(t, "{:.2f}"))
    L.append("\n## 7. Regime: 2020-2024 vs other TEST years\n")
    L.append(md_table(regime, "{:.2f}"))
    L.append("\n## 8. Per-capital, K=10 slots, 1/K fixed weights, hold 20 sessions, TEST\n")
    for cname, p in pc.items():
        L.append(f"- {cname}_h20: {p['taken']:,} trades taken, {p['skipped']:,} skipped (slots full); final equity x{p['final']:.2f}, CAGR {p['cagr']*100:+.2f}%, ann. vol {p['ann_vol']*100:.1f}%, max drawdown {p['maxdd']*100:.1f}%, average exposure {p['avg_exposure']:.0%}. SPY buy-and-hold same span: x{p['spy_final']:.2f}, CAGR {p['spy_cagr']*100:+.2f}%, max drawdown {p['spy_maxdd']*100:.1f}%. Yearly %: {p['yearly']}")
    L.append("\n## 9. Caveats\n")
    L.append("- Survivorship: symbol list as of 2026-09; delisted names are absent, which flatters long cells and understates the short cell.\n- The month-quintile threshold uses events later in the same month (mild look-ahead); the trailing-threshold variant C1_trail is the look-ahead-free check.\n- SEC 8-K 2.02 coverage before ~2008 is incomplete; the 2004-2013 panel has 1,225 names.\n- Controls are matched within 1 pp of day-0 excess; extreme reactions have fewer controls (coverage per cell is reported).\n- No borrow fees or hard-to-borrow constraints in C3.\n")
    open(PFX + "report.md", "w", encoding='utf-8').write("\n".join(L))
    summary = {'family': fam.to_dict(orient='records'), 'nom': nom, 'per_capital': pc,
               'timing': {f'{k[0]}_h{k[1]}': v.to_dict(orient='records') for k, v in timing_tabs.items()},
               'size': {f'h{k}': v.to_dict(orient='records') for k, v in size_tabs.items()}, 'regime': regime.to_dict(orient='records'),
               'yearly': yearly.to_dict(orient='records'), 'n_events': {'all': len(ev), 'TEST': n_t, 'DISC': n_d}, 'things_tried_rows': len(ttdf)}
    json.dump(summary, open(PFX + "summary.json", "w"), indent=1, default=lambda o: None if (isinstance(o, float) and np.isnan(o)) else (o.item() if hasattr(o, 'item') else str(o)))
    log("report written")
    print(md_table(show, "{:.3f}"))
    for h in [5, 20]:
        print("NOM", h, nom[h])
    for cname, p in pc.items():
        print("PC", cname, {k: v for k, v in p.items() if k != 'yearly'})

if __name__ == "__main__":
    if os.path.exists(CACHE) and '--fresh' not in sys.argv:
        d = pd.read_pickle(CACHE); Cn = np.load(PFX + "cache_Cn.npy"); On = np.load(PFX + "cache_On.npy")
        ev, cal, spyC, spyO = d['ev'], d['cal'], d['spyC'], d['spyO']
        log("stage 1 loaded from cache", ev.shape)
    else:
        ev, cal, spyC, spyO, Cn, On = stage1()
    stage2(ev, cal, spyC, spyO, Cn, On)
    log("done")
