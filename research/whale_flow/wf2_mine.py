"""WF2 mining on the training period only (entries 2006-02-01 .. 2019-09-30). Reads *_train files,
never the holdout. Writes the frozen strategies to wf2/frozen/.

  python research/whale_flow/wf2_mine.py
"""
import itertools
import json
import pickle

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

import wf1_run as w
from wf2_common import FROZEN, OUT, DayBase, feature_cols, one_position, summarise, wilson_lo
from wf_common import WF

FOLDS = [("2011-09-30", "2012-01-01", "2013-12-31"), ("2013-09-30", "2014-01-01", "2015-12-31"),
         ("2015-09-30", "2016-01-01", "2017-12-31"), ("2017-09-30", "2018-01-01", "2019-09-30")]
OOF_YEARS = 7.75
RULE_DISC_END, RULE_VAL_START = "2015-09-30", "2016-01-01"
SEED = 20261004
log = []


def say(*a):
    msg = " ".join(str(x) for x in a)
    print(msg, flush=True)
    log.append(msg)


def gbm(**kw):
    return HistGradientBoostingClassifier(learning_rate=0.05, l2_regularization=1.0, random_state=SEED, **kw)


def oof_scores(df, feats, ycol, params):
    """Walk-forward out-of-fold probabilities (NaN outside the scored windows)."""
    X, y, e = df[feats].to_numpy("float32"), df[ycol].values, df.entry.values
    out = np.full(len(df), np.nan)
    for fit_end, s0, s1 in FOLDS:
        tr = e <= np.datetime64(fit_end)
        te = (e >= np.datetime64(s0)) & (e <= np.datetime64(s1))
        out[te] = gbm(**params).fit(X[tr], y[tr]).predict_proba(X[te])[:, 1]
    return out


def pick_cut(df, score, h, min_per_year, quantiles):
    """Largest out-of-fold precision lift over the same-day base rate that still gives enough
    signals; ties to the looser cut."""
    ok = np.isfinite(score)
    rows = []
    for q in quantiles:
        cut = float(np.quantile(score[ok], q))
        f = one_position(df, h, ok & (score >= cut))
        folds_pos = 0
        for _, s0, s1 in FOLDS:
            m = (f.entry >= s0) & (f.entry <= s1)
            a = (df.entry >= s0) & (df.entry <= s1)
            folds_pos += int(m.sum() >= 10 and f[m][f"y{h}"].mean() > f[m][f"base_win{h}"].mean())
        rows.append({"q": q, "cut": cut, "n": len(f), "per_year": len(f) / OOF_YEARS,
                     "precision": f[f"y{h}"].mean() if len(f) else np.nan,
                     "base": f[f"base_win{h}"].mean() if len(f) else np.nan,
                     "lift": (f[f"y{h}"] - f[f"base_win{h}"]).mean() if len(f) else np.nan,
                     "mean_exc": f[f"exc{h}"].mean() if len(f) else np.nan, "folds_above_base": folds_pos})
    t = pd.DataFrame(rows)
    good = t[t.per_year >= min_per_year]
    best = (good if len(good) else t).sort_values(["lift", "q"], ascending=[False, True]).iloc[0]
    return t, best


def mine_model(df, name, horizons, grid, min_per_year, quantiles):
    feats = feature_cols(df)
    best = None
    for h in horizons:
        base = df[df.entry >= FOLDS[0][1]][f"y{h}"].mean()
        for params in grid:
            sc = oof_scores(df, feats, f"y{h}", params)
            ok = np.isfinite(sc)
            auc = roc_auc_score(df[f"y{h}"].values[ok], sc[ok])
            t, b = pick_cut(df, sc, h, min_per_year, quantiles)
            say(f"{name} h={h} {params} OOF AUC {auc:.4f} | best cut q={b.q} "
                f"precision {b.precision:.3f} same-day base {b.base:.3f} lift {b.lift:+.3f} "
                f"n/yr {b.per_year:.0f} mean exc {b.mean_exc:+.4f} "
                f"folds>base {int(b.folds_above_base)}/4")
            cand = {"h": h, "params": params, "auc": auc, "lift": b.lift, "cut": b.cut, "q": b.q,
                    "table": t, "oof": sc}
            # horizon and hyper-parameters by out-of-fold AUC (stable); the cut by precision
            if best is None or auc > best["auc"]:
                best = cand
    h = best["h"]
    model = gbm(**best["params"]).fit(df[feats].to_numpy("float32"), df[f"y{h}"].values)
    say(f"{name} chosen: h={h} {best['params']} cut {best['cut']:.4f} (OOF quantile {best['q']})")
    say(best["table"].round(4).to_string(index=False))
    return {"name": name, "h": h, "features": feats, "cut": best["cut"], "params": best["params"],
            "model": model}, best


def mine_rule(df):
    """Readable rule of <= 3 conditions: search on entries <= 2015-09, choose on 2016-01 .. 2019-09."""
    feats = feature_cols(df) + ["ib_lag"]
    feats = list(dict.fromkeys(feats))
    disc, val = df[df.entry <= RULE_DISC_END], df[df.entry >= RULE_VAL_START]
    conds = []
    for f in feats:
        x = disc[f].to_numpy(float)
        u = np.unique(x[np.isfinite(x)])
        if len(u) <= 2:
            thr = [(">=", 0.5)] if len(u) == 2 else []
            thr += [("<=", 0.5)] if len(u) == 2 else []
        else:
            qs = np.unique(np.nanquantile(x, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]))
            thr = [(op, float(t)) for t in qs for op in (">=", "<=")]
        conds += [(f, op, t) for op, t in thr]
    say(f"S1 rule search: {len(conds)} single conditions, discovery n {len(disc)}, validation n {len(val)}")

    def mask(d, rule):
        m = np.ones(len(d), bool)
        for f, op, t in rule:
            x = d[f].to_numpy(float)
            m &= (x >= t) if op == ">=" else (x <= t)
        return m

    best_rule = None
    for h in (21, 63):
        yd = disc[f"y{h}"].values
        cm = {c: mask(disc, [c]) for c in conds}
        min_sup = int(25 * 9.65)  # 25 signals a year over the discovery years
        beam, seen, pool = [()], set(), []
        for depth in range(3):
            nxt = []
            for rule in beam:
                base_m = np.ones(len(disc), bool)
                for c in rule:
                    base_m &= cm[c]
                used = {c[0] for c in rule}
                for c in conds:
                    if c[0] in used:
                        continue
                    key = tuple(sorted(rule + (c,)))
                    if key in seen:
                        continue
                    seen.add(key)
                    m = base_m & cm[c]
                    n = int(m.sum())
                    if n < min_sup:
                        continue
                    nxt.append((wilson_lo(int(yd[m].sum()), n), key, n, yd[m].mean()))
            nxt.sort(reverse=True)
            beam = [r[1] for r in nxt[:40]]
            pool += nxt[:40]
        pool.sort(reverse=True)
        vb = val[f"y{h}"].mean()
        tested = 0
        for lo, rule, n, prec in pool[:60]:
            f = one_position(val, h, mask(val, rule))
            tested += 1
            if len(f) < 25 * 3.75:
                continue
            vp, vexc = f[f"y{h}"].mean(), f[f"exc{h}"].mean()
            if vexc <= 0:
                continue
            cand = {"h": h, "rule": [list(c) for c in rule], "disc_n": n, "disc_precision": float(prec),
                    "disc_base": float(yd.mean()), "val_n": len(f), "val_precision": float(vp),
                    "val_base": float(vb), "val_lift": float(vp - vb), "val_mean_exc": float(vexc)}
            if best_rule is None or cand["val_lift"] > best_rule["val_lift"]:
                best_rule = cand
        say(f"S1 h={h}: {len(seen)} rules searched on discovery, top 60 scored on validation "
            f"(discovery base {yd.mean():.3f}, validation base {vb:.3f})")
    say("S1 chosen:", json.dumps(best_rule))
    return best_rule


def main():
    FROZEN.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)
    cand = pd.read_pickle(OUT / "cand_train.pkl").sort_values("entry").reset_index(drop=True)
    say(f"insider candidates (train): {len(cand):,}; beat-SPY rate 21d {cand.y21.mean():.3f}, 63d {cand.y63.mean():.3f}")
    say(cand.groupby(cand.entry.dt.year).agg(n=("y21", "size"), y21=("y21", "mean"), y63=("y63", "mean"),
                                             exc21=("exc21", "mean"), exc63=("exc63", "mean")).round(3).to_string())

    # ---- S1
    rule = mine_rule(cand)

    # ---- S2
    grid = [dict(max_depth=d, max_iter=it, min_samples_leaf=leaf)
            for d, it, leaf in itertools.product((2, 3), (150, 300), (50, 150))]
    s2, b2 = mine_model(cand, "S2", (21, 63), grid, 25, (0.5, 0.7, 0.8, 0.9, 0.95))

    # ---- S4
    week = pd.read_pickle(OUT / "week_train.pkl").sort_values("entry").reset_index(drop=True)
    say(f"weekly panel (train): {len(week):,} rows; beat-SPY rate 21d {week.y21.mean():.3f}")
    grid4 = [dict(max_depth=3, max_iter=200, min_samples_leaf=500), dict(max_depth=5, max_iter=200, min_samples_leaf=500)]
    s4, b4 = mine_model(week, "S4", (21,), grid4, 100, (0.9, 0.95, 0.98, 0.99, 0.995))

    # ---- train-side report with the random-stock base rate (out-of-fold for the models)
    panel = w.Panel()
    db = DayBase(panel)
    rep = []
    h = rule["h"]
    m = np.ones(len(cand), bool)
    for f, op, t in rule["rule"]:
        x = cand[f].to_numpy(float)
        m &= (x >= t) if op == ">=" else (x <= t)
    val = (cand.entry >= RULE_VAL_START).values
    rep.append({"strategy": "S1 rule (validation 2016-19)", "h": h,
                **summarise(db.attach(one_position(cand, h, m & val), h), h, rng, 3.75)})
    rep.append({"strategy": "S1 rule (search years 2006-15, in-sample)", "h": h,
                **summarise(db.attach(one_position(cand, h, m & ~val), h), h, rng, 9.65)})
    ok = np.isfinite(b2["oof"])
    rep.append({"strategy": "S2 model (out-of-fold 2012-19)", "h": s2["h"],
                **summarise(db.attach(one_position(cand, s2["h"], ok & (b2["oof"] >= s2["cut"])), s2["h"]),
                            s2["h"], rng, OOF_YEARS)})
    ok4 = np.isfinite(b4["oof"])
    rep.append({"strategy": "S4 broad model (out-of-fold 2012-19)", "h": 21,
                **summarise(db.attach(one_position(week, 21, ok4 & (b4["oof"] >= s4["cut"])), 21), 21, rng, OOF_YEARS)})
    # S3: WF1 cluster + conviction events, 21 sessions, no fitting
    f = pd.read_pickle(WF / "insider_purchases.pkl")
    u = w.make_events(f, panel.cal)["U"]
    u = u[(u.entry_date >= "2006-02-01") & (u.entry_date <= "2019-09-30")]
    blocked = np.zeros(panel.elig.shape, bool)
    r3 = w.evaluate(panel, u, blocked, np.random.default_rng(w.SEED))
    r3 = r3[r3.status == "tradeable"]
    rep.append({"strategy": "S3 simple (all train years, nothing fitted)", "h": 21,
                **summarise(db.attach(one_position(r3.reset_index(drop=True), 21, np.ones(len(r3), bool)), 21),
                            21, rng, 13.65)})
    rep = pd.DataFrame(rep)
    rep.to_csv(FROZEN / "train_report.csv", index=False)
    with pd.option_context("display.width", 250, "display.max_columns", 40, "display.float_format", "{:.4f}".format):
        say(rep[["strategy", "h", "n", "per_year", "precision", "base_rate", "lift", "lift_lo", "p", "mean_exc",
                 "exc_lo", "years_lift_pos", "share_worse_m10"]].to_string(index=False))

    # feature importance for the two models (permutation-free: split gain is not exposed, so use
    # the drop in out-of-fold AUC is skipped; report top features by univariate OOF separation instead)
    for nm, spec, df, oof in (("S2", s2, cand, b2["oof"]), ("S4", s4, week, b4["oof"])):
        ok_ = np.isfinite(oof)
        top = oof >= spec["cut"]
        d = df[ok_]
        diff = {f_: float(d[f_][top[ok_]].median() - d[f_].median()) / (float(d[f_].std()) or 1.0) for f_ in spec["features"]}
        lead = sorted(diff.items(), key=lambda kv: -abs(kv[1]))[:12]
        say(f"{nm}: how the bought slice differs from all candidates (median shift in std units):",
            ", ".join(f"{k} {v:+.2f}" for k, v in lead))

    # ---- freeze
    json.dump(rule, open(FROZEN / "s1_rule.json", "w"), indent=1)
    for nm, spec in (("s2", s2), ("s4", s4)):
        pickle.dump(spec["model"], open(FROZEN / f"{nm}_model.pkl", "wb"))
        json.dump({k: v for k, v in spec.items() if k != "model"}, open(FROZEN / f"{nm}_spec.json", "w"), indent=1)
    json.dump({"h": 21, "events": "wf1_run.make_events U", "fitted": False}, open(FROZEN / "s3_spec.json", "w"), indent=1)
    (FROZEN / "mine_log.txt").write_text("\n".join(log), encoding="utf-8")
    say("frozen to", FROZEN)


if __name__ == "__main__":
    main()
