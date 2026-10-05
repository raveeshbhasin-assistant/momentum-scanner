"""Filing-level insider purchase table for the v6 Whale Flow study. No returns are computed.

Input : insider_trans.pkl, bars/ (2026-listed), bars_delisted/ + delisted_meta.jsonl
Output: insider_purchases.pkl   one row per original Form 4 with a purchase, for EVERY issuer,
                                including issuers with no price series (has_series False) so the
                                engine can count what it cannot trade
        issuer_match.csv        per issuer series: full-history agreement of Form 4 prices with bars (QA only)

Series key: the 2026 ticker where the issuer CIK still lists, else `{cik}_{SYMBOL}` (FMP history
for the symbol typed on the form). Symbols get reused, so a series is trusted through the
point-in-time fields `pit_n` / `pit_rate`: the share of the issuer's Form 4 purchase and sale
prices, on forms filed on or before this filing's date, that lie within PX_TOL of the day's
as-traded low-high range.

  python research/whale_flow/build_events.py
"""
import numpy as np
import pandas as pd

from wf_common import WF, SYM, load_delisted, load_survivor, norm_symbol, next_session, trading_calendar

PX_TOL = 0.05  # a trade price may sit up to 5% outside the day's as-traded low-high range


def main():
    t = pd.read_pickle(WF / "insider_trans.pkl")
    t = t[t.cik.notna() & (t.price > 0) & (t.shares > 0) & t.trans_date.notna() & t.code.isin(["P", "S"])].copy()
    t["cik"] = t.cik.astype(int)
    t["sym"] = norm_symbol(t.symbol_on_form)
    t["delisted"] = t.ticker.isna()
    good = t.sym.map(lambda s: bool(SYM.match(s)))
    t["key"] = np.where(~t.delisted, t.ticker.astype(object),
                        np.where(good, t.cik.astype(str) + "_" + t.sym, t.cik.astype(str) + "_?"))

    # routine flag: the owner bought in the same calendar month in each of the three prior years,
    # on forms already filed before this filing (keyed to the earliest filing date per owner-month)
    pa = t[(t.code == "P") & (t.acq_disp == "A")]
    hist = (pa.assign(y=pa.trans_date.dt.year, m=pa.trans_date.dt.month)
            .groupby(["owner_cik", "y", "m"]).filing_date.min().to_dict())

    cal = trading_calendar()
    rows, match = [], []
    for n, (key, g) in enumerate(t.groupby("key", sort=False), 1):
        is_p = (g.code == "P") & (g.acq_disp == "A") & (g.form == "4")
        if not is_p.any():
            continue
        delisted = bool(g.delisted.iloc[0])
        bars = load_delisted(key) if delisted else load_survivor(key)
        if bars is None:
            g = g.assign(raw_low=np.nan, raw_high=np.nan, raw_volume=np.nan, px_ok=False, has_bar=False)
            pit_at = lambda d: (0, np.nan)
        else:
            g = g.join(bars[["raw_low", "raw_high", "raw_volume"]], on="trans_date")
            has = g.raw_low.notna()
            ok = has & (g.price >= g.raw_low * (1 - PX_TOL)) & (g.price <= g.raw_high * (1 + PX_TOL))
            g = g.assign(px_ok=ok, has_bar=has)
            match.append((key, delisted, len(bars), int(has.sum()), ok[has].mean() if has.any() else np.nan))
            c = g[has].groupby("filing_date").px_ok.agg(["sum", "count"]).sort_index().cumsum()
            cd, cs, cn = c.index.values, c["sum"].values, c["count"].values

            def pit_at(d, cd=cd, cs=cs, cn=cn):
                i = np.searchsorted(cd, np.datetime64(d), side="right") - 1
                return (0, np.nan) if i < 0 else (int(cn[i]), cs[i] / cn[i])
        p = g[is_p.reindex(g.index)]
        p = p.assign(value=p.shares * p.price,
                     too_big=p.shares > 5 * p.raw_volume.fillna(np.inf),
                     ok_value=np.where(p.px_ok, p.shares * p.price, 0.0),
                     direct_shares=np.where(p.direct == "D", p.shares, 0.0))
        f = p.sort_values("trans_sk").groupby("accession").agg(
            cik=("cik", "first"), owner_cik=("owner_cik", "first"), n_owners=("n_owners", "first"),
            filing_date=("filing_date", "first"), first_trans=("trans_date", "min"),
            last_trans=("trans_date", "max"), shares=("shares", "sum"), value=("value", "sum"),
            ok_value=("ok_value", "sum"), n_trans=("value", "size"), any_too_big=("too_big", "any"),
            has_bar=("has_bar", "all"), shares_after=("shares_after", "last"),
            n_lines=("direct", "nunique"), direct_shares=("direct_shares", "sum"),
            is_officer=("is_officer", "first"), is_director=("is_director", "first"),
            is_tenpct=("is_tenpct", "first"), title=("title", "first")).reset_index()
        f["key"], f["delisted"], f["has_series"] = key, delisted, bars is not None
        pit = [pit_at(d) for d in f.filing_date]
        f["pit_n"], f["pit_rate"] = [x[0] for x in pit], [x[1] for x in pit]
        rows.append(f)
        if n % 2000 == 0:
            print(f"{n} issuer series", flush=True)

    f = pd.concat(rows, ignore_index=True)
    f["px_ok_share"] = f.ok_value / f.value
    f["form_px"] = f.value / f.shares
    f["lag_days"] = (f.filing_date - f.last_trans).dt.days
    f["entry_date"] = next_session(cal, f.filing_date)
    # holding increase: only where every purchase line on the form is the same ownership line,
    # so the closing balance and the shares bought refer to the same holding. Zero prior = new holder.
    prior = f.shares_after - f.shares
    f["pct_increase"] = np.where(f.n_lines != 1, np.nan,
                                 np.where(prior > 0, f.shares / prior.where(prior > 0), np.where(prior == 0, np.inf, np.nan)))
    y, m = f.last_trans.dt.year, f.last_trans.dt.month
    never = pd.Timestamp.max
    f["routine"] = [all(hist.get((o, yy - k, mm), never) < fd for k in (1, 2, 3))
                    for o, yy, mm, fd in zip(f.owner_cik, y, m, f.filing_date)]
    ttl = f.title.fillna("").str.lower()
    former = ttl.str.contains(r"former|emeritus|retired")
    f["is_ceo"] = ttl.str.contains(r"\bceo\b|chief executive") & ~former
    f["is_cfo"] = ttl.str.contains(r"\bcfo\b|chief financial") & ~former
    f["is_chair"] = ttl.str.contains(r"chair") & ~ttl.str.contains(r"vice|committee") & ~former
    f.to_pickle(WF / "insider_purchases.pkl")
    pd.DataFrame(match, columns=["key", "delisted", "n_bars", "n_trades_with_bar", "match_rate"]).to_csv(
        WF / "issuer_match.csv", index=False)
    print(f"insider_purchases.pkl filings {len(f):,}  survivor issuers {int((~f.delisted).sum()):,}  "
          f"delisted with series {int((f.delisted & f.has_series).sum()):,}  "
          f"no series {int((~f.has_series).sum()):,}", flush=True)


if __name__ == "__main__":
    main()
