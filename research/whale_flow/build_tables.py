"""Build the two raw event tables for the v6 Whale Flow study. No returns are computed here.

  insider   SEC Form 4 non-derivative transactions (codes P, S, M, G) joined to filing,
            issuer and reporting-owner fields -> insider_trans.pkl
            Ticker comes from the issuer CIK (SEC company_tickers.json, 2026 symbols),
            never from the symbol typed on the form (symbols get reused).
  darkflow  FINRA Reg SHO daily short-sale volume summed over reporting facilities,
            restricted to bar-universe symbols -> darkflow.pkl

  python research/whale_flow/build_tables.py [insider|darkflow|all]
"""
import gzip
import io
import json
import os
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests

DATA = Path("C:/dev/Trader-v3-data")
WF = DATA / "whale_flow"
SEC_UA = {"User-Agent": "Raveesh Singh raveeshsingh@gmail.com"}
CODES = {"P", "S", "M", "G"}


def universe():
    u = set()
    for d in ("bars1d_10y", "bars1d_2004_2013", "bars1d_all"):
        u |= {f[:-4] for f in os.listdir(DATA / d) if f.endswith(".csv")}
    return u


def cik_map(uni):
    """issuer CIK (int) -> universe ticker. Share classes: first listed (SEC primary)."""
    path = WF / "company_tickers.json"
    if not path.exists():
        r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=SEC_UA, timeout=60)
        r.raise_for_status()
        path.write_bytes(r.content)
    m = {}
    for row in json.loads(path.read_text()).values():
        t = row["ticker"].upper()
        if t in uni:
            m.setdefault(int(row["cik_str"]), t)
    return m


def _tsv(z, name, cols):
    with z.open(name) as f:
        return pd.read_csv(f, sep="\t", dtype=str, usecols=cols, on_bad_lines="skip",
                           quoting=3, encoding="latin-1")


def build_insider():
    """All issuers are kept (delisted ones too); `ticker` is the 2026 symbol where the issuer
    CIK still lists, else NaN - delisted issuers are matched later by CIK / symbol-on-form."""
    cmap = cik_map(universe())
    frames, qa = [], []
    for zp in sorted((WF / "insider_zips").glob("*_form345.zip")):
        z = zipfile.ZipFile(zp)
        sub = _tsv(z, "SUBMISSION.tsv", ["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE",
                                          "ISSUERCIK", "ISSUERTRADINGSYMBOL"])
        tr = _tsv(z, "NONDERIV_TRANS.tsv", ["ACCESSION_NUMBER", "NONDERIV_TRANS_SK",
                                             "TRANS_DATE", "TRANS_CODE", "TRANS_SHARES",
                                             "TRANS_PRICEPERSHARE", "TRANS_ACQUIRED_DISP_CD",
                                             "SHRS_OWND_FOLWNG_TRANS", "DIRECT_INDIRECT_OWNERSHIP"])
        own = _tsv(z, "REPORTINGOWNER.tsv", ["ACCESSION_NUMBER", "RPTOWNERCIK",
                                              "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"])
        n_all = len(tr)
        tr = tr[tr.TRANS_CODE.isin(CODES)]
        sub = sub[sub.DOCUMENT_TYPE.isin(["4", "4/A"])].copy()
        sub["cik"] = pd.to_numeric(sub.ISSUERCIK, errors="coerce")
        sub["ticker"] = sub.cik.map(cmap)
        # one row per filing for owners: joint filers collapsed, relationship flags OR-ed
        own = own[own.ACCESSION_NUMBER.isin(set(tr.ACCESSION_NUMBER))].copy()
        own["rel"] = own.RPTOWNER_RELATIONSHIP.fillna("")
        own["RPTOWNER_TITLE"] = own.RPTOWNER_TITLE.fillna("")
        og = own.groupby("ACCESSION_NUMBER").agg(
            owner_cik=("RPTOWNERCIK", "first"), n_owners=("RPTOWNERCIK", "nunique"),
            rel=("rel", ",".join), title=("RPTOWNER_TITLE", " | ".join))
        df = tr.merge(sub, on="ACCESSION_NUMBER").merge(og, left_on="ACCESSION_NUMBER",
                                                         right_index=True, how="left")
        for c in ("FILING_DATE", "TRANS_DATE"):
            df[c] = pd.to_datetime(df[c], format="%d-%b-%Y", errors="coerce")
        for c in ("TRANS_SHARES", "TRANS_PRICEPERSHARE", "SHRS_OWND_FOLWNG_TRANS"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        frames.append(df.drop(columns=["ISSUERCIK"]))
        isp = df.TRANS_CODE == "P"
        qa.append((zp.name[:6], n_all, int(isp.sum()), int((isp & df.ticker.notna()).sum())))
        print(qa[-1], flush=True)
    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={
        "ACCESSION_NUMBER": "accession", "NONDERIV_TRANS_SK": "trans_sk",
        "FILING_DATE": "filing_date", "TRANS_DATE": "trans_date", "TRANS_CODE": "code",
        "TRANS_SHARES": "shares", "TRANS_PRICEPERSHARE": "price", "TRANS_ACQUIRED_DISP_CD": "acq_disp",
        "SHRS_OWND_FOLWNG_TRANS": "shares_after", "DIRECT_INDIRECT_OWNERSHIP": "direct",
        "DOCUMENT_TYPE": "form", "ISSUERTRADINGSYMBOL": "symbol_on_form"})
    df = df.drop_duplicates(["accession", "trans_sk"])
    df["rel"] = df.rel.fillna("")
    df["is_officer"] = df.rel.str.contains("Officer")
    df["is_director"] = df.rel.str.contains("Director")
    df["is_tenpct"] = df.rel.str.contains("TenPercentOwner")
    for c in ("code", "acq_disp", "direct", "form", "rel"):
        df[c] = df[c].astype("category")
    df.to_pickle(WF / "insider_trans.pkl")
    pd.DataFrame(qa, columns=["quarter", "trans_rows", "p_rows", "p_rows_2026_ticker"]).to_csv(
        WF / "insider_build_qa.csv", index=False)
    print(f"insider_trans.pkl rows {len(df):,}  by code {df.code.value_counts().to_dict()}", flush=True)


def build_darkflow():
    uni = universe()
    files = sorted((WF / "finra_shvol").glob("*.txt.gz"))
    parts = []
    for i, fp in enumerate(files, 1):
        with gzip.open(fp, "rb") as f:
            raw = f.read()
        try:
            # columns by name: files before 2011 have no ShortExemptVolume column
            d = pd.read_csv(io.BytesIO(raw), sep="|", dtype={"Symbol": str}, on_bad_lines="skip")
            d = d.rename(columns={"Date": "date", "Symbol": "symbol", "ShortVolume": "short",
                                  "ShortExemptVolume": "exempt", "TotalVolume": "total"})
            if "exempt" not in d.columns:
                d["exempt"] = 0.0
            d = d[["date", "symbol", "short", "exempt", "total"]]
        except Exception as e:
            print(f"bad file {fp.name}: {e}", flush=True)
            continue
        d = d[d.symbol.isin(uni)]
        d = d[pd.to_numeric(d.date, errors="coerce").notna()]  # drops the trailer row
        d["fac"] = fp.name[:4]
        parts.append(d)
        if i % 1000 == 0:
            print(f"{i}/{len(files)}", flush=True)
    df = pd.concat(parts, ignore_index=True)
    df["date"] = pd.to_datetime(df.date.astype(int).astype(str), format="%Y%m%d")
    for c in ("short", "exempt", "total"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    nfac = df.groupby(["date", "symbol"]).fac.nunique().rename("n_fac")
    out = df.groupby(["date", "symbol"])[["short", "exempt", "total"]].sum().join(nfac).reset_index()
    out.to_pickle(WF / "darkflow.pkl")
    print(f"darkflow.pkl rows {len(out):,}  symbols {out.symbol.nunique():,}  "
          f"{out.date.min().date()} .. {out.date.max().date()}", flush=True)


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    if what in ("insider", "all"):
        build_insider()
    if what in ("darkflow", "all"):
        build_darkflow()
