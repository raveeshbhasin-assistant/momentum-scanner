"""Referral-Moat research universe.

Curated top listed companies per industry group. Groups are chosen so that
referral / word-of-mouth economics are *plausible and measurable* — plus two
deliberate low-referral control groups (Energy, Legacy Telecom & Cable) so the
theory can be falsified, not just confirmed.

Selection rule: largest / most prominent liquid US-listed names per group,
picked by industry prominence — NEVER by past stock performance (see
README.md: returns are an output, never an input).
"""

UNIVERSE = {
    "Enterprise Software & SaaS": [
        "CRM", "NOW", "ADBE", "INTU", "WDAY", "HUBS", "TEAM", "DDOG",
        "SNOW", "ZS", "CRWD", "PANW", "MNDY", "VEEV", "TTD", "PLTR",
    ],
    "Consumer Internet & Platforms": [
        "GOOGL", "META", "NFLX", "SPOT", "UBER", "DASH", "PINS", "SNAP",
        "RDDT", "DUOL", "ROKU", "ZG",
    ],
    "Fintech & Payments": [
        "V", "MA", "PYPL", "XYZ", "COIN", "HOOD", "AXP", "SOFI",
        "NU", "TOST", "AFRM", "GPN",
    ],
    "Consumer Brands": [
        "NKE", "LULU", "DECK", "ELF", "CELH", "MNST", "CROX", "SKX",
        "PG", "KO", "PEP", "YETI",
    ],
    "Restaurants": [
        "CMG", "SBUX", "MCD", "YUM", "WING", "CAVA", "TXRH", "DPZ",
    ],
    "Healthcare & Medtech": [
        "ISRG", "DXCM", "PODD", "ALGN", "HIMS", "IDXX", "TDOC", "EW",
        "SYK", "RMD",
    ],
    "Financial Services & Insurance": [
        "PGR", "ALL", "SCHW", "IBKR", "KNSL", "LMND", "TRV", "AJG",
    ],
    "Travel & Hospitality": [
        "ABNB", "BKNG", "EXPE", "MAR", "HLT", "RCL", "CCL", "DAL",
    ],
    "Retail": [
        "COST", "AMZN", "WMT", "TGT", "HD", "ULTA", "TJX", "ORLY",
        "AZO", "FIVE",
    ],
    "Consumer Tech & Autos": [
        "AAPL", "TSLA", "GRMN", "SONY", "GPRO", "RIVN",
    ],
    # ── Control groups: commodity / contract businesses where customer
    #    referrals should NOT drive acquisition. If the theory is real,
    #    the score→return link should be weak or absent here.
    "Control: Energy": [
        "XOM", "CVX", "COP", "SLB", "OXY", "DVN",
    ],
    "Control: Telecom & Cable": [
        "T", "VZ", "TMUS", "CMCSA", "CHTR",
    ],
}

ALL_TICKERS = [t for group in UNIVERSE.values() for t in group]
BENCHMARK = "SPY"
