"""Qualitative referral-evidence layer.

Hand-curated notes on *disclosed* referral / word-of-mouth acquisition
evidence from filings (10-K, S-1/F-1), shareholder letters and earnings
calls. This layer does NOT feed the RES score — it annotates it, and marks
where the quantitative fingerprint agrees or disagrees with what management
actually says about how customers arrive.

direction: "+" = disclosed evidence of referral-driven acquisition,
           "-" = disclosed evidence of paid/bought growth (counterexample),
           "~" = mixed / network-effect-but-not-referral.
"""

QUALITATIVE = {
    "TSLA": {"direction": "+", "note": (
        "Historically ~zero traditional advertising (10-K marketing spend "
        "negligible vs auto peers); ran an owner referral program for years. "
        "The canonical word-of-mouth carmaker.")},
    "COST": {"direction": "+", "note": (
        "Membership renewal rates ~90%+ (US/Canada), reported every "
        "earnings call; minimal advertising. Renewal + member-brings-friend "
        "dynamics are the business model.")},
    "DUOL": {"direction": "+", "note": (
        "Shareholder letters repeatedly attribute the large majority of new "
        "users to organic/word-of-mouth (S-1 cited ~80% organic); marketing "
        "spend low relative to user growth.")},
    "NU": {"direction": "+", "note": (
        "F-1 and calls: majority of customers acquired organically via "
        "word-of-mouth/member-get-member; CAC disclosed among the lowest in "
        "fintech.")},
    "ELF": {"direction": "+", "note": (
        "Community/social-driven brand; growth driven by viral social "
        "engagement rather than heavy trade spend — though marketing "
        "investment has been rising with scale.")},
    "CELH": {"direction": "+", "note": (
        "Grew largely on grassroots/fitness-community advocacy before the "
        "PepsiCo distribution deal scaled it.")},
    "IBKR": {"direction": "+", "note": (
        "Famously low marketing spend; management repeatedly credits "
        "word-of-mouth among sophisticated traders and introducing brokers.")},
    "PGR": {"direction": "~", "note": (
        "Direct model with heavy measured advertising — high ad spend but "
        "industry-leading efficiency; a paid-growth machine, not referral.")},
    "ABNB": {"direction": "+", "note": (
        "Post-2020 permanently cut performance marketing; management states "
        "~90% of traffic arrives direct/organic. Guests convert into hosts.")},
    "SPOT": {"direction": "+", "note": (
        "Freemium + sharing features as acquisition engine; S&M% of revenue "
        "structurally below media peers.")},
    "NFLX": {"direction": "~", "note": (
        "Word-of-mouth around hit content drives cycles, but paid marketing "
        "is substantial; ad tier adds a bought-growth channel.")},
    "WING": {"direction": "+", "note": (
        "Franchise ad fund is small vs sales; digital/social fan-driven "
        "demand repeatedly cited on calls for AUV growth.")},
    "CMG": {"direction": "+", "note": (
        "Historically minimal advertising vs QSR peers; throughput and "
        "reputation carry demand.")},
    "CAVA": {"direction": "+", "note": (
        "Management attributes new-market openings' fast ramps to brand "
        "buzz/word-of-mouth; ad spend modest vs fast-casual peers.")},
    "AAPL": {"direction": "+", "note": (
        "Perennial NPS leader; ecosystem lock-in plus advocacy. Note: also "
        "one of the world's larger advertisers — mixed mechanism.")},
    "V": {"direction": "~", "note": (
        "Network effects, not customer referrals — banks and merchants must "
        "join, consumers don't 'refer'. Good control for the distinction.")},
    "MA": {"direction": "~", "note": "Same network-effect (not referral) profile as Visa."},
    "PLTR": {"direction": "~", "note": (
        "Historically near-zero traditional sales/marketing (engineers "
        "forward-deployed); expansion is usage-led inside customers.")},
    "CRWD": {"direction": "+", "note": (
        "Module cross-sell + strong net retention (>119% for years); CISO "
        "peer referencing repeatedly cited on calls.")},
    "MNDY": {"direction": "-", "note": (
        "Openly performance-marketing-driven (large paid acquisition "
        "budget disclosed) — a deliberate counterexample to watch.")},
    "HIMS": {"direction": "-", "note": (
        "Marketing is the dominant opex line (~45-50% of revenue); growth "
        "is largely bought — watch whether intensity declines with scale.")},
    "SOFI": {"direction": "-", "note": (
        "Heavy brand + performance marketing (stadium naming rights); "
        "member growth is substantially paid.")},
    "HOOD": {"direction": "+", "note": (
        "Grew on referral program + viral waitlists; historically low CAC "
        "cited in S-1.")},
    "TOST": {"direction": "+", "note": (
        "Calls repeatedly cite restaurant-to-restaurant referrals and "
        "inbound as top-of-funnel; S&M efficiency improving with density.")},
    "ULTA": {"direction": "+", "note": (
        "Loyalty program (~95% of sales from members) is a retention/"
        "advocacy flywheel disclosed every call.")},
    "ORLY": {"direction": "+", "note": (
        "Professional (DIFM) side grows on counter-service reputation among "
        "mechanics — classic trade word-of-mouth.")},
    "KNSL": {"direction": "~", "note": (
        "Broker-submitted E&S flow; growth from underwriting reputation "
        "among brokers rather than end-customer referral.")},
    "TDOC": {"direction": "-", "note": (
        "B2B contracts + heavy DTC ad spend (BetterHelp); bought growth "
        "that deteriorated — useful negative case.")},
}
