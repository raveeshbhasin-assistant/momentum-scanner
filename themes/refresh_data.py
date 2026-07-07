"""
themes/refresh_data.py — Daily fundamentals + price refresh

Pulls fundamentals and price action for the AI Data Center candidate universe
(and any other active themes). Writes JSON (machine-readable, source of truth)
plus a regenerated Markdown snapshot for human review.

Design notes
────────────
- Source of truth is JSON. Markdown is a derived view.
- Sources: FMP for snapshot fundamentals (price, mcap, P/E, 52w), yfinance for
  daily history (1Y/3M/1M returns, RS rank, distance from 52w high).
- Idempotent — re-running overwrites the day's JSON and saves a dated snapshot
  to themes/<theme>/history/candidates_YYYY-MM-DD.json for historical comparison.
- Universe per theme is defined here at the top of the file (UNIVERSES dict).
  When supply_chain.md changes, update this dict and re-run.
- Run via: python themes/refresh_data.py [theme1 theme2 ...]
  Defaults to ACTIVE_THEMES below if no args.

Scheduling
──────────
For now, run manually as needed. The plan is to wire this into APScheduler
inside the future `themes_web/` FastAPI app (Stage 3 of the themes build) so
it runs nightly at 6 PM ET weekdays on Railway, where the FMP_API_KEY is set.
Until then, the JSON snapshot from the last manual run is the source of truth.

Hard rule: this module does NOT import from app.py. It imports from
scanner.py / fmp_data.py for shared data helpers (allowed direction).
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import httpx
import pandas as pd
import yfinance as yf

# Add parent dir to path so we can import fmp_data / config
_HERE = Path(__file__).parent
_PARENT = _HERE.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

import config  # noqa: E402
from fmp_data import fetch_batch_quotes  # noqa: E402

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Universe definitions
# ═══════════════════════════════════════════════════════════════

# Per-theme: ticker -> metadata. When supply_chain.md changes, mirror here.
UNIVERSES: dict[str, dict[str, dict[str, Any]]] = {
    "ai_data_center": {
        # Bucket 1: Optical Networking
        "AVGO": {"company": "Broadcom",          "bucket": "Optical Networking", "sub": "Switch Silicon",       "specificity": 5, "note": "Tomahawk merchant silicon — duopoly with MRVL"},
        "ANET": {"company": "Arista Networks",   "bucket": "Optical Networking", "sub": "Switches",             "specificity": 4, "note": "Hyperscaler customer moat; software differentiation"},
        "CLS":  {"company": "Celestica",         "bucket": "Optical Networking", "sub": "Switch Manufacturing", "specificity": 3, "note": "Contract manufacturer; HPS division picks up white-box share"},
        "AAOI": {"company": "Applied Optoelectronics", "bucket": "Optical Networking", "sub": "Transceivers", "specificity": 3, "note": "800G hyperscaler wins; share-gain thesis"},
        "LITE": {"company": "Lumentum",          "bucket": "Optical Networking", "sub": "Transceivers / Lasers", "specificity": 4, "note": "Vertically integrated; InP laser exposure"},
        "COHR": {"company": "Coherent",          "bucket": "Optical Networking", "sub": "Lasers / Datacom",     "specificity": 3, "note": "Post-II-VI merger scale; some commodity exposure"},
        "FN":   {"company": "Fabrinet",          "bucket": "Optical Networking", "sub": "Optical Contract Mfg", "specificity": 4, "note": "Picks-and-shovels for LITE, COHR; high revenue concentration"},
        "MRVL": {"company": "Marvell",           "bucket": "Optical Networking", "sub": "DSPs / SerDes",        "specificity": 5, "note": "PAM4 DSPs for 800G + custom silicon programs"},
        "CRDO": {"company": "Credo Technology",  "bucket": "Optical Networking", "sub": "AECs / SerDes",        "specificity": 4, "note": "Smaller cap, fastest growth, AEC narrative"},
        "ALAB": {"company": "Astera Labs",       "bucket": "Optical Networking", "sub": "PCIe/CXL Retimers",    "specificity": 4, "note": "Inside-rack connectivity; recent IPO"},
        "AXTI": {"company": "AXT Inc.",          "bucket": "Optical Networking", "sub": "InP Substrates ↑",     "specificity": 5, "note": "Extreme upstream bottleneck; few global suppliers"},

        # Bucket 2: Heavy Electrical
        "ETN":  {"company": "Eaton",              "bucket": "Heavy Electrical", "sub": "MV Transformers / Switchgear / UPS", "specificity": 5, "note": "Flagship of heavy-electrical bet; massive backlogs"},
        "ABB":  {"company": "ABB (ADR)",          "bucket": "Heavy Electrical", "sub": "MV/HV Transformers",   "specificity": 5, "note": "Global #1 transformers; backlog through 2027+"},
        "GEV":  {"company": "GE Vernova",         "bucket": "Heavy Electrical", "sub": "Power Equipment",      "specificity": 4, "note": "Transformers + gas turbines + grid; spin-off 2024"},
        "HUBB": {"company": "Hubbell",            "bucket": "Heavy Electrical", "sub": "Electrical Components", "specificity": 3, "note": "Diversified electrical; smaller transformer exposure"},
        "VRT":  {"company": "Vertiv",             "bucket": "Heavy Electrical", "sub": "UPS / Switchgear / Thermal", "specificity": 5, "note": "ALSO Bucket 3 — spans power + thermal; highest concentration risk"},
        "POWL": {"company": "Powell Industries",  "bucket": "Heavy Electrical", "sub": "Custom Switchgear",    "specificity": 3, "note": "Industrial/utility switchgear; narrower customer base"},
        "NVT":  {"company": "nVent Electric",     "bucket": "Heavy Electrical", "sub": "PDUs / Containment / Liquid Cooling", "specificity": 3, "note": "ALSO Bucket 3 — spans containment + liquid cooling"},
        "ATKR": {"company": "Atkore",             "bucket": "Heavy Electrical", "sub": "Conduit / Busbars",    "specificity": 2, "note": "Commoditized; high data-center volume exposure"},
        "CLF":  {"company": "Cleveland-Cliffs",   "bucket": "Heavy Electrical", "sub": "GOES Steel ↑",         "specificity": 5, "note": "Largest US producer of grain-oriented electrical steel"},
        "CRS":  {"company": "Carpenter Technology", "bucket": "Heavy Electrical", "sub": "Specialty Alloys ↑", "specificity": 3, "note": "Some GOES grades; broader specialty steel exposure"},

        # Bucket 3: Thermal Management
        "MOD":  {"company": "Modine Manufacturing", "bucket": "Thermal Management", "sub": "Heat Exchangers / CDUs", "specificity": 4, "note": "Airedale brand; fastest-growing thermal pure-play"},
        "ECL":  {"company": "Ecolab",             "bucket": "Thermal Management", "sub": "Direct-to-Chip Cooling (via CoolIT)", "specificity": 5, "note": "Acquiring CoolIT for $4.75B, closes Q3 2026; emerging as pure-play DTC cooling owner"},
        "TT":   {"company": "Trane Technologies", "bucket": "Thermal Management", "sub": "Immersion (via LiquidStack)", "specificity": 4, "note": "Acquired LiquidStack Feb 2026; building immersion cooling portfolio inside HVAC giant"},
        "FLEX": {"company": "Flex",               "bucket": "Thermal Management", "sub": "Micro-Convective (via JetCool)", "specificity": 3, "note": "Acquired JetCool late 2024; small slice of broad EMS business"},
        "AAON": {"company": "AAON",               "bucket": "Thermal Management", "sub": "Custom HVAC",        "specificity": 2, "note": "Industrial HVAC; limited liquid cooling exposure"},
        "MMM":  {"company": "3M",                 "bucket": "Thermal Management", "sub": "Dielectric Fluids ↑", "specificity": 3, "note": "PFAS exit completed Mar 2025 — DECLINING exposure, replaced by Syensqo / Chemours / Honeywell"},
        "HON":  {"company": "Honeywell",          "bucket": "Thermal Management", "sub": "Thermal Materials",  "specificity": 2, "note": "Watch-list: potential PFAS alternative supplier"},
    },

    "robotics": {
        # Bucket 1: Machine Vision & Sensors (up-stack moat — Structural)
        "CGNX": {"company": "Cognex",              "bucket": "Machine Vision & Sensors", "sub": "Machine vision pure-play", "specificity": 5, "note": "Load-bearing pillar. Pure up-stack moat; In-Sight 6900/3900 AI-vision; GM 71%, rev +24% YoY. P/E ~77 — mind entry"},
        "ZBRA": {"company": "Zebra Technologies",  "bucket": "Machine Vision & Sensors", "sub": "Machine vision + warehouse base", "specificity": 4, "note": "Vision (Matrox) + dominant scanning/mobility base; cheaper than CGNX. MV sub-segment in cyclical downturn (EV/semi)"},
        "QCOM": {"company": "Qualcomm",            "bucket": "Machine Vision & Sensors", "sub": "Edge-AI silicon",          "specificity": 5, "note": "Powers CGNX 3900; Dragonwing IQ10 robotics chip. Cheapest sensing-layer exposure; robotics small vs phones"},
        "TEL":  {"company": "TE Connectivity",     "bucket": "Machine Vision & Sensors", "sub": "Sensors / connectors",     "specificity": 3, "note": "Watch — diversified sensing exposure into robotics/industrial"},
        "OUST": {"company": "Ouster",              "bucket": "Machine Vision & Sensors", "sub": "Industrial/robotics LiDAR", "specificity": 3, "note": "Watch — early, thin margins"},

        # Bucket 2: Mature Automation & Integrators (installed-base AI upgrade — Structural)
        "ROK":  {"company": "Rockwell Automation", "bucket": "Mature Automation",        "sub": "PLC / automation incumbent", "specificity": 4, "note": "Cleanest US automation incumbent. FY26 5-9% organic, double-digit orders on reshoring, op margin 21.5%"},
        "ABB":  {"company": "ABB (ADR)",           "bucket": "Mature Automation",        "sub": "Robotics + electrification",  "specificity": 4, "note": "Only cleanly-holdable foreign name. CATALYST: 100% robotics spin-off to separate listing Q2 2026"},
        "PH":   {"company": "Parker Hannifin",     "bucket": "Mature Automation",        "sub": "Motion & control / drives",   "specificity": 2, "note": "Watch-line. Quality but expensive (~$905 vs ~$512 FV); moat-location low (mechanical)"},
        "AME":  {"company": "AMETEK",              "bucket": "Mature Automation",        "sub": "Miniature motors / actuators","specificity": 3, "note": "Watch — diversified motion exposure"},

        # Bucket 3: Mechanical Components (TARIFF TRADE only — Optionality)
        "SYM":   {"company": "Symbotic",          "bucket": "Mechanical Components",     "sub": "AI warehouse automation",     "specificity": 3, "note": "Demoted from structural. Now GAAP-profitable ($2.0B cash, $22.7B backlog) BUT ~85% Walmart concentration. Sized small"},
        "MOG-A": {"company": "Moog",              "bucket": "Mechanical Components",     "sub": "Precision actuators",         "specificity": 2, "note": "Holdable US proxy for the actuator/tariff bet (Tokyo reducer names unbuyable). Moat-location deliberately low — geopolitical option"},
        # NOTE: Keyence (KYCCF), Fanuc (FANUY), Harmonic Drive (6324.T), Nabtesco (6268.T), SMC (6273.T)
        # are EXCLUDED — not cleanly Fidelity-holdable (unsponsored/thin OTC). The vehicles-wrong risk
        # made literal. They are tracked as benchmarks in scoring.md but NOT priced here.
    },

    "space_economy": {
        # Bucket 1: Mission-Critical Subsystems & Components (HIGH PRIORITY)
        "MRCY":  {"company": "Mercury Systems",       "bucket": "Components",       "sub": "Rad-hard chips / secure processors", "specificity": 4, "note": "~5 global competitors at this tier; specialized defense electronics"},
        "CW":    {"company": "Curtiss-Wright",        "bucket": "Components",       "sub": "Defense electronics / sensors / nuclear", "specificity": 3, "note": "Broad portfolio; real but diluted space exposure"},
        "HEI":   {"company": "Heico",                 "bucket": "Components",       "sub": "Specialty aerospace parts compounder", "specificity": 3, "note": "Exceptional execution + pricing power; space is one slice"},
        "MOG-A": {"company": "Moog",                  "bucket": "Components",       "sub": "Precision motion / propulsion components", "specificity": 4, "note": "Specialized aerospace-grade actuators + valves"},
        "KTOS":  {"company": "Kratos Defense",        "bucket": "Components",       "sub": "Small sats / drones / defense electronics", "specificity": 3, "note": "Real revenue, small cap, growing"},
        "BAESY": {"company": "BAE Systems (ADR)",     "bucket": "Components",       "sub": "UK defense prime (ADR)",         "specificity": 3, "note": "Diluted but real space exposure; recent US acquisitions"},
        "THLEY": {"company": "Thales (ADR)",          "bucket": "Components",       "sub": "French prime + Thales Alenia Space JV", "specificity": 4, "note": "Major EU satellite OEM via Thales Alenia"},
        "SAFRY": {"company": "Safran (ADR)",          "bucket": "Components",       "sub": "Propulsion + actuators (ADR)",   "specificity": 3, "note": "Aerospace propulsion + small-sat platforms; engines dominate"},

        # Bucket 2: Space Comms & Ground Infrastructure (HIGH PRIORITY)
        "IRDM":  {"company": "Iridium Communications","bucket": "Comms / Ground",    "sub": "L-band satellite comms pure-play", "specificity": 5, "note": "Cleanest public pure-play in entire space supply chain; ~$800M rev"},
        "VSAT":  {"company": "Viasat",                "bucket": "Comms / Ground",    "sub": "Broadband + IFC + maritime + gov", "specificity": 3, "note": "Post-Inmarsat acquisition; execution challenged"},
        "LHX":   {"company": "L3Harris Technologies", "bucket": "Comms / Ground",    "sub": "Space comms + tactical communications", "specificity": 4, "note": "More pure-play comms than other defense primes"},
        "GILT":  {"company": "Gilat Satellite Networks", "bucket": "Comms / Ground", "sub": "Israeli ground systems / VSAT terminals / IFC", "specificity": 3, "note": "$300M+ rev specialized; narrow but real"},
        "CMTL":  {"company": "Comtech Telecommunications", "bucket": "Comms / Ground", "sub": "Modems / ground systems / microwave", "specificity": 2, "note": "Small cap, troubled execution — turnaround story"},
        "MYNA":  {"company": "Mynaric",               "bucket": "Comms / Ground",    "sub": "Optical inter-satellite links pure-play", "specificity": 4, "note": "OPEN RESEARCH: financial health uncertain (cash burn concerns)"},

        # Bucket 3: Payload Data & Defense Analytics (MEDIUM PRIORITY)
        "LDOS":  {"company": "Leidos Holdings",       "bucket": "Analytics",         "sub": "Defense IT + space mission support", "specificity": 3, "note": "Largest defense IT; deep NRO/Space Force relationships"},
        "CACI":  {"company": "CACI International",    "bucket": "Analytics",         "sub": "Defense IT / intelligence community / EW", "specificity": 3, "note": "Real revenue + clean balance sheet"},
        "BAH":   {"company": "Booz Allen Hamilton",   "bucket": "Analytics",         "sub": "Government consulting / space mission support", "specificity": 3, "note": "Highest quality balance sheet of the three"},
        "PLTR":  {"company": "Palantir Technologies", "bucket": "Analytics",         "sub": "Foundry for defense / NRO / Maven", "specificity": 3, "note": "Broad applicability; high SBC likely caps CS score"},

        # Bucket 4: In-Space Logistics & Satellite Architecture (LOW / SPECULATIVE)
        "RKLB":  {"company": "Rocket Lab USA",        "bucket": "In-Space Logistics","sub": "Small launch + space systems (Photon bus)", "specificity": 4, "note": "Graduated from SPAC to real biz: $350M+ rev; Space Force/NASA customers"},
        "NOC":   {"company": "Northrop Grumman",      "bucket": "In-Space Logistics","sub": "SpaceLogistics MEV + B-21 + strategic systems", "specificity": 4, "note": "Only operating commercial in-orbit servicer; otherwise diluted"},
        "BWXT":  {"company": "BWX Technologies",      "bucket": "In-Space Logistics","sub": "Nuclear propulsion for space (DARPA DRACO) + navy reactors", "specificity": 4, "note": "Extreme nuclear regulatory moat; space rev is small slice"},
        "ASTSF": {"company": "Astroscale Holdings (JP)","bucket": "In-Space Logistics","sub": "Active debris removal / life extension (JP ADR)", "specificity": 5, "note": "Pure-play but PRE-REVENUE — will struggle on CS criterion"},
    },

    "modern_defense": {
        # Bucket 1: Counter-Drone / EW (Highest weight — Structural)
        "LHX":   {"company": "L3Harris Technologies", "bucket": "Counter-Drone / EW", "sub": "Tactical EW + space comms (cross-theme)", "specificity": 4, "note": "Most EW pure-play among defense primes; also Space tracker"},
        "MRCY":  {"company": "Mercury Systems",       "bucket": "Counter-Drone / EW", "sub": "Rad-hard edge compute for autonomous systems", "specificity": 4, "note": "Cross-theme with Space; specialized defense electronics"},
        "BAESY": {"company": "BAE Systems (ADR)",     "bucket": "Counter-Drone / EW", "sub": "EW + targeting + radar (UK ADR)", "specificity": 3, "note": "Cross-theme with Space; UK prime with real EW capability"},
        "DRS":   {"company": "Leonardo DRS",          "bucket": "Counter-Drone / EW", "sub": "Counter-UAS + force protection + ISR", "specificity": 4, "note": "US arm of Leonardo; narrower pure-play C-UAS"},
        "RTX":   {"company": "RTX Corp",              "bucket": "Counter-Drone / EW", "sub": "Patriot/NASAMS (over-hyped per thesis)", "specificity": 2, "note": "Legacy kinetic — thesis explicitly says uneconomic vs cheap drones"},
        "HII":   {"company": "Huntington Ingalls",    "bucket": "Counter-Drone / EW", "sub": "Ship-based defense (over-hyped per thesis)", "specificity": 2, "note": "Thesis-misaligned; diluted into legacy shipbuilding"},

        # Bucket 2: Munitions & Restocking (Highest weight — Structural)
        "RNMBY": {"company": "Rheinmetall (ADR)",     "bucket": "Munitions",          "sub": "German artillery + 155mm + propellants", "specificity": 5, "note": "Flagship pure-play; massive Europe restocking ramp with multi-year backlogs"},
        "NOC":   {"company": "Northrop Grumman",      "bucket": "Munitions",          "sub": "Propellants + ammo + strategic systems", "specificity": 4, "note": "Cross-theme with Space; munitions slice is real but diluted"},
        "GD":    {"company": "General Dynamics",      "bucket": "Munitions",          "sub": "Abrams + Stryker + ordnance + subs", "specificity": 3, "note": "Broad portfolio dilutes munitions exposure"},
        "LMT":   {"company": "Lockheed Martin",       "bucket": "Munitions",          "sub": "Javelin + HIMARS + GMLRS (also vehicles-wrong anchor)", "specificity": 3, "note": "Benchmark anchor — diluted by F-35; thesis-skeptical on legacy aero"},

        # Bucket 3: Drones & Autonomous Platforms (Moderate weight — Catalyst)
        "AVAV":  {"company": "AeroVironment",         "bucket": "Drones",             "sub": "Switchblade + Puma (fielded DoD)", "specificity": 4, "note": "The ONE battle-tested US pure-play; explicitly exempt from over-hype"},
        "ESLT":  {"company": "Elbit Systems (ADR)",   "bucket": "Drones",             "sub": "Israeli — Hermes UAVs + EW + ground", "specificity": 4, "note": "Battle-tested allied pure-play; high thesis fit"},
        "SAABY": {"company": "Saab AB (ADR)",         "bucket": "Drones",             "sub": "Swedish — NLAW + Carl-Gustaf + Gripen", "specificity": 4, "note": "Europe rearmament beneficiary"},
        "KTOS":  {"company": "Kratos Defense",        "bucket": "Drones",             "sub": "Target drones + Valkyrie XQ-58", "specificity": 3, "note": "Cross-theme with Space; smaller customer base"},
        "TXT":   {"company": "Textron",               "bucket": "Drones",             "sub": "Bell + AAI Shadow drones + Cessna", "specificity": 2, "note": "Diluted — drones are small slice of industrial conglomerate"},
        "ONDS":  {"company": "Ondas Holdings",        "bucket": "Drones",             "sub": "Small drones + Replicator participant", "specificity": 2, "note": "Pre-profitable + heavy dilution — likely fails CS bar"},
        "RCAT":  {"company": "Red Cat Holdings",      "bucket": "Drones",             "sub": "Small drone manufacturer (SRR Tranche 2)", "specificity": 2, "note": "Real but small revenue; SPAC-origin dilution"},

        # Bucket 4: Defense Software & C2 (Lightest weight — 15%)
        "PLTR":  {"company": "Palantir Technologies", "bucket": "Software / C2",      "sub": "Foundry + Maven program + DoD C2", "specificity": 3, "note": "Pinned to Defense only (not Space); broad applicability"},
        "CACI":  {"company": "CACI International",    "bucket": "Software / C2",      "sub": "Defense IT + intelligence community + EW services", "specificity": 3, "note": "Cross-theme with Space; flagged for acquisition-funded debt growth"},
        "LDOS":  {"company": "Leidos Holdings",       "bucket": "Software / C2",      "sub": "Largest defense IT contractor", "specificity": 3, "note": "Diluted but high quality; cross-theme with Space"},
        "BAH":   {"company": "Booz Allen Hamilton",   "bucket": "Software / C2",      "sub": "Government consulting + mission support", "specificity": 3, "note": "Cross-theme with Space; cleanest BS of the three IT names"},
    },
    "glp1": {
        # Long/Short theme (short-weighted). `side` = short (demand destruction / stranded)
        # or long (beneficiary). Fetching only reads ticker keys; `side` is an analyst overlay.
        # LLY/NVO deliberately EXCLUDED from universe — benchmark reference only, never held.

        # Bucket 1: Demand Destruction — Med-Tech (HIGH PRIORITY — SHORT)
        "RMD":  {"company": "ResMed",            "bucket": "Med-Tech (Demand Destruction)", "sub": "CPAP / sleep apnea",        "side": "short", "specificity": 4, "note": "Weight loss reduces OSA severity → device demand; first-line displacement bet"},
        "DXCM": {"company": "Dexcom",            "bucket": "Med-Tech (Demand Destruction)", "sub": "CGM / diabetes management", "side": "short", "specificity": 4, "note": "Fewer progressing diabetics shrinks the CGM funnel"},
        "MDT":  {"company": "Medtronic",         "bucket": "Med-Tech (Demand Destruction)", "sub": "Bariatric + orthopedic",    "side": "short", "specificity": 3, "note": "Diluted mega-cap; bariatric + weight-driven ortho volume decline"},

        # Bucket 2: Demand Destruction — Packaged Food & Beverage (HIGH PRIORITY — SHORT)
        "MDLZ": {"company": "Mondelez",          "bucket": "Food & Beverage (Demand Destruction)", "sub": "Snacking / confection",  "side": "short", "specificity": 4, "note": "Impulse snacking core to volume; craving-modulation hit"},
        "PEP":  {"company": "PepsiCo",           "bucket": "Food & Beverage (Demand Destruction)", "sub": "Snacks + sugary bev",    "side": "short", "specificity": 4, "note": "Frito-Lay + beverage caloric volume exposure"},
        "HSY":  {"company": "Hershey",           "bucket": "Food & Beverage (Demand Destruction)", "sub": "Confection",             "side": "short", "specificity": 5, "note": "Purest sugar / impulse exposure in the short book"},

        # Bucket 3: The Dead Bottleneck (MEDIUM PRIORITY — SHORT / AVOID)
        "WST":  {"company": "West Pharmaceutical Services", "bucket": "Injectable Supply (Stranded)", "sub": "Injectable components",     "side": "short", "specificity": 4, "note": "Glass syringe / auto-injector capex stranded by oral pivot"},
        "STVN": {"company": "Stevanato Group",   "bucket": "Injectable Supply (Stranded)", "sub": "Injectable glass + assembly", "side": "short", "specificity": 4, "note": "Same stranded-capex thesis; ADR, watch borrow/liquidity"},

        # Bucket 4: Downstream Beneficiaries (MEDIUM PRIORITY — LONG)
        "CAVA": {"company": "Cava Group",        "bucket": "Beneficiary",  "sub": "Health-forward QSR",           "side": "long",  "specificity": 3, "note": "Health-conscious spend-shift beneficiary"},
        "LULU": {"company": "Lululemon",         "bucket": "Beneficiary",  "sub": "Active lifestyle apparel",     "side": "long",  "specificity": 2, "note": "Discretionary lifestyle beneficiary; diffuse exposure"},
        "CELH": {"company": "Celsius Holdings",  "bucket": "Beneficiary",  "sub": "Functional / zero-sugar bev",  "side": "long",  "specificity": 3, "note": "Substitution winner vs sugary beverage"},
        "TMO":  {"company": "Thermo Fisher",     "bucket": "API / CDMO (Long)", "sub": "Small-molecule solid-dose API (Catalent integ.)", "side": "long", "specificity": 3, "note": "US CDMO layer that scales the oral pill; diluted large-cap"},
    },
}

# Cross-theme tickers (LHX, MRCY, BAESY, NOC, KTOS, CACI, BAH, LDOS) intentionally appear in
# both Space and Defense universes — they have legitimate exposure to both themes. The nested
# dict structure means no key collision (UNIVERSES["space_economy"]["NOC"] and
# UNIVERSES["modern_defense"]["NOC"] are distinct entries). At tracker init time we explicitly
# decide whether names ending up in both trackers should be held 2x or pinned to one.
# PLTR is the exception — explicitly Defense-only per locked thesis.


# ── Seed-universe themes ─────────────────────────────────────────
# Themes built via _seed_from_universe.py keep their universe in
# themes/<slug>/_universe.json (flat TICKER -> meta map, same shape as a
# UNIVERSES entry). Rather than duplicate ~150 rows here, we auto-merge any
# _universe.json for a theme not already hard-coded above. This keeps the seed
# file the single source of truth for the universe and still lets the nightly
# Railway refresh (with the FMP key) keep those candidates fresh.
_SEED_UNIVERSE_THEMES = [
    "nuclear_renaissance", "ai_healthcare", "personalized_medicine", "rare_earth",
    "power_grid", "cyber_infrastructure", "reshoring", "climate_adaptation",
    "aging_population",
]


def _merge_seed_universes() -> None:
    for slug in _SEED_UNIVERSE_THEMES:
        if slug in UNIVERSES:
            continue
        uni_path = _HERE / slug / "_universe.json"
        if not uni_path.exists():
            continue
        try:
            UNIVERSES[slug] = json.loads(uni_path.read_text(encoding="utf-8"))
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"could not load seed universe for {slug}: {e}")


_merge_seed_universes()


# Themes whose data this script refreshes. Theme dir must exist under themes/.
ACTIVE_THEMES = [
    "ai_data_center", "space_economy", "modern_defense", "robotics", "glp1",
    *_SEED_UNIVERSE_THEMES,
]


# ═══════════════════════════════════════════════════════════════
#  Data fetching helpers
# ═══════════════════════════════════════════════════════════════

def _fetch_fmp_profile(ticker: str, client: httpx.Client) -> Optional[dict]:
    """Fetch /stable/profile for sector + industry + description."""
    try:
        resp = client.get(
            f"https://financialmodelingprep.com/stable/profile",
            params={"symbol": ticker, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        logger.debug(f"FMP profile failed for {ticker}: {e}")
        return None


def _fetch_fmp_key_metrics(ticker: str, client: httpx.Client) -> Optional[dict]:
    """Fetch /stable/key-metrics-ttm for P/S, revenue, margins."""
    try:
        resp = client.get(
            f"https://financialmodelingprep.com/stable/key-metrics-ttm",
            params={"symbol": ticker, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        logger.debug(f"FMP key-metrics failed for {ticker}: {e}")
        return None


def _fetch_fmp_income_growth(ticker: str, client: httpx.Client) -> Optional[dict]:
    """Fetch /stable/income-statement-growth for revenue growth YoY."""
    try:
        resp = client.get(
            f"https://financialmodelingprep.com/stable/income-statement-growth",
            params={"symbol": ticker, "period": "annual", "limit": 1, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        logger.debug(f"FMP income-growth failed for {ticker}: {e}")
        return None


def _fetch_fmp_financial_growth(ticker: str, client: httpx.Client) -> Optional[dict]:
    """Fetch /stable/financial-growth for shares-out and debt growth YoY."""
    try:
        resp = client.get(
            f"https://financialmodelingprep.com/stable/financial-growth",
            params={"symbol": ticker, "period": "annual", "limit": 1, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        logger.debug(f"FMP financial-growth failed for {ticker}: {e}")
        return None


def _fetch_fmp_cash_flow(ticker: str, client: httpx.Client) -> Optional[dict]:
    """Fetch latest annual cash flow statement for SBC + FCF + revenue."""
    try:
        resp = client.get(
            f"https://financialmodelingprep.com/stable/cash-flow-statement",
            params={"symbol": ticker, "period": "annual", "limit": 1, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        logger.debug(f"FMP cash-flow failed for {ticker}: {e}")
        return None


def _fetch_fmp_income_statement(ticker: str, client: httpx.Client) -> Optional[dict]:
    """Fetch latest annual income statement for revenue (for SBC% calc)."""
    try:
        resp = client.get(
            f"https://financialmodelingprep.com/stable/income-statement",
            params={"symbol": ticker, "period": "annual", "limit": 1, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        logger.debug(f"FMP income-statement failed for {ticker}: {e}")
        return None


def _fetch_daily_bars(tickers: list[str]) -> dict[str, pd.DataFrame]:
    """Pull 1Y of daily bars from yfinance for return calculations."""
    out: dict[str, pd.DataFrame] = {}
    try:
        df = yf.download(
            tickers=" ".join(tickers),
            period="1y",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        if len(tickers) == 1:
            out[tickers[0]] = df.dropna()
        else:
            for t in tickers:
                try:
                    sub = df[t].dropna()
                    if len(sub) > 0:
                        out[t] = sub
                except KeyError:
                    logger.debug(f"yfinance: no daily data for {t}")
    except Exception as e:
        logger.warning(f"yfinance batch failed: {e}")
    return out


def _pct_change_from(bars: pd.DataFrame, days_back: int) -> Optional[float]:
    """Return pct change from `days_back` trading days ago to last close."""
    if bars is None or bars.empty or len(bars) < 2:
        return None
    last = float(bars["Close"].iloc[-1])
    if days_back >= len(bars):
        ref = float(bars["Close"].iloc[0])
    else:
        ref = float(bars["Close"].iloc[-1 - days_back])
    if ref <= 0:
        return None
    return round((last / ref - 1) * 100, 2)


def _dist_from_52w_high(bars: pd.DataFrame) -> Optional[float]:
    """How far below 52w high is the last close? Negative = below high."""
    if bars is None or bars.empty:
        return None
    high = float(bars["High"].max())
    last = float(bars["Close"].iloc[-1])
    if high <= 0:
        return None
    return round((last / high - 1) * 100, 2)


def _compute_rs_ranks(returns_by_ticker: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    """
    Within the candidate universe, rank each ticker 0-100 on 1M, 3M, 6M, 12M returns.
    100 = best performer in the cohort. This is a relative measure within the theme,
    not vs. the broad market — use it to compare candidates against each other.
    """
    periods = ["1m_pct", "3m_pct", "6m_pct", "1y_pct"]
    rs_by_ticker = {t: {} for t in returns_by_ticker.keys()}
    for period in periods:
        # Collect (ticker, return) pairs
        pairs = [(t, returns_by_ticker[t].get(period)) for t in returns_by_ticker]
        pairs = [(t, r) for t, r in pairs if r is not None]
        if not pairs:
            continue
        # Sort descending — highest return is rank 100
        pairs.sort(key=lambda x: -x[1])
        n = len(pairs)
        for i, (t, _) in enumerate(pairs):
            # Rank: best=100, worst=0
            rs_by_ticker[t][f"rs_{period.replace('_pct','')}"] = round(100 * (n - 1 - i) / max(1, n - 1), 0)
    return rs_by_ticker


# ═══════════════════════════════════════════════════════════════
#  Per-ticker refresh
# ═══════════════════════════════════════════════════════════════

def _fill_from_yfinance_info(ticker: str, out: dict) -> None:
    """
    Fallback path when FMP key is unavailable. Pulls fundamentals from
    yfinance .info dict — slower per-call but works without API keys.
    """
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as e:
        logger.debug(f"yfinance info failed for {ticker}: {e}")
        return

    # Price
    if out.get("price") is None:
        out["price"] = (info.get("currentPrice") or info.get("regularMarketPrice")
                         or info.get("regularMarketPreviousClose"))
    # Daily change %
    if out.get("change_pct") is None and info.get("regularMarketChangePercent") is not None:
        out["change_pct"] = info.get("regularMarketChangePercent")

    # Market cap, P/E, P/S
    if out.get("market_cap") is None:
        out["market_cap"] = info.get("marketCap")
    if out.get("pe") is None:
        out["pe"] = info.get("trailingPE") or info.get("forwardPE")
    if out.get("ps") is None:
        out["ps"] = info.get("priceToSalesTrailing12Months")
    if out.get("eps") is None:
        out["eps"] = info.get("trailingEps")

    # 52-week extremes
    if out.get("year_high") is None:
        out["year_high"] = info.get("fiftyTwoWeekHigh")
    if out.get("year_low") is None:
        out["year_low"] = info.get("fiftyTwoWeekLow")

    # Volume
    if out.get("volume") is None:
        out["volume"] = info.get("volume") or info.get("regularMarketVolume")
    if out.get("avg_volume") is None:
        out["avg_volume"] = info.get("averageVolume")

    # Sector / industry / beta
    if out.get("sector") is None:
        out["sector"] = info.get("sector")
    if out.get("industry") is None:
        out["industry"] = info.get("industry")
    if out.get("beta") is None:
        out["beta"] = info.get("beta")

    # Margins
    if out.get("gross_margin_ttm") is None:
        out["gross_margin_ttm"] = info.get("grossMargins")

    # Revenue growth YoY — yfinance gives decimal (0.25 = 25%)
    if out.get("revenue_growth_yoy") is None:
        out["revenue_growth_yoy"] = info.get("revenueGrowth")


def refresh_ticker(ticker: str, meta: dict, client: httpx.Client,
                    bars: Optional[pd.DataFrame]) -> dict:
    """Pull all data for one ticker, return a candidate dict ready to JSON-serialize."""
    out = {
        "ticker": ticker,
        "company": meta.get("company"),
        "bucket": meta.get("bucket"),
        "sub": meta.get("sub"),
        "specificity": meta.get("specificity"),
        "note": meta.get("note"),
    }

    have_fmp = bool(config.FMP_API_KEY)

    # ── FMP path (preferred when key is available) ───────────────
    if have_fmp:
        # Quote
        try:
            resp = client.get(
                f"https://financialmodelingprep.com/stable/quote",
                params={"symbol": ticker, "apikey": config.FMP_API_KEY},
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                quote = data[0]
                out["price"] = quote.get("price")
                out["change_pct"] = quote.get("changesPercentage") or quote.get("changePercentage")
                out["volume"] = quote.get("volume")
                out["avg_volume"] = quote.get("avgVolume")
                out["market_cap"] = quote.get("marketCap")
                out["pe"] = quote.get("pe")
                out["eps"] = quote.get("eps")
                out["year_high"] = quote.get("yearHigh")
                out["year_low"] = quote.get("yearLow")
                out["exchange"] = quote.get("exchange")
        except Exception as e:
            logger.debug(f"FMP quote failed for {ticker}: {e}")

        profile = _fetch_fmp_profile(ticker, client)
        if profile:
            out["sector"] = profile.get("sector")
            out["industry"] = profile.get("industry")
            out["beta"] = profile.get("beta")

        km = _fetch_fmp_key_metrics(ticker, client)
        if km:
            out["ps"] = km.get("priceToSalesRatioTTM") or km.get("priceToSalesRatio")
            out["revenue_per_share"] = km.get("revenuePerShareTTM")
            out["gross_margin_ttm"] = km.get("grossProfitMarginTTM") or km.get("grossProfitMargin")
            out["roe_ttm"] = km.get("roeTTM") or km.get("roe")

        ig = _fetch_fmp_income_growth(ticker, client)
        if ig:
            out["revenue_growth_yoy"] = ig.get("growthRevenue")

        # ── Capital structure inputs ──
        fg = _fetch_fmp_financial_growth(ticker, client)
        if fg:
            # FMP returns these as decimals (0.05 = 5%)
            out["shares_growth_yoy"] = fg.get("weightedAverageSharesDilutedGrowth")
            out["debt_growth_yoy"] = fg.get("debtGrowth")
            out["fcf_growth_yoy"] = fg.get("freeCashFlowGrowth")

        cf = _fetch_fmp_cash_flow(ticker, client)
        inc = _fetch_fmp_income_statement(ticker, client)
        if cf and inc:
            try:
                sbc = float(cf.get("stockBasedCompensation") or 0)
                fcf = float(cf.get("freeCashFlow") or 0)
                rev = float(inc.get("revenue") or 0)
                if rev > 0:
                    out["sbc_pct_revenue"] = round(sbc / rev, 4)
                    out["fcf_margin"] = round(fcf / rev, 4)
            except Exception as e:
                logger.debug(f"capital structure calc failed for {ticker}: {e}")

    # ── yfinance fill — covers what FMP missed or runs solo when no key ──
    _fill_from_yfinance_info(ticker, out)

    # ── Returns from daily bars (always from yfinance) ───────────
    if bars is not None and not bars.empty:
        out["1m_pct"] = _pct_change_from(bars, 21)
        out["3m_pct"] = _pct_change_from(bars, 63)
        out["6m_pct"] = _pct_change_from(bars, 126)
        out["1y_pct"] = _pct_change_from(bars, 252)
        out["dist_from_52w_high_pct"] = _dist_from_52w_high(bars)

    # Record data source for observability
    out["_data_source"] = "fmp+yf" if have_fmp else "yf"

    return out


# ═══════════════════════════════════════════════════════════════
#  Top-level refresh
# ═══════════════════════════════════════════════════════════════

def refresh_theme(theme: str) -> dict:
    """Refresh fundamentals + history for a theme. Returns the JSON-serializable result."""
    if theme not in UNIVERSES:
        raise ValueError(f"Unknown theme: {theme}")

    universe = UNIVERSES[theme]
    tickers = list(universe.keys())
    logger.info(f"[{theme}] Refreshing {len(tickers)} tickers")

    # Pull daily bars in one batch (much faster than per-ticker)
    bars_map = _fetch_daily_bars(tickers)
    logger.info(f"[{theme}] Got daily bars for {len(bars_map)}/{len(tickers)} tickers")

    candidates: list[dict] = []
    with httpx.Client(timeout=15) as client:
        for i, ticker in enumerate(tickers):
            meta = universe[ticker]
            try:
                row = refresh_ticker(ticker, meta, client, bars_map.get(ticker))
                candidates.append(row)
            except Exception as e:
                logger.warning(f"[{theme}] {ticker} failed: {e}")
                candidates.append({"ticker": ticker, "company": meta.get("company"),
                                    "bucket": meta.get("bucket"), "error": str(e)})

            # Rate limit: FMP starter is 300/min, we do ~4 calls per ticker = 1200/min if no spacing
            # Pause every 4 tickers (16 calls) to stay polite
            if (i + 1) % 4 == 0:
                time.sleep(1.0)

    # Compute RS ranks across the cohort
    returns = {c["ticker"]: c for c in candidates if "1m_pct" in c}
    rs_ranks = _compute_rs_ranks(returns)
    for c in candidates:
        if c["ticker"] in rs_ranks:
            c.update(rs_ranks[c["ticker"]])

    result = {
        "theme": theme,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "trade_date": datetime.now().strftime("%Y-%m-%d"),
        "universe_size": len(tickers),
        "candidates": candidates,
    }
    return result


def save_theme(theme: str, result: dict):
    """Write candidates.json + dated history snapshot. Regenerate candidates.md."""
    theme_dir = _HERE / theme
    if not theme_dir.exists():
        raise FileNotFoundError(f"Theme dir missing: {theme_dir}")

    # candidates.json — current snapshot (source of truth)
    json_path = theme_dir / "candidates.json"
    json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    logger.info(f"Wrote {json_path}")

    # history/candidates_YYYY-MM-DD.json — dated snapshot
    hist_dir = theme_dir / "history"
    hist_dir.mkdir(exist_ok=True)
    date_str = result["trade_date"]
    hist_path = hist_dir / f"candidates_{date_str}.json"
    hist_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    logger.info(f"Wrote {hist_path}")

    # candidates.md — regenerated from JSON
    md = _render_markdown(result)
    md_path = theme_dir / "candidates.md"
    md_path.write_text(md, encoding="utf-8")
    logger.info(f"Wrote {md_path}")


# ═══════════════════════════════════════════════════════════════
#  Markdown rendering
# ═══════════════════════════════════════════════════════════════

def _fmt_num(v, suffix="", decimals=2, na="—"):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return na
    try:
        return f"{float(v):,.{decimals}f}{suffix}"
    except Exception:
        return str(v)


def _fmt_mcap(v):
    if v is None:
        return "—"
    try:
        v = float(v)
        if v >= 1e12: return f"${v/1e12:.2f}T"
        if v >= 1e9:  return f"${v/1e9:.1f}B"
        if v >= 1e6:  return f"${v/1e6:.0f}M"
        return f"${v:,.0f}"
    except Exception:
        return str(v)


def _fmt_pct(v, plus=True):
    if v is None:
        return "—"
    try:
        sign = "+" if plus and float(v) >= 0 else ""
        return f"{sign}{float(v):.2f}%"
    except Exception:
        return str(v)


def _render_markdown(result: dict) -> str:
    theme = result["theme"]
    theme_display = theme.replace("_", " ").title()
    date = result["trade_date"]
    cands = result["candidates"]

    # Group by bucket for the per-candidate section
    buckets = {}
    for c in cands:
        b = c.get("bucket", "—")
        buckets.setdefault(b, []).append(c)

    lines = []
    lines.append(f"# {theme_display} — Candidates")
    lines.append("")
    lines.append(f"_Generated **{date}** from `refresh_data.py`. "
                  f"Source of truth: `candidates.json`. Re-runs nightly after market close. "
                  f"Universe of {result['universe_size']} tickers spans the three supply-chain buckets._")
    lines.append("")
    lines.append("**Note:** RS ranks are computed *within this candidate cohort* (0-100, higher = better), "
                  "not against the full market. Use them to compare candidates against each other.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Master table
    lines.append("## Quick reference table")
    lines.append("")
    lines.append("| Ticker | Company | Bucket | Mkt Cap | Price | 1Y% | 3M% | 1M% | Δ 52wH | RS 3M | P/E | P/S | Rev YoY |")
    lines.append("|--------|---------|--------|---------|-------|-----|-----|-----|--------|-------|-----|-----|---------|")
    # Sort by bucket then by 3M% desc
    bucket_order = ["Optical Networking", "Heavy Electrical", "Thermal Management"]
    sorted_cands = sorted(cands, key=lambda c: (
        bucket_order.index(c.get("bucket")) if c.get("bucket") in bucket_order else 99,
        -(c.get("3m_pct") or -999),
    ))
    for c in sorted_cands:
        lines.append(
            f"| **{c['ticker']}** | {c.get('company','—')} | {c.get('bucket','—')[:18]} | "
            f"{_fmt_mcap(c.get('market_cap'))} | "
            f"${_fmt_num(c.get('price'), decimals=2)} | "
            f"{_fmt_pct(c.get('1y_pct'))} | {_fmt_pct(c.get('3m_pct'))} | {_fmt_pct(c.get('1m_pct'))} | "
            f"{_fmt_pct(c.get('dist_from_52w_high_pct'))} | "
            f"{_fmt_num(c.get('rs_3m'), decimals=0) if c.get('rs_3m') is not None else '—'} | "
            f"{_fmt_num(c.get('pe'), decimals=1) if c.get('pe') else '—'} | "
            f"{_fmt_num(c.get('ps'), decimals=1) if c.get('ps') else '—'} | "
            f"{_fmt_pct((c.get('revenue_growth_yoy') or 0) * 100) if c.get('revenue_growth_yoy') is not None else '—'} |"
        )
    lines.append("")
    lines.append("---")
    lines.append("")

    # Per-bucket detail
    for bucket in bucket_order:
        if bucket not in buckets:
            continue
        lines.append(f"## {bucket}")
        lines.append("")
        for c in sorted(buckets[bucket], key=lambda x: -(x.get("3m_pct") or -999)):
            lines.append(f"### {c['ticker']} — {c.get('company','—')}")
            lines.append("")
            lines.append(f"**Bucket:** {c.get('bucket','—')} · **Sub:** {c.get('sub','—')}  ")
            lines.append(f"**Specificity:** {c.get('specificity','—')}/5 · **Sector:** {c.get('sector','—')} · **Industry:** {c.get('industry','—')}")
            lines.append("")
            lines.append(f"_{c.get('note','')}_")
            lines.append("")
            lines.append(f"| Price | Mkt Cap | P/E | P/S | Rev YoY | Beta |")
            lines.append(f"|-------|---------|-----|-----|---------|------|")
            lines.append(
                f"| ${_fmt_num(c.get('price'))} | {_fmt_mcap(c.get('market_cap'))} | "
                f"{_fmt_num(c.get('pe'), decimals=1) if c.get('pe') else '—'} | "
                f"{_fmt_num(c.get('ps'), decimals=1) if c.get('ps') else '—'} | "
                f"{_fmt_pct((c.get('revenue_growth_yoy') or 0) * 100) if c.get('revenue_growth_yoy') is not None else '—'} | "
                f"{_fmt_num(c.get('beta'), decimals=2) if c.get('beta') else '—'} |"
            )
            lines.append("")
            lines.append(f"| 1Y % | 6M % | 3M % | 1M % | Δ 52w high | RS 3M (cohort) |")
            lines.append(f"|------|------|------|------|------------|----------------|")
            lines.append(
                f"| {_fmt_pct(c.get('1y_pct'))} | {_fmt_pct(c.get('6m_pct'))} | {_fmt_pct(c.get('3m_pct'))} | "
                f"{_fmt_pct(c.get('1m_pct'))} | {_fmt_pct(c.get('dist_from_52w_high_pct'))} | "
                f"{_fmt_num(c.get('rs_3m'), decimals=0) if c.get('rs_3m') is not None else '—'} |"
            )
            lines.append("")
            if c.get("error"):
                lines.append(f"> ⚠ Data error: `{c.get('error')}`")
                lines.append("")
        lines.append("---")
        lines.append("")

    # Footer with run metadata
    lines.append("## Run metadata")
    lines.append("")
    lines.append(f"- **Generated at:** {result['generated_at']}")
    lines.append(f"- **Source:** FMP (snapshot fundamentals) + yfinance (1Y daily bars)")
    lines.append(f"- **Cohort size:** {len(cands)} candidates")
    err_count = sum(1 for c in cands if c.get("error"))
    if err_count:
        lines.append(f"- **Errors:** {err_count} ticker(s) had partial data — flagged inline")
    lines.append("")
    lines.append("_Next step: review this table, then move to `scoring.md` to apply the rubric and shortlist for the tracker._")

    return "\n".join(lines) + "\n"


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
#  Benchmarks file — index reference prices for vs-SPY math
# ═══════════════════════════════════════════════════════════════

# Index tickers the trackers reference in their `benchmarks_at_init` blocks.
# Add new ones here when a new theme locks against a new benchmark.
BENCHMARK_TICKERS = [
    "SPY", "ITA", "XLU", "SMH", "LMT", "BOTZ", "XLV", "XLP", "LLY",
    # Added for the 2026-07-06 theme batch (nuclear, genomics, rare earth,
    # power grid, cyber, reshoring, climate adaptation, aging population):
    "URA", "XBI", "XME", "GRID", "PHO", "CIBR", "XLI",
]


def refresh_benchmarks() -> dict:
    """
    Fetch current prices for benchmark indexes. Tries FMP first, falls back
    to yfinance. Writes themes/_benchmarks.json which the web app reads to
    compute SPY-since-lock for each tracker.
    """
    out = {"trade_date": datetime.now().strftime("%Y-%m-%d"),
           "fetched_at": datetime.now().isoformat(),
           "source": None,
           "prices": {}}

    # Try FMP first
    if config.FMP_API_KEY:
        try:
            quotes = fetch_batch_quotes(BENCHMARK_TICKERS)
            for tk in BENCHMARK_TICKERS:
                q = quotes.get(tk)
                if q and q.get("price"):
                    out["prices"][tk] = float(q["price"])
            if out["prices"]:
                out["source"] = "fmp"
        except Exception as e:
            logger.warning(f"FMP benchmark fetch failed: {e}")

    # Fallback to yfinance for any missing ones
    missing = [t for t in BENCHMARK_TICKERS if t not in out["prices"]]
    if missing:
        try:
            data = yf.download(missing, period="5d", progress=False, auto_adjust=False)
            close = data["Close"] if "Close" in data.columns.get_level_values(0) else data
            for tk in missing:
                try:
                    px = float(close[tk].dropna().iloc[-1])
                    out["prices"][tk] = px
                except Exception:
                    pass
            if out["prices"] and not out["source"]:
                out["source"] = "yf"
            elif out["source"] == "fmp":
                out["source"] = "fmp+yf"
        except Exception as e:
            logger.warning(f"yfinance benchmark fallback failed: {e}")

    return out


def save_benchmarks(result: dict):
    """Write themes/_benchmarks.json. Leading underscore so it sorts above
    theme dirs and discover_themes_full() skips it."""
    path = _HERE / "_benchmarks.json"
    path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    logger.info(f"Wrote {path} — {len(result['prices'])} prices from {result['source']}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    themes = sys.argv[1:] if len(sys.argv) > 1 else ACTIVE_THEMES
    for theme in themes:
        logger.info(f"━━━━━ Refreshing theme: {theme} ━━━━━")
        try:
            result = refresh_theme(theme)
            save_theme(theme, result)
            n_with_price = sum(1 for c in result["candidates"] if c.get("price"))
            logger.info(f"[{theme}] OK — {n_with_price}/{result['universe_size']} candidates with price data")
        except Exception as e:
            logger.exception(f"Theme {theme} failed: {e}")

    # Refresh benchmark prices once per run — used by web app for vs-SPY math.
    # Cheap (~5 API calls) and refreshing it on every theme-refresh keeps it
    # in sync with the trade_date stamped on candidates.json.
    logger.info("━━━━━ Refreshing benchmarks (SPY/ITA/XLU/SMH/LMT) ━━━━━")
    try:
        bench = refresh_benchmarks()
        save_benchmarks(bench)
    except Exception as e:
        logger.exception(f"Benchmark refresh failed: {e}")


if __name__ == "__main__":
    main()
