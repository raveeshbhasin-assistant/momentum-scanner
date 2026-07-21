# Investing POV — The Referral Moat

*First written 2026-07-20 against the v1 scorecards (111 companies, FY data
through latest filings). Picks are made on theory metrics only; returns are
the report card, never the input.*

## What the first run actually shows

1. **The theory is domain-specific, and that's evidence it's real.**
   RES–alpha correlation (3y, concurrent) is strong where customers choose
   with their own money and can talk to each other — Fintech (ρ +0.78),
   Healthcare/Medtech (+0.61), Consumer Internet (+0.51) — and ~zero in
   Restaurants (0.00) and Enterprise SaaS (+0.07), where either everyone
   advertises the same way or buying is committee-driven. In the Energy
   control it's *negative* (−0.66). A score that merely measured "good
   company" would work everywhere; this one works where word-of-mouth
   plausibly operates. That asymmetry is the single most encouraging fact.

2. **The payoff is a power law, not a steady edge.** Top RES quintile
   averaged +3.5%/yr alpha but with a *negative median* (−5.8%): a few
   compounders (PLTR +70%/yr, HOOD +79%/yr, IBKR +43%/yr alpha) carry the
   bucket. Bottom quintile is uniformly bad (−24%/yr avg, −19.5% median).
   Practical translation: **the score's reliable use is avoiding
   referral-poor businesses and concentrating attention on a basket of
   referral-rich ones** — not sniping single names.

3. **The fingerprint tracks current economics, not brand legend.** Tesla —
   the canonical word-of-mouth story — scores 29/100 today: S&M is still
   tiny (6.2% of revenue) but the magic number went *negative* (spending
   sales dollars while revenue shrinks). The referral engine is a state,
   not a birthright, and the statements catch the state change before the
   narrative does. Costco scores a solid-but-not-spectacular 63 — mature
   referral economics: superb intensity (9.1% S&M/SG&A), modest growth.

4. **Quantitative and qualitative agree more than they should by chance.**
   Of the names whose filings explicitly disclose organic/word-of-mouth
   acquisition, most land in the top quartile (NU #2, DUOL #8, HOOD #7,
   IBKR #11, CELH #12, ELF #22); the disclosed counterexamples (MNDY,
   SOFI — paid-growth machines) land mid-to-bottom (#46, #92). The
   statements are hearing the same thing the earnings calls say.

## How I'd invest on this

**The referral flywheel test (all three, not any one).** The first two-and-
a-half are now computed in the pipeline (`build.py::flywheel_test`, shown as
PASS in the app and frozen into every snapshot):
1. *Growing:* latest revenue growth ≥ 8% and 3y average ≥ 10% — the theory
   is about growth companies; efficiency without growth is harvesting.
2. *Efficiency:* magic number ≥ 1 sustained (each S&M dollar returns
   ≥ $1 of new annual revenue) — NU 10.4, HOOD 4.5, IBKR 3.7 clear it.
3. *Intensity not rising:* S&M% flat-to-falling while growing. Falling
   S&M% with fading growth fails gate 1 instead (see TSLA).
4. *Disclosed mechanism (qualitative, not computed):* management describes
   *how* customers refer (member-get-member, community, professional
   reputation). Without a named mechanism, high efficiency may just be a
   monopoly or a cycle.

**Portfolio construction, given the power law:**
- Own a **basket of 8–12** flywheel-passers rather than 2–3 convictions —
  the alpha lives in the tail you can't pre-identify.
- Weight toward groups where the signal works (fintech, consumer internet,
  healthcare-consumer); demand extra qualitative evidence in groups where
  it doesn't (SaaS, restaurants).
- **Sell rule from the theory:** exit when the flywheel test breaks
  (magic < 0.5 for two years, or S&M% rising to defend growth) — not on
  price. The theory says the economics decay before the stock narrative.
- Valuation is a separate, second gate. The score deliberately ignores
  price; nothing here says a great flywheel at 40× sales outperforms.

**Current flywheel-passers (v1, for tracking — not advice):**
NU, HOOD, IBKR, PLTR, DUOL, META, AMZN, HIMS* — with CELH, ELF, TOST,
CAVA on watch. (*HIMS passes on efficiency but fails the intensity test —
marketing is ~40% of revenue; it stays only if intensity falls with scale.)

## The early-indicators lens (added 2026-07-21)

The flywheel list skews to proven incumbents (Alphabet, Amazon, Meta pass
because their engines genuinely still compound — informative, but the
asymmetric payoff isn't there). The early list is where the theory has
torque: a young or pivoting company whose *latest* year shows fast growth,
magic ≥ 1, and an improving engine, before three years of history can
prove it. How to use the two lists together:

- **Flywheel picks** = the confirmed basket; position-sized, sell-rule
  driven.
- **Early indicators** = the research queue; smaller starter positions at
  most, and each name needs the qualitative gate *more*, not less — one
  good year is easily a marketing pause or a price hike. Demand a named
  referral mechanism before treating the quantitative pass as real.
- Graduation: an early name that accumulates three years and passes the
  full flywheel gate moves to the confirmed basket (RDDT and CAVA are the
  current examples of names passing both).

## Traps to respect

- **Concurrent ≠ forward.** Today's ρ values overlap score and return
  windows. The real test is the snapshot series (`data/snapshots/`):
  score today, measure alpha from today. Judge the theory in 4–6 quarters.
- **Survivorship:** today's prominent-name universe flatters history —
  another reason only forward results count.
- **Small groups + percentiles = coarse ranks** (SLB looking great inside
  a 6-name energy control is an artifact, which is why controls are
  labeled). Low-coverage names (KNSL at 30%) carry wide error bars.
- **Operating leverage can come from cost cuts,** not referrals — hence it
  gets only a 10 weight and never overrides the intensity test.

## Tracking cadence

- **Quarterly** (post earnings season): `python build.py && python
  make_site.py`, republish the artifact. Each run appends a snapshot.
- **Judge**: forward alpha of the flywheel-passer basket vs SPY and vs the
  bottom quintile, from each snapshot date. Also watch whether the control
  groups stay uncorrelated — if they light up, the score has drifted into
  measuring momentum.
- **Upgrade path**: Finnhub as-reported history (10–17y) for slope
  stability; advertising-expense footnotes; disclosed customer counts and
  NRR into the qualitative layer.
