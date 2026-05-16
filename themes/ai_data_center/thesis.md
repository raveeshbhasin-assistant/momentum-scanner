# AI Data Center Build-Out — Thesis

**Author:** Raveesh (drafted with Gemini, refined with Claude)
**Started:** 2026-05-11
**Locked:** 2026-05-11
**Time horizon:** 1-2 years
**Status:** Active

**Benchmark anchors (recorded 2026-05-11 close):**
- **SPY:** $739.30 _(broad market)_
- **XLU:** $45.14 / **SMH:** $576.31 _(sector proxies — utilities + semis)_
- **ETN:** $419.00 _(key candidate, "underestimate" pocket — note: closed up +4.36% on the day, anchored at an elevated price)_
- **CEG:** $299.69 _(reference for "over-hype" pocket — closed down -1.30%)_

---

## The future I'm betting on

Over the next 12 to 24 months, the deployment of AI infrastructure will hit two simultaneous, physical walls: **interconnect bandwidth** and **base-load power distribution**. The market has correctly identified that power is scarce, but it is mispricing where the value will be captured. The winners won't just be the IPPs generating the megawatts; the outsized alpha lies in the "picks and shovels" bridging the gap between the grid and the GPU. As rack densities exceed 100 kW, the critical bottlenecks are the optical networking required to link GPU clusters at the speeds modern workloads demand, the heavy electrical equipment required to step down and distribute the power, and the liquid cooling systems required to stop the silicon from melting.

## Why now (the catalyst)

We have exited the theoretical phase of generative AI and entered the physical deployment phase. In Q1/Q2 2026, hyperscalers have made it clear that their primary inhibitors to growth are infrastructure shortages, not software demand. At the same time, the regulatory environment is shifting. Recent FERC scrutiny and PJM rulings on "behind-the-meter" data center deals show that regulators are pushing back on tech giants monopolizing localized power. This forces hyperscalers to invest exponentially more in grid upgrades, high-efficiency power routing, and thermal infrastructure to maximize the power they do have.

## What I expect the market to underestimate

- **The margin expansion in heavy electrical equipment.** Standard power transformer lead times are currently sitting at roughly 128 weeks, driven by a global shortage of grain-oriented electrical steel. The market is pricing these companies (ETN, ABB) based on historical cyclical volume growth, underestimating the massive, durable pricing power they now hold over hyperscalers who cannot risk a 3-year project delay over a switchgear shortage.

- **Thermal management spend per data center is materially higher than legacy assumptions.** As rack densities push past 100 kW, cooling infrastructure shifts from a small line item to a major capex category — across new-build campuses, retrofits of existing sites, and equipment replacement when accelerator generations cycle. The market is still modeling cooling as a percentage of historical data-center capex norms. The actual spend per megawatt of compute is rising sharply, and the suppliers of CDUs, cold plates, and direct-liquid integration capture the upside regardless of whether hyperscalers retrofit Tier 3 sites or build greenfield.

## What I expect the market to over-hype

- **Small Modular Reactors (SMRs) and pre-revenue nuclear.** The market is desperately bidding up SMR names (NNE, OKLO, SMR) as the ultimate clean-power solution for AI. While technologically sound, the regulatory approval, site permitting, and construction timelines place commercialization well into the 2030s. They will not solve the 2026-2028 crunch and are currently trading on pure narrative, making them highly vulnerable to a timeline reality check.

- **Pure-play behind-the-meter IPPs.** While the Constellation/Microsoft deals made headlines, recent FERC interventions regarding cost-shifting to residential ratepayers signal that rapid, localized power deals will face severe regulatory friction. The pure IPP trade is crowded and politically fragile.

## The supply chain — what the future needs

For the US to support the projected scale of multi-gigawatt AI workloads, capital must flow into three distinct, separated buckets:

1. **Optical Networking (The Bandwidth Bottleneck).** 800G/1.6T High-Speed Ethernet switches, DSPs, and optical transceivers. If clusters can't talk fast enough, the compute is wasted — and this is a hard physical constraint that scales with rack density.

2. **Long-Cycle Heavy Electrical (The Distribution Bottleneck).** Transformers, medium-voltage switchgear, uninterruptible power supplies (UPS), and power management ICs. These are the 128-week lead-time physical gatekeepers. Few global suppliers, hard-to-substitute inputs, long-cycle pricing power.

3. **Thermal Management (The Physics Bottleneck).** Coolant distribution units (CDUs), cold plates, high-density liquid cooling integrators, and immersion-cooling systems. Smallest cap of the three buckets, fastest demand growth.

_Regulated regional utilities — initially considered as a 4th bucket — were dropped from this thesis. Stable rate-base businesses don't fit the "explode" framing, and the FERC dynamic that makes IPPs fragile also creates real ambiguity about whether utilities capture the rate-base lift or get forced to pass costs through to hyperscalers. Worth tracking but not part of this bet._

## Who is positioned to benefit

_Detail moved to supply_chain.md and candidates.md — explicitly separating the cyclical optical trades from the long-cycle industrial multiple-expansion trades._

**Expected portfolio shape:** 2-3 names per bucket, ~7-9 names total. Sizing weighted toward **bucket 2 (Heavy Electrical)** given the longest and most durable demand profile and the clearest pricing-power story. Lighter weight on **bucket 1 (Optical)** given higher cyclicality and a more competitive supplier landscape. **Bucket 3 (Thermal)** sized for asymmetric upside — smallest caps, fastest growth, highest beta to the thesis.

## What would make me wrong

- **Algorithmic efficiency leaps.** Small Language Models (SLMs) and architectural breakthroughs drastically reduce the compute, networking, and power required for inference, crashing the demand for gigawatt-scale, liquid-cooled campuses.

- **The AI bubble bursts.** Enterprise adoption stalls and software revenues fail to materialize, leading hyperscalers to aggressively slash their infrastructure capex plans, instantly evaporating the 128-week equipment backlogs.

- **Thesis is right, but the vehicles are wrong.** Value capture happens upstream or privately. Hyperscalers strong-arm suppliers into fixed-price contracts, or bypass public markets by funding private infrastructure joint-ventures, leaving public equities with anemic growth despite the boom.

- **Supply chain snarls become terminal.** Crippling shortages for vital components (specialized steel, copper, rare earth magnets) don't just stretch lead times — they stall construction entirely, resulting in billions of dollars in stranded capital and forcing a pause in the supercycle.

## Re-check triggers

I revisit this thesis if:

- Any of the "wrong" conditions above appear.
- Hyperscaler quarterly capex guidance (Microsoft, Meta, Google, Amazon) flattens or decreases for two consecutive quarters.
- Backlogs for major electrical equipment providers (ETN, ABB, etc.) contract by more than 10% sequentially, indicating demand destruction or capacity catch-up.
- Networking hardware suppliers report flat sequential optical transceiver sales, signaling that the interconnect bottleneck has been solved or bypassed.

---

## Addenda

### 2026-05-16 — Tracker reshuffle: GEV → ECL

**Trigger:** Research findings R3 and R4 in `supply_chain.md` (dated 2026-05-16).

**Change:** Swapped GE Vernova (GEV) out of the 7-name tracker, swapped Ecolab (ECL) in.

**Reasoning:**
- **GEV is less of a pure-play than initially scored.** FY2025 segment mix: Power (gas turbines + nuclear services) ~50% of revenue, Electrification (transformers, grid equipment, grid software — the thesis-aligned slice) only ~28%, Wind ~22% and declining. The original scoring overweighted the electrification narrative.
- **ECL is the cleanest new direct-to-chip cooling vehicle.** Ecolab announced March 2026 it is acquiring CoolIT Systems for $4.75B (closes Q3 2026). CoolIT is the leading direct-to-chip cooling provider — VRT's partner of record. Once the deal closes, ECL becomes the dominant public-market vehicle for DTC cooling exposure, with the financial firepower and global industrial reach of a $66B specialty chemicals giant behind it.
- **Entry-price asymmetry favors ECL.** ECL is trading ~20% off its 52-week high after the market reacted negatively to the deal price ($4.75B was seen as expensive). RS 0 in the cohort. This is exactly the "low-but-rising" setup the rubric was designed to reward — not a thesis breakdown, just deal-mechanics noise.

**No change to the locked thesis itself.** The picks-and-shovels framing, the four falsifiers, and the bucket structure remain intact. This is a vehicles update within the existing thesis, not a new thesis.

**Resulting tracker (7 names):** ETN · CRDO · ECL · ANET · FN · MOD · VRT. Bucket distribution: 3 Optical (CRDO, ANET, FN) / 2 Heavy Electrical (ETN, VRT) / 2 Thermal (ECL, MOD).

**Watching list (potential future promotions if any of the 7 breaks down):** GEV (now back in candidates only — still a real bet, just less pure), TT (Trane), AVGO (Broadcom — switch silicon king but VMware dilution hurts theme exposure score).

**Signed.** Raveesh / Claude, 2026-05-16.

