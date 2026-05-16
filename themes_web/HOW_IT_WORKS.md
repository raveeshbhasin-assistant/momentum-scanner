# How this works

_A plain-language explanation of the system behind the Themes tracker._

---

## In two paragraphs

This is a research workflow for finding long-term growth stocks by mapping the supply chain behind a future you believe in. Instead of trying to predict which winners emerge from a new trend, we walk from the trend ("AI data centers will keep getting built") down to the physical inputs it requires (power transformers, optical networking, liquid cooling) and identify the public companies that supply those inputs. The bet is that the boring picks-and-shovels suppliers — especially the ones at hard-to-substitute bottlenecks — capture more value than the flashy names everyone already knows about.

Every theme gets its own written thesis (locked once signed, so we can't quietly rewrite history), a hand-curated supply chain map, a candidate list of ~25-30 public companies, and a rubric-driven scoring step that promotes the top ~5-7 names into a tracker. The tracker watches those names with live prices, news, earnings, and institutional fund flows so we can spot when something material changes. One or two themes are active at a time; the rest sit in a backlog. The whole point is intellectual honesty across long horizons — we want to be able to look back in a year and cleanly assess whether we were right or wrong, not what we vaguely remember believing.

---

## The starting belief

Markets reward you for seeing the future before everyone else — but predicting a future is hard and most attempts fail. There's a softer version of the same idea that's much more reliable: when **enough smart people already see a particular future as inevitable**, you can ride that wave by figuring out what physical things that future *needs*, then betting on the companies that supply those things.

Think of it like a gold rush. You don't have to pick which prospector strikes it rich. You can just sell shovels.

The catch is that everyone who reads the news knows the obvious shovel companies. The real money lives in the **upstream bottlenecks** — the inputs no one's paying attention to until suddenly there's a shortage. When you map the chain from "the future" → "what it needs" → "what's hardest to substitute," you usually find a few names that are essential but boring, that haven't been hyped up yet.

## The workflow, step by step

The system has six stages. Each one is its own short document, and there's a hard rule: you have to finish one stage before starting the next.

### 1. Pick a theme

A theme is a future you genuinely believe in. Not "AI is big" — something specific enough you can be wrong about. We work one theme at a time so we don't spread thin. The full list of candidate themes (active, on deck, and in the backlog) is maintained as a prioritized list — currently 15 themes long, with one active and one on deck.

### 2. Write the thesis

A 600-1000 word document where you commit to the bet. It has to include four required sections:

- **What future you're betting on** — articulated in plain language
- **Why now** — what catalyst makes this timely?
- **What would make me wrong** — mandatory. Without falsifiers it's just a vibe.
- **Re-check triggers** — specific monitorable signals that would prompt a review

Once you sign the thesis, **it's locked**. You can never quietly edit it — only append a dated note saying "X happened, here's what I'm updating my view on." The point is intellectual honesty: you have to be able to look back in a year and see what you actually believed at the time, not what you remember believing.

### 3. Map the supply chain

Walk from the future to the inputs it needs. For each theme we identify 3-4 buckets, each broken down further into sub-components, each sub-component mapped to public companies. We rate **bottleneck specificity** 1-5 — is this a hard-to-substitute input from a small number of global suppliers, or a commodity?

For AI data centers, the three buckets are:

- **Optical networking** — the high-speed cables and switches that let thousands of GPUs talk to each other
- **Heavy electrical equipment** — transformers, switchgear, the boring industrial stuff that gets power from the grid to the server rack (current lead times: ~128 weeks, the bottleneck of bottlenecks)
- **Thermal management** — liquid cooling systems, because the newest AI chips literally melt under air cooling

### 4. List candidates

Pull every public company that touches any of the supply chain buckets. For each one we pull live data: price, market cap, P/E, P/S, revenue growth, 1-year and 3-month price moves, distance from 52-week high, and a relative-strength rank vs the other candidates. Refreshed daily.

### 5. Score them

Apply a rubric — eight criteria, each weighted, totaling 100 points per stock:

| Criterion | Weight | What it measures |
|---|---|---|
| Bottleneck specificity | 20 | Hard-to-substitute supplier vs. commodity |
| RS inflection | 15 | We *want* low-but-rising — penalize names already at all-time highs |
| Theme exposure | 15 | How much of revenue actually ties to this theme |
| Revenue growth | 15 | YoY, accelerating beats flat |
| Margin durability | 10 | Pricing power, not just volume |
| Valuation runway | 15 | Cheap relative to growth — room to re-rate |
| Institutional 13F | 5 | Are smart-money funds adding or trimming |
| Catalyst proximity | 5 | Near-term events that could re-rate |

The math runs, every candidate gets a score 0-100, top ~7 get promoted to a tracker. Importantly: the scoring favors **boring asymmetric setups over flashy momentum names**. A stock at relative-strength rank 22 with positive recent action scores higher than one at rank 99 — because the rank-99 names have already moved.

### 6. Track the picks

A live dashboard for the promoted names. Shows entry price vs current price, performance vs SPY, real news headlines, upcoming earnings dates, the top institutional holders for each name with their quarter-over-quarter change. Plus a strip at the top showing the four falsifiers from the thesis — green/yellow/red based on whether any of them are firing.

## The seven things we evaluate

Pull this out for any conversation about a specific name:

1. **Does it sit at a bottleneck?** (Few global suppliers, hard to substitute = good)
2. **What % of its revenue is actually tied to the theme?** (Pure-plays preferred; mega-caps get diluted)
3. **Is revenue growing fast and accelerating?**
4. **Are margins durable or under pressure?**
5. **Is it cheap relative to its growth?** (Avoid priced-for-perfection)
6. **Has it already run, or is it just starting to wake up?** (We want the latter)
7. **Are large funds adding or trimming this quarter?**

Plus a soft eighth: any near-term catalyst that could re-rate it (earnings beat, deal close, contract win)?

## What this system is *not*

Worth being honest about the limits:

- **It doesn't predict winners.** It surfaces well-positioned candidates based on a thesis the human has to commit to. The judgment that the thesis is correct is yours.
- **It doesn't time entries.** It picks names to hold for 12-24 months, not when to buy them this Tuesday.
- **It doesn't capture private winners.** A lot of the best companies in any thesis (e.g., the data-center cooling startups that just got acquired by industrial strategics) are private. Public-market vehicles are a constrained subset of the actual opportunity.
- **It can't be backtested cleanly.** Some of the scoring criteria are human judgments that didn't exist as data points when historical conditions were different. We can only validate the workflow over time as themes play out.

## The discipline

What makes this work over years instead of weeks is the rules everything operates under:

- **The thesis is the contract.** Once locked, you don't edit it. You either append a dated addendum acknowledging what changed, or you mark it as Failed and start over. The reason: a year from now, you need to be able to honestly assess whether you got it right or rationalized away your wrongness.
- **Falsifiers are mandatory.** Every thesis has to specify what would prove it wrong, with at least one "thesis right but vehicles wrong" scenario. (The most common way obvious-in-hindsight theses lose money is the bet is right but the public-market names you picked weren't the right way to express it.)
- **One or two themes active at a time.** Each theme needs deep attention. Spreading across five themes turns this into a screener, which defeats the point.
- **Real benchmarks recorded at the start.** Every thesis logs the price of SPY, the sector ETF, and the key candidates on the day it's locked. A year later we compare honestly. A thesis that worked but underperformed SPY is not actually a win.

---

_Build a thesis you can defend, map what it needs, find the public-market suppliers, score them on a few simple traits, watch a handful of names over a long horizon, and let the system tell you cleanly if you were right or wrong._
