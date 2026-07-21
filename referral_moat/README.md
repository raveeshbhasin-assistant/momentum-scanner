# Referral-Moat Research

**Thesis under test:** companies whose customers recruit their next customers
(high referral / word-of-mouth acquisition) grow faster and more profitably
than peers, and that quality eventually shows up in stock returns.

**Theory purity rule:** companies are scored on fundamentals only. Price
returns are computed in a separate pass and attached as an *evaluation*
output. Nothing in the score can see a stock price. (Enforced in
`build.py` — the scoring path has no access to price data.)

## Why second-order signals?

No company reports "referral rate" in its financial statements. NPS, when
disclosed, is self-selected and unaudited. But referral-driven acquisition
leaves *mechanical fingerprints* in the statements, because a referred
customer is (a) nearly free to acquire, (b) higher-intent, (c) faster to
convert, and (d) better-retained. Those four properties each map to a line
item:

| Referral property | Financial fingerprint | Metric used |
|---|---|---|
| Nearly free to acquire | New revenue arrives without proportional S&M spend | **Growth efficiency** ("magic number"): ΔRevenue ÷ S&M |
| Compounds as base grows | Sales intensity falls while growth holds | **S&M % of revenue** — level *and* slope |
| Better-retained, repeat-buying | Growth is steady, not bought in bursts | **Growth persistence**: 3y avg growth, penalized for volatility |
| Higher willingness to pay, no discounting needed | Pricing power | **Gross margin** — level and trend |
| Acquisition cost doesn't scale with revenue | Incremental dollars are fatter than average dollars | **Operating leverage**: ΔOpInc ÷ ΔRev |
| The whole engine self-funds | Growth + cash generation together | **Rule of 40**: revenue growth + FCF margin |

Your starting intuition (revenue growth, client growth, CAC, SG&A % of
revenue) is exactly right — this framework just makes each observable from
public statements:

- **CAC** is not disclosed, but *S&M ÷ new revenue* (inverse of the magic
  number) is the statement-level shadow of blended CAC.
- **Client growth** is disclosed only sporadically (sub counts, members,
  monetized users), so it lives in the qualitative layer, not the score.
- **SG&A % of revenue** is used, but where available we prefer the pure
  **Selling & Marketing** line — G&A pollutes the signal with overhead
  that has nothing to do with acquisition.

### Signals considered and deliberately excluded

- **Advertising expense** (10-K footnote): excellent signal (Tesla, Costco
  famously ≈ zero) but not machine-readable from our sources — captured
  qualitatively.
- **Deferred revenue growth**: prepayment is commitment, but it conflates
  billing-terms changes with love-of-product; noisy across industries.
- **DSO trend**: too dominated by industry billing norms.
- **NPS / app ratings / search-trend share**: strong external signals, no
  reliable free historical source; noted as a future extension.

## Scoring — the Referral Economics Score (RES)

Each pillar is percentile-ranked **within its industry group** (a 70% gross
margin means nothing across software vs. retail), then blended:

| Pillar | Weight |
|---|---|
| Growth efficiency (ΔRev ÷ S&M, 2y avg) | 25 |
| Sales intensity (S&M% level + slope) | 20 |
| Growth persistence (3y avg, volatility-penalized) | 20 |
| Gross quality (margin level + trend) | 15 |
| Operating leverage (incremental margin, 2y avg) | 10 |
| Rule of 40 (growth + FCF margin) | 10 |

Missing pillars (e.g. insurers without gross profit) renormalize the
weights; a `coverage` field records how much of the scorecard was
measurable. RES is 0–100, higher = stronger referral economics.

### The flywheel gate (picks)

RES ranks quality; **picks** additionally require evidence that the company
is *growing, and growing because of the referral fingerprint* — all three,
from fundamentals only (thresholds in `build.py::FLYWHEEL`):

1. **Growing** — latest revenue growth ≥ 8% and 3y average ≥ 10%.
2. **Efficient acquisition** — magic number (2y avg) ≥ 1.0: each S&M
   dollar returns at least $1 of new annual revenue.
3. **Intensity not rising** — S&M% slope ≤ +0.5pp/yr (flat or falling
   while growing = customers arriving on their own); if no slope is
   measurable, S&M% must be at/below the group median.

Passers (excluding control groups, coverage ≥ 50%) form the `picks` list,
frozen into each dated snapshot **before** the returns pass runs.

## Universe

~130 top listed companies across 10 industry groups where word-of-mouth
plausibly matters, **plus two control groups** (Energy; Telecom & Cable)
where it shouldn't. If RES predicts returns equally well in the controls,
the score is probably just measuring "good business," not referral
economics specifically. Names were chosen by industry prominence — never
by past performance.

## Data sources & pipeline

- **yfinance** (Yahoo Finance): 5y annual income/cash-flow statements —
  includes the separate *Selling And Marketing Expense* line for many
  companies; monthly adjusted prices for the returns pass.
- **Finnhub** (as-reported SEC filings, up to ~17y) — extension path for
  deeper history and advertising-expense footnotes.
- `python build.py` → `data/scorecards.json` + a dated snapshot in
  `data/snapshots/` (the forward-tracking record: future runs compare
  today's scores against future returns, which is the *real* test).
- `python make_site.py` → `site/index.html`, the self-contained app.

## Deployment & refresh

Served by the **themes_web Railway service** at `/referrals` (static file,
no imports — themes_web only reads `site/index.html`). The themes_web
scheduler rebuilds everything **monthly** (1st, 19:00 ET) via subprocess;
manual trigger: `POST <themes_web-domain>/api/refresh_referrals` (~3–6 min).
`data/` + `site/` are committed as the deploy-time seed — Railway's
filesystem is ephemeral, so runtime snapshots persist only until the next
deploy; committing after a local refresh keeps the snapshot record durable.

## Honest caveats

- The 3y-alpha "theory check" in the app is a **concurrent association**,
  not a forward test — score and return windows overlap. The forward test
  starts with the first snapshot (2026-07-20) and accrues from here.
- 5 years of statements ⇒ trend slopes use ≤4 points. Deeper history via
  Finnhub as-reported is the first upgrade.
- S&M falls back to SG&A where not broken out (flagged `sm_is_sga`), which
  disadvantages nobody within a group as long as peers report similarly —
  but check the flag before comparing across groups.
- Survivorship: the universe is today's prominent names; historical
  returns of today's leaders are upward-biased. Another reason the
  forward snapshots are the test that counts.
