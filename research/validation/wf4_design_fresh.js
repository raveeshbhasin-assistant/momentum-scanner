export const meta = {
  name: 'design-fresh-momentum-algo',
  description: 'Three independent designers (event-driven quant, execution/risk practitioner, statistician-skeptic) propose a fresh short-horizon momentum algorithm from the problem statement and the evidence; a judge synthesizes a build spec and a registered rule family',
  phases: [
    { title: 'Design', detail: '3 independent proposals' },
    { title: 'Judge', detail: 'score, synthesize, register' },
  ],
}
const SP = 'C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad'
const REPO = 'C:/dev/Trader-v3'

const BRIEF = `
PROBLEM STATEMENT (operator's words): "Start afresh from an algo perspective and not start with what we have done before. The problem statement is really pick up trades early, when they are building momentum, and sell them when they are above a threshold or technical or made a fixed return. The objective remains to have sustainable trades that have entry and exit and make money. Perhaps the daily-out requirement isn't critical but this isn't a long-hold strategy."
Operator context: individual trader, US equities, long-only in practice (shorting not assumed), retail costs (assume 5 bp/side in liquid names, 10 bp in $20-100M ADV names), executes through a broker (market-on-close and opening orders possible), one person's attention: at most ~5-10 positions.

EVIDENCE YOU MUST RESPECT (all verified in this project; do not re-propose what it rules out):
- Intraday momentum continuation in liquid US large caps at 5-minute granularity is negative after costs from every bar (753k bars, 60 days); the live scanner built on it lost -0.23R/trade over 100 days; exits, timing tweaks, payload features, multi-day context and prior-day regime features carried no out-of-sample information. Read ${REPO}/research_findings_100day_review.md sections 2, 3 and 6 (one Read call).
- The same intraday continuation on the day's 20 biggest gap-ups (broad universe) is negative in both discovery and test windows; a 2-year multi-day continuation family (gap continuation 3d, 20d breakout with RS 5d, big-gap drift 5d, entered at T+1 OPEN) underperforms SPY by 0.15-1.9% per trade. Read ${REPO}/research_plan_v4_inplay.md section 6 (one Read call).
- NEW exploratory decomposition (2024-09..2026-09, 2,700 names, SPY-adjusted, gross), file ${SP}/research/explore_overnight_decomp.py and .csv.gz: after a big-catalyst day (gap>=8%, volume>=3x ADV20, green close; n=504) the OVERNIGHT leg (close T -> open T+1) is +0.64% [+0.05,+1.24] while the next intraday session is +0.02% and the 5-day close-to-close is -1.25%; gap-up-held days: overnight +0.19%, next intraday -0.22%; top-20 movers on volume: +0.08% / -0.04%; all names +0.01% / -0.01%. Positive in 5 of 8 quarters; 2024 negative. This matches the published pattern that individual-stock momentum accrues overnight while intraday returns reverse (Lou, Polk & Skouras 2019) and post-earnings drift.
- A pre-registered holdout test of four overnight event rules on 2014-2024 daily data is in ${SP}/research/preregistration_overnight.json; results, if present, are in ${SP}/research/OVERNIGHT_holdout_cells.csv and OVERNIGHT_holdout_report.md. Use them if they exist.
- Data available now: 2 years daily bars for 2,814 liquid US names (${SP}/bars1d_all), 10 years for the same list (${SP}/bars1d_10y, if fetched), 60 days of 5-min bars for 236 large caps and 929 gappers, SPY/QQQ/sector ETFs/VIX. Not available locally: earnings calendar (FMP key exists only in production), news content classification (Finnhub headlines exist in the repo's news.py; an LLM classifier would be new), order flow.

DISCIPLINE: any proposed rule must be pre-registered with explicit thresholds before its test window is touched; temporal or holdout split; day/quarter-cluster CIs; costs on the trade's own risk; benchmark = SPY over the identical leg and matched non-event names; things-tried log; shadow >= 60 days before display; power stated (at overnight return sd ~3%, n=225 events gives a 0.2% standard error).
`

const PROPOSAL_SCHEMA = {
  type: 'object',
  properties: {
    lens: { type: 'string' },
    thesis: { type: 'string', description: 'where the continuation you intend to harvest actually comes from, in 3-5 sentences' },
    algorithm: { type: 'object', properties: {
      universe: { type: 'string' }, event_or_signal_definition: { type: 'string' }, entry_timing_and_order_type: { type: 'string' },
      exit_rules: { type: 'string', description: 'fixed-return, technical and time exits, with numbers' }, position_sizing_and_risk: { type: 'string' },
      ranking_and_capacity: { type: 'string' }, data_required: { type: 'string' }, daily_operations: { type: 'string' } }, required: ['universe', 'event_or_signal_definition', 'entry_timing_and_order_type', 'exit_rules', 'position_sizing_and_risk'] },
    preregistered_rules: { type: 'array', items: { type: 'object', properties: { id: { type: 'string' }, definition: { type: 'string' }, exit: { type: 'string' }, test_window: { type: 'string' }, pass_criterion: { type: 'string' } }, required: ['id', 'definition', 'exit', 'test_window', 'pass_criterion'] } },
    expected_economics: { type: 'string', description: 'per-trade gross and net, trades per day, drawdown character, with the evidence basis' },
    biggest_risks: { type: 'array', items: { type: 'string' } },
    what_would_kill_it: { type: 'string' },
    build_steps: { type: 'array', items: { type: 'string' } },
  },
  required: ['lens', 'thesis', 'algorithm', 'preregistered_rules', 'expected_economics', 'biggest_risks', 'what_would_kill_it', 'build_steps'],
}

const JUDGE_SCHEMA = {
  type: 'object',
  properties: {
    scores: { type: 'array', items: { type: 'object', properties: { lens: { type: 'string' }, evidence_fit: { type: 'integer' }, tradeability: { type: 'integer' }, validation_rigor: { type: 'integer' }, originality_vs_prior_work: { type: 'integer' }, notes: { type: 'string' } }, required: ['lens', 'evidence_fit', 'tradeability', 'validation_rigor'] } },
    synthesized_design: { type: 'string', description: 'the single recommended algorithm, complete: universe, event definition, entry, exits, sizing, capacity, data, operations' },
    registered_family: { type: 'array', items: { type: 'object', properties: { id: { type: 'string' }, definition: { type: 'string' }, exit: { type: 'string' }, test_window: { type: 'string' }, pass_criterion: { type: 'string' } }, required: ['id', 'definition', 'exit', 'test_window', 'pass_criterion'] } },
    build_plan: { type: 'array', items: { type: 'string' } },
    open_questions_for_operator: { type: 'array', items: { type: 'string' } },
    rejected_ideas_and_why: { type: 'array', items: { type: 'string' } },
  },
  required: ['scores', 'synthesized_design', 'registered_family', 'build_plan', 'open_questions_for_operator'],
}

const LENSES = [
  { key: 'event_quant', budget: 8, text: 'LENS: EVENT-DRIVEN QUANT. Design the algorithm around catalysts and the overnight/post-event drift (earnings, guidance, analyst actions, M&A, contract news), with cross-sectional ranking. Be specific about how to detect the event at or before the close, how to distinguish earnings from other gaps without an earnings API locally (and with FMP in production), and how the exit menu (fixed return, technical, time) should differ by event type. Use the decomposition numbers as your quantitative anchor.' },
  { key: 'execution_risk', budget: 8, text: 'LENS: EXECUTION AND RISK PRACTITIONER. Assume the overnight-after-catalyst effect is real and design so that a retail trader can actually capture it: order types (MOC vs 15:50 limit), realistic fills and costs, gap risk and position sizing, halts, avoiding holding into the next binary event, capacity in small names, how the exit executes at the open (opening auction, first 15 minutes), tax/wash considerations only if material. Quantify what fraction of the gross edge survives each friction.' },
  { key: 'skeptic_stats', budget: 8, text: 'LENS: STATISTICIAN-SKEPTIC. Assume the decomposition is a regime artefact until proven otherwise. Design the validation that would convince you: holdout choice, survivorship handling for a 10-year symbol list, event-clustering (many events on the same day are one bet), power at realistic event counts, regime splits (2020-21 vs the rest), the benchmark set (SPY overnight, matched non-event names, random names with the same day return), and the kill rules. Then propose the smallest rule family worth registering and what result would make you recommend live shadow.' },
]

phase('Design')
const proposals = (await parallel(LENSES.map(l => () => agent(`You are one of three independent designers hired to start a short-horizon equity momentum algorithm afresh. TOOL-CALL BUDGET: ${l.budget} (plan first; read only the files named; no data crunching beyond reading existing result files; one Write of your proposal to ${SP}/research/DESIGN_${l.key}.md). Return the structured proposal.\n${BRIEF}\n${l.text}`, { label: `design:${l.key}`, phase: 'Design', schema: PROPOSAL_SCHEMA })))).filter(Boolean)

phase('Judge')
const judge = await agent(`You are the JUDGE and synthesizer. TOOL-CALL BUDGET: 8. Below are three independent proposals for a fresh short-horizon momentum algorithm, written against the same brief and evidence. Score each (1-5) on evidence fit, tradeability for one retail operator, validation rigor and originality versus this project's failed prior work; then synthesize ONE recommended design (take the best elements, resolve conflicts explicitly), a registered rule family of at most 6 cells with explicit thresholds, test windows and pass criteria, an ordered build plan (data, tests, shadow, production), open questions for the operator, and the ideas you rejected with reasons. Write ${SP}/research/DESIGN_synthesis.md. If ${SP}/research/OVERNIGHT_holdout_cells.csv exists, read it and let it shape the recommendation (a failed holdout must be reflected honestly).\n${BRIEF}\n\nPROPOSALS:\n${JSON.stringify(proposals, null, 1)}`, { label: 'judge', phase: 'Judge', schema: JUDGE_SCHEMA })

return { proposals, judge }
