# Pre-registration: profit-protecting exits for Ignition Watch (2026-09-26)

Written BEFORE any new rule was evaluated. Only the two reference rules
(base_fail, hold252) were run, to check the engine against the published study.

## Question (operator)
"Are we sure the sell is only after it's given back all its gains? Can there be
a logic to exit with some positive still left if X days have passed? I'm worried
we only sell mainly when the gains are gone."

## Fact established before testing
The current sell line (close < pre-ignition base) sits a median 13.9% BELOW the
entry price (90th pct: -10.7%). So every sell under the current rule is a loss by
construction; the only profitable exit is the 252-session age-out. Of 334 sells
in the 748-trade study, 58 (17%) had been up >= +20% first (avg peak +41%) and
sold at -18.6% on average.

## Data / protocol (unchanged from research/ignition_exits/)
748 trades; S&P 500+400 fires 2016-2026; buy next open; close-triggered exits
sell at the next open; max hold 252. Splits: TRAIN = entries 2016-2021 (477),
HOLDOUT = 2022 through the last complete 252-session window (200),
LIVE = later entries, still open/marked to market (71).
The current rule (base_fail) stays active as a floor under every candidate.

## Reference (train): base_fail mean +36.7%, median +20.6%, worst10 -18.8%,
roundtrip20 = 35 (trades up >= 20% at some point that exited at a loss).

## Selection rule (TRAIN only)
A candidate PASSES if, versus base_fail on train:
- G1 mean >= +33.7% (gives up at most 3.0 pp of mean return), AND
- G2 roundtrip20 <= 17 (at least halves the trades that round-trip a +20% gain into a loss), AND
- G3 worst10 >= -20.8% (tail no more than 2 pp worse).
Among passers, rank by TRAIN MEDIAN return (the typical trade); tie-break by
train mean. The single top-ranked rule across all families is the LOCKED WINNER.
If nothing passes, the answer is "no rule met the bar" and we report the
closest candidates as exploratory only.

## Holdout confirmation (one shot, after the winner is locked)
The locked winner is CONFIRMED only if, on HOLDOUT versus base_fail:
- H1 median > base_fail median (-12.2%), AND
- H2 mean >= base_fail mean - 5.0 pp (>= +28.7%), AND
- H3 roundtrip20 < base_fail's (31).
LIVE is reported, never used to decide. Runners-up get holdout numbers too,
labelled exploratory: we do not switch to a runner-up because it did better
on holdout.

## Multiple testing
Each family declares at most 12 variants up front and reports ALL of them.
Total variants tested is reported with the result.

## Known biases (carried over)
Survivorship bias (current index members) flatters holding and understates the
value of any sell rule. No costs/slippage. Treat results as relative, not absolute.
