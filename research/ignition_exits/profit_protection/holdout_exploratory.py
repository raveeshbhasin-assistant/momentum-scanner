"""
EXPLORATORY holdout look (pre-registered outcome: no rule passed G1-G3 on train,
so there is no locked winner and nothing here can be "confirmed").

Candidates declared BEFORE this file was run, one per idea, chosen from train
results only:
  A10  after 126 sessions, sell if 0 < gain < +30%   (operator's literal idea, closest to passing)
  C09  once up +30%, sell after giving back 50% of the peak gain  (lock half the gain)
  E05  sell 1/3 at +30%, 1/3 at +60%, let 1/3 run   (best train median; scale-out)
  C05  once up +50%, 25% trailing stop               (only family member that raised the mean)
"""
import family_A as FA
import family_C as FC
import family_E as FE
import engine as E

rules = {
    "base_fail (live rule)": (E.never, True),
    "hold 252": (E.never, False),
    "A10 126d if 0-30% up": ({n: r for n, _, r in FA.VARIANTS}["A10_after126_band0_30"], True),
    "C09 keep half of gain once +30%": (FC.make_rule(*[v[1:] for v in FC.VARIANTS if v[0] == "C09_give_g30_f50"][0]), True),
    "E05 thirds at +30/+60%": ({n: r for n, _, r in FE.VARIANTS}["E05_thirds@30/60"], True),
    "C05 25% trail once +50%": (FC.make_rule(*[v[1:] for v in FC.VARIANTS if v[0] == "C05_trail_g50_w25"][0]), True),
}
cols = ("mean", "median", "win", "worst10", "roundtrip20", "held", "big_capture", "post_exit_fwd63")
for split in ("train", "holdout", "live"):
    print(f"\n==== {split.upper()}")
    print(f"{'rule':34s} " + " ".join(f"{c[:10]:>10s}" for c in cols))
    for name, (fn, fl) in rules.items():
        m = E.evaluate(fn, floor=fl, splits=(split,))[split]
        print(f"{name:34s} " + " ".join(f"{m[c]:>10.3f}" if isinstance(m[c], float) else f"{m[c]:>10d}" for c in cols))
