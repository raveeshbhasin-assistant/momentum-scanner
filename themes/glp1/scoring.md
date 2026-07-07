# GLP-1 Mass Adoption — Scoring Notes

**Not yet run.** First scoring pass happens after the universe is wired into `refresh_data.py` and `themes/refresh_data.py glp1` is executed.

## Scoring wrinkle for a short-weighted theme
The standard theme scoring ranks a universe on momentum (1M/3M/6M/12M returns) + capital-structure quality. That logic assumes LONG candidates. For this theme:

- **LONG candidates (Bucket 4):** score normally — strong momentum + clean CS = higher rank.
- **SHORT candidates (Buckets 1-3):** the read inverts. What we want is names showing early *volume/margin deterioration* while still trading at full multiples (i.e., market hasn't re-priced yet — maximum short asymmetry). A short candidate that has *already* de-rated hard is less interesting (thesis partly played out); one still near 52w highs on a deteriorating volume story is the prize.

This inversion is a manual scoring overlay for now — the automated `_score_run.py` (if added later) treats the universe as long-only and the analyst flips the sign on Bucket 1-3 names when reading the output. Flagged for a future `side`-aware scoring enhancement.

## Borrow-cost / liquidity gate
Because shorts dominate, any candidate that fails a liquidity threshold (thin ADV, hard-to-borrow, high borrow fee) is disqualified regardless of thesis fit. This is why the universe is strict US-listed and the European CDMOs are excluded.
