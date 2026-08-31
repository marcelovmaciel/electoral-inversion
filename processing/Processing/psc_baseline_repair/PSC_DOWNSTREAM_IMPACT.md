# PSC downstream impact

## Bottom line

The cabinet-source repair changes the empirical role of the 2018-linked Bolsonaro mandate. Adding PSC contemporaneously for Gilson Machado changes exactly 476 daily cabinet sets (9 December 2020 through 29 March 2022), creates two additional period boundaries, and converts two previously negative configurations into observed inversions. The separately adjudicated 2018 PSC seat count remains seven, so election totals and ideological-interval results do not change.

The complete overlap-level comparison is in `observed_cabinet_old_vs_new.csv`; candidate-level seat evidence is in `psc_2018_elected_candidates.csv`.

## Source and period changes

- Old authoritative cabinet series: 22 periods.
- Corrected series: 24 periods.
- Daily membership changes: 476 days; every changed day is exactly `+PSC` and no other daily party membership changes.
- PSC applies from 2020-12-09 through 2022-03-29 inclusive.
- PL applies to Gilson's final ministerial day, 2022-03-30, but PL was already present through another minister and therefore does not change the aggregate cabinet set that day.
- The corrected series has no gaps, overlaps, duplicate identifiers, or adjacent identical party sets.

Ten old/new interval overlaps are affected by membership, boundary, or period-label changes. Six have PSC added and metrics recomputed; four record boundary or label consequences without a party-set change. No 2014-linked or 2022-election-linked cabinet day changes.

## New 2018 observed inversions

| Period | Dates | Days | Vote share | Seats | q_C | d_C | r_C | R_C |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2021.3 | 2021-08-04--2022-02-07 | 188 | 47.2469% | 257 | 242.3763 | 14.6237 | 14.6237 | 1.0603 |
| 2022.1 | 2022-02-08--2022-03-29 | 50 | 47.2469% | 257 | 242.3763 | 14.6237 | 14.6237 | 1.0603 |

Both translated election-year party sets are `DEM, PATRIOTA, PP, PR, PRB, PSC, PSD, PSDB, PSL`. Relative to the old baseline, each gains 1,719,754 votes and seven seats: votes rise from 44,706,982 to 46,426,736; vote share from 45.4967% to 47.2469%; and seats from 250 to 257. They remain vote minorities and become exact seat majorities.

The corrected sole closest 2018 negative is period 2022.2, 30--31 March 2022: 45.4967% of the vote, 250 seats, `q_C=233.3982`, `d_C=16.6018`, `r_C=23.6018`, and `R_C=1.0711`.

## Aggregate observed-cabinet results

- Observed inversion periods: three to five.
- Corrected list: 2014/2016.2; 2014/2017.1; 2018/2021.3; 2018/2022.1; 2022/2023.1.
- 2018 inversion periods: zero to two.
- 2018 inversion days: zero to 238 (188 plus 50).
- Other mandate duration totals remain 102 inversion days after 2014 and 255 in the available 2022-linked window.
- Figure 2 now contains 24 periods. Twenty-one have `R_C>1`; the three exceptions are 2018/2020.2, 2018/2020.4, and 2018/2021.1.

## Cabinet-to-ideological-interval bridge

The bridge is regenerated for all 24 periods. The new 2018 inversions are sparse right-side cabinets whose connected closure is PSD--DEM: 17 parties, eight gaps, 57.37% of the vote, and 292 seats. Their closest minimal connected winning interval remains PSDB--DEM (51.52%, 257 seats). No 2018 minimal connected inversion exists.

## Ideological interval results

They are unchanged. The old/new hash audit is identical for every party seat-differential, ideology order, ideological interval, interval inversion, minimal inversion, heatmap-data, and ideological summary artifact.

| Election | Ideological inversions | Minimal inversions |
|---|---:|---:|
| 2014 | 8 | 4 |
| 2018 | 0 | 0 |
| 2022 | 6 | 2 |

This invariance follows from the independent seat adjudication: the project already used the final retotalized 2018 allocation of PSC 7 and PT 56, so no party seat moved in the repaired pipeline.

## Artifact and manuscript impact

Across the existing paper artifact inventory, 34 artifacts are hash-identical, 16 change, and one new machine-readable Figure 2 data file is added. Changed artifacts are confined to cabinet membership, cabinet metrics, duration/inversion summaries, cabinet translation/bridge diagnostics, and tables derived from observed cabinet composition.

`manuscript_stale_claims.csv` records 20 stale empirical claim locations plus three stale generated/rendered manuscript assets. The manuscript and its assets were deliberately not modified.

## Scope guard

No decomposition quantity, table, focal-case choice, manuscript prose, theoretical framing, election loader, party alias, ideology source, or ideological algorithm was changed. The existing runner's focal-case artifact was regenerated as part of the old analysis contract; it is not an approved decomposition-case selection.
