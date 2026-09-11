# Cabinet V5 analytical handoff

This is downstream electoral analysis, not a historical evidence archive.

**Transformation:** V5 historical daily party set -> audited election-year party mapping -> maximal consecutive analytical recompression -> electoral quantities.

The source is `cabinet_dataset/releases/2026-03-19-history-v5-candidate`. Historical identities and all historical UNKNOWN records remain unchanged. The 2014, 2018 and 2022 elections apply respectively to 2015-2018, 2019-2022 and 2023-2026-03-19. The audited crosswalk is unchanged. DEM/PSL -> UNIÃO maps to DEM+PSL for 2018; renames retain election-year identities. No historical reconstruction runs in the paper pipeline.

There are **53 analytical periods** and **2 primary inversions**. All 4,096 dates have primary sets: 3,996 established and 100 provisional. The 100 days rely on separate no-additional-party assumptions for nine historically unresolved people, not evidence of non-affiliation. Both inversions are entirely evidence-established. Period status is established, mixed or provisional; daily historical_status retains V5's established/unidentified meaning while primary_set_status records primary_adjudicated/primary_provisional.

- `cabinet_analysis_daily.csv`: exactly one row per date, historical display labels/stable IDs, election parties, provenance and all quantities.
- `cabinet_analysis_periods.csv`: authoritative maximal periods, stable CV5 IDs plus compatibility year.sequence labels, full quantities and evidence counts.
- `cabinet_inversions.csv`: primary inversion registry and recorded-concrete-sensitivity robustness.
- `cabinet_sensitivity_results.csv`: each nonbaseline concrete alternative, recompressed over its affected window. Blank affected-party fields mark composition-neutral or unbounded records, not invented parties.
- `cabinet_sensitivity_periods.csv`: complete recompressed calendar for every scenario. Cases are changed one at a time. All comparisons change one released fact at a time and hold all other facts at their primary values. Unknown affiliations remain unbounded.
- `cabinet_analysis_before_after.csv`: interval-overlap comparison for BOTH the 23-period manuscript narrative and the 33-period preintegration generated baseline. Do not add repeated old/new durations across overlap rows. Old unavailable days have blank quantities/status, never false inversion flags. Changed memberships are diagnosed using V5 historical comparison decisions; date-boundary differences are recorded separately. No new party mapping is introduced.
- `historical_analytical_linkage.csv`: exact historical starts, exclusive ends and overlaps underlying each analytical period.
- `cabinet_timeline_source.csv`, `figure5_panel_a_source.csv`, `figure_data/`, `tables/`, `figures/`: reviewable figure data and generated assets.
- `CABINET_INVERSION_ROBUSTNESS.md`, `quantitative_validation.csv`, `noncabinet_invariance.csv`, `cabinet_dependency_inventory.csv`: outcome and verification records.
- `cabinet_code_dependency_search.csv`: repository-wide code references, separated from historical audits and tests; maintained asset producers are in the dependency inventory.
- `manuscript_required_changes.csv` and root `CABINET_V5_MANUSCRIPT_MODIFICATIONS.org`: editing checklist, with no replacement prose. The PDF intentionally retains stale prose.
- `v5_provenance/`: compact release metadata/assumptions/constraints, without raw evidence snapshots.

Shares are proportions, not percentages; A, B, q and d use seats. R is dimensionless. An inversion requires vote share < 0.5 and seats >=257. Dates are ISO; end_exclusive is half-open. Semicolons separate parties/IDs; historical_party_sets is a JSON array. Repeated periods with the same set at nonconsecutive dates remain separate.

Reproduce from the repository root:

```bash
JULIA_BIN=processing/julia_paper_runtime.sh processing/rebuild_manuscript.sh --freeze-prose
```

`processing/Processing/data/cabinet_release_pin.json` is the single release selector. Reproduction validates hashes, runs the existing Julia election/decomposition pipelines, independently checks exact district/party closure, regenerates all figures/tables, compiles both PDFs and creates the reports/ZIP. Manuscript prose is never changed. The checked-in comparison inputs/signatures are under `processing/Processing/data/cabinet_v5_comparison_baseline/`.
