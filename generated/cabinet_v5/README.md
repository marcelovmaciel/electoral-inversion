# V5 chronology and compatibility assets

The current analytical unit is the distinct election-year cabinet party set.
Read [the current registry documentation](../cabinet_party_sets/README.md) and
[the migration report](../../CABINET_PARTY_SET_MIGRATION_REPORT.md).

This directory retains the unchanged V5 daily reconstruction and dated electoral
analysis: 4,096 days, 3,996 established and 100 provisional; 55 historical
periods become 53 translated analytical periods. The periods are chronology,
not the primary sample. Grouping their translated memberships across the full
election window gives 34 sets and the same two inverted sets/260 inversion days.
Historical UNKNOWN affiliations and all primary assumptions keep their meanings.
No history builder runs in manuscript production.

- `cabinet_analysis_daily.csv`: every primary date, membership, evidence and electoral vector.
- `cabinet_analysis_periods.csv`, `historical_analytical_linkage.csv`: maximal dated periods and source links.
- `cabinet_inversions.csv`, `cabinet_timeline_source.csv`: temporal inversion and full chronology products.
- `cabinet_sensitivity_results.csv`, `cabinet_sensitivity_periods.csv`, `sensitivity_register.csv`: preserved date-level comparisons and uncertainty records. Scenario set registries are separate under `generated/cabinet_party_sets/`.
- `tables/`, `figures/`, `figure_data/`, `figure5_panel_a_source.csv`: compatibility mirrors refreshed from current set-based analytical outputs. Figure 2's historical filename is retained; its current PDF is a point comparison of distinct sets.
- `cabinet_analysis_before_after.csv`, old manuscript change checklists and dependency inventories: historical V5 integration records. They do not describe the current revised manuscript.
- `provenance.json`, `v5_provenance/`, `quantitative_validation.csv`, `noncabinet_invariance.csv`: release and validation metadata.

The release pin and audited crosswalk remain unchanged. Shares are proportions;
A, B, q and d use seats; R is dimensionless. A strict vote minority with at least
257 seats is an inversion. Dates use [start_inclusive,end_exclusive). Full
recurrences remain separate temporal intervals even when they share a set ID.

Run the complete validated workflow from the repository root:

```bash
JULIA_BIN=processing/julia_paper_runtime.sh processing/rebuild_manuscript.sh
```

The current manuscript source is revised in place, with literal numerical values
and validated adjacent provenance. Root `CABINET_V5_INTEGRATION_REPORT.md` and
`CABINET_V5_MANUSCRIPT_MODIFICATIONS.org` are preserved historical documents;
the current report and upload are `CABINET_PARTY_SET_MIGRATION_REPORT.md` and
`handoff/cabinet_party_sets_handoff.zip`.
