# Literal prose and local CSV provenance migration

## Scope and preserved state

The active manuscript is `writing/submission_inversions_review/manuscript/main_rw_again.tex`.
The migration preserves the user's pre-existing working-tree edits. It replaces
**295 occurrences of 232 distinct empirical prose macros**, adds **53 adjacent
paragraph provenance blocks**, and retires **234 definitions** (including two
already unused definitions). All occurrences were ordinary prose, including the
abstract and appendices. None occurred inside a generated table or figure/caption.
No value was selected from a macro expansion, manuscript text, old report, or PDF.

Before replacements, every former macro was traced independently through the
production selection and display rules to current CSV observations. Each key
was checked for uniqueness; raw values and displays were then compared to the
old registry and macro definitions. This complete mapping was finished before
editing the manuscript. `PROSE_MACRO_INVENTORY.csv` preserves it, including the
full list of contributing observations for multi-row aggregates. All row numbers
are **1-based data rows, excluding the header**.

## Retired and retained architecture

The `Ideology...`, `Cabinet...`, `Party...`, and `District...` prose macro families
are retired in their entirety. Their `Words`, percent, seat, ratio, differential,
component, duration, summary, and empirical-label values are now literal text.
`ManuscriptValues.jl`, `ManuscriptValueSupport.jl`, the generated
`manuscript_values.tex` copies, and the old `manuscript_values.csv` registry are
removed from production, synchronization, manifests, and submission packages.

`ProseSummaries.jl` preserves only 32 existing raw aggregate/intermediate
statistics. It writes `prose_analysis_summaries.csv`, without macro names,
formatting rules, TeX output, or source injection. Its semantic key is
`summary; metric; aggregation`. Upstream CSV paths, filters, and selected-row
counts remain in that CSV. Direct observations are cited directly in their
original CSVs. This is an output-summary export, not a new electoral analysis.

All generated tables and figures retain their analysis code and existing
inclusion paths. Retained manuscript macros are notation/formatting or fixed
institutional definitions: `Votes`, `Seats`, `Quota`, `Diff`, `Parties`,
`Coalition`, `ChamberSeats`, `MajoritySeats`, and the existing column/theorem
and package facilities. No generated empirical macro has a remaining structured
output dependency. The historic `Acct...` references in older drafts and the
frozen submission are outside the active manuscript; those pre-existing drafts
and archives are not publication inputs and were not rewritten.

## Provenance validation

`validate_prose_provenance.py` validates sources, uniquely resolving semantic
keys, all recorded CSV fields, strict numeric/string value agreement, closed and
well-formed blocks, and optional paragraph-local display strings. Numeric
comparison permits equivalent decimal spellings but has no rounding tolerance.
CSV row numbers are diagnostic only; reordering produces warnings and reports
the newly located row. No semantic lookup depends on row position.

The validator is integrated into `audit_empirical_assets.py`, which now defaults
to the active `main_rw_again.tex`. The full `processing/rebuild_manuscript.sh`
workflow invokes that final audit after PDF compilation and before packaging;
`writing/package_submission_assets.py` also validates provenance before writing
archives. The audit writes every current row and field to
`output/decomposition/audit/manuscript_prose_provenance.csv`.

The optional `display` check is intentionally just a bounded literal match with
normalized whitespace. It does not infer prose meaning, selection rules, or
formatting. Future substantive changes still require reviewing the sentences.

## Authoritative CSVs referenced by manuscript blocks

- `processing/Processing/output/decomposition/raw/accounting_all_inversion_decomposition.csv`
- `processing/Processing/output/decomposition/raw/coalition_period_quantities.csv`
- `processing/Processing/output/decomposition/raw/party_accounting_all_years.csv`
- `processing/Processing/output/decomposition/tables/report/party_fragmentation_summary.csv`
- `processing/Processing/output/decomposition/tables/report/party_size_cabinet_summary.csv`
- `processing/Processing/output/decomposition/tables/report/party_size_groups.csv`
- `processing/Processing/output/paper/raw/ideology_k_gap_accounting_both_universes.csv`
- `processing/Processing/output/paper/raw/observed_cabinet_duration_summary.csv`
- `processing/Processing/output/paper/tables/ideological_universe_comparison.csv`
- `processing/Processing/output/paper/tables/prose_analysis_summaries.csv`
- `processing/Processing/output/paper/tables/table_appendix_cabinet_interval_bridge.csv`

The summary CSV additionally records its upstream district-accounting,
cabinet-period linkage, cabinet concentration, ideological, and party/cabinet
accounting sources. Full contributing-row keys and values at migration are in
the historical inventory.

## Discrepancies and limits

No macro has ambiguous or missing source provenance. The independent pre-edit
CSV/macro comparison found no numerical discrepancies. Prose and generated
table CSVs agree; the existing two-decimal prose versus three-decimal table
component displays and closure-preserving table rounding are retained.
For example, the primary 2022 MDB--UNIÃO components appear as 2.95 and 9.55
in prose and 2.953 and 9.554 in its generated table. No precision was changed.

Normalized expanded manuscript text was compared before and after migration and
is identical. All migrated paragraphs have adjacent provenance; tables and
figures receive no new paragraph blocks simply for containing empirical values.

## Validation results

- Python decomposition/provenance suite: 19 tests passed.
- Python writing/figure/table/packaging suite: 13 tests passed.
- Full decomposition/accounting/summary suite: **1,329 Julia assertions passed**,
  including 149 raw-summary and independent table-source assertions.
- The initial full rebuild using the default Julia 1.12.2 produced a last-bit raw
  mean difference (0.018508552895346038 versus 0.01850855289534604 for 2018
  never-cabinet parties). Its existing 1.85-percent prose display did not change.
  The strict final audit correctly failed rather than accepting this drift.
  Four pre-existing bitwise party-size regression comparisons also failed with
  that runtime. The established baseline used Julia 1.12.7 with generic CPU code,
  compiled modules/pkgimages disabled, and one Julia/GC thread; final verification
  uses that same runtime through the existing `JULIA_BIN` override. No production
  calculation, tolerance, prose value, or provenance value was changed to mask
  this runtime difference.
- The full baseline-runtime production workflow completed successfully, including
  all table and figure generators, both PDF compilations, the integrated final
  asset/provenance audit, and submission packaging.
- Independent normal analysis audit: 12 domains, 31,238 coalition rows, 475,290
  member rows, and 23 cabinet bridge rows passed every assertion.
- **All 196 original substantive CSVs and all 76 generated table TeX files are
  byte-identical to the pre-refactor files.** No data, calculations, memberships,
  domains, numerical precision, or substantive numerical claims changed.
- **53 provenance blocks, 292 recorded fields, 11 source CSVs: all passed.**
  Every semantic key resolves uniquely; there are zero row-number warnings.
  All optional local display checks passed.
- The final audit validated 163 paper artifacts and all 18 manuscript asset
  references (11 generated table inputs and 7 figure references).
- The final **36-page PDF** has identical layout-preserving extracted text and
  pixel-identical rendered pages at 65 dpi. Rendered prose, figures, and a
  landscape appendix table were also inspected. No undefined commands,
  references/citations, multiply defined labels, or overfull boxes remain.
- Submission archives contain the current literal-prose manuscript and its
  generated assets: 11 table files, 8 figure files (including the retained
  repository diagnostic), and 22 files in each full manuscript archive. No
  retired macro file is included.
- Final repository searches find no commands from any of the 234 retired macro
  definitions and no remaining generated `manuscript_values.*` file. Broad
  namespace searches find only the intentional validator rejection fixture;
  the two pre-existing historical drafts still reference their older retired
  `accounting_numeric_macros.tex` input and are excluded from packaging.
- `git diff --check` passes. Nothing was committed, staged, or pushed.

Run logs and the pre-edit source, inventory, PDF, page renders, and fingerprints
are retained in `/tmp/electoral-provenance-before/` for local review. The permanent
historical mapping and this report do not feed any production calculation.


## Files changed

Source, documentation, tests, and the historical inventory:

- modified: `README.md`
- modified: `processing/Processing/decomposition/CabinetDistrictTable.jl`
- modified: `processing/Processing/decomposition/MANUSCRIPT_VALUE_MIGRATION.md`
- removed: `processing/Processing/decomposition/ManuscriptValueSupport.jl`
- removed: `processing/Processing/decomposition/ManuscriptValues.jl`
- added: `processing/Processing/decomposition/PROSE_MACRO_INVENTORY.csv`
- added: `processing/Processing/decomposition/ProseSummaries.jl`
- modified: `processing/Processing/decomposition/audit_empirical_assets.py`
- modified: `processing/Processing/decomposition/run_decomposition.jl`
- modified: `processing/Processing/decomposition/runtests.jl`
- removed: `processing/Processing/decomposition/test_manuscript_values.jl`
- added: `processing/Processing/decomposition/test_prose_summaries.jl`
- added: `processing/Processing/decomposition/tests/test_prose_provenance.py`
- added: `processing/Processing/decomposition/validate_prose_provenance.py`
- modified: `processing/audit_ideological_universes.py`
- modified: `processing/rebuild_manuscript.sh`
- modified: `writing/package_submission_assets.py`
- modified: `writing/submission_inversions_review/manuscript/main_rw_again.tex`
- modified: `writing/tests/test_package_submission_assets.py`

Changed, added, or retired generated deliverables and audit metadata:

- modified: `processing/Processing/output/decomposition/artifact_manifest.csv`
- modified: `processing/Processing/output/decomposition/audit/decomposition_input_manifest.csv`
- modified: `processing/Processing/output/decomposition/audit/intermediate_accounting_input_manifest.csv`
- modified: `processing/Processing/output/decomposition/audit/intermediate_accounting_report_artifact_manifest.csv`
- added: `processing/Processing/output/decomposition/audit/manuscript_empirical_asset_manifest.csv`
- added: `processing/Processing/output/decomposition/audit/manuscript_prose_provenance.csv`
- added: `processing/Processing/output/decomposition/audit/paper_artifact_hashes.csv`
- modified: `processing/Processing/output/decomposition/audit/publication_artifact_manifest.csv`
- removed: `processing/Processing/output/decomposition/latex/manuscript_values.tex`
- modified: `processing/Processing/output/decomposition/report/intermediate_accounting_report.pdf`
- removed: `processing/Processing/output/decomposition/tables/manuscript_values.csv`
- added: `processing/Processing/output/decomposition/tables/prose_analysis_summaries.csv`
- modified: `processing/Processing/output/paper/artifact_manifest.csv`
- modified: `processing/Processing/output/paper/ideological_universe_refactor_report.md`
- removed: `processing/Processing/output/paper/latex/manuscript_values.tex`
- removed: `processing/Processing/output/paper/tables/manuscript_values.csv`
- added: `processing/Processing/output/paper/tables/prose_analysis_summaries.csv`
- modified: `writing/submission_inversions_review/manuscript.zip`
- modified: `writing/submission_inversions_review/manuscript/accounting_state_weighting_anatomy.pdf`
- modified: `writing/submission_inversions_review/manuscript/district_electoral_weight_by_magnitude.pdf`
- modified: `writing/submission_inversions_review/manuscript/figures.zip`
- modified: `writing/submission_inversions_review/manuscript/ideological_interval_heatmap_2014.pdf`
- modified: `writing/submission_inversions_review/manuscript/ideological_interval_heatmap_2018.pdf`
- modified: `writing/submission_inversions_review/manuscript/ideological_interval_heatmap_2022.pdf`
- modified: `writing/submission_inversions_review/manuscript/inversion_decomposition_components.pdf`
- modified: `writing/submission_inversions_review/manuscript/main_rw_again.pdf`
- modified: `writing/submission_inversions_review/manuscript/manuscript.zip`
- removed: `writing/submission_inversions_review/manuscript/manuscript_values.tex`
- modified: `writing/submission_inversions_review/manuscript/observed_coalition_timeline.pdf`
- modified: `writing/submission_inversions_review/manuscript/party_representation_profile.pdf`
- modified: `writing/submission_inversions_review/manuscript/party_vote_share_vs_seat_share.pdf`
- modified: `writing/submission_inversions_review/manuscript/tables.zip`

The normal build also refreshes its analysis archive `processing/Processing/output/paper.zip`
and routine LaTeX control files, auxiliary logs, and test logs. All unchanged
generated tables and substantive CSVs are intentionally omitted from the changed
file list above. New CSV/audit outputs under `processing/Processing/output` are
covered by the existing output ignore rules; their generators are source files.
