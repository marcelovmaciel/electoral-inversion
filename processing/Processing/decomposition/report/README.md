# Intermediate party--district accounting report

This directory contains the standalone LaTeX shell for the intermediate
party--district accounting report. The report is deliberately separate from
the manuscript under `writing/`. Its standalone build does not synchronize, copy, or modify manuscript files.
The production `processing/rebuild_manuscript.sh --clean` command regenerates
these report inputs through the main decomposition runner and compiles this
report together with the manuscript.

## Ideological universes and accounting

The report's primary ideological cases are connected intervals among
`seat_winning` parties: the existing election-year ideological ranking is
filtered to parties with positive Chamber seats, preserving relative order.
The national accounting universe still includes every party receiving valid
federal-deputy votes. Both ideological specifications therefore use the same
all-valid-vote denominator, party quotas, observed seats, and exact A/B
accounting panels. Observed cabinet membership retains its existing definition.

The `all_parties` ordering is generated separately through the same routines.
Connectedness, gaps, winning status, and minimality are recomputed within each
universe and each existing k domain. Focal ideological accounting cases are
selected from regenerated exact-connected minimal inversions: the strongest
by vote deficit in each election, together with any case whose B component
exceeds A. The contribution appendix retains every primary exact-connected
minimal inversion. No historical ideological case IDs or counts define these
selections.

## Build

Run from any working directory:

```bash
bash processing/Processing/decomposition/report/build_report.sh
```

The script performs two steps:

1. It runs `../run_intermediate_accounting_report.jl` with Julia optimization
   disabled and startup customization suppressed. It explicitly sets
   `SYNC_REVIEW_ASSETS=false`.
2. It compiles the standalone report with `latexmk` and writes the PDF and TeX
   build products outside this source directory.

The resulting PDF is:

```text
processing/Processing/output/decomposition/report/intermediate_accounting_report.pdf
```

The stable commands executed by the script are equivalent to:

```bash
ALLOW_OVERWRITE=true SYNC_REVIEW_ASSETS=false \
  julia -O0 --startup-file=no \
  --project=processing/Processing \
  processing/Processing/decomposition/run_intermediate_accounting_report.jl

latexmk -cd -pdf -interaction=nonstopmode -halt-on-error \
  -outdir=../../output/decomposition/report \
  processing/Processing/decomposition/report/intermediate_accounting_report.tex
```

The main `run_decomposition.jl` entry point also generates every report table
and interpretation fragment directly from its already loaded exact accounting
objects. It uses the same report functions without rereading the election data.
The report retains its own artifact manifest; its complete diagnostic tables
are not added to the manuscript.

The Julia project must already be instantiated, and `latexmk` with a standard
pdfLaTeX installation must be available.

## Generation boundary

The checked-in `intermediate_accounting_report.tex` file contains definitions,
interpretive cautions, section structure, and `\input` statements only. It has
no empirical table rows. The report runner writes machine-readable results
first, reads the table inputs back from disk, validates them, and then creates
the LaTeX fragments consumed by the wrapper.

Generated files live under:

```text
processing/Processing/output/decomposition/
```

In particular, the wrapper reads these generated fragments from
`output/decomposition/latex/report/`:

```text
generated_interpretation.tex
party_size_diagnostics.tex
table_year_accounting_closure.tex
table_district_weight_extremes.tex
table_inversion_case_registry.tex
table_all_inversion_decomposition.tex
table_case_component_extremes.tex
table_case_party_vectors.tex
table_case_district_vectors.tex
table_case_party_district_extremes.tex
table_party_district_accounting_2014.tex
table_party_district_accounting_2018.tex
table_party_district_accounting_2022.tex
```

Do not edit these fragments or their table CSVs manually. They are generated
artifacts and will be replaced on the next run.

## Machine-readable outputs

The main decomposition runner and standalone report runner write reusable CSVs below
`processing/Processing/output/decomposition/`:

- `raw/party_district_accounting_all_years.csv` contains the complete
  party-by-electoral-unit panel, including votes, seats, quota terms,
  \(a_{id}\), \(b_{id}\), \(d_{id}\), district weights, and exact-rational audit
  fields.
- `raw/party_accounting_all_years.csv` contains national party aggregates
  \(A_i\), \(B_i\), and \(d_i\); `raw/district_accounting_all_years.csv`
  contains electoral-unit weights and district closure quantities.
- `raw/inversion_case_registry.csv` links every selected cabinet or ideological
  case to its source identifier, election, composition, majority quantities,
  and case qualifications.
- `raw/all_inversion_decomposition.csv` contains coalition-level
  \(A_C\), \(B_C\), and \(d_C\). The corresponding
  `all_inversion_party_contributions.csv`,
  `all_inversion_district_contributions.csv`, and
  `all_inversion_party_district_contributions.csv` contain the complete party,
  electoral-unit, and linked party-by-electoral-unit vectors.
- The same four case-output levels are provided separately with
  `cabinet_inversion_*.csv` and `ideological_inversion_*.csv` filenames. These
  are generated domain slices of the combined files, not independent
  calculations.
- `raw/all_inversion_contribution_rankings.csv` contains the complete
  deterministic positive, negative, absolute, ascending, and descending ranks
  for party, district, and party-by-district accounting components.
- `audit/intermediate_accounting_identity_checks.csv` records exact and
  floating-point closure checks; `intermediate_accounting_input_manifest.csv`
  records input provenance; `intermediate_accounting_generation_checks.csv`
  records output-boundary cardinality checks; and
  `intermediate_accounting_report_artifact_manifest.csv` inventories the
  generated report artifacts and their hashes.

The display-oriented CSVs under `tables/report/` are derived from these raw
files and are then reloaded before the LaTeX fragments are rendered.

The shared `DualUniverseAccounting.jl` integration also writes:

- `output/decomposition/all_parties/`: the robustness inversion, focal,
  member-party, state, and concentration outputs, produced by the same
  accounting integration used for the primary files.
- `output/paper/raw/ideology_k_gap_accounting_both_universes.csv`: every
  admissible k=0/1 coalition, retaining explicit universe, span, gap, membership,
  minimality, inversion, all-valid-vote denominator, quota, differential,
  representation ratio, and exact A/B quantities.
- `output/paper/raw/ideology_k_gap_minimal_accounting_both_universes.csv`:
  the audited minimal-winning subset of that complete export.
- `output/paper/raw/ideology_k_gap_party_contributions_both_universes.csv.gz`:
  every member-party vector for both universes, compressed deterministically.
- `output/paper/figure_data/cross_domain_components_seat_winning.csv` and
  `cross_domain_components_all_parties.csv`: validated normalized components
  and configuration metadata for each universe.
- `output/paper/diagnostics/ideological_accounting_both_universes.json`:
  complete-domain minimality, exact accounting, denominator, and member-closure
  audit metadata.

These are shared replication outputs; the main manuscript uses the
`seat_winning` ideological specification. The all-party exact-connected
minimal-inversion tabular is also generated as
`latex/table_accounting_minimal_ideological_all_parties.tex` for the compact
sensitivity appendix.


## Later manuscript reuse

Each empirical table is kept in its own generated LaTeX fragment so that it can
later be redirected into the main manuscript without transcribing values. That
future synchronization should occur only after substantive review. Building
this intermediate report performs no such synchronization.

## Permanent party-size and cabinet diagnostics

The main `run_decomposition.jl` and the report runner both use
`PartySizeDiagnostics.jl`, included in `IntermediateAccountingReport.jl`.
It consumes the existing in-memory exact accounting objects and current
translated/coalesced cabinet registry. It never changes that registry.

The existing `raw/party_accounting_all_years.csv` is extended with `A_over_q`,
`B_over_q`, `ever_in_cabinet`, `cabinet_observation_count`,
`cabinet_source_period_count`, and `cabinet_days`. Existing columns retain
their definitions and values. Days sum inclusive `days_overlapping_mandate`.

Permanent outputs under `output/decomposition/` are:

- `tables/report/party_size_cabinet_summary.csv`: eight election/pooled,
  ever/never size-comparison rows.
- `tables/report/party_size_correlations.csv`: Pearson and Spearman
  associations with absolute and normalized A, by election and pooled.
- `tables/report/party_size_groups.csv`: the four descriptive vote-share
  bins, including counts, means, medians, and positive-A shares.
- `raw/cabinet_party_set_accounting.csv`: 22 distinct translated sets,
  associated period IDs, signed gross contributions, contributions from
  members with at least 5% of votes, and contributor ranks.
- `raw/cabinet_party_set_period_linkage.csv`: all 23 chronology rows with
  stable composition-based set IDs and exact A/B totals.
- `audit/party_size_diagnostic_checks.csv` and
  `audit/party_size_diagnostic_metadata.csv`: separate mathematical identity
  and frozen-data regression checks, plus definitions and qualifications.
- `latex/report/party_size_diagnostics.tex`: the compact central-report
  section after Generated interpretation. It is not a manuscript asset.

The main decomposition runner mirrors the reusable outputs into
`output/paper/` and registers them in its artifact manifest. Its normal rebuild also refreshes the complete party and district accounting
base and the standalone report fragments. The standalone report runner updates
`output/decomposition/` plus the shared dual-universe replication outputs under
`output/paper/`; it does not synchronize manuscript assets.

Five percent is strictly a descriptive national-vote benchmark, not an
institutional threshold or theoretical cutoff. All pooled rows give equal
weight to each election-year party, including zero-seat parties. The
negative gross contribution is signed: `A_C = gross_positive_A +
gross_negative_A`. Normalizing by q does not turn the association into a
causal estimate. A distinct-set grouping may combine the nonadjacent
2020.4/2021.1 repetition, but their chronology rows remain separate.

Frozen-data expectations live separately from accounting identities in the
shared implementation and the focused tests. They assert the 23 observations,
22 sets, positive A in 23 and positive B in 6 observations, the 21/23 versus
12/76 sign comparison, the size correlations, and the descriptive sufficiency
of contributions from members with at least 5%. The test fixture
`../../test/fixtures/cabinet_party_size_chronology.csv` preserves the complete
current chronology and metrics. Discrepancies fail loudly and require review;
expected values are never inferred from a changed output.
