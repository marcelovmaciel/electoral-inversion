# Seat Majorities without Vote Majorities

Replication package for the paper "Seat Majorities without Vote Majorities:
Coalition Inversions in Brazil's Chamber of Deputies."

The repository computes whether party coalitions in Brazil's Chamber of
Deputies hold a seat majority without a national federal-deputy vote majority.
It covers observed cabinet-period coalitions and ideologically constrained
potential coalitions for the mandates tied to the 2014, 2018, and 2022
elections. The primary ideological analysis filters the existing election-year order to
seat-winning parties (`seat_winning`). Exact-connected intervals form the
\(k=0\) domain; \(k=1\) allows one omitted interior seat-winning party. The
original full ideological order is generated independently as `all_parties`
robustness. Both universes retain **all valid federal-deputy votes** in the
national denominator. Observed cabinet construction is unchanged.

## Main Workflow

Run commands from the repository root unless the command changes directory.
The complete production workflow, including both universes, exact accounting,
figures, independent audits, and `main_rw_again.pdf`, is:

```bash
processing/rebuild_manuscript.sh --clean
```

`--clean` removes only the two generated analysis trees before rebuilding from
the original inputs. Omit it for an overwrite rebuild. Set `PYTHON_BIN` or
`JULIA_BIN` to select an installed runtime. The equivalent individual stages
are listed below.

1. Install Julia dependencies.

```bash
julia --project=processing/Processing -e 'using Pkg; Pkg.instantiate()'
```

2. Run the main analysis from a stable Julia 1.12.2 invocation.

```bash
ALLOW_OVERWRITE=true SYNC_REVIEW_ASSETS=true julia -O0 --startup-file=no --project=processing/Processing processing/Processing/running/running.jl
ALLOW_OVERWRITE=true SYNC_REVIEW_ASSETS=true julia -O0 --startup-file=no --project=processing/Processing processing/Processing/decomposition/run_decomposition.jl
```

This writes the paper analysis artifacts under:

```text
processing/Processing/output/paper/
```

3. Generate all manuscript figures, including the separately rendered party
representation profile.

```bash
julia -O0 --startup-file=no --project=processing/Processing processing/make_representation_profile.jl
python writing/make_coalition_figures.py --artifact-root processing/Processing/output/paper --figure-dir writing/submission_inversions_review/manuscript
```

This writes figure PDFs under:

```text
writing/submission_inversions_review/manuscript/
```

The figure runner also regenerates `cross_domain_components.pdf` from the
audited cabinet and ideological registries under `output/paper/` and exact
party/district accounting under the sibling `output/decomposition/` directory.
The shared extractor is `processing/Processing/decomposition/cross_domain_components.py`;
manuscript production does not require the exploratory report.

4. Compile the manuscript.

```bash
cd writing/submission_inversions_review/manuscript
latexmk -pdf -interaction=nonstopmode main_rw_again.tex
```

The compiled manuscript is:

```text
writing/submission_inversions_review/manuscript/main_rw_again.pdf
```

The source at `writing/submission_inversions_review/manuscript/main_rw_again.tex` is the
authoritative current manuscript. `writing/main.tex` is retained only as a
legacy draft and should not be used to build the submission.

The decomposition rebuild also produces permanent party-size/cabinet
accounting diagnostics. The existing party accounting panel gains normalized
A/B components and cabinet participation counts/days. Descriptive summaries,
22 distinct-set decompositions, and links to all 23 unchanged cabinet
observations are documented in
[`processing/Processing/decomposition/report/README.md`](processing/Processing/decomposition/report/README.md#permanent-party-size-and-cabinet-diagnostics).
They enter the central accounting report and are not manuscript tables or
figures. The 5% national-vote benchmark is descriptive only.

## Data Inputs

The replication workflow uses these input locations:

```text
data/raw/electionsBR/
scraping/output/partidos_por_periodo.json
scrape_classification/output/classificacao_2023/
scrape_classification/output/classificacao_2025/
processing/Processing/data/
```

`data/raw/electionsBR/` contains the raw TSE election files used for votes and
seats.

`scraping/output/partidos_por_periodo.json` contains the cabinet-period party
sets used for observed coalition analysis.

`scrape_classification/output/classificacao_2023/` and
`scrape_classification/output/classificacao_2025/` contain the party ideology
classification inputs used to construct the fixed election-year party orders.

`processing/Processing/data/` contains party harmonization inputs used by the
Julia runner.

## Software

The Julia project is defined by:

```text
processing/Processing/Project.toml
processing/Processing/Manifest.toml
```

Use Julia with the project environment shown in the workflow command.

The figure script requires Python with `numpy`, `pandas`, and `matplotlib`.

Manuscript compilation requires a LaTeX installation with `latexmk`.

## Checks

After running the main analysis, the high-level replication results should be:

- observed cabinet observations: 23 (from 24 historical periods)
- observed cabinet inversion periods: 4
- 2014 cabinet inversions: `2016.2`, `2017.1`
- 2018 cabinet inversions: `2021.3/2022.1` (2021-08-04 through 2022-03-29,
  238 days, 47.2469 percent of the vote, and exactly 257 seats)
- 2022 cabinet inversion: `2023.1`
The generated [universe comparison](processing/Processing/output/paper/tables/ideological_universe_comparison.csv)
contains all twelve election/universe/k headline rows. Primary exact-connected
inversions occur in all three elections; the all-party robustness retains the
2018 exact-connected null. Thus the null is sensitive to whether zero-seat
parties determine parliamentary adjacency. The 2014 and 2022 strongest endpoint
regions survive, while member sets, vote shares, minimality, and decomposition
components can differ.

Use the files under `processing/Processing/output/paper/` to inspect the
generated tables and diagnostics.

The primary machine-readable files keep the existing `ideology_k_gap_*.csv`
names and carry `ideological_universe=seat_winning`. Matching
`*_all_parties.csv` files contain the robustness domain. Complete combined
outputs are:

- `raw/ideology_k_gap_accounting_both_universes.csv`: every admissible k=0/1
  coalition, its members, filtered and original span indices, gaps, observed
  seats, all-valid-vote share, q, d, R, A, B, inversion, and minimality.
- `raw/ideology_k_gap_party_contributions_both_universes.csv.gz`: every member's
  q, d, A, B, including exact rational values, linked to universe/election/k.
- `raw/ideology_k_gap_minimal_accounting_both_universes.csv`: complete minimal
  winning sets for both specifications.
- `tables/ideological_universe_comparison.csv`: headline counts and strongest
  cases, including endpoint-region sensitivity.
- `tables/table_appendix_cabinet_interval_bridge{,_all_parties}.csv`: separate
  closure, gap, and overlap calculations in each ideological universe. Primary
  overlaps compare represented cabinet members, while cabinet votes, seats,
  and observed composition retain their original definitions.
- `diagnostics/ideology_k_gap_checks.csv` and
  `diagnostics/ideological_universe_reproduction_audit.json`: fail-loud audits.

The primary decomposition is produced under `output/decomposition/`; the same
routines write all-party robustness under `output/decomposition/all_parties/`.
Manuscript-facing numerical tables and figure inputs are generated by the
production pipeline and synchronized into the manuscript directory. The main
summary (`table_03_ideology_exact_connected_summary.tex`) contains only the
exact-connected baseline, with one row per election. The appendix's
"Robustness of the ideological coalition domain" section first reports the
one-gap comparison (`table_03_ideology_k_gap_summary.tex`), then the all-party
sensitivity (`table_appendix_ideological_universe_comparison.tex`) and its
exact-connected decomposition. Both summary tables are generated from the same
validated `ideology_k_gap_summary.csv` registry. Complete compositions remain in CSV.

Run the independent serialized-output audit after decomposition:

```bash
python3 processing/audit_ideological_universes.py
```

## Manuscript numerical values

The production provenance chain is:

```text
analysis -> decomposition/ManuscriptValues.jl
         -> output/paper/tables/manuscript_values.csv
         -> output/paper/latex/manuscript_values.tex
         -> writing/submission_inversions_review/manuscript/main_rw_again.tex
```

`processing/rebuild_manuscript.sh --clean` builds the registry after the
analysis, writes its CSV and TeX from the same in-memory rows, synchronizes the
TeX into the manuscript directory, compiles the PDF, and packages its inputs.
**Do not edit `manuscript_values.tex` manually.** Inspect `manuscript_values.csv`
to audit manuscript prose values. Empirical regression expectations live in
`decomposition/test_accounting_integration.jl`, not production code.

The readable specification in `ManuscriptValues.jl` gives each claim an explicit
source, election, universe/k, selector, metric, and formatting rule. Fixed
ideological cases assert full membership; fixed cabinet cases use case IDs.
Strongest cases select the lowest vote share, then coalition size and canonical
ID, supplying their identity and numbers together. Main-text ideological names
contain `SeatWinning`; sensitivity names contain `AllParties`. Cabinet accounting
values use `not_applicable`. Cabinet-to-interval diagnostics explicitly use the
seat-winning order without changing cabinet accounting.

The CSV has one row per semantic macro: `macro`, `semantic_key`, `description`,
`domain`, `ideological_universe`, `k`, `election`, `case_id`, `case_label`,
`selector_rule`, `metric`, `raw_value`, `exact_value`, `display_value`, `format`,
`source_object`, `source_file`, `selector_source_file`, `selection_group`, and
`source_row_count`. Paths refer to production objects under `output/paper`;
linked strongest-span/omission selectors additionally identify their selection
source. Missing metadata means that dimension does not apply.

Prose formatting preserves the manuscript's decimals, percentages, integers,
and spelled-out counts. Generated tables retain their table builders and their
separate closure-preserving rounding. The previously handwritten cabinet
concentration table now shares a generated CSV with its prose diagnostics.
Institutional rules, design constants, election years, historical chronology,
and bibliographic facts remain in the manuscript.

Run the focused provenance, drift, and table-consistency tests with:

```bash
julia -O0 --startup-file=no --project=processing/Processing processing/Processing/decomposition/test_manuscript_values.jl
```

The [migration audit](processing/Processing/decomposition/MANUSCRIPT_VALUE_MIGRATION.md)
records all 20 used legacy macros and their semantic replacements. The new API
also covers the literal empirical prose values identified during that audit;
it does not reproduce the 98 unused legacy macros.

## Tests

The manuscript cabinet output combines adjacent periods only when their translated
election-year party sets match. Original IDs are preserved as JSON in
`source_periods`; uncombined translated rows and before/after counts are saved
under `output/paper/diagnostics/`. The current sample guard permits only
`2021.3 + 2022.1`.

Run its regression checks after generating the analysis outputs:

```bash
julia -O0 --startup-file=no --project=processing/Processing processing/Processing/test/test_cabinet_period_coalescing.jl
```

Run the focused decomposition suite from the repository root:

```bash
julia -O0 --startup-file=no --project=processing/Processing processing/Processing/decomposition/runtests.jl
```

Run the focused ideological-domain suite with:

    julia -O0 --startup-file=no --project=processing/Processing processing/Processing/test/test_ideological_interval_coalitions.jl

This focused suite is the empirical gate for the decomposition and checks the
four-case registry, coalition compositions, district accounting identities, and
party contribution identities against the corrected PSC baseline. The focused ideological suite includes synthetic adjacency/denominator tests
and checks both empirical universes. The full Julia suite is run with the
repository project environment; the figure and decomposition suites validate
generated registries and accounting identities.
