# Seat Majorities without Vote Majorities

Replication package for the paper "Seat Majorities without Vote Majorities:
Coalition Inversions in Brazil's Chamber of Deputies."

The repository computes whether party coalitions in Brazil's Chamber of
Deputies hold a seat majority without a national federal-deputy vote majority.
It covers observed cabinet-period coalitions and ideologically constrained
potential coalitions for the mandates tied to the 2014, 2018, and 2022
elections. The ideological analysis uses exact-connected intervals as the
\(k=0\) baseline and a nested \(k=1\) domain permitting one omitted interior
party as a sensitivity check.

## Main Workflow

Run commands from the repository root unless the command changes directory.

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

4. Compile the manuscript.

```bash
cd writing/submission_inversions_review/manuscript
latexmk -pdf -interaction=nonstopmode main.tex
```

The compiled manuscript is:

```text
writing/submission_inversions_review/manuscript/main.pdf
```

The source at `writing/submission_inversions_review/manuscript/main.tex` is the
authoritative current manuscript. `writing/main.tex` is retained only as a
legacy draft and should not be used to build the submission.

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

- observed cabinet inversion periods: 5
- 2014 cabinet inversions: `2016.2`, `2017.1`
- 2018 cabinet inversions: `2021.3`, `2022.1` (47.2469 percent of the vote and
  exactly 257 seats in each period)
- 2022 cabinet inversion: `2023.1`
- exact-connected (\(k=0\)) ideological inversions: 2014 = 8, 2018 = 0,
  2022 = 6
- exact-connected minimal seat-majority coalitions: 2014 = 8, 2018 = 8,
  2022 = 4
- one-gap (\(k=1\)) ideological inversions: 2014 = 74, 2018 = 34, 2022 = 43
- one-gap minimal seat-majority coalitions: 2014 = 88, 2018 = 118, 2022 = 53
- strongest exact-connected 2022 inversion: PP--PL, 258 seats and 45.35
  percent vote share
- strongest one-gap inversions: PTB--PR omitting PPL (2014), PCdoB--PODE
  omitting PSDB (2018), and PP--PL omitting DC (2022)

Use the files under `processing/Processing/output/paper/` to inspect the
generated tables and diagnostics.

The k-gap outputs are:

    processing/Processing/output/paper/raw/ideology_k_gap_coalitions.csv
    processing/Processing/output/paper/raw/ideology_k_gap_minimal_majorities.csv
    processing/Processing/output/paper/raw/ideology_k_gap_inversions.csv
    processing/Processing/output/paper/tables/ideology_k_gap_summary.csv
    processing/Processing/output/paper/diagnostics/ideology_k_gap_checks.csv
    processing/Processing/output/paper/diagnostics/ideology_k_gap_strongest_inversion_ties.csv
    processing/Processing/output/paper/latex/table_03_ideology_k_gap_summary_tabular.tex

## Tests

Run the focused decomposition suite from the repository root:

```bash
julia -O0 --startup-file=no --project=processing/Processing processing/Processing/decomposition/runtests.jl
```

Run the focused ideological-domain suite with:

    julia -O0 --startup-file=no --project=processing/Processing processing/Processing/test/test_ideological_interval_coalitions.jl

This focused suite is the empirical gate for the decomposition and checks the
five-case registry, coalition compositions, district accounting identities, and
party contribution identities against the corrected PSC baseline. The
repository-wide Julia suite is not the gate for this revision: under Julia
1.12.2 its normal invocation has exhibited a compiler crash, and a separate
pre-existing PCA14 loader error remains outside the federal-deputy analysis.
