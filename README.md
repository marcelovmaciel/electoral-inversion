# Seat Majorities without Vote Majorities

Replication package for the paper "Seat Majorities without Vote Majorities:
Coalition Inversions in Brazil's Chamber of Deputies."

The repository computes whether party coalitions in Brazil's Chamber of
Deputies hold a seat majority without a national federal-deputy vote majority.
It covers observed cabinet-period coalitions and contiguous ideological
intervals for the mandates tied to the 2014, 2018, and 2022 elections.

## Main Workflow

Run commands from the repository root unless the command changes directory.

1. Install Julia dependencies.

```bash
julia --project=processing/Processing -e 'using Pkg; Pkg.instantiate()'
```

2. Run the main analysis.

```bash
ALLOW_OVERWRITE=true julia --project=processing/Processing processing/Processing/running/running.jl
```

This writes the paper analysis artifacts under:

```text
processing/Processing/output/paper/
```

3. Generate manuscript figures.

```bash
python writing/make_coalition_figures.py --artifact-root processing/Processing/output/paper --figure-dir writing/figures
```

This writes figure PDFs under:

```text
writing/figures/
```

4. Compile the manuscript.

```bash
cd writing && latexmk -pdf -interaction=nonstopmode main.tex
```

The compiled manuscript is:

```text
writing/main.pdf
```

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
classification inputs used to order parties for interval analysis.

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

- observed cabinet inversion periods: 3
- 2014 cabinet inversions: `2016.2`, `2017.1`
- 2018 cabinet inversions: 0
- 2022 cabinet inversion: `2023.1`
- ideological interval inversions: 2014 = 8, 2018 = 0, 2022 = 6
- 2022 PP to PL interval: 258 seats and 45.35 percent vote share

Use the files under `processing/Processing/output/paper/` to inspect the
generated tables and diagnostics.

## Tests

Run the Julia test suite from the repository root:

```bash
julia --project=processing/Processing processing/Processing/test/runtests.jl
```

The tests should pass with the input files listed above available in the
repository.
