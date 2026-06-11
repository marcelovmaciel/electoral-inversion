# Seat Majorities without Vote Majorities: Coalition Inversions in Brazil's Chamber of Deputies

## Overview

This repository is a scientific replication package for the manuscript "Seat Majorities without Vote Majorities: Coalition Inversions in Brazil's Chamber of Deputies."

The paper asks whether party sets in Brazil's Chamber of Deputies can control a legislative majority without receiving a majority of the national federal-deputy vote. The empirical object is a coalition inversion: a set of parties that holds at least 257 of the 513 Chamber seats while receiving no more than half of the national valid federal-deputy vote.

The repository studies coalition inversions under Brazilian proportional representation in two ways. First, it evaluates observed cabinet-period coalitions during the mandates linked to the 2014, 2018, and 2022 federal-deputy elections. Second, it searches all contiguous ideological intervals in the election-year party order to identify connected party sets that satisfy the inversion criterion.

The main findings are:

- Three observed cabinet periods are coalition inversions: 2014/2016.2, 2014/2017.1, and 2022/2023.1.
- No observed cabinet inversion occurs in the mandate following the 2018 election.
- Ideological interval inversions occur in 2014 and 2022, but not in 2018.
- The 2022 PP--PL ideological interval is a minimal connected inversion: it holds 258 seats with 45.35 percent of the national federal-deputy vote.

## Repository Structure

The inspected workspace contains the following major directories and files:

```text
.
|-- data/raw/electionsBR/
|   |-- 1998/
|   |-- 2002/
|   |-- 2006/
|   |-- 2010/
|   |-- 2014/
|   |-- 2018/
|   `-- 2022/
|-- r_scripts/
|-- test_scripts/
|-- scraping/
|   |-- output/
|   `-- *.py
|-- dashboard/cabinet_timeline/
|-- scrape_classification/
|   |-- tools/classificacao_extractor/
|   |-- scripts/
|   `-- output/
|-- pdfs/classificacao/
|-- processing/Processing/
|   |-- Project.toml
|   |-- Manifest.toml
|   |-- data/
|   |-- src/
|   |-- running/
|   |-- test/
|   `-- output/paper/
|-- writing/
|   |-- main.tex
|   |-- main.pdf
|   |-- make_coalition_figures.py
|   `-- figures/
|-- reporting/
|-- readmes/
|-- notes/
|-- reports/
|-- tmp/
`-- votacao_candidato_munzona_2024/
```

`data/raw/electionsBR/` stores raw election CSVs downloaded from TSE through `electionsBR`. The paper runner uses the 2014, 2018, and 2022 `party_mun_zone.csv` files for votes and the corresponding `candidate.csv` files for elected-seat counts. The directory also contains older elections and large `vote_mun_zone.csv` files, but those are not used by the current paper runner.

`r_scripts/` contains R scripts for downloading and preprocessing election data. `r_scripts/downloading_data.R` is a raw `electionsBR` downloader, but as inspected it ends with `run_extraction(c(2018))`. `r_scripts/preprocessing_data.R` can write derived tables under `data/derived/inversion_prep/`; that derived directory was not present in the inspected workspace and is not used by the paper-oriented Julia runner.

`test_scripts/downloading_data.R` is another downloader script. It is configured to download the listed election years from 1998 through 2022 for `legends`, `party_mun_zone`, `candidate`, and `seats`.

`scraping/` reconstructs cabinet composition and cabinet-period party sets from Brazilian Wikipedia cabinet-member pages. Its main stored outputs are in `scraping/output/`, especially `partidos_por_periodo.json`, `partidos_por_periodo.csv`, `ministerios_eventos.json`, `ministerios_nomeacoes_intervalos.csv`, and review/audit reports.

`dashboard/cabinet_timeline/` is a static audit dashboard for inspecting the cabinet timeline. It is not a manuscript output, but it documents the cabinet reconstruction logic and loads the generated cabinet JSON files.

`scrape_classification/` contains the ideology-classification extraction tools and stored classification outputs. The Julia runner reads `scrape_classification/output/classificacao_2023/party_ordinal_classificacao.{json,csv}` and `scrape_classification/output/classificacao_2025/party_classificacao_2025.csv`.

`pdfs/classificacao/` contains the ideology-source PDFs present in this workspace: `bolognesi23_uma_nova_class_ideol_dos_ann.pdf`, `codato_2023.pdf`, and `codato_2025.pdf`.

`processing/Processing/` is the main Julia analysis project. It is not a general-purpose software package for users; it is the replication engine. It reads election data, cabinet periods, party-name metadata, and ideology classifications, then writes paper tables, diagnostics, raw analytical outputs, and figure input data under `processing/Processing/output/paper/`.

`processing/Processing/data/` contains party-name and cabinet-to-election harmonization metadata. The inspected workspace includes `party_aliases.csv`, `party_lineage_events.csv`, and `cabinet_to_election_party_crosswalk.csv`; all three are required by the paper runner.

`writing/` contains the manuscript source, the generated PDF, the figure-generation script, and the manuscript figure PDFs. `writing/make_coalition_figures.py` reads the figure-data CSVs from `processing/Processing/output/paper/figure_data/` and writes PDFs to `writing/figures/`.

`reporting/`, `readmes/`, `notes/`, `reports/`, and `tmp/` contain auxiliary audits, TSE readme PDFs, working notes, and temporary checks. They are useful for provenance but are not the main execution path.

`votacao_candidato_munzona_2024/` contains 2024 municipal candidate-zone voting files. These files were present in the workspace but are not used by the manuscript replication workflow.

## Data Sources

| Source | Origin | Years covered in repository | Role in analysis | Stored or downloaded |
|---|---|---:|---|---|
| Federal-deputy election returns | Tribunal Superior Eleitoral, accessed with the R package `electionsBR` | Raw files for 1998, 2002, 2006, 2010, 2014, 2018, and 2022 | Party federal-deputy vote totals and elected-seat counts; the paper runner uses 2014, 2018, and 2022 | Stored under `data/raw/electionsBR/`; can be downloaded with the R scripts |
| Cabinet composition | Brazilian Wikipedia cabinet-member pages for Dilma Rousseff, Michel Temer, Jair Bolsonaro, and Lula III, accessed through the MediaWiki API | Cabinet periods used for mandates beginning in 2015, 2019, and 2023; current data run through 2026-03-19 | Defines observed cabinet-period party sets | Stored under `scraping/output/`; can be regenerated with `scraping/*.py` using network access |
| Party ideology classifications | PDFs and extracted classification tables associated with Bolognesi, Ribeiro, Codato, and related 2025 update | 2023 classification source for 2014 and 2018 analyses; 2025 classification source for 2022 analysis | Orders parties for contiguous ideological interval analysis | Source PDFs are in `pdfs/classificacao/`; extracted tables are in `scrape_classification/output/` |
| Party-name aliases and lineage metadata | Repository harmonization files | Election and cabinet labels for 2014, 2018, and 2022 analysis | Canonicalizes party names, handles renames, and handles fusion/translation cases such as DEM/PSL/UNIÃO | Stored under `processing/Processing/data/` |
| TSE documentation PDFs | Readme PDFs downloaded with `electionsBR` | 1998 through 2022 readmes present | Documentation for raw TSE schemas | Stored under `readmes/` and `r_scripts/readmes/`; not read by the paper runner |

The cabinet outputs in `scraping/output/` are the operative cabinet-period inputs for the paper. The repository also contains review and reconciliation artifacts for the cabinet timeline, including `ministerios_eventos_review_report.md`, `dashboard_input_vs_event_issues_readable.md`, and `election_year_departure_audit_readable.md`.

## Computational Environment

The Julia environment is specified by:

- `processing/Processing/Project.toml`
- `processing/Processing/Manifest.toml`

The inspected Julia manifest records:

- Julia `1.12.2`
- Manifest format `2.0`

The installed tools observed in this workspace were:

- Julia `1.12.2`
- R `4.3.3`
- Python `3.12.1`
- `latexmk` `4.83`
- `biber` `2.19`
- pdfTeX from TeX Live 2023/Debian
- Poppler `pdfinfo`/`pdftotext` `24.02.0`

Julia dependencies are locked in `Manifest.toml`. The main packages include `CSV`, `DataFrames`, `Dates`, `Glob`, `Humanize`, `JSON`, `JSON3`, `Statistics`, and `Unicode`.

R dependencies are installed by the scripts if missing:

- `r_scripts/downloading_data.R`: `electionsBR`, `readr`
- `r_scripts/preprocessing_data.R`: `dplyr`, `readr`, `stringr`, `purrr`, `tidyr`

Python dependencies are not locked in a top-level environment file. The cabinet scraping requirements are listed in `scraping/requirements.txt`:

- `requests`
- `beautifulsoup4`

The figure script also imports `numpy`, `pandas`, and `matplotlib`. The ideology extraction tools optionally use PDF/text extraction libraries and command-line tools, including `PyMuPDF`, `pdfplumber`, `pdftotext`, and `pdfinfo`.

The workflow was inspected on Linux. Several files contain local absolute paths, especially `writing/main.tex` and the default paths in `writing/make_coalition_figures.py`. The commands below pass explicit paths where needed. If the repository is moved, check the `\RepoRoot` definition in `writing/main.tex` before compiling the manuscript.

## Replication Workflow

Run commands from the repository root unless a command changes directory explicitly.

### 1. Environment setup

```bash
cd /home/marcelovmaciel/Sync/Projects/electoral_inversions
julia --project=processing/Processing -e 'using Pkg; Pkg.instantiate()'
```

For the Python scripts:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r scraping/requirements.txt numpy pandas matplotlib pymupdf pdfplumber
```

For the R scripts:

```bash
Rscript -e 'install.packages(c("electionsBR","readr","dplyr","stringr","purrr","tidyr"), repos="https://cloud.r-project.org")'
```

### 2. Data acquisition

The inspected workspace already contains the raw TSE CSVs required by the paper under `data/raw/electionsBR/`.

To download the core raw election files for all listed years:

```bash
Rscript test_scripts/downloading_data.R
```

This script downloads `legends`, `party_mun_zone`, `candidate`, and `seats`. It does not download the large `vote_mun_zone.csv` files that are present in the inspected workspace; those files are not read by the paper runner.

The tracked downloader in `r_scripts/downloading_data.R` also exists, but as inspected it runs only `run_extraction(c(2018))` at the end of the file:

```bash
Rscript r_scripts/downloading_data.R
```

The main paper runner does not use `data/derived/inversion_prep/`. If a replicator wants to run the separate R preprocessing script, use:

```bash
Rscript r_scripts/preprocessing_data.R
```

### 3. Cabinet-period generation

The paper runner reads `scraping/output/partidos_por_periodo.json`. To regenerate the cabinet-period files from the Wikipedia sources:

```bash
python scraping/scrape_wikipedia_orgaos_ministeriais.py
python scraping/scrape_wikipedia_ministerios.py
python scraping/reconstruct_cabinet_timeline.py
python scraping/reconcile_dashboard_inputs_vs_events.py
python scraping/audit_election_year_departures.py
```

The first script writes ministry-level office lists. The second writes cabinet party periods. The reconstruction script writes event-level and dashboard files. The reconciliation and election-year audit scripts write diagnostic reports; they are not directly read by the Julia paper runner.

### 4. Ideology classification inputs

The paper runner reads stored classification outputs from:

- `scrape_classification/output/classificacao_2023/party_ordinal_classificacao.json`
- `scrape_classification/output/classificacao_2023/party_ordinal_classificacao.csv`
- `scrape_classification/output/classificacao_2025/party_classificacao_2025.csv`

The extractor entrypoint is `scrape_classification/scripts/run_classificacao_extractor.py`, but the exact command that regenerated the stored `scrape_classification/output/` layout could not be determined from the repository. The extractor resolves both PDF input and output paths from the current working directory, while the inspected PDFs are in the repository-level `pdfs/classificacao/` directory and the stored outputs are under `scrape_classification/output/`.

Before running the main analysis, verify that the required stored ideology files are present:

```bash
test -f scrape_classification/output/classificacao_2023/party_ordinal_classificacao.json
test -f scrape_classification/output/classificacao_2025/party_classificacao_2025.csv
```

A command that regenerates `scrape_classification/output/classificacao_2025/party_classificacao_2025.csv` exactly was not identified in the repository. The file is present in the inspected workspace and is required for the 2022 ideology ordering.

### 5. Main analysis, coalition construction, and table generation

The paper-oriented Julia runner performs the substantive analysis:

```bash
ALLOW_OVERWRITE=true julia --project=processing/Processing processing/Processing/running/running.jl
```

This command:

1. checks required inputs;
2. loads the 2014, 2018, and 2022 TSE vote and candidate files;
3. computes party vote shares, seat shares, quotas, and seat differentials;
4. links cabinet periods to election mandates;
5. translates cabinet-period party labels into election-year party space;
6. evaluates observed cabinet coalitions;
7. builds election-year ideology orders;
8. enumerates all contiguous ideological intervals;
9. identifies minimal connected interval inversions;
10. writes tables, raw outputs, diagnostics, LaTeX appendix tables, and figure-data CSVs.

The runner refuses to overwrite existing outputs unless `ALLOW_OVERWRITE=true` is set.

### 6. Figure generation

Generate manuscript figures from the CSVs written by the Julia runner:

```bash
python writing/make_coalition_figures.py \
  --artifact-root processing/Processing/output/paper \
  --figure-dir writing/figures
```

This writes:

- `writing/figures/party_vote_share_vs_seat_share.pdf`
- `writing/figures/observed_coalition_timeline.pdf`
- `writing/figures/ideological_interval_heatmap_2014.pdf`
- `writing/figures/ideological_interval_heatmap_2018.pdf`
- `writing/figures/ideological_interval_heatmap_2022.pdf`

### 7. Manuscript artifact generation

Compile the manuscript:

```bash
cd writing
latexmk -pdf -interaction=nonstopmode main.tex
cd ..
```

The inspected `writing/main.tex` points to a local bibliography path. `writing/references.bib` is present, and `writing/coalition_inversions_first_draft.tex` contains a fallback bibliography setup, but `main.tex` itself may need its `\addbibresource` line adjusted if compiled outside the author's machine.

### 8. Verification tests

Run the Julia test suite:

```bash
julia --project=processing/Processing processing/Processing/test/runtests.jl
```

The tests cover party classification loading, party-name drift checks, coalition strictness, coalition-period linkage, and ideological interval construction.

## Reproducing the Manuscript

The manuscript tables are mostly written directly in `writing/main.tex`, while the replication tables they summarize are generated by `processing/Processing/running/running.jl`. The figures are generated by `writing/make_coalition_figures.py` from the Julia runner's figure-data CSVs.

| Manuscript output | Source script | Generated artifact path |
|---|---|---|
| Table: Largest party-level seat differentials by election | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/tables/table_02_party_over_underrepresentation.csv` |
| Figure: Party vote shares and Chamber seat shares | `writing/make_coalition_figures.py` | `writing/figures/party_vote_share_vs_seat_share.pdf` |
| Table: Observed cabinet-period coalition inversions | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/tables/table_04_observed_cabinet_inversions_only.csv` |
| Figure: Observed cabinet-period coalition vote shares and seat shares | `writing/make_coalition_figures.py` | `writing/figures/observed_coalition_timeline.pdf` |
| Table: Ideological interval summary by election | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/tables/table_05_ideological_interval_summary_by_election.csv` |
| Table: Minimal connected interval inversions | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/tables/table_06_minimal_ideological_interval_inversions.csv` |
| Figure: Ideological interval status by election | `writing/make_coalition_figures.py` | `writing/figures/ideological_interval_heatmap_2014.pdf`; `writing/figures/ideological_interval_heatmap_2018.pdf`; `writing/figures/ideological_interval_heatmap_2022.pdf` |
| Appendix table: Party ideology order by election | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/raw/ideology_order_all_years.csv` |
| Appendix table: Cabinet-period composition and transitions | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/tables/table_03_observed_cabinet_coalitions.csv`; `processing/Processing/output/paper/raw/observed_cabinet_coalitions_all_years.csv` |
| Appendix table: Minimal connected winning ideological intervals | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/tables/table_appendix_minimal_connected_winning_intervals.csv`; `processing/Processing/output/paper/latex/table_appendix_minimal_connected_winning_intervals.tex` |
| Appendix table: Cabinet coalitions and ideological intervals | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/tables/table_appendix_cabinet_interval_bridge.csv`; `processing/Processing/output/paper/latex/table_appendix_cabinet_interval_bridge.tex` |
| Appendix audit table: Vote columns and crosswalk | `processing/Processing/running/running.jl` | `processing/Processing/output/paper/tables/table_07_audit_vote_columns_crosswalk.csv` |

The complete paper-runner manifest is:

```text
processing/Processing/output/paper/artifact_manifest.csv
```

## Main Outputs

Principal generated outputs from the paper runner are under `processing/Processing/output/paper/`:

- `artifact_manifest.csv`: manifest of generated paper-runner artifacts.
- `raw/party_seat_differentials_all_years.csv`: party vote shares, seat shares, quotas, and seat differentials.
- `raw/observed_cabinet_coalitions_all_years.csv`: observed cabinet-period coalition metrics.
- `raw/observed_cabinet_inversions_only.csv`: observed cabinet inversions only.
- `raw/observed_cabinet_duration_summary.csv`: number of cabinet periods and inversion days by election.
- `raw/ideology_order_all_years.csv`: ideology order used by election.
- `raw/ideological_intervals_all_years.csv`: all contiguous ideological intervals.
- `raw/ideological_interval_inversions_only.csv`: all ideological interval inversions.
- `raw/minimal_ideological_interval_inversions.csv`: endpoint-minimal connected interval inversions.
- `tables/table_01_definitions_classification_rules.csv`: definitions, thresholds, ideology-source rules, and vote-column rules.
- `tables/table_02_party_over_underrepresentation.csv`: selected party over- and under-representation.
- `tables/table_03_observed_cabinet_coalitions.csv`: observed cabinet-period coalitions.
- `tables/table_04_observed_cabinet_inversions_only.csv`: observed cabinet inversions.
- `tables/table_05_ideological_interval_summary_by_election.csv`: interval counts by election.
- `tables/table_06_minimal_ideological_interval_inversions.csv`: minimal connected interval inversions.
- `tables/table_07_audit_vote_columns_crosswalk.csv`: vote-column audit.
- `tables/table_appendix_minimal_connected_winning_intervals.csv`: all minimal connected winning intervals.
- `tables/table_appendix_cabinet_interval_bridge.csv`: bridge between observed cabinets and ideological intervals.
- `figure_data/party_vote_share_vs_seat_share.csv`: input for the party vote-seat figure.
- `figure_data/observed_coalition_timeline.csv`: input for the cabinet timeline figure.
- `figure_data/ideological_interval_heatmap.csv`: input for the heatmaps.
- `latex/table_appendix_minimal_connected_winning_intervals.tex`: LaTeX appendix table.
- `latex/table_appendix_cabinet_interval_bridge.tex`: LaTeX appendix table.

Generated manuscript artifacts are:

- `writing/main.pdf`
- `writing/figures/party_vote_share_vs_seat_share.pdf`
- `writing/figures/observed_coalition_timeline.pdf`
- `writing/figures/ideological_interval_heatmap_2014.pdf`
- `writing/figures/ideological_interval_heatmap_2018.pdf`
- `writing/figures/ideological_interval_heatmap_2022.pdf`

## Expected Results

A successful replication should recover the following checks from the generated CSVs:

| Check | Expected value |
|---|---:|
| Chamber seats in each analyzed election | 513 |
| Seat majority threshold | 257 |
| 2014 valid federal-deputy votes used | 97,355,354 |
| 2018 valid federal-deputy votes used | 98,264,190 |
| 2022 valid federal-deputy votes used | 109,413,508 |
| Observed cabinet inversion periods | 3 |
| 2014 observed cabinet inversion periods | 2016.2 and 2017.1 |
| 2018 observed cabinet inversion periods | 0 |
| 2022 observed cabinet inversion periods | 2023.1 |
| 2014 ideological intervals | 528 |
| 2014 ideological interval inversions | 8 |
| 2014 minimal ideological interval inversions | 4 |
| 2018 ideological intervals | 630 |
| 2018 ideological interval inversions | 0 |
| 2022 ideological intervals | 528 |
| 2022 ideological interval inversions | 6 |
| 2022 minimal ideological interval inversions | 2 |

The observed cabinet inversion table should contain:

- 2014/2016.2: 2 days, 46.54 percent of the vote, 259 seats.
- 2014/2017.1: 100 days, 49.91 percent of the vote, 266 seats.
- 2022/2023.1: 255 days, 48.89 percent of the vote, 263 seats.

The 2018 cabinet periods should remain below the 257-seat threshold. The closest 2018 observed cabinet cases in the stored results are 2021.3 and 2022.1, each with 250 seats and 45.50 percent of the vote.

The minimal connected ideological interval table should include the 2022 PP--PL interval:

- Parties: `PP, DC, REPUBLICANOS, PSC, UNIÃO, PATRIOTA, NOVO, PL`
- Vote share: 45.35 percent
- Seats: 258
- Seat differential: 25.36

## Runtime Notes

No runtime benchmark or memory log was found in the repository. The following operational facts are observable from the files:

- `data/raw/electionsBR/` is large because it includes historical raw TSE files and multi-gigabyte `vote_mun_zone.csv` files. The paper runner does not read the large `vote_mun_zone.csv` files.
- The main Julia runner reads the 2014, 2018, and 2022 `party_mun_zone.csv` and `candidate.csv` files, plus cabinet and ideology metadata.
- The full TSE download step depends on network access and the `electionsBR` service.
- The cabinet scraping step depends on network access to the Brazilian Wikipedia API.
- The paper runner does not silently cache or overwrite paper outputs. If outputs already exist, it stops unless `ALLOW_OVERWRITE=true` is set.
- Older or auxiliary outputs exist under `processing/Processing/output/running/`, but the manuscript replication path uses `processing/Processing/output/paper/`.

## Replication Checklist

Use this checklist after running the workflow:

- `processing/Processing/output/paper/artifact_manifest.csv` exists.
- All `tables/table_*.csv` files listed above exist.
- `figure_data/party_vote_share_vs_seat_share.csv`, `figure_data/observed_coalition_timeline.csv`, and `figure_data/ideological_interval_heatmap.csv` exist.
- `writing/figures/` contains all five manuscript figure PDFs.
- `writing/main.pdf` compiles.
- `table_04_observed_cabinet_inversions_only.csv` has exactly 3 rows.
- `table_05_ideological_interval_summary_by_election.csv` reports inversion counts `8`, `0`, and `6` for 2014, 2018, and 2022.
- `table_06_minimal_ideological_interval_inversions.csv` includes the 2022 PP--PL interval with 258 seats and 45.35 percent vote share.
- The Julia test suite completes with `julia --project=processing/Processing processing/Processing/test/runtests.jl`.

## Citation

No BibTeX entry for the replication package itself was found in the repository. Until a DOI or archive identifier is assigned, cite the manuscript and replication package as:

```bibtex
@misc{maciel_coalition_inversions_brazil_replication,
  author = {Maciel, Marcelo Veloso},
  title = {Replication Package for "Seat Majorities without Vote Majorities: Coalition Inversions in Brazil's Chamber of Deputies"},
  year = {2026},
  note = {Scientific replication repository}
}
```
