# Post-PSC baseline

## Checkpoint status

The two PSC questions are adjudicated and the existing analysis has been regenerated. Decomposition remains paused. No manuscript prose or manuscript asset was overwritten.

The repaired PSC facts and their downstream calculations are suitable for empirical review. The repository is **not yet a fully green baseline for resuming decomposition**, because the mandated default Julia suite does not complete under Julia 1.12.2 and the full suite under a lower-optimization diagnostic still exposes the pre-existing PCA14 non-federal-row canonicalization error.

## Repository and environment

- Branch: `main`
- HEAD: `2493239435f1de3fae0c87d97233df91fb115dbf`
- Julia: 1.12.2
- Python: 3.12.1
- Repair date: 2026-08-28
- Analysis cutoff retained from the source snapshot: 2026-03-19
- Stage 1 decomposition files: preserved; only a new pre-repair provenance notice was added beside the old baseline artifacts.

## Adjudicated source facts

1. Gilson Machado was a PSC member before his 9 December 2020 Tourism appointment and remained PSC through 29 March 2022.
2. He formally moved to PL on 30 March 2022. That civil date is encoded as PL and is also his final ministerial day.
3. Carlos Brito begins on 31 March 2022.
4. The raw `PSC / PL` field is therefore a temporal sequence, not simultaneous or unresolved membership.
5. The correct 2018 PSC seat count for the paper's final election-result object is seven. Valdevan Noventa's provisional PSC seat was nullified and retotalized to Márcio Macêdo (PT). The existing pipeline already represented PSC 7 / PT 56, so no election seat total changed in this repair.

## Commands and test outcomes

### Source and audit regeneration

- `python scraping/reconstruct_cabinet_timeline.py`
  - Successful during implementation and again on the final retry.
  - One intervening final-validation attempt timed out while connecting to an immutable Wikipedia revision; it failed before writing. The required retry succeeded.
  - The six principal regenerated cabinet files were byte-identical to their pre-rerun corrected hashes.
- `python processing/Processing/psc_baseline_repair/build_candidate_audit.py`
  - Passed; wrote 513 unique elected rows; raw seats 513; canonical seats 513; PSC seats 7.
- `python processing/Processing/psc_baseline_repair/build_cabinet_diagnostics.py`
  - Passed; wrote 409 source-spell rows and 23 adjacent party-set changes.
- `python processing/Processing/psc_baseline_repair/build_downstream_audit.py`
  - Passed; wrote 10 affected old/new overlap rows; artifact comparison 34 unchanged, 16 changed, 1 added.
- `python writing/make_coalition_figures.py --artifact-root processing/Processing/output/paper --figure-dir /tmp/psc_post_repair_figures`
  - Passed; rebuilt Figure 2 and the other existing figures in a temporary review directory. No manuscript PDF was replaced.

### Regression tests

- `python -m unittest discover -s scraping/tests -p 'test_*.py' -v`
  - **2 passed, 0 failed, 0 errored.**
- Focused Julia PSC test under minimal compilation:
  - `julia --compile=min -O0 --project=processing/Processing -e 'using Processing; include("processing/Processing/test/test_psc_baseline_repair.jl")'`
  - **28 passed, 0 failed, 0 errored, 0 broken.**
- The same focused test at `-O1` reproduced a Julia compiler segmentation fault before the assertions completed. An earlier `-O1` focused run completed 28/28, demonstrating that this optimizer failure is intermittent.

### Mandated full suite

Exact required command:

```bash
julia --project=processing/Processing processing/Processing/test/runtests.jl
```

It was run twice. Both runs terminated with signal 11 in Julia 1.12.2 compiler/type-inference code before a complete Test summary. Therefore the exact suite **does not pass** and no pass/fail/broken total is available for that exact execution.

Diagnostic full-suite command at lower optimization:

```bash
julia -O1 --project=processing/Processing processing/Processing/test/runtests.jl
```

Completed summary:

- Pass: 206
- Fail: 0
- Error: 1
- Broken: 1
- Total: 208

The sole error is the pre-existing strict canonicalization of raw party `PCA14` before the federal-deputy office filter in `test_party_name_drift.jl`. The row is a Fernando de Noronha `Conselheiro Distrital` record, not a federal-deputy record. It was documented in the Stage 1 blocker and intentionally not repaired in this narrow PSC task.

### Existing paper runner

```bash
ALLOW_OVERWRITE=true SYNC_REVIEW_ASSETS=false julia -O1 --project=processing/Processing processing/Processing/running/running.jl
```

Passed and regenerated the existing `processing/Processing/output/paper/` analysis. `SYNC_REVIEW_ASSETS=false` prevented any manuscript table or figure synchronization.

## Election totals and invariants

| Election | National valid votes | Seats | Sum of party seat differentials |
|---|---:|---:|---:|
| 2014 | 97,355,354 | 513 | -4.205e-15 |
| 2018 | 98,264,190 | 513 | 4.592e-15 |
| 2022 | 109,413,508 | 513 | -1.149e-14 |

Additional checks:

- All 513 elected 2018 candidate identifiers are unique and accounted for exactly once.
- Raw-party and canonical-party 2018 seat aggregations both sum to 513.
- PSC has exactly seven seats under both aggregations and exactly the seven protected candidate IDs.
- The 2018 winner whitelist covers every winner status present: `ELEITO POR QP` and `ELEITO POR MÉDIA`.
- No canonicalization rule moves a 2018 winner into or out of PSC.
- Cabinet-to-election translation continues to use historical election-year labels.
- Cabinet coverage is continuous and non-overlapping:
  - 2014-linked mandate: 8 periods, 2015-01-01 through 2018-12-31.
  - 2018-linked mandate: 13 periods, 2019-01-01 through 2022-12-31.
  - 2022-linked window: 3 periods, 2023-01-01 through 2026-03-19.
- No adjacent cabinet periods have identical party sets.

## Corrected observed cabinet results

Corrected observed inversion list:

1. 2014 / 2016.2 — 2016-04-14 through 2016-04-15.
2. 2014 / 2017.1 — 2017-12-28 through 2018-04-06.
3. 2018 / 2021.3 — 2021-08-04 through 2022-02-07.
4. 2018 / 2022.1 — 2022-02-08 through 2022-03-29.
5. 2022 / 2023.1 — 2023-01-01 through 2023-09-12.

Inversion-duration totals:

| Election | Periods | Inversion periods | Covered days | Inversion days |
|---|---:|---:|---:|---:|
| 2014 | 8 | 2 | 1,461 | 102 |
| 2018 | 13 | 2 | 1,461 | 238 |
| 2022 | 3 | 1 | 1,174 | 255 |

The sole closest 2018 negative case in the regenerated existing runner output is 2022.2 (30--31 March 2022), at 45.4967% of votes and 250 seats. This output is an existing-analysis diagnostic, not an approved focal decomposition case.

## Corrected ideological interval results

These results are byte-identical to the pre-repair baseline because the adjudicated election seat allocation did not change.

- 2014 inversions: PSB--PTN; PSB--PPL; PSB--PRTB; PV--PR; PTB--PR; PT DO B--PSDC; PT DO B--PSL; SOLIDARIEDADE--PSL.
- 2018 inversions: none.
- 2022 inversions: MDB--UNIÃO; PROS--PL; PRTB--PL; AGIR--PL; PTB--PL; PP--PL.
- Minimal inversions:
  - 2014: PSB--PTN; PTB--PR; PT DO B--PSDC; SOLIDARIEDADE--PSL.
  - 2018: none.
  - 2022: MDB--UNIÃO; PP--PL.

## Reproducibility hashes

Corrected principal cabinet outputs:

| File | SHA-256 |
|---|---|
| `scraping/output/partidos_por_periodo.csv` | `222702cf6fd2157fc6379525547a550aace51615bf7f345ee719394ee3f50165` |
| `scraping/output/partidos_por_periodo.json` | `5165189cd32f05a984a065178ece6bc61dac7494f1568d64be9b4030f203dc65` |
| `scraping/output/ministerios_nomeacoes_intervalos.csv` | `da091a0a8fd1dfeeaae650e6e4a522729e47004e58f806fd8e93003579b3e621` |
| `scraping/output/cabinet_timeline_dashboard.json` | `7777b1fc5699a3bb2270d4c20a17f2f6eb74a0cfc4c3582d7230e45e32c9a2b7` |
| `scraping/output/ministerios_eventos.csv` | `1fe7c58b61b2ffb0e83f38ddc0eb93b4e7f0f147d143c5ea2874157099f1b0cb` |
| `scraping/output/ministerios_eventos.json` | `4a849d9645e6d7f6bad9ee6af41f6baa22cec97a8a179833c8c7904b3701bb26` |

For comparison, the preserved pre-repair cabinet CSV/JSON hashes are `58c710e81fb00689bca1ac14fe0916fffca90c326f64f8da08b7f0f74c12bca9` and `b96593efc2135dd7ef8f10655d9e2f969470d0805f446437d21e5a2e62561309`.

`post_psc_baseline_manifest.csv` contains 96 deterministic SHA-256 entries spanning source inputs, election inputs, pipeline code, tests, preserved Stage 1 provenance, repaired paper artifacts, and repair documentation. It deliberately excludes its own hash to avoid a self-reference cycle.

## Regenerated artifacts

The complete artifact-level hash comparison is `paper_artifact_old_vs_new_hashes.csv`.

Changed or added paper artifacts:

- `artifact_manifest.csv`
- `diagnostics/cabinet_period_party_set_changes.csv`
- `diagnostics/cabinet_period_source_spells.csv`
- `diagnostics/cabinet_translation_report.csv`
- `diagnostics/coalition_period_linkage.csv`
- `diagnostics/party_mapping_coalitions.csv`
- `figure_data/observed_coalition_timeline.csv` (added)
- `latex/table_02_cabinet_inversion_tabular.tex`
- `latex/table_appendix_cabinet_interval_bridge.tex`
- `raw/cabinet_coalition_focal_cases.csv`
- `raw/cabinet_coalition_metrics.csv`
- `raw/observed_cabinet_coalitions_2018.csv`
- `raw/observed_cabinet_duration_summary.csv`
- `raw/observed_cabinet_inversions_only.csv`
- `tables/table_03_observed_cabinet_coalitions.csv`
- `tables/table_04_observed_cabinet_inversions_only.csv`
- `tables/table_appendix_cabinet_interval_bridge.csv`

Thirty-four other existing paper artifacts, including every seat and ideological-interval artifact, are unchanged by hash.

## Files intentionally changed or added

Tracked diff:

- `scraping/reconstruct_cabinet_timeline.py`
- `processing/Processing/running/running.jl`
- `processing/Processing/test/runtests.jl`

New authoritative inputs and regression tests:

- `scraping/data/cabinet_source_snapshot.json`
- `scraping/data/cabinet_party_affiliation_spells.csv`
- `scraping/tests/test_psc_cabinet_chronology.py`
- `processing/Processing/test/test_psc_baseline_repair.jl`

Regenerated source outputs:

- `scraping/output/partidos_por_periodo.csv`
- `scraping/output/partidos_por_periodo.json`
- `scraping/output/cabinet_timeline_dashboard.json`
- `scraping/output/ministerios_eventos.csv`
- `scraping/output/ministerios_eventos.json`
- `scraping/output/ministerios_nomeacoes_intervalos.csv`
- `scraping/output/ministerios_eventos_review_report.md`

Repair package:

- all files under `processing/Processing/psc_baseline_repair/`;
- `processing/Processing/output/decomposition/baseline/PRE_PSC_REPAIR_PROVENANCE.md`.

The tracked diff is 3 files, 396 insertions, and 64 deletions. `git diff --check` passes. The workspace also retains unrelated pre-existing untracked work recorded in the Stage 1 status snapshot; it was not modified as part of this repair.

## Deliberately unchanged

- The four Stage 1 decomposition documents.
- All original files in the old Stage 1 baseline directory.
- The protected 2018 mandate fixture; the corrected source now satisfies it.
- Election, party-alias, ideology, and decomposition implementation code.
- Manuscript prose and manuscript figures/tables. The pre-existing manuscript table assets remain byte-identical to the preserved pre-repair versions.
- Theoretical framing and focal decomposition-case selection.

## Baseline disposition

The PSC repair itself is complete, source-grounded, reproducible, and protected by focused regression tests. The corrected artifacts are the proper empirical baseline for review.

Do **not** resume decomposition yet under a “full baseline suite passes” rule. The separate PCA14 loader error and Julia 1.12.2 default compiler crash remain open baseline-stability blockers. Resolve or explicitly waive those issues only in a separate authorized task.
