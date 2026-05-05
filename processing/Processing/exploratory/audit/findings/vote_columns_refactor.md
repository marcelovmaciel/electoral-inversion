# Vote columns refactor

- Status: fixed and validated with local raw data.
- Problem summary: federal-deputy party vote aggregation used the wrong valid legend-vote concept in 2018 and 2022. The analysis selected direct valid legend votes instead of total valid legend votes, excluding converted nominal-to-legend votes.
- Root cause: `first_in_df` iterated over dataframe columns first. Even though `detect_vote_cols` listed `QT_TOTAL_VOTOS_LEG_VALIDOS` before `QT_VOTOS_LEGENDA_VALIDOS`, the TSE file order placed `QT_VOTOS_LEGENDA_VALIDOS` first, so dataframe order overrode candidate priority.
- Substantive decision: for 2018 and 2022, valid party votes for this proportional-representation analysis are `QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS`. `QT_TOTAL_VOTOS_LEG_VALIDOS` includes direct valid legend votes plus converted nominal-to-legend votes.
- 2014 exception: the legacy 2014 file uses `QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA`.

## Files changed

- `processing/Processing/src/code.jl`
  - Changed `first_in_df` to respect candidate-priority order while preserving the actual dataframe column-name object returned by `names(df)`.
  - Changed automatic party vote detection to use explicit component schemes by default.
  - Removed silent broad fallback to `QT_VOTOS`, `TOTAL_VOTOS`, or `QT_VOTOS_VALIDOS` from automatic detection.
- `processing/Processing/src/analysis_runner_core.jl`
  - Made `vote_kwargs_for_year` explicit for 2018 and 2022: `QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS`.
- `processing/Processing/exploratory/audit/check_vote_columns_refactor.jl`
  - Added data-optional regression/audit check for priority selection, selected vote components, and converted-vote identities.
- `processing/Processing/exploratory/audit/out/test_running_vote_columns_refactor_stdout.txt`
  - Captured stdout from the post-fix exploratory run.

## Validation commands

```bash
julia --project=processing/Processing processing/Processing/exploratory/audit/check_vote_columns_refactor.jl
julia --project=processing/Processing processing/Processing/exploratory/test_running.jl > processing/Processing/exploratory/audit/out/test_running_vote_columns_refactor_stdout.txt 2>&1
```

## Selected columns

Before the refactor, from `vote_columns.md` and `audit/out/test_running_stdout.txt`:

- 2014: `QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA`, total `97,355,354`.
- 2018: `QT_VOTOS_NOMINAIS_VALIDOS + QT_VOTOS_LEGENDA_VALIDOS`, total `98,057,503`.
- 2022: `QT_VOTOS_NOMINAIS_VALIDOS + QT_VOTOS_LEGENDA_VALIDOS`, total `109,290,050`.

After the refactor:

- 2014: `QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA`, total `97,355,354`.
- 2018: `QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS`, total `98,264,190`.
- 2022: `QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS`, total `109,413,508`.

## Converted-vote identities

- 2018: `sum(QT_TOTAL_VOTOS_LEG_VALIDOS) - sum(QT_VOTOS_LEGENDA_VALIDOS) = 206,687`, matching `sum(QT_VOTOS_NOMINAIS_CONVR_LEG)`.
- 2022: `sum(QT_TOTAL_VOTOS_LEG_VALIDOS) - sum(QT_VOTOS_LEGENDA_VALIDOS) = 123,458`, matching `sum(QT_VOTOS_NOM_CONVR_LEG_VALIDOS)`.

The 2018 national total increased by `206,687`; the 2022 national total increased by `123,458`; 2014 was unchanged.

Party deltas asserted by the audit check:

- 2018: PT `+124,647`; PP `+37,558`; PSL `+15,444`; PPL `+12,095`; PRB `+10,490`; PSOL `+3,274`; PPS `+2,544`; MDB `+635`.
- 2022: PL `+85,911`; PODE `+29,923`; PSB `+6,799`; PTB `+587`; PDT `+207`; PSC `+31`.

## Downstream changes observed

- Party totals changed only for parties receiving converted nominal-to-legend votes.
- Quotas, vote shares, and seat differentials changed mechanically in 2018 and 2022 because the denominator and some party totals changed.
- Observed coalition classifications did not change in the captured run:
  - 2018 observed periods remained `neither`.
  - 2022 observed periods remained `seats_only`, `votes+seats`, `votes+seats`.
- One ideology sweep classification changed in 2022:
  - `MDB -> UNIÃO` changed from `votes+seats`, `candidate_inversion=false`, vote share `50.01%` to `seats_only`, `candidate_inversion=true`, vote share `49.98%`.
