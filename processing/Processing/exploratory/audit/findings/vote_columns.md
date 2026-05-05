# Vote column detection

- Audit item: Validate vote column detection, including 2014 forced component columns, 2018/2022 automatic detection, comparison of component and total vote columns, fallback risks, federal-deputy filtering, and plausibility of party totals after canonicalization.
- Status: closed after post-fix verification.

## Original problem

- 2014 was already handled explicitly by `Processing.AnalysisRunnerCore.vote_kwargs_for_year(2014)`, which forced `QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA`.
- 2018 and 2022 intended to prefer `QT_TOTAL_VOTOS_LEG_VALIDOS` over `QT_VOTOS_LEGENDA_VALIDOS`, but automatic detection selected `QT_VOTOS_LEGENDA_VALIDOS`.
- Root cause: `Processing.first_in_df` previously returned the first matching dataframe column rather than the first preferred candidate. The TSE files place `QT_VOTOS_LEGENDA_VALIDOS` before `QT_TOTAL_VOTOS_LEG_VALIDOS`, so dataframe order overrode the intended candidate priority.
- Consequence: converted nominal-to-legend votes were excluded from 2018 and 2022 valid party vote totals.

## Code path inspected

- `processing/Processing/exploratory/test_running.jl`
  - `load_votes_for_year` filters `party_mun_zone.csv` to `DS_CARGO == "DEPUTADO FEDERAL"`, calls `ARC.vote_kwargs_for_year(year)`, then calls `Processing.detect_vote_cols(pmz; vote_kwargs...)`.
- `processing/Processing/src/analysis_runner_core.jl`
  - `vote_kwargs_for_year(2014)` returns `nom_col = "QT_VOTOS_NOMINAIS", leg_col = "QT_VOTOS_LEGENDA"`.
  - `vote_kwargs_for_year(2018)` and `vote_kwargs_for_year(2022)` return `nom_col = "QT_VOTOS_NOMINAIS_VALIDOS", leg_col = "QT_TOTAL_VOTOS_LEG_VALIDOS"`.
- `processing/Processing/src/code.jl`
  - `detect_vote_cols` resolves explicit columns with `pick_col`, then falls back to preferred candidate lists.
  - `first_in_df` now normalizes dataframe names into a lookup and iterates over candidate names in order.
- `processing/Processing/exploratory/audit/check_vote_columns_refactor.jl`
  - Confirms candidate-priority behavior, selected columns, national totals, and converted-vote identities.

## Patch / behavioral change

The behavioral fix is in `Processing.first_in_df`: candidate priority now determines the selected column.

```julia
function first_in_df(df::DataFrame, cands::Vector{String})
    nn = names(df)
    by_name = Dict(uppercase(strip(String(n))) => n for n in nn)
    for c in cands
        key = uppercase(strip(String(c)))
        if haskey(by_name, key)
            return by_name[key]
        end
    end
    return nothing
end
```

`processing/Processing/exploratory/audit/check_vote_columns_refactor.jl` confirms this with a dataframe whose column order is `QT_VOTOS_LEGENDA_VALIDOS`, then `QT_TOTAL_VOTOS_LEG_VALIDOS`; `first_in_df(df, ["QT_TOTAL_VOTOS_LEG_VALIDOS", "QT_VOTOS_LEGENDA_VALIDOS"])` selects `QT_TOTAL_VOTOS_LEG_VALIDOS`.

## Selected columns

- 2014 before and after: `QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA`.
- 2018 before: `QT_VOTOS_NOMINAIS_VALIDOS + QT_VOTOS_LEGENDA_VALIDOS`.
- 2018 after: `QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS`.
- 2022 before: `QT_VOTOS_NOMINAIS_VALIDOS + QT_VOTOS_LEGENDA_VALIDOS`.
- 2022 after: `QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS`.

## Vote totals

After filtering to federal-deputy rows:

| Year | Nominal column sum | Direct legend valid sum | Total valid legend sum | Converted nominal-to-legend | Old total | New total |
|---:|---:|---:|---:|---:|---:|---:|
| 2018 | 91,406,378 | 6,651,125 | 6,857,812 | 206,687 | 98,057,503 | 98,264,190 |
| 2022 | 105,120,146 | 4,169,904 | 4,293,362 | 123,458 | 109,290,050 | 109,413,508 |

2014 remains unchanged:

| Year | Nominal column sum | Legend column sum | Total |
|---:|---:|---:|---:|
| 2014 | 89,147,333 | 8,208,021 | 97,355,354 |

## Converted-vote verification

- `QT_TOTAL_VOTOS_LEG_VALIDOS` does not already include `QT_VOTOS_NOMINAIS_VALIDOS`; it is the total valid legend vote field.
- For 2018 and 2022, `QT_TOTAL_VOTOS_LEG_VALIDOS` equals direct valid legend votes plus converted nominal-to-legend votes.
- 2018: `sum(QT_TOTAL_VOTOS_LEG_VALIDOS) - sum(QT_VOTOS_LEGENDA_VALIDOS) = 206,687`, matching `sum(QT_VOTOS_NOMINAIS_CONVR_LEG)`.
- 2022: `sum(QT_TOTAL_VOTOS_LEG_VALIDOS) - sum(QT_VOTOS_LEGENDA_VALIDOS) = 123,458`, matching `sum(QT_VOTOS_NOM_CONVR_LEG_VALIDOS)`.

## Verification run

- `julia --project=processing/Processing processing/Processing/exploratory/audit/check_vote_columns_refactor.jl`
  - Completed with status 0.
  - Confirmed selected columns and expected converted nominal-to-legend votes.
- `julia processing/Processing/exploratory/test_running.jl 2>&1 | tee processing/Processing/exploratory/audit/out/test_running_stdout_after_vote_column_fix.txt`
  - Completed with status 0.
  - Post-fix run reported:
    - 2014 total valid votes: `97,355,354`
    - 2018 total valid votes: `98,264,190`
    - 2022 total valid votes: `109,413,508`

## Downstream note

All downstream 2018/2022 `vote_share`, `quota`, `seat_diff`, coalition `vote_share`, coalition `seat_diff`, and inversion flags must be regenerated from the post-fix run captured at `processing/Processing/exploratory/audit/out/test_running_stdout_after_vote_column_fix.txt`.

## Post-vote-column-fix revalidation

- Status: superseded by vote-column fix.
- Current verification:
  - `julia --project=processing/Processing processing/Processing/exploratory/audit/check_vote_columns_refactor.jl` completed with status `0`.
  - `julia test_running.jl` completed with status `0` and stdout/stderr were captured at `processing/Processing/exploratory/audit/out/test_running_stdout_after_vote_column_fix.txt`.
- Old selected columns:
  - 2014: `QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA`.
  - 2018: `QT_VOTOS_NOMINAIS_VALIDOS + QT_VOTOS_LEGENDA_VALIDOS`.
  - 2022: `QT_VOTOS_NOMINAIS_VALIDOS + QT_VOTOS_LEGENDA_VALIDOS`.
- New selected columns:
  - 2014: `QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA`.
  - 2018: `QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS`.
  - 2022: `QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS`.
- Old versus new totals:
  - 2014 unchanged: `97,355,354`.
  - 2018 old total: `98,057,503`; new total: `98,264,190`; delta: `206,687`.
  - 2022 old total: `109,290,050`; new total: `109,413,508`; delta: `123,458`.
- Converted nominal-to-legend votes included after the fix:
  - 2018: `206,687` from `QT_VOTOS_NOMINAIS_CONVR_LEG`.
  - 2022: `123,458` from `QT_VOTOS_NOM_CONVR_LEG_VALIDOS`.
- Conclusion:
  - The prior problem finding is fixed and verified for the audited code path.
  - The old captured run is superseded for 2018 and 2022 vote-dependent quantities.
  - 2014 remains unchanged because it already used the correct forced component columns.
