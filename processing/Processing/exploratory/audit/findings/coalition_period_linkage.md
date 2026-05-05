# Coalition Period Linkage

## Audit item

Validate coalition period linkage to election years:

- Confirm the script links 2014 election results to 2015-2018 coalition periods, 2018 election results to 2019-2022 coalition periods, and 2022 election results to 2023-2025 coalition periods.
- Decide whether mandate-end years are included in full.
- Check whether `coalitions_by_year` returns periods by direct `YYYY.` prefix or date-window overlap.
- Compare printed period keys to `period_start` and `period_end` dates from `coalition_period_windows`.
- Confirm raw parties in `partidos_por_periodo.json` are sensible before canonicalization.
- Flag periods whose dates or labels suggest another electoral mandate.

## Files inspected

- `processing/Processing/exploratory/audit/AUDIT.md`
- `processing/Processing/exploratory/test_running.jl`
- `processing/Processing/src/code.jl`
- `processing/Processing/src/analysis_runner_core.jl`
- `processing/Processing/exploratory/audit/out/test_running_stdout_after_vote_column_fix.txt`
- `scraping/output/partidos_por_periodo.json`

## Diagnostic run

Read-only Julia diagnostic:

```julia
using JSON, Dates
path = "scraping/output/partidos_por_periodo.json"
raw = JSON.parsefile(path)
parsemaybe(x) = (x === nothing || isempty(strip(String(x)))) ? nothing : Date(strip(String(x)))
years_by_election = Dict(2014 => [2015, 2016, 2017, 2018], 2018 => [2019, 2020, 2021, 2022], 2022 => [2023, 2024, 2025])
```

For each election year, the diagnostic printed direct `YYYY.` period keys, their `data_inicio` / `data_fim`, selected-year overlaps, and raw parties before canonicalization. It also compared direct-prefix keys to full date-window overlaps for each calendar year from 2015 through 2025.

Additional read-only search:

```sh
rg -n "Coalition periods linked|period_start|period_end|2015\.1|2018\.1|2019\.1|2022\.1|2023\.1|2025" \
  processing/Processing/exploratory/audit/out/test_running_stdout_after_vote_column_fix.txt
```

## Evidence observed

`test_running.jl` explicitly constructs the linkage as:

- 2014 election: `coalitions_by_year(..., 2015)`, `2016`, `2017`, `2018`, then `merge(...)`.
- 2018 election: `2019`, `2020`, `2021`, `2022`, then `merge(...)`.
- 2022 election: `2023`, `2024`, `2025`, then `merge(...)`.

The shared runner constant matches this same intended mapping:

```julia
const COALITION_YEARS_BY_ELECTION = Dict(
    2014 => [2015, 2016, 2017, 2018],
    2018 => [2019, 2020, 2021, 2022],
    2022 => [2023, 2024, 2025],
)
```

The post-fix captured run printed:

- 2014 election: `2015.1, 2016.1, 2016.2, 2016.3, 2016.4, 2017.1, 2018.1, 2018.2`
- 2018 election: `2019.1, 2020.1, 2020.2, 2020.3, 2020.4, 2021.1, 2021.2, 2021.3, 2022.1, 2022.2, 2022.3`
- 2022 election: `2023.1, 2023.2, 2025.1`

The JSON date windows for these linked periods were:

- `2015.1`: `2015-01-01` to `2016-03-23`; raw parties `MDB, PCdoB, PDT, PL, PP, PSD, PT, PTB, Republicanos`.
- `2016.1`: `2016-03-24` to `2016-04-13`; raw parties `MDB, PCdoB, PDT, PL, PP, PSD, PT, PTB`.
- `2016.2`: `2016-04-14` to `2016-04-15`; raw parties `MDB, PCdoB, PDT, PL, PSD, PT, PTB`.
- `2016.3`: `2016-04-16` to `2016-05-11`; raw parties `MDB, PCdoB, PDT, PL, PT, PTB`.
- `2016.4`: `2016-05-12` to `2017-12-27`; raw parties `DEM, MDB, PP, PPS, PSB, PSD, PSDB, PTB, PV`.
- `2017.1`: `2017-12-28` to `2018-04-06`; raw parties `DEM, MDB, PP, PPS, PSB, PSD, PSDB, PV`.
- `2018.1`: `2018-04-07` to `2018-05-27`; raw parties `MDB, PP, PPS, PSD, PSDB`.
- `2018.2`: `2018-05-28` to `2018-12-31`; raw parties `MDB, PODE, PP, PPS, PSD, PSDB`.
- `2019.1`: `2019-01-01` to `2020-02-10`; raw parties `DEM, MDB, NOVO, PL, PSL, Patriota, Republicanos`.
- `2020.1`: `2020-02-11` to `2020-02-14`; raw parties `DEM, MDB, NOVO, PL, PSDB, PSL, Patriota, Republicanos`.
- `2020.2`: `2020-02-15` to `2020-06-16`; raw parties `DEM, NOVO, PL, PSDB, PSL, Patriota, Republicanos`.
- `2020.3`: `2020-06-17` to `2020-12-09`; raw parties `DEM, NOVO, PL, PSD, PSDB, PSL, Patriota, Republicanos`.
- `2020.4`: `2020-12-10` to `2021-03-28`; raw parties `DEM, NOVO, PL, PSD, PSDB, Patriota, Republicanos`.
- `2021.1`: `2021-03-29` to `2021-06-23`; raw parties `DEM, NOVO, PL, PSD, PSDB, PSL, Patriota, Republicanos`.
- `2021.2`: `2021-06-24` to `2021-08-03`; raw parties `DEM, PL, PSD, PSDB, PSL, Patriota, Republicanos`.
- `2021.3`: `2021-08-04` to `2022-03-30`; raw parties `DEM, PL, PP, PSD, PSDB, PSL, Patriota, Republicanos`.
- `2022.1`: `2022-03-31` to `2022-03-31`; raw parties `PL, PP, PSD, PSDB, PSL, Patriota, Republicanos`.
- `2022.2`: `2022-04-01` to `2022-04-01`; raw parties `PL, PP, PSD, PSL, Patriota, Republicanos`.
- `2022.3`: `2022-04-02` to `2022-12-31`; raw parties `PP, PSD, PSL, Patriota, Republicanos`.
- `2023.1`: `2023-01-01` to `2023-09-12`; raw parties `MDB, PCdoB, PDT, PSB, PSD, PSOL, PT, REDE, UNIÃO`.
- `2023.2`: `2023-09-13` to `2025-12-23`; raw parties `MDB, PCdoB, PDT, PP, PSB, PSD, PSOL, PT, REDE, Republicanos, UNIÃO`.
- `2025.1`: `2025-12-24` to `2026-03-19`; raw parties `MDB, PCdoB, PDT, PP, PSB, PSD, PSOL, PT, REDE, Republicanos`.

`coalitions_by_year` first returns keys whose label starts with the requested year prefix. It only uses date-window overlap when no direct `YYYY.` keys exist. The diagnostic found these direct-prefix versus date-overlap differences:

- 2016 direct keys omit overlapping `2015.1`.
- 2017 direct keys omit overlapping `2016.4`.
- 2018 direct keys omit overlapping `2017.1`.
- 2020 direct keys omit overlapping `2019.1`.
- 2021 direct keys omit overlapping `2020.4`.
- 2022 direct keys omit overlapping `2021.3`.
- 2024 has no direct key, so it falls back to overlap and returns `2023.2`.
- 2025 direct keys return `2025.1` and omit overlapping `2023.2`.

## Status

Problem found.

The high-level election-to-mandate linkage is implemented as intended, and no selected period obviously belongs to a different electoral mandate. The mandate-end years are included in full by construction: all 2018 periods are linked to the 2014 election, all 2022 periods are linked to the 2018 election, and all available 2025 periods are linked to the 2022 election.

However, the mechanism is not a consistent date-window linkage. `coalitions_by_year` prioritizes direct `YYYY.` labels and ignores overlapping periods from previous labels whenever any direct keys exist. This matters because the JSON has periods that span calendar years. The current output is therefore best interpreted as a label-year period selection, with date-overlap fallback only for years that lack their own labels.

The 2022-election linkage also has a notable 2025 issue: `2023.2` runs through `2025-12-23`, but the selected 2025 bucket contains only `2025.1`, starting `2025-12-24`. The merged 2022-election coalition table still includes `2023.2` because it is returned by the 2023 call, but a year-specific reading of the 2025 call would omit the coalition covering almost all of 2025.

## Consequence for the analysis

For 2014 and 2018, merging all mandate years still recovers the spanning periods somewhere in the election-level set, because omitted overlaps usually appear under their own earlier label-year calls. This makes the election-level period list broadly coherent, although individual `coalitions_by_year(year)` calls should not be read as all periods active during that calendar year.

For 2022, the omission is substantive: the election-level observed coalition set includes `2023.1`, `2023.2`, and `2025.1`, but not `2023.2` as a 2025-overlapping period through `2025-12-23` if one expects full year-window coverage for 2025. Since `2023.2` is already included via the 2023 call, this does not remove it from the merged election-level table, but it affects the interpretation of "2025 periods" and any year-specific reasoning about 2025.

Including mandate-end years in full is a substantive research choice. The script currently treats the full calendar end year as part of the prior election's governing mandate. That is plausible for comparing election results to post-election governing coalitions through the end of the presidential term, but it should be stated explicitly because election-year coalition changes can occur during the next campaign and transition.

## Possible follow-up

- Decide whether `coalitions_by_year` should always return date-overlapping periods instead of preferring direct `YYYY.` labels.
- If label-year selection is intentional, rename or document the helper so printed tables are not mistaken for date-window coverage.
- Add a diagnostic print that distinguishes period label year from actual `period_start` / `period_end` overlap years.
- For the 2022-election analysis, decide whether `2025.1` should be included now even though it begins on `2025-12-24` and extends into `2026-03-19`.
- Document explicitly why the mandate-end year is included in full for 2014 and 2018, and why the available 2025 portion is included for 2022.

## Resolution

The ambiguous helper was replaced with explicit selectors:

- `coalition_periods_by_label_year(periods, year)` keeps the old direct `YYYY.` prefix semantics only for diagnostics about period labels.
- `coalition_periods_overlapping_year(periods, year; path=...)` selects all periods active during a calendar year by inclusive date overlap.
- `coalition_periods_overlapping_window(periods, start_date, end_date; path=...)` selects all periods active during a mandate or other substantive window by inclusive date overlap.

The old `coalitions_by_year` behavior mixed label-year selection with fallback overlap. That made `2025.1` look like "the 2025 coalition" even though `2023.2` runs from `2023-09-13` through `2025-12-23` and covers almost all of 2025. Under the new date-overlap semantics, calendar year 2025 correctly includes both `2023.2` and `2025.1`.

Main mandate linkage now uses mandate-window overlap instead of merging label-year buckets. The configured analysis windows are:

- 2014 election: `2015-01-01` through `2018-12-31`.
- 2018 election: `2019-01-01` through `2022-12-31`.
- 2022 election: `2023-01-01` through `2025-12-31`.

The 2022 endpoint remains `2025-12-31` because the available analysis is explicitly covering 2023-2025; the code comments flag that this should not be silently extended through 2026. The election-level 2022 period set remains `2023.1`, `2023.2`, and `2025.1`, but the interpretation is now date-correct: these are the periods overlapping the 2023-2025 mandate window, not simply periods with labels in 2023, 2024, or 2025.

`test_running.jl` now prints a compact linkage diagnostic with election year, mandate window, selected period key, `period_start`, `period_end`, date-overlap selection status, and raw party list before canonicalization. A deterministic test was added in `processing/Processing/test/test_coalition_period_linkage.jl`, and a read-only audit script was added at `processing/Processing/exploratory/audit/check_coalition_period_linkage.jl`, to pin the 2025 calendar-year edge case, the 2022 mandate period set, duplicate-free mandate selection, and start-date/key sorting used in the audit.
