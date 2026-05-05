# Party Identity Refactor Note

## Strategy chosen
Use exactly one identity pipeline: `normalize_party` + `canonical_party` backed by a single CSV alias table (`processing/Processing/data/party_aliases.csv`) with optional year windows.

- No lineage IDs.
- No secondary canonicalization dictionaries in code.
- One canonical label per alias-year resolution.

## Files removed
- `processing/Processing/src/party_name_canonicalization.jl`
- `processing/Processing/src/party_lineage.jl `
 
- `processing/Processing/src/party_lineage_audit.jl`
- `processing/Processing/src/explore.jl`
- `processing/Processing/src/coalition_strict.jl`

Reason: these files implemented overlapping identity systems (lexical maps, lineage IDs, strict crosswalk logic, duplicate audit paths).

## Edge cases found
Historical rename aliases require year windows to avoid wrong merges:
- `PMDB` -> `PMDB` (until 2017), `MDB` (from 2018)
- `PR` -> `PR` (until 2018), `PL` (from 2019)
- `PRB` -> `PRB` (until 2018), `REPUBLICANOS` (from 2019)
- `PPS` -> `PPS` (until 2018), `CIDADANIA` (from 2019)
- `PEN` -> `PEN` (until 2016), `PATRIOTA` (from 2017)
- `PTN` -> `PTN` (until 2016), `PODE` (from 2017)
- `PT DO B` -> `PT DO B` (until 2016), `AVANTE` (from 2017)
- `PARTIDO ECOLOGICO NACIONAL` uses the same rename window as `PEN`.
