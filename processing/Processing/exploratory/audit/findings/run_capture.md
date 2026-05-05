# Run capture baseline

- Audit item: Run `test_running.jl` from a fresh Julia session and capture stdout/stderr; identify whether the first failure occurs in `PART 1. LOAD DATA` or `PART 2. ANALYZE SEAT DIFFERENTIALS`; if the script stops, record exact year/subsection; save printed parties, coalition periods, and ideology source names.
- Files inspected:
  - `processing/Processing/exploratory/test_running.jl`
  - `processing/Processing/src/Processing.jl`
  - `processing/Processing/src/analysis_runner_core.jl`
- Diagnostic run:
  - From repository root: `/bin/bash -lc 'mkdir -p processing/Processing/exploratory/audit/out && cd processing/Processing/exploratory && julia test_running.jl 2>&1 | tee audit/out/test_running_stdout.txt'`
  - Captured output: `processing/Processing/exploratory/audit/out/test_running_stdout.txt`
- Evidence observed:
  - The command exited with status `0`.
  - Captured output has `477` lines.
  - `PART 1. LOAD DATA` appears before data loading summaries.
  - `PART 2. ANALYZE SEAT DIFFERENTIALS` appears after party summaries are built.
  - No `ERROR`, `LoadError`, or `Stacktrace` marker was found in the captured output scan.
  - The script printed classification party lists for 2023 and 2025.
  - The script printed vote-party and seat-party lists for 2014, 2018, and 2022.
  - The script printed coalition periods linked to the 2014, 2018, and 2022 elections.
  - The script printed ideology sources: 2014 uses 2023, 2018 uses 2023, and 2022 uses 2025.
- Status: confirmed
- Consequence for the analysis:
  - This establishes a captured baseline run for later audit items. It does not establish substantive correctness of vote columns, party identity translation, coalition translation, ideology ordering, or `candidate_inversion`.
  - Because the script completed, there is no first failing year or subsection to record for this item.
- Possible follow-up:
  - Use the captured party lists, coalition-period lists, and ideology source names as the reference output when auditing party identity, observed coalitions, ideology sweeps, and vote-column handling.

## Post-vote-column-fix revalidation

- Status: changed but still confirmed.
- Diagnostic run:
  - From repository root: `/bin/bash -lc 'cd processing/Processing/exploratory && julia test_running.jl > audit/out/test_running_stdout_after_vote_column_fix.txt 2>&1; status=$?; printf "STATUS=%s\n" "$status"; wc -l audit/out/test_running_stdout_after_vote_column_fix.txt'`
  - Captured stdout/stderr: `processing/Processing/exploratory/audit/out/test_running_stdout_after_vote_column_fix.txt`
- Evidence observed:
  - The post-fix command exited with status `0`.
  - The post-fix captured output has `458` lines.
  - No `ERROR`, `LoadError`, or `Stacktrace` marker was found in the post-fix captured output scan.
  - `PART 1. LOAD DATA` and `PART 2. ANALYZE SEAT DIFFERENTIALS` both appear in the post-fix capture.
  - The script still prints classification party lists, vote-party and seat-party lists for 2014, 2018, and 2022, coalition periods linked to the audited elections, and ideology source names.
- Supersession note:
  - The original capture at `processing/Processing/exploratory/audit/out/test_running_stdout.txt` remains pre-fix evidence that the script completed, printed the expected diagnostic blocks, and reached both major parts.
  - For 2018 and 2022, the original capture is superseded for vote-dependent quantities, including vote totals, vote shares, quotas, seat differentials, coalition vote shares, coalition seat differentials, and inversion flags.
  - 2014 vote-dependent quantities are not superseded by this fix because the selected vote columns were unchanged.
