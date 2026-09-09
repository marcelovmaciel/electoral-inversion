#!/usr/bin/env bash
# Full production workflow. No generated numerical table is edited by hand.
set -euo pipefail
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$project_root"
export ALLOW_OVERWRITE=true SYNC_REVIEW_ASSETS=true MPLBACKEND=Agg
julia_cmd=${JULIA_BIN:-julia}
python_cmd=${PYTHON_BIN:-python3}
export PYTHON="$python_cmd"
if [[ ${1:-} == --clean ]]; then
  # Only generated analysis trees; source data and manuscript source are untouched.
  "$python_cmd" - <<'PY'
from pathlib import Path
import shutil
for name in ('paper', 'decomposition'):
    path = Path('processing/Processing/output') / name
    if path.exists():
        shutil.rmtree(path)
PY
fi
"$julia_cmd" -O0 --startup-file=no --project=processing/Processing processing/Processing/running/running.jl
"$julia_cmd" -O0 --startup-file=no --project=processing/Processing processing/Processing/decomposition/run_decomposition.jl
"$julia_cmd" -O0 --startup-file=no --project=processing/Processing processing/make_representation_profile.jl
"$python_cmd" writing/make_coalition_figures.py --artifact-root processing/Processing/output/paper --figure-dir writing/submission_inversions_review/manuscript
"$python_cmd" processing/audit_ideological_universes.py
latexmk -cd -g -pdf -interaction=nonstopmode -halt-on-error -outdir=../../output/decomposition/report processing/Processing/decomposition/report/intermediate_accounting_report.tex
cd writing/submission_inversions_review/manuscript
latexmk -g -pdf -interaction=nonstopmode -halt-on-error main_rw_again.tex

cd "$project_root"
"$python_cmd" processing/Processing/decomposition/audit_empirical_assets.py
"$python_cmd" writing/package_submission_assets.py
