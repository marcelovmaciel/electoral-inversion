#!/usr/bin/env bash
set -euo pipefail

report_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
decomposition_dir="$(dirname -- "$report_dir")"
processing_root="$(dirname -- "$decomposition_dir")"
output_dir="$processing_root/output/decomposition/report"
runner="$decomposition_dir/run_intermediate_accounting_report.jl"
report_source="$report_dir/intermediate_accounting_report.tex"

mkdir -p -- "$output_dir"

ALLOW_OVERWRITE=true \
SYNC_REVIEW_ASSETS=false \
julia -O0 --startup-file=no --project="$processing_root" "$runner"

latexmk -cd -pdf -interaction=nonstopmode -halt-on-error \
  -outdir=../../output/decomposition/report \
  "$report_source"
