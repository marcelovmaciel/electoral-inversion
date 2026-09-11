#!/usr/bin/env bash
# Reproduce the existing paper's Julia 1.12.7 numerical runtime.
set -euo pipefail
paper_julia=${JULIA_PAPER_EXECUTABLE:-"$HOME/.julia/juliaup/julia-1.12.7+0.x64.linux.gnu/bin/julia"}
exec "$paper_julia" -Cgeneric --compiled-modules=no --pkgimages=no --threads=1 --gcthreads=1 "$@"
