#!/usr/bin/env julia

using Pkg

const INTERMEDIATE_REPORT_PROCESSING_ROOT = normpath(joinpath(@__DIR__, ".."))
Pkg.activate(INTERMEDIATE_REPORT_PROCESSING_ROOT)

include(joinpath(@__DIR__, "test_intermediate_accounting_report.jl"))
