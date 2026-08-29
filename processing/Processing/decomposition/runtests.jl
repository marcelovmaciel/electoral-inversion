#!/usr/bin/env julia

using Pkg

const PROCESSING_ROOT = normpath(joinpath(@__DIR__, ".."))
Pkg.activate(PROCESSING_ROOT)

include(joinpath(@__DIR__, "test_decomposition.jl"))

