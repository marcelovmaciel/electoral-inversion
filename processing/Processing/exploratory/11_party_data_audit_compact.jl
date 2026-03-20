"""
Deprecated wrapper.

Use `00_party_alias_audit.jl` as the single manual audit entrypoint.
This file is kept only to avoid broken includes from old notes.
"""

println("[deprecated] Use exploratory/00_party_alias_audit.jl")
include(joinpath(@__DIR__, "00_party_alias_audit.jl"))
