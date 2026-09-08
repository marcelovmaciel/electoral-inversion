#!/usr/bin/env julia

using Pkg

const DECOMPOSITION_DIR = @__DIR__
const PROCESSING_ROOT = normpath(joinpath(DECOMPOSITION_DIR, ".."))
const REPO_ROOT = normpath(joinpath(PROCESSING_ROOT, "..", ".."))

Pkg.activate(PROCESSING_ROOT)

using CSV
using DataFrames
using SHA
using Processing

include(joinpath(DECOMPOSITION_DIR, "CoalitionDecomposition.jl"))
using .CoalitionDecomposition
include(joinpath(DECOMPOSITION_DIR, "IntermediateAccountingReport.jl"))
using .IntermediateAccountingReport
include(joinpath(DECOMPOSITION_DIR, "AccountingIntegration.jl"))
using .AccountingIntegration
include(joinpath(DECOMPOSITION_DIR, "DualUniverseAccounting.jl"))

const PAPER_ROOT = joinpath(PROCESSING_ROOT, "output", "paper")
const OUTPUT_ROOT = joinpath(PROCESSING_ROOT, "output", "decomposition")
const DATA_ROOT = joinpath(REPO_ROOT, "data", "raw", "electionsBR")
const ALLOW_OVERWRITE = lowercase(get(ENV, "ALLOW_OVERWRITE", "false")) in
    ("1", "true", "yes")
const EXPECTED_NATIONAL_VOTES = Dict(
    2014 => 97_355_354,
    2018 => 98_264_190,
    2022 => 109_413_508,
)

ALLOW_OVERWRITE || error(
    "The intermediate accounting report overwrites generated artifacts. " *
    "Set ALLOW_OVERWRITE=true.",
)

function require_file(path::AbstractString)
    isfile(path) || error("Required corrected-baseline input is missing: $(path)")
    return path
end

function sha256_file(path::AbstractString)
    return open(path, "r") do io
        bytes2hex(SHA.sha256(io))
    end
end

println("Intermediate party-district accounting report rebuild")
println("Julia version: ", VERSION)
println("Processing root: ", PROCESSING_ROOT)
println("Output root: ", OUTPUT_ROOT)
println("ACCOUNTING_ATOL: ", ACCOUNTING_ATOL)
println("ACCOUNTING_RTOL: ", ACCOUNTING_RTOL)

observed_path = require_file(
    joinpath(PAPER_ROOT, "raw", "cabinet_coalition_metrics.csv"),
)
party_path = require_file(
    joinpath(PAPER_ROOT, "raw", "party_seat_differentials_all_years.csv"),
)
ideological_path = require_file(
    joinpath(PAPER_ROOT, "raw", "ideological_interval_metrics.csv"),
)

observed = CSV.read(observed_path, DataFrame)
party_baseline = CSV.read(party_path, DataFrame)
ideological_intervals = CSV.read(ideological_path, DataFrame)

accounting_by_year = Dict{Int,Any}()
raw_input_paths = String[]
for year in sort(collect(keys(EXPECTED_NATIONAL_VOTES)))
    vote_path = require_file(
        joinpath(DATA_ROOT, string(year), "party_mun_zone.csv"),
    )
    candidate_path = require_file(
        joinpath(DATA_ROOT, string(year), "candidate.csv"),
    )
    apportionment_path = require_file(
        joinpath(DATA_ROOT, string(year), "seats.csv"),
    )
    append!(raw_input_paths, (vote_path, candidate_path, apportionment_path))
    accounting_by_year[year] = build_year_accounting(
        year,
        vote_path,
        candidate_path;
        apportionment_path = apportionment_path,
        expected_national_votes = EXPECTED_NATIONAL_VOTES[year],
    )
end

coalition_periods = recompute_coalition_periods(
    observed,
    accounting_by_year;
    party_baseline = party_baseline,
)
cabinet_reference = decompose_inversions(coalition_periods, accounting_by_year)
ideological_regression = validate_ideological_counts(ideological_intervals)

full = build_full_accounting_outputs(accounting_by_year)
party_size_diagnostics = build_party_size_diagnostics!(
    full.parties, coalition_periods, accounting_by_year,
)
registry = build_inversion_case_registry(
    coalition_periods,
    ideological_intervals,
    accounting_by_year,
)
cases = decompose_case_registry(registry, accounting_by_year)
validate_cabinet_compatibility!(cases, cabinet_reference)
rankings = build_case_rankings(cases)
accounting_integration = build_accounting_integration(registry, accounting_by_year)
integration_artifacts = write_accounting_integration_outputs(OUTPUT_ROOT, accounting_integration)


processing_source_paths = sort(filter(
    path -> isfile(path) && endswith(lowercase(path), ".jl"),
    readdir(joinpath(PROCESSING_ROOT, "src"); join = true),
))
input_paths = String[
    observed_path,
    party_path,
    ideological_path,
    joinpath(DECOMPOSITION_DIR, "CoalitionDecomposition.jl"),
    joinpath(DECOMPOSITION_DIR, "IntermediateAccountingReport.jl"),
    joinpath(DECOMPOSITION_DIR, "PartySizeDiagnostics.jl"),
    joinpath(DECOMPOSITION_DIR, "AccountingIntegration.jl"),
    joinpath(DECOMPOSITION_DIR, "run_intermediate_accounting_report.jl"),
    joinpath(DECOMPOSITION_DIR, "report", "intermediate_accounting_report.tex"),
    joinpath(DECOMPOSITION_DIR, "report", "build_report.sh"),
    joinpath(PROCESSING_ROOT, "Project.toml"),
    joinpath(PROCESSING_ROOT, "Manifest.toml"),
    joinpath(PROCESSING_ROOT, "data", "party_aliases.csv"),
    joinpath(PROCESSING_ROOT, "data", "party_lineage_events.csv"),
    joinpath(PROCESSING_ROOT, "psc_baseline_repair", "POST_PSC_BASELINE.md"),
    joinpath(PROCESSING_ROOT, "psc_baseline_repair", "post_psc_baseline_manifest.csv"),
]
append!(input_paths, processing_source_paths)
append!(input_paths, raw_input_paths)
input_paths = sort(unique(require_file.(input_paths)))
input_manifest = DataFrame([
    (
        path = relpath(path, REPO_ROOT),
        bytes = filesize(path),
        sha256 = sha256_file(path),
    ) for path in input_paths
])
sort!(input_manifest, :path)

manifest = write_intermediate_report_outputs(
    OUTPUT_ROOT,
    full,
    registry,
    cases,
    rankings;
    input_manifest = input_manifest,
    party_size_diagnostics = party_size_diagnostics,
)

DualUniverseAccounting.write_dual_universe_outputs(
    PAPER_ROOT, OUTPUT_ROOT, coalition_periods, accounting_by_year,
)

println("Validated ideological inversion counts:")
show(stdout, MIME("text/plain"), ideological_regression; allrows = true, allcols = true)
println()
println("Complete party-district cells: ", nrow(full.cells))
println("Combined inversion cases: ", nrow(registry))
println("  cabinet: ", sum(registry.case_domain .== "cabinet"))
println("  ideological: ", sum(registry.case_domain .== "ideological"))
println("Case-party rows: ", nrow(cases.party_contributions))
println("Case-district rows: ", nrow(cases.district_contributions))
println("Case-party-district rows: ", nrow(cases.party_district_contributions))
println("Ranking rows: ", nrow(rankings))
println("Inversion accounting summary:")
for row in eachrow(cases.decomposition)
    println(
        "- $(row.case_id): q_C=$(row.q_C), d_C=$(row.d_C), r_C=$(row.r_C), " *
        "A_C=$(row.A_C), B_C=$(row.B_C)",
    )
end
println("Generated accounting-integration artifacts: ", length(integration_artifacts))
println("Generated intermediate-report artifacts: ", nrow(manifest))
