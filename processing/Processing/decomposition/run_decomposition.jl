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

const PAPER_ROOT = joinpath(PROCESSING_ROOT, "output", "paper")
const OUTPUT_ROOT = joinpath(PROCESSING_ROOT, "output", "decomposition")
const REVIEW_MANUSCRIPT_DIR = joinpath(
    REPO_ROOT,
    "writing",
    "submission_inversions_review",
    "manuscript",
)
const DATA_ROOT = joinpath(REPO_ROOT, "data", "raw", "electionsBR")
const ALLOW_OVERWRITE = lowercase(get(ENV, "ALLOW_OVERWRITE", "false")) in
    ("1", "true", "yes")
const SYNC_REVIEW_ASSETS = lowercase(get(ENV, "SYNC_REVIEW_ASSETS", "true")) in
    ("1", "true", "yes")
const EXPECTED_NATIONAL_VOTES = Dict(
    2014 => 97_355_354,
    2018 => 98_264_190,
    2022 => 109_413_508,
)

ALLOW_OVERWRITE || error(
    "The decomposition rebuild overwrites generated artifacts. Set ALLOW_OVERWRITE=true.",
)

function sha256_file(path::AbstractString)
    return open(path, "r") do io
        bytes2hex(SHA.sha256(io))
    end
end

function require_file(path::AbstractString)
    isfile(path) || error("Required corrected-baseline input is missing: $(path)")
    return path
end

const ACCOUNTING_ARTIFACT_PREFIXES = (
    "raw/accounting_",
    "tables/table_accounting_",
    "figure_data/accounting_",
    "latex/accounting_",
    "latex/table_accounting_",
    "accounting_",
    "table_accounting_",
    "raw/coalition_party_contribution",
    "tables/table_coalition_party_contribution",
    "latex/table_coalition_party_contribution",
    "coalition_party_contribution",
    "table_coalition_party_contribution",
)

function is_accounting_integration_artifact(relative_path::AbstractString)
    normalized = replace(String(relative_path), '\\' => '/')
    return any(prefix -> startswith(normalized, prefix), ACCOUNTING_ARTIFACT_PREFIXES)
end

function prune_stale_accounting_artifacts!(root::AbstractString, allowed_paths::Set{String})
    isdir(root) || return
    for (directory, _, filenames) in walkdir(root)
        for filename in filenames
            path = joinpath(directory, filename)
            relative = replace(relpath(path, root), '\\' => '/')
            if is_accounting_integration_artifact(relative) && !(relative in allowed_paths)
                rm(path; force = true)
            end
        end
    end
end


function update_manifest!(manifest_path::AbstractString, additions::DataFrame)
    manifest = CSV.read(require_file(manifest_path), DataFrame)
    required = Set([:path, :artifact_type, :description, :rows, :columns])
    required ⊆ Set(propertynames(manifest)) || error(
        "Paper artifact manifest schema changed: $(propertynames(manifest)).",
    )
    addition_paths = Set(String.(additions.path))
    filter!(row -> begin
        path = String(row.path)
        path != "artifact_manifest.csv" &&
            !is_accounting_integration_artifact(path) &&
            !(path in addition_paths)
    end, manifest)
    append!(manifest, select(additions, :path, :artifact_type, :description, :rows, :columns))
    push!(manifest, (
        path = "artifact_manifest.csv",
        artifact_type = "manifest",
        description = "Manifest of every generated paper-runner and decomposition artifact.",
        rows = nrow(manifest) + 1,
        columns = 5,
    ))
    CSV.write(manifest_path, manifest)
    return manifest
end

function sync_decomposition_to_paper!(manifest::DataFrame)
    paper_records = NamedTuple[]
    for row in eachrow(manifest)
        relative = String(row.path)
        startswith(relative, "audit/") && continue
        source = joinpath(OUTPUT_ROOT, relative)
        destination = joinpath(PAPER_ROOT, relative)
        mkpath(dirname(destination))
        cp(source, destination; force = true)
        push!(paper_records, (
            path = relative,
            artifact_type = String(row.artifact_type),
            description = String(row.description),
            rows = row.rows,
            columns = row.columns,
        ))
    end

    validation_source = joinpath(OUTPUT_ROOT, "audit", "decomposition_identity_checks.csv")
    validation_relative = joinpath("diagnostics", "decomposition_identity_checks.csv")
    validation_destination = joinpath(PAPER_ROOT, validation_relative)
    mkpath(dirname(validation_destination))
    cp(validation_source, validation_destination; force = true)
    validation = CSV.read(validation_source, DataFrame)
    push!(paper_records, (
        path = validation_relative,
        artifact_type = "diagnostic",
        description = "Exact and floating-point decomposition identity results.",
        rows = nrow(validation),
        columns = length(names(validation)),
    ))

    paper_additions = DataFrame(paper_records)
    paper_manifest = update_manifest!(joinpath(PAPER_ROOT, "artifact_manifest.csv"), paper_additions)
    prune_stale_accounting_artifacts!(PAPER_ROOT, Set(String.(paper_manifest.path)))

    if SYNC_REVIEW_ASSETS
        isdir(REVIEW_MANUSCRIPT_DIR) || error(
            "Review manuscript directory not found: $(REVIEW_MANUSCRIPT_DIR)",
        )
        review_filenames = (
            "table_observed_inversion_decomposition.tex",
            "table_inversion_party_contribution_extremes.tex",
            "accounting_numeric_macros.tex",
            "table_accounting_focal_cases.tex",
            "table_accounting_gross_components.tex",
            "table_accounting_selected_party_geography.tex",
            "table_accounting_minimal_ideological.tex",
            "table_coalition_party_contributions.tex",
        )
        for filename in review_filenames
            source = joinpath(OUTPUT_ROOT, "latex", filename)
            cp(source, joinpath(REVIEW_MANUSCRIPT_DIR, filename); force = true)
        end
        review_allowed = Set(String.(review_filenames))
        push!(review_allowed, "accounting_state_weighting_anatomy.pdf")
        prune_stale_accounting_artifacts!(REVIEW_MANUSCRIPT_DIR, review_allowed)
    end
    return paper_additions
end

function append_output_manifest_rows!(rows::AbstractVector{<:NamedTuple})
    manifest_path = joinpath(OUTPUT_ROOT, "artifact_manifest.csv")
    manifest = CSV.read(manifest_path, DataFrame)
    paths = Set(String.(getfield.(rows, :path)))
    filter!(row -> !(String(row.path) in paths), manifest)
    for row in rows
        push!(manifest, row)
    end
    sort!(manifest, :path)
    CSV.write(manifest_path, manifest)
    return manifest
end

println("Coalition decomposition rebuild")
println("Julia version: ", VERSION)
println("Processing root: ", PROCESSING_ROOT)
println("Output root: ", OUTPUT_ROOT)
println("ACCOUNTING_ATOL: ", ACCOUNTING_ATOL)
println("ACCOUNTING_RTOL: ", ACCOUNTING_RTOL)

observed_path = require_file(joinpath(PAPER_ROOT, "raw", "cabinet_coalition_metrics.csv"))
party_path = require_file(joinpath(PAPER_ROOT, "raw", "party_seat_differentials_all_years.csv"))
ideology_input_path = require_file(joinpath(PAPER_ROOT, "raw", "ideological_interval_metrics.csv"))
observed = CSV.read(observed_path, DataFrame)
party_baseline = CSV.read(party_path, DataFrame)
ideological_intervals = CSV.read(ideology_input_path, DataFrame)

accounting_by_year = Dict{Int,Any}()
for year in sort(collect(keys(EXPECTED_NATIONAL_VOTES)))
    vote_path = require_file(joinpath(DATA_ROOT, string(year), "party_mun_zone.csv"))
    candidate_path = require_file(joinpath(DATA_ROOT, string(year), "candidate.csv"))
    apportionment_path = require_file(joinpath(DATA_ROOT, string(year), "seats.csv"))
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
outputs = decompose_inversions(coalition_periods, accounting_by_year)
manifest = write_decomposition_outputs(OUTPUT_ROOT, coalition_periods, outputs)

ideological_regression = validate_ideological_counts(ideological_intervals)
ideological_audit_path = joinpath(OUTPUT_ROOT, "audit", "ideological_regression.csv")
CSV.write(ideological_audit_path, ideological_regression)
case_registry = build_inversion_case_registry(
    coalition_periods,
    ideological_intervals,
    accounting_by_year,
)
accounting_integration = build_accounting_integration(case_registry, accounting_by_year)
integration_artifacts = write_accounting_integration_outputs(OUTPUT_ROOT, accounting_integration)


input_paths = [
    observed_path,
    party_path,
    ideology_input_path,
    joinpath(DECOMPOSITION_DIR, "CoalitionDecomposition.jl"),
    joinpath(DECOMPOSITION_DIR, "IntermediateAccountingReport.jl"),
    joinpath(DECOMPOSITION_DIR, "AccountingIntegration.jl"),
    joinpath(DECOMPOSITION_DIR, "run_decomposition.jl"),
    joinpath(PROCESSING_ROOT, "psc_baseline_repair", "POST_PSC_BASELINE.md"),
    joinpath(PROCESSING_ROOT, "psc_baseline_repair", "post_psc_baseline_manifest.csv"),
]
for year in sort(collect(keys(EXPECTED_NATIONAL_VOTES)))
    append!(input_paths, [
        joinpath(DATA_ROOT, string(year), "party_mun_zone.csv"),
        joinpath(DATA_ROOT, string(year), "candidate.csv"),
        joinpath(DATA_ROOT, string(year), "seats.csv"),
    ])
end
input_manifest = DataFrame([
    (
        path = relpath(require_file(path), REPO_ROOT),
        bytes = filesize(path),
        sha256 = sha256_file(path),
    ) for path in input_paths
])
sort!(input_manifest, :path)
input_manifest_path = joinpath(OUTPUT_ROOT, "audit", "decomposition_input_manifest.csv")
CSV.write(input_manifest_path, input_manifest)

manifest = append_output_manifest_rows!(vcat(integration_artifacts, [
    (
        path = "audit/ideological_regression.csv",
        artifact_type = "audit",
        description = "Ideological inversion-count regression after the decomposition rebuild.",
        rows = nrow(ideological_regression),
        columns = length(names(ideological_regression)),
        sha256 = sha256_file(ideological_audit_path),
    ),
    (
        path = "audit/decomposition_input_manifest.csv",
        artifact_type = "audit",
        description = "SHA-256 provenance for corrected-baseline and district inputs.",
        rows = nrow(input_manifest),
        columns = length(names(input_manifest)),
        sha256 = sha256_file(input_manifest_path),
    ),
]))

prune_stale_accounting_artifacts!(OUTPUT_ROOT, Set(String.(manifest.path)))
paper_additions = sync_decomposition_to_paper!(manifest)

println("Recovered inversion cases:")
for row in eachrow(outputs.decomposition)
    println(
        "- $(row.coalition_id): q_C=$(row.q_C), d_C=$(row.d_C), r_C=$(row.r_C), " *
        "A_C=$(row.A_C), B_C=$(row.B_C)",
    )
end
println("Ideological regression:")
show(stdout, MIME("text/plain"), ideological_regression; allrows = true, allcols = true)
println()
println("Generated accounting-integration artifacts: ", length(integration_artifacts))
println("Focal accounting vectors: ", nrow(accounting_integration.focal.total))
println("Generated decomposition artifacts: ", nrow(manifest))
println("Synchronized paper artifacts: ", nrow(paper_additions))
