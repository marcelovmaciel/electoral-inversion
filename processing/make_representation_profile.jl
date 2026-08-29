#!/usr/bin/env julia

ENV["MPLBACKEND"] = "Agg"

using Processing
using CSV
using DataFrames


const REPOSITORY_ROOT = normpath(joinpath(@__DIR__, ".."))
const INPUT_PATH = joinpath(
    REPOSITORY_ROOT,
    "processing",
    "Processing",
    "output",
    "paper",
    "figure_data",
    "party_vote_share_vs_seat_share.csv",
)
const DATA_OUTPUT_PATH = joinpath(
    REPOSITORY_ROOT,
    "processing",
    "Processing",
    "output",
    "paper",
    "figure_data",
    "party_representation_profile.csv",
)
const FIGURE_OUTPUT_PATH = get(
    ENV,
    "REPRESENTATION_PROFILE_FIGURE_PATH",
    joinpath(
        REPOSITORY_ROOT,
        "writing",
        "submission_inversions_review",
        "manuscript",
        "party_representation_profile.pdf",
    ),
)


data = Processing.make_representation_profile(
    INPUT_PATH,
    DATA_OUTPUT_PATH,
    FIGURE_OUTPUT_PATH,
)
summary = Processing.representation_profile_summary(data)

# Keep the separately rendered representation-profile data inside the paper
# artifact inventory. The runner rebuilds the base manifest first; this script
# then replaces its own row and restores the self-describing manifest row.
manifest_path = joinpath(
    REPOSITORY_ROOT,
    "processing",
    "Processing",
    "output",
    "paper",
    "artifact_manifest.csv",
)
manifest = CSV.read(manifest_path, DataFrame)
relative_data_path = "figure_data/party_representation_profile.csv"
filter!(
    row -> String(row.path) != relative_data_path &&
           String(row.path) != "artifact_manifest.csv",
    manifest,
)
push!(manifest, (
    path = relative_data_path,
    artifact_type = "figure_data",
    description = "Full-precision party representation-ratio profile used by the manuscript figure.",
    rows = nrow(data),
    columns = length(names(data)),
))
push!(manifest, (
    path = "artifact_manifest.csv",
    artifact_type = "manifest",
    description = "Manifest of every generated paper-runner and decomposition artifact.",
    rows = nrow(manifest) + 1,
    columns = 5,
))
CSV.write(manifest_path, manifest)

println("Wrote $DATA_OUTPUT_PATH")
println("Wrote $FIGURE_OUTPUT_PATH")
println("Updated $manifest_path")
for row in eachrow(summary)
    println(
        "$(row.election_year): parties=$(row.parties), " *
        "representation_ratio_min=$(round(row.representation_ratio_min; digits = 6)), " *
        "representation_ratio_max=$(round(row.representation_ratio_max; digits = 6)), " *
        "zero_seat_parties=$(row.zero_seat_parties)",
    )
end
