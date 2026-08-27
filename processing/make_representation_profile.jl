#!/usr/bin/env julia

ENV["MPLBACKEND"] = "Agg"

using Processing


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
const FIGURE_OUTPUT_PATH = joinpath(
    REPOSITORY_ROOT,
    "writing",
    "submission_inversions_review",
    "manuscript",
    "party_representation_profile.pdf",
)


data = Processing.make_representation_profile(
    INPUT_PATH,
    DATA_OUTPUT_PATH,
    FIGURE_OUTPUT_PATH,
)
summary = Processing.representation_profile_summary(data)

println("Wrote $DATA_OUTPUT_PATH")
println("Wrote $FIGURE_OUTPUT_PATH")
for row in eachrow(summary)
    println(
        "$(row.election_year): parties=$(row.parties), " *
        "representation_ratio_min=$(round(row.representation_ratio_min; digits = 6)), " *
        "representation_ratio_max=$(round(row.representation_ratio_max; digits = 6)), " *
        "zero_seat_parties=$(row.zero_seat_parties)",
    )
end
