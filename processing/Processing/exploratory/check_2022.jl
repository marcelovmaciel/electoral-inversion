import Processing.AnalysisRunnerCore as core

repo_root = joinpath(@__DIR__, "..", "..", "..")
data_root = joinpath(repo_root, "data", "raw", "electionsBR")
coalition_json_path = joinpath(repo_root, "scraping", "output", "partidos_por_periodo.json")

inputs_2022 = core.load_inputs(
    2022;
    root = data_root,
    coalition_path = coalition_json_path,
)

votes_2022 = inputs_2022.votes
seats_2022 = inputs_2022.seats
foo = core.compute_seat_differentials(inputs_2022)

coalitions_2023_2025 = core.compute_inversion_tables(foo; year = 2022)

coalitions_2023 = filter(:coalition_year => ==(2023), coalitions_2023_2025)
coalitions_2024 = filter(:coalition_year => ==(2024), coalitions_2023_2025)
coalitions_2025 = filter(:coalition_year => ==(2025), coalitions_2023_2025)

coalitions_2023_2025_unique = unique(coalitions_2023_2025, :coalition_col)

coalition_stability_2022 = core.compute_coalition_stability(
    coalitions_2023_2025;
    election_year = 2022,
    coalition_path = coalition_json_path,
)


coalition_stability_2022 |> println

mandate_summary_2022 = core.summarize_mandate_stability(
    coalition_stability_2022;
    election_year = 2022,
)

mandate_summary_2022 |> println

1-851/1106


covered_days_2022 = Int(mandate_summary_2022.covered_days[1])
mandate_total_days_2022 = Int(mandate_summary_2022.mandate_total_days[1])
covered_days_2022 <= mandate_total_days_2022 || error("Cobertura 2022 excede o mandato.")

if covered_days_2022 < mandate_total_days_2022
    println(
        "WARN: cobertura parcial para 2022: $covered_days_2022 / $mandate_total_days_2022 dias do mandato.",
    )
end

stable_share_of_mandate_2022 = Float64(mandate_summary_2022.coverage_share_of_mandate[1])
inversion_share_of_mandate_2022 = Float64(mandate_summary_2022.inversion_true_share_of_mandate[1])

0.0 <= stable_share_of_mandate_2022 <= 1.0 || error("share estável 2022 fora de [0,1].")
0.0 <= inversion_share_of_mandate_2022 <= 1.0 || error("share inversão 2022 fora de [0,1].")
