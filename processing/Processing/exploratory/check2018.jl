import Processing as p
import Processing.AnalysisRunnerCore as core

repo_root = joinpath(@__DIR__, "..", "..", "..")
data_root = joinpath(repo_root, "data", "raw", "electionsBR")
coalition_json_path = joinpath(repo_root, "scraping", "output", "partidos_por_periodo.json")

inputs_2018 = core.load_inputs(
    2018;
    root = data_root,
    coalition_path = coalition_json_path,
)

votes_2018 = inputs_2018.votes
seats_2018 = inputs_2018.seats
foo = core.compute_seat_differentials(inputs_2018)

# Mantém disponível para inspeção exploratória, sem afetar o core.
invalid_votes_2018 = p.national_invalid_votes(2018)

coalitions_2019_2022 = core.compute_inversion_tables(foo; year = 2018)

coalitions_2019 = filter(:coalition_year => ==(2019), coalitions_2019_2022)
coalitions_2020 = filter(:coalition_year => ==(2020), coalitions_2019_2022)
coalitions_2021 = filter(:coalition_year => ==(2021), coalitions_2019_2022)
coalitions_2022 = filter(:coalition_year => ==(2022), coalitions_2019_2022)

coalition_stability_2018 = core.compute_coalition_stability(
    coalitions_2019_2022;
    election_year = 2018,
    coalition_path = coalition_json_path,
)
mandate_summary_2018 = core.summarize_mandate_stability(
    coalition_stability_2018;
    election_year = 2018,
)

covered_days_2018 = Int(mandate_summary_2018.covered_days[1])
mandate_total_days_2018 = Int(mandate_summary_2018.mandate_total_days[1])
covered_days_2018 <= mandate_total_days_2018 || error("Cobertura 2018 excede o mandato.")
covered_days_2018 == mandate_total_days_2018 || error(
    "Cobertura 2018 incompleta: $covered_days_2018 / $mandate_total_days_2018 dias.",
)

stable_share_of_mandate_2018 = Float64(mandate_summary_2018.coverage_share_of_mandate[1])
inversion_share_of_mandate_2018 = Float64(mandate_summary_2018.inversion_true_share_of_mandate[1])

0.0 <= stable_share_of_mandate_2018 <= 1.0 || error("share estável 2018 fora de [0,1].")
0.0 <= inversion_share_of_mandate_2018 <= 1.0 || error("share inversão 2018 fora de [0,1].")
