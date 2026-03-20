import Processing.AnalysisRunnerCore as core

repo_root = joinpath(@__DIR__, "..", "..", "..")
data_root = joinpath(repo_root, "data", "raw", "electionsBR")
coalition_json_path = joinpath(repo_root, "scraping", "output", "partidos_por_periodo.json")

inputs_2014 = core.load_inputs(
    2014;
    root = data_root,
    coalition_path = coalition_json_path,
)

votes_2014 = inputs_2014.votes
seats_2014 = inputs_2014.seats

foo = core.compute_seat_differentials(inputs_2014)

coalitions_2015_2018 = core.compute_inversion_tables(foo; year = 2014)
coalitions_2015 = filter(:coalition_year => ==(2015), coalitions_2015_2018)
coalitions_2016 = filter(:coalition_year => ==(2016), coalitions_2015_2018)
coalitions_2017 = filter(:coalition_year => ==(2017), coalitions_2015_2018)
coalitions_2018 = filter(:coalition_year => ==(2018), coalitions_2015_2018)

coalition_stability_2014 = core.compute_coalition_stability(
    coalitions_2015_2018;
    election_year = 2014,
    coalition_path = coalition_json_path,
)
mandate_summary_2014 = core.summarize_mandate_stability(
    coalition_stability_2014;
    election_year = 2014,
)

covered_days_2014 = Int(mandate_summary_2014.covered_days[1])
mandate_total_days_2014 = Int(mandate_summary_2014.mandate_total_days[1])
covered_days_2014 <= mandate_total_days_2014 || error("Cobertura 2014 excede o mandato.")
covered_days_2014 == mandate_total_days_2014 || error(
    "Cobertura 2014 incompleta: $covered_days_2014 / $mandate_total_days_2014 dias.",
)

stable_share_of_mandate_2014 = Float64(mandate_summary_2014.coverage_share_of_mandate[1])
inversion_share_of_mandate_2014 = Float64(mandate_summary_2014.inversion_true_share_of_mandate[1])

0.0 <= stable_share_of_mandate_2014 <= 1.0 || error("share estável 2014 fora de [0,1].")
0.0 <= inversion_share_of_mandate_2014 <= 1.0 || error("share inversão 2014 fora de [0,1].")
