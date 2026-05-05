using Pkg

processing_root = normpath(joinpath(@__DIR__, "..", ".."))
Pkg.activate(processing_root)

using Dates

include(joinpath(processing_root, "src", "Processing.jl"))
using .Processing
import .Processing.AnalysisRunnerCore as ARC

repo_root = normpath(joinpath(processing_root, "..", ".."))
coalition_json_path = joinpath(repo_root, "scraping", "output", "partidos_por_periodo.json")

function period_keys_by_window(periods, windows)
    ks = collect(keys(periods))
    sort!(ks, by = k -> begin
        start_date, _ = windows[k]
        (start_date, Processing.period_sort_key(k))
    end)
    return ks
end

function require_equal(label, actual, expected)
    actual == expected || error("$label: esperado $(join(expected, ", ")), obtido $(join(actual, ", ")).")
    println(label, ": ", join(actual, ", "))
end

periods = Processing.coalitions_by_period_raw(; path = coalition_json_path)
windows = Processing.coalition_period_windows(; path = coalition_json_path)

overlap_2025 = Processing.coalition_periods_overlapping_year(periods, 2025; path = coalition_json_path)
label_2025 = Processing.coalition_periods_by_label_year(periods, 2025)
mandate_2022 = Processing.coalition_periods_overlapping_window(
    periods,
    Date(2023, 1, 1),
    Date(2025, 12, 31);
    path = coalition_json_path,
)

require_equal("calendar_2025_date_overlap", period_keys_by_window(overlap_2025, windows), ["2023.2", "2025.1"])
require_equal("calendar_2025_label_year", sort(collect(keys(label_2025)); by = Processing.period_sort_key), ["2025.1"])
require_equal("mandate_2022_date_overlap", period_keys_by_window(mandate_2022, windows), ["2023.1", "2023.2", "2025.1"])

mandate_keys = collect(keys(mandate_2022))
length(mandate_keys) == length(unique(mandate_keys)) || error("mandate_2022_date_overlap: chaves duplicadas.")

println()
println("Mandate linkage diagnostic")
for election_year in ARC.SUPPORTED_YEARS
    window = ARC.mandate_window(election_year)
    selected = Processing.coalition_periods_overlapping_window(
        periods,
        window.start_date,
        window.end_date;
        path = coalition_json_path,
    )

    for period in period_keys_by_window(selected, windows)
        period_start, period_end = windows[period]
        selected_by_date_overlap = Processing.overlaps_window(
            period_start,
            period_end,
            window.start_date,
            window.end_date,
        )
        println(
            election_year, " | ",
            window.start_date, " to ", window.end_date, " | ",
            period, " | ",
            period_start, " | ",
            period_end, " | ",
            selected_by_date_overlap, " | ",
            join(selected[period], ", "),
        )
    end
end
