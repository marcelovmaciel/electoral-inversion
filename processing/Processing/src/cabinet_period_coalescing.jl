"""
    coalesce_adjacent_cabinet_periods(translated; expected_merges=nothing)

Combine calendar-adjacent rows within an election only when their translated
election-year party sets are identical. Electoral quantities are checked for
exact equality and inherited, never summed. `source_periods` is a JSON array
stored as text so provenance survives CSV round trips. The input is unchanged.

`expected_merges`, when supplied by the manuscript runner, guards its current
sample against any additional (or missing) merges.
"""
function coalesce_adjacent_cabinet_periods(translated::DataFrame; expected_merges = nothing)
    ordered = sort(copy(translated), [:election_year, :period_start, :period_end])
    ordered[!, :period] = String.(ordered.period)
    ordered[!, :parties] = String.(ordered.parties)
    ordered[!, :source_periods] = [JSON3.write([String(period)]) for period in ordered.period]
    result = ordered[1:0, :]
    temporal_columns = Set([
        :coalition_year, :period, :period_start, :period_end, :period_days,
        :days_overlapping_mandate, :share_of_mandate, :source_periods, :parties,
    ])
    quantity_columns = setdiff(propertynames(ordered), collect(temporal_columns))
    party_set(cell) = Set(strip.(split(String(cell), ',')))
    for row in eachrow(ordered)
        if nrow(result) > 0 && result.election_year[end] == row.election_year &&
            Date(result.period_end[end]) + Day(1) == Date(row.period_start) &&
            party_set(result.parties[end]) == party_set(row.parties)
            previous = result[end, :]
            for column in quantity_columns
                isequal(previous[column], row[column]) || error(
                    "Cabinet coalescing: identical translated parties have unequal $(column) " *
                    "in $(previous.period) and $(row.period).",
                )
            end
            sources = String.(JSON3.read(previous.source_periods))
            push!(sources, String(row.period))
            result.period[end] = join(sources, "/")
            result.source_periods[end] = JSON3.write(sources)
            result.period_end[end] = row.period_end
            for column in (:period_days, :days_overlapping_mandate, :share_of_mandate)
                result[end, column] += row[column]
            end
        else
            push!(result, row)
        end
    end
    actual_merges = Set(
        (Int(row.election_year), Tuple(String.(JSON3.read(row.source_periods))))
        for row in eachrow(result) if length(JSON3.read(row.source_periods)) > 1
    )
    if expected_merges !== nothing && actual_merges != Set(expected_merges)
        error(
            "Cabinet coalescing regression failed: expected merges $(Set(expected_merges)); " *
            "found $(actual_merges). Unexpected merges: $(setdiff(actual_merges, Set(expected_merges))); " *
            "missing merges: $(setdiff(Set(expected_merges), actual_merges)).",
        )
    end
    return result
end
