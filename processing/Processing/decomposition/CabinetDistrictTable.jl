# Table-specific aggregation of existing exact district contributions. Scalar prose
# cites this table's machine-readable output and its raw concentration summaries.
function cabinet_district_concentration(states::DataFrame)
    records = NamedTuple[]
    cabinets = states[states.case_domain .== "cabinet", :]
    for rows in groupby(cabinets, :case_id)
        first_row = first(eachrow(rows))
        contributions = parse_exact.(rows.a_Cd_exact)
        positive = sort(filter(>(0), contributions); rev = true)
        negative = filter(<(0), contributions)
        positive_sum = sum(positive)
        top_three_share = sum(first(positive, min(3, length(positive)))) / positive_sum
        ninety_count = findfirst(>=(9//10 * positive_sum), cumsum(positive))
        push!(records, (case_id = first_row.case_id, election_year = first_row.election_year,
            cabinet_period = first_row.cabinet_period, case_domain = "cabinet",
            ideological_universe = "not_applicable", positive_count = length(positive),
            negative_count = length(negative), positive_sum = Float64(positive_sum),
            negative_sum = Float64(sum(negative)), top_three_positive_share = Float64(top_three_share),
            districts_for_ninety_pct = ninety_count,
            positive_sum_exact = exact_text(positive_sum), negative_sum_exact = exact_text(sum(negative)),
            top_three_positive_share_exact = exact_text(top_three_share)))
    end
    DataFrame(records)
end

function cabinet_district_concentration_latex(data)
    lines = String[
        raw"\begin{tabular}{llcrrrr}", raw"\toprule",
        raw"Election & Period &",
        raw"\shortstack{Districts\\positive/negative} &",
        raw"\shortstack{Positive\\sum} &",
        raw"\shortstack{Negative\\sum} &",
        raw"\shortstack{Top three\\share (\%)} &",
        raw"\shortstack{Districts\\for 90\%} \\ ", raw"\midrule",
    ]
    for row in eachrow(data)
        push!(lines, "$(row.election_year) & $(row.cabinet_period) & " *
            "$(row.positive_count)/$(row.negative_count) & $(fmt2(row.positive_sum)) & " *
            "$(fmt2(row.negative_sum)) & $(@sprintf("%.1f", 100 * row.top_three_positive_share)) & " *
            "$(row.districts_for_ninety_pct) \\\\")
    end
    append!(lines, [raw"\bottomrule", raw"\end{tabular}"])
    join(lines, "\n")
end
