"""Validate actual membership in the domain-minimal winning coalition registry."""
function validate_k_gap_membership_registry!(registry)
    required = (:election, :k, :coalition_id, :parties, :party_count, :gap_count,
                :omitted_party, :minimal_seat_majority, :seat_majority, :inversion,
                :votes, :national_vote_total, :seats, :seat_majority_threshold)
    all(column -> column in propertynames(registry), required) || error(
        "K-gap membership registry is missing required columns.")
    seen = Set{Tuple{Int,Int,String}}()
    for row in eachrow(registry)
        row.election isa Integer && !(row.election isa Bool) || error("Invalid election year.")
        row.k isa Integer && !(row.k isa Bool) && row.k in (0, 1) || error("Invalid k-gap domain.")
        key = (Int(row.election), Int(row.k), String(row.coalition_id))
        key in seen && error("Duplicate minimal coalition: $(key).")
        push!(seen, key)
        row.minimal_seat_majority === true && row.seat_majority === true || error(
            "Membership registry contains a non-minimal or non-winning coalition: $(key).")
        row.seats >= row.seat_majority_threshold || error("Invalid winning status: $(key).")
        row.inversion isa Bool && row.inversion == (2 * row.votes < row.national_vote_total) || error(
            "Invalid inversion status: $(key).")
        row.party_count isa Integer && !(row.party_count isa Bool) && row.party_count > 0 || error(
            "Coalition member counts must be positive integers: $(key).")
        # coalition_id is the canonical ordered member list, including seatless parties.
        # Never substitute an endpoint distance or ideological span for this count.
        members = split(row.coalition_id, '|'; keepempty = true)
        all(member -> !isempty(strip(member)), members) && allunique(members) || error(
            "Invalid canonical coalition membership: $(key).")
        members == strip.(split(row.parties, ','; keepempty = true)) || error(
            "Canonical ID and party list disagree: $(key).")
        row.party_count == length(members) || error("Member count disagrees with canonical ID: $(key).")
        omitted = ismissing(row.omitted_party) ? "" : String(row.omitted_party)
        row.gap_count isa Integer && 0 <= row.gap_count <= row.k || error("Invalid gap count: $(key).")
        row.gap_count == Int(!isempty(omitted)) || error("Omission and gap count disagree: $(key).")
        omitted in members && error("Omitted interior party counted as a member: $(key).")
    end
    return true
end

function coalition_membership_statistics(counts)
    isempty(counts) && return (median = missing, min = missing, max = missing)
    return (median = median(counts), min = minimum(counts), max = maximum(counts))
end

"""Summarize domain-minimal winning coalitions, using verified canonical party_count."""
function build_k_gap_membership_summary(registry)
    validate_k_gap_membership_registry!(registry)
    rows = NamedTuple[]
    for domain in groupby(registry, [:election, :k]; sort = true)
        inverted = domain.party_count[domain.inversion]
        non_inverted = domain.party_count[.!domain.inversion]
        inv = coalition_membership_statistics(inverted)
        non = coalition_membership_statistics(non_inverted)
        push!(rows, (
            election = Int(first(domain.election)), k = Int(first(domain.k)),
            minimal_seat_majority_coalitions = nrow(domain),
            minimal_inversions = length(inverted),
            non_inverted_minimal_majorities = length(non_inverted),
            inverted_members_median = inv.median, inverted_members_min = inv.min,
            inverted_members_max = inv.max,
            non_inverted_members_median = non.median, non_inverted_members_min = non.min,
            non_inverted_members_max = non.max,
        ))
    end
    return DataFrame(rows)
end

"""Reconcile every count and inversion-specific statistic with the source registry."""
function validate_k_gap_membership_summary!(summary, registry)
    expected = build_k_gap_membership_summary(registry)
    all(column -> column in propertynames(summary), propertynames(expected)) || error(
        "K-gap summary is missing membership columns.")
    nrow(summary) == nrow(expected) || error("K-gap membership summary row count changed.")
    for (row, source) in zip(eachrow(summary), eachrow(expected))
        for column in propertynames(expected)
            isequal(row[column], source[column]) || error(
                "K-gap membership summary disagrees with registry at $(source.election)/k=$(source.k), $(column): $(row[column]) != $(source[column]).")
        end
        row.minimal_inversions + row.non_inverted_minimal_majorities == row.minimal_seat_majority_coalitions || error(
            "Inverted and non-inverted counts do not sum to minimal majorities.")
    end
    return true
end

"""Audited expectations are validation gates only; no rendering data come from here."""
function validate_k_gap_membership_regression!(summary)
    expected_keys = [(year, k) for year in (2014, 2018, 2022) for k in (0, 1)]
    collect(zip(summary.election, summary.k)) == expected_keys || error(
        "K-gap membership summary must contain exactly six rows ordered by election (2014, 2018, 2022), then k (0, 1).")
    # Counts, then (median, minimum, maximum) for each inversion subset.
    expected = [
        (8, 4, (15, 12, 16), (12, 12, 14)),
        (88, 43, (14, 11, 19), (14, 11, 19)),
        (8, 0, (missing, missing, missing), (16.5, 15, 20)),
        (118, 24, (17, 14, 19), (17, 14, 21)),
        (4, 2, (11.5, 8, 15), (20, 19, 21)),
        (53, 17, (14, 7, 16), (19, 14, 21)),
    ]
    for (row, target) in zip(eachrow(summary), expected)
        actual = (row.minimal_seat_majority_coalitions, row.minimal_inversions,
                  (row.inverted_members_median, row.inverted_members_min, row.inverted_members_max),
                  (row.non_inverted_members_median, row.non_inverted_members_min, row.non_inverted_members_max))
        isequal(actual, target) || error(
            "Audited membership regression failed at $(row.election)/k=$(row.k): computed $(actual), expected $(target). Diagnose the source records before publishing.")
        row.non_inverted_minimal_majorities == target[1] - target[2] || error(
            "Audited non-inverted count failed at $(row.election)/k=$(row.k).")
    end
    return true
end

function coalition_membership_cell(med, low, high)
    all(ismissing, (med, low, high)) && return "None"
    any(ismissing, (med, low, high)) && error("Incomplete membership statistics.")
    low > 0 && isinteger(low) && isinteger(high) && low <= med <= high && isinteger(2 * med) || error(
        "Invalid membership median or range.")
    median_text = isinteger(med) ? string(Int(med)) : string(Float64(med))
    return "$(median_text) [$(Int(low))--$(Int(high))]"
end

"""Render the complete manuscript float from the validated machine-readable summary."""
function ideology_k_gap_summary_latex(summary)
    io = IOBuffer()
    println(io, raw"\begin{table}[htbp]")
    println(io, raw"\centering")
    println(io, raw"\caption{Minimal winning coalitions and membership by ideological domain}")
    println(io, raw"\label{tab:interval-summary}")
    println(io, raw"\small")
    println(io, raw"\setlength{\tabcolsep}{4pt}")
    println(io, raw"\begin{tabularx}{\textwidth}{@{}rc>{\centering\arraybackslash}Xc cc@{}}")
    println(io, raw"\toprule")
    println(io, raw" & & & & \multicolumn{2}{c}{\shortstack{Coalition members:\\median [min--max]}} \\\\")
    println(io, raw"\cmidrule(l){5-6}")
    println(io, raw"Election & \(k\) & Minimal majorities & Inversions & Inverted & Non-inverted \\\\")
    println(io, raw"\midrule")
    for row in eachrow(summary)
        inverted = coalition_membership_cell(row.inverted_members_median, row.inverted_members_min, row.inverted_members_max)
        non_inverted = coalition_membership_cell(row.non_inverted_members_median, row.non_inverted_members_min, row.non_inverted_members_max)
        println(io, join((row.election, row.k, row.minimal_seat_majority_coalitions,
                         row.minimal_inversions, inverted, non_inverted), " & "), raw" \\\\")
    end
    println(io, raw"\bottomrule")
    println(io, raw"\end{tabularx}")
    println(io, raw"\begin{flushleft}")
    println(io, raw"\footnotesize Notes: Coalition membership is the number of parties in the coalition.")
    println(io, raw"Entries in the final two columns report median [minimum--maximum].")
    println(io, raw"Membership counts include parties receiving votes but no seats; they are not counts of seat-winning parliamentary partners.")
    println(io, raw"Minimality is defined within the stated domain.")
    println(io, raw"\end{flushleft}")
    println(io, raw"\end{table}")
    return String(take!(io))
end
