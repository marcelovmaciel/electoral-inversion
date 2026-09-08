# Included by the main runner; all numerical cells come from the generated registry.
function ideological_universe_comparison_latex(df)
    io = IOBuffer()
    println(io, raw"\begin{table}[!htbp]")
    println(io, raw"\centering\small")
    println(io, raw"\caption{Sensitivity to the ideological party universe}")
    println(io, raw"\label{tab:ideological-universe-comparison}")
    println(io, raw"\setlength{\tabcolsep}{3pt}")
    println(io, raw"\begin{tabularx}{\textwidth}{@{}rlrrr>{\raggedright\arraybackslash}Xrr@{}}")
    println(io, raw"\toprule")
    println(io, raw"Election/$k$ & Universe & $|P^*|$ & MW & Inv. & Strongest inversion & Vote \% & Seats " * repeat("\\", 2))
    println(io, raw"\midrule")
    for row in eachrow(df)
        universe = row.ideological_universe == "seat_winning" ? "Seat-winning" : "All parties"
        votes = ismissing(row.strongest_inversion_vote_share_pct) ? "--" : fmt2(row.strongest_inversion_vote_share_pct)
        seats = ismissing(row.strongest_inversion_seats) ? "--" : string(row.strongest_inversion_seats)
        println(io, "$(row.election)/$(row.k) & $(universe) & $(row.ideological_party_count) & $(row.minimal_seat_majority_coalitions) & $(row.minimal_inversions) & $(latex_escape(row.strongest_inversion_coalition)) & $(votes) & $(seats) " * repeat("\\",2))
    end
    println(io, raw"\bottomrule\end{tabularx}")
    println(io, raw"\begin{minipage}{\textwidth}\footnotesize Notes: $|P^*|$ is the number of parties in the specified ideological order; MW counts domain-minimal seat-majority coalitions, and Inv. counts inversions among them. Both universes use all valid federal-deputy votes in $V$. Strongest means the smallest national vote share, with ties resolved by membership count and coalition identifier.")
    changed = df[df.strongest_region_changed, :]
    if nrow(changed) == 0
        println(io, raw"The strongest endpoint region is unchanged in every election/$k$ comparison (including joint nulls); omitted parties and member sets can differ.")
    else
        pairs = unique(["$(r.election)/$(r.k)" for r in eachrow(changed)])
        println(io, "The strongest endpoint region changes for election/\$k\$ = $(join(pairs, ", ")); other comparisons preserve it.")
    end
    println(io, raw"\end{minipage}\end{table}")
    return String(take!(io))
end
