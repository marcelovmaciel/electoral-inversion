# Included in IntermediateAccountingReport: these are views of the established
# exact accounting objects, never an alternative electoral/cabinet calculation.
const PARTY_SIZE_BENCHMARK = 5 // 100
const PARTY_SIZE_NOTE = "5% is a descriptive national-vote benchmark, not an electoral threshold or theoretical cutoff."
const PARTY_SIZE_BINS = ((0 // 1, 1 // 100, "<1%"),
    (1 // 100, 3 // 100, "1 to <3%"), (3 // 100, 5 // 100, "3 to <5%"),
    (5 // 100, 1 // 1, ">=5%"))
# Frozen-data regression expectations; none is used to construct a diagnostic.
const PARTY_SIZE_FROZEN_PERIODS = Dict(
    2014 => ["2015.1", "2016.1", "2016.2", "2016.3", "2016.4", "2017.1", "2018.1", "2018.2"],
    2018 => ["2019.1", "2020.1", "2020.2", "2020.3", "2020.4", "2020.5", "2021.1", "2021.2", "2021.3/2022.1", "2022.2", "2022.3", "2022.4"],
    2022 => ["2023.1", "2023.2", "2025.1"],
)
const PARTY_SIZE_FROZEN_CORRELATIONS = Dict(
    "2014" => (0.45482190512178483, 0.505933038744468, 0.4750733137829912, 0.623533724340176),
    "2018" => (0.5204787629284205, 0.6467504811579127, 0.40756302521008403, 0.780952380952381),
    "2022" => (0.8099565801882369, 0.7075851857962916, 0.1499266862170088, 0.8031524926686217),
    "pooled" => (0.6336150950811178, 0.6093429436808258, 0.3825726654298083, 0.7737167594310451),
)

function party_size_check!(checks, kind, name, observed, expected; atol = 0.0)
    passed = atol == 0 ? isequal(observed, expected) :
        isapprox(observed, expected; atol = atol, rtol = 0)
    push!(checks, (check_kind = kind, check_name = name,
        observed = string(observed), expected = string(expected), passed = passed))
    passed || error("Party-size $(kind) failed: $(name); observed=$(observed), expected=$(expected). " *
        "Frozen-data expectations must be reviewed explicitly, not silently updated.")
    return nothing
end

"""Average ranks for ties; empty/constant correlations are reported as missing."""
function party_size_ranks(values)
    order = sortperm(values)
    ranks = zeros(Float64, length(values))
    i = 1
    while i <= length(order)
        j = i
        while j < length(order) && values[order[j + 1]] == values[order[i]]
            j += 1
        end
        ranks[order[i:j]] .= (i + j) / 2
        i = j + 1
    end
    return ranks
end

function party_size_group(votes::Integer, national_votes::Integer)
    share = CD.exact_fraction(votes, national_votes)
    0 <= share <= 1 || error("Party vote share must be in [0, 1].")
    return findlast(bin -> first(bin) <= share, PARTY_SIZE_BINS)
end

function party_size_correlation(x, y; ranked = false)
    length(x) == length(y) || error("Correlation vectors differ in length.")
    length(x) < 2 && return missing
    xx, yy = ranked ? (party_size_ranks(x), party_size_ranks(y)) : (Float64.(x), Float64.(y))
    (all(==(first(xx)), xx) || all(==(first(yy)), yy)) && return missing
    return cor(xx, yy)
end

function validate_party_size_regressions(diagnostic)
    checks = NamedTuple[]
    check(name, observed, expected; atol = 0.0) =
        party_size_check!(checks, "frozen_data_regression", name, observed, expected; atol)
    parties, links, sets = diagnostic.parties, diagnostic.period_linkage, diagnostic.cabinet_sets
    check("party-election observations", nrow(parties), 99)
    check("current cabinet observations", nrow(links), 23)
    check("distinct election-year cabinet party sets", nrow(sets), 22)
    check("positive A_C current observations", count(>(0), links.A_C), 23)
    check("positive B_C current observations", count(>(0), links.B_C), 6)
    for year in (2014, 2018, 2022)
        yy = links[links.election_year .== year, :]
        check("$(year) unchanged ordered cabinet period IDs", String.(yy.cabinet_period), PARTY_SIZE_FROZEN_PERIODS[year])
        expected_sources = [p == "2021.3/2022.1" ? ["2021.3", "2022.1"] : [p] for p in PARTY_SIZE_FROZEN_PERIODS[year]]
        check("$(year) source periods / no extra coalescing", [String.(JSON3.read(s)) for s in yy.source_periods], expected_sources)
        check("$(year) covered days", sum(yy.days_overlapping_mandate), year == 2022 ? 1174 : 1461)
        check("$(year) party count", count(==(year), parties.election_year), EXPECTED_PARTIES_BY_YEAR[year])
        ss = diagnostic.cabinet_size[diagnostic.cabinet_size.election .== string(year), :]
        ever = only(eachrow(ss[ss.ever_in_cabinet, :]))
        never = only(eachrow(ss[.!ss.ever_in_cabinet, :]))
        check("$(year) cabinet-ever party count", ever.n, year == 2014 ? 15 : 11)
        for col in (:mean_vote_share, :median_vote_share, :mean_q_i, :median_q_i)
            check("$(year) cabinet-ever > never $(col)", ever[col] > never[col], true)
        end
    end
    repeated = sets[sets.cabinet_observation_count .> 1, :]
    check("nonadjacent repeated set periods", [String.(JSON3.read(s)) for s in repeated.cabinet_periods], [["2020.4", "2021.1"]])
    large = [CD.exact_fraction(r.v_i, r.V) >= PARTY_SIZE_BENCHMARK for r in eachrow(parties)]
    check("parties at least 5%", count(large), 23)
    check("positive A_i at least 5%", count(>(0), parties.A_i[large]), 21)
    check("parties below 5%", count(.!large), 76)
    check("positive A_i below 5%", count(>(0), parties.A_i[.!large]), 12)
    check("sets where >=5% positives exceed ALL member negatives", count(>(0), sets.large_positive_minus_all_negative_A), 22)
    for r in eachrow(diagnostic.correlations)
        observed = (r.pearson_vote_share_A_i, r.pearson_vote_share_A_over_q,
            r.spearman_vote_share_A_i, r.spearman_vote_share_A_over_q)
        for (index, (actual, expected)) in enumerate(zip(observed, PARTY_SIZE_FROZEN_CORRELATIONS[r.election]))
            check("$(r.election) size correlation $(index)", actual, expected; atol = 1e-12)
        end
    end
    exceptions = sort([(Int(r.election_year), String(r.party)) for r in eachrow(parties)
        if r.vote_share >= Float64(PARTY_SIZE_BENCHMARK) && r.A_i < 0])
    check("large negative A_i exceptions", exceptions, [(2014, "PT"), (2018, "PSL")])
    return DataFrame(checks)
end

"""
Extend the existing persistable party panel, deriving summaries from the exact
in-memory party objects and the unchanged, already translated cabinet registry.
The distinct-set grouping has no path back into that registry.
"""
function build_party_size_diagnostics!(parties::DataFrame, periods::DataFrame, accounting_by_year::AbstractDict)
    chronology_before = deepcopy(periods)
    checks = NamedTuple[]
    check(name, observed, expected) = party_size_check!(checks, "accounting_identity", name, observed, expected)
    source = Dict((Int(y), String(r.party)) => r for (y, a) in accounting_by_year for r in eachrow(a.party))
    check("party panel key coverage", Set((Int(r.election_year), String(r.party)) for r in eachrow(parties)) == Set(keys(source)), true)
    check("party panel key uniqueness", nrow(parties), length(source))
    check("period key uniqueness", nrow(periods), length(unique(periods.coalition_id)))
    for year in sort(collect(keys(accounting_by_year)))
        a = accounting_by_year[year]
        for col in (:A_exact, :B_exact, :d_exact)
            check("$(year) annual $(col) closes", sum(a.party[!, col]), CD.exact_fraction(0, 1))
        end
    end
    counts = Dict(k => 0 for k in keys(source))
    days = copy(counts)
    raw_counts = copy(counts)
    sets = Dict{String,Any}()
    set_order = String[]
    link_rows = NamedTuple[]
    for period in eachrow(periods)
        year = Int(period.election_year)
        names = sort(ordered_parties(period.coalition_parties))
        set_id = string(year, ":", join(names, "|"))
        members = [source[year, name] for name in names]
        source_periods = String.(JSON3.read(String(period.source_periods)))
        for name in names
            counts[year, name] += 1
            days[year, name] += Int(period.days_overlapping_mandate)
            raw_counts[year, name] += length(source_periods)
        end
        A = sum(r.A_exact for r in members)
        B = sum(r.B_exact for r in members)
        q = sum(r.quota_exact for r in members)
        a = accounting_by_year[year]
        district = [CD.exact_coalition_district(a, names, String(d)) for d in a.district.district]
        check("$(period.coalition_id) member A equals district A_C", A, sum(d.a_exact for d in district))
        check("$(period.coalition_id) member B equals district B_C", B, sum(d.b_exact for d in district))
        check("$(period.coalition_id) coalition A+B closure", A+B, CD.exact_fraction(period.s_C, 1)-q)
        check("$(period.coalition_id) member votes", sum(r.votes for r in members), Int(period.v_C))
        require_approx(A+B, period.d_C, "$(period.coalition_id) saved differential")
        require_approx(q, period.q_C, "$(period.coalition_id) saved quota")
        check("$(period.coalition_id) calendar days", Dates.value(Date(period.period_end)-Date(period.period_start))+1, Int(period.period_days))
        push!(link_rows, (coalition_id = String(period.coalition_id), election_year = year,
            cabinet_period = String(period.cabinet_period), source_periods = String(period.source_periods),
            period_start = period.period_start, period_end = period.period_end,
            period_days = Int(period.period_days), days_overlapping_mandate = Int(period.days_overlapping_mandate),
            cabinet_party_set_id = set_id, A_C = Float64(A), B_C = Float64(B), q_C = Float64(q),
            A_over_q = Float64(A/q), B_over_q = Float64(B/q), A_C_exact = exact_text(A), B_C_exact = exact_text(B)))
        if !haskey(sets, set_id)
            push!(set_order, set_id)
            pos = sort([r for r in members if r.A_exact > 0]; by = r -> (-r.A_exact, String(r.party)))
            neg = sort([r for r in members if r.A_exact < 0]; by = r -> (r.A_exact, String(r.party)))
            positive = sum((r.A_exact for r in pos); init = CD.exact_fraction(0, 1))
            negative = sum((r.A_exact for r in neg); init = CD.exact_fraction(0, 1))
            large_positive = sum((r.A_exact for r in pos if CD.exact_fraction(r.votes, a.national_votes) >= PARTY_SIZE_BENCHMARK); init = CD.exact_fraction(0, 1))
            top_size = first(sort(members; by = r -> (-r.quota_exact, String(r.party))), min(3, length(members)))
            top_size_positive = sum(max(r.A_exact, 0) for r in top_size)
            row = (cabinet_party_set_id = set_id, election_year = year, coalition_parties = join(names, ", "),
                coalition_party_count = length(names), A_C = Float64(A), B_C = Float64(B), q_C = Float64(q),
                gross_positive_A = Float64(positive), gross_negative_A = Float64(negative),
                large_party_positive_A = Float64(large_positive),
                large_party_share_gross_positive_A = positive == 0 ? missing : Float64(large_positive/positive),
                large_positive_minus_all_negative_A = Float64(large_positive+negative),
                descriptive_vote_share_benchmark = Float64(PARTY_SIZE_BENCHMARK),
                top_three_by_q_parties = join((r.party for r in top_size), ", "),
                top_three_by_q_share_gross_positive_A = positive == 0 ? missing : Float64(top_size_positive/positive),
                A_C_exact = exact_text(A), B_C_exact = exact_text(B),
                gross_positive_A_exact = exact_text(positive), gross_negative_A_exact = exact_text(negative),
                large_party_positive_A_exact = exact_text(large_positive),
                benchmark_note = PARTY_SIZE_NOTE)
            for (label, ranked) in (("positive", pos), ("negative", neg)), rank in 1:3
                name_col, value_col = Symbol("$(label)_party_$(rank)"), Symbol("$(label)_A_i_$(rank)")
                row = merge(row, NamedTuple{(name_col, value_col)}((rank <= length(ranked) ? String(ranked[rank].party) : missing,
                    rank <= length(ranked) ? Float64(ranked[rank].A_exact) : missing)))
            end
            sets[set_id] = row
        end
    end
    links = DataFrame(link_rows)
    set_rows = NamedTuple[]
    for id in set_order
        linked = links[links.cabinet_party_set_id .== id, :]
        push!(set_rows, merge(sets[id], (cabinet_periods = JSON3.write(String.(linked.cabinet_period)),
            coalition_ids = JSON3.write(String.(linked.coalition_id)),
            cabinet_observation_count = nrow(linked), total_cabinet_days = sum(linked.days_overlapping_mandate))))
    end
    keys_in_order = [(Int(r.election_year), String(r.party)) for r in eachrow(parties)]
    for key in sort(collect(keys(source)))
        r = source[key]
        check("$(key) party A+B=d", r.A_exact+r.B_exact, r.d_exact)
        r.quota_exact > 0 || error("Party-size normalization requires positive q_i: $(key)")
    end
    parties[!, :A_over_q] = [Float64(source[k].A_exact/source[k].quota_exact) for k in keys_in_order]
    parties[!, :B_over_q] = [Float64(source[k].B_exact/source[k].quota_exact) for k in keys_in_order]
    parties[!, :ever_in_cabinet] = [counts[k] > 0 for k in keys_in_order]
    parties[!, :cabinet_observation_count] = [counts[k] for k in keys_in_order]
    parties[!, :cabinet_source_period_count] = [raw_counts[k] for k in keys_in_order]
    parties[!, :cabinet_days] = [days[k] for k in keys_in_order]
    cabinet_rows, correlation_rows, bin_rows = NamedTuple[], NamedTuple[], NamedTuple[]
    for election in [string.(sort(collect(keys(accounting_by_year)))); "pooled"]
        panel = election == "pooled" ? parties : parties[parties.election_year .== parse(Int, election), :]
        for ever in (true, false)
            group = panel[panel.ever_in_cabinet .== ever, :]
            push!(cabinet_rows, (election = election, ever_in_cabinet = ever, n = nrow(group),
                mean_vote_share = mean(group.vote_share), median_vote_share = median(group.vote_share),
                mean_q_i = mean(group.q_i), median_q_i = median(group.q_i)))
        end
        push!(correlation_rows, (election = election, n = nrow(panel), size_variable = "national vote share (q_i has identical correlations)",
            pearson_vote_share_A_i = party_size_correlation(panel.vote_share, panel.A_i),
            pearson_vote_share_A_over_q = party_size_correlation(panel.vote_share, panel.A_over_q),
            spearman_vote_share_A_i = party_size_correlation(panel.vote_share, panel.A_i; ranked = true),
            spearman_vote_share_A_over_q = party_size_correlation(panel.vote_share, panel.A_over_q; ranked = true)))
        for (order, (lo, hi, label)) in enumerate(PARTY_SIZE_BINS)
            # Use exact vote fractions to classify observations at bin boundaries.
            mask = [party_size_group(r.v_i, r.V) == order for r in eachrow(panel)]
            group = panel[mask, :]
            n = nrow(group)
            push!(bin_rows, (election = election, size_group_order = order, size_group = label,
                lower_vote_share_inclusive = Float64(lo), upper_vote_share_exclusive = order == 4 ? missing : Float64(hi),
                n = n, mean_A_i = n == 0 ? missing : mean(group.A_i), median_A_i = n == 0 ? missing : median(group.A_i),
                mean_A_over_q = n == 0 ? missing : mean(group.A_over_q), median_A_over_q = n == 0 ? missing : median(group.A_over_q),
                positive_A_i_count = count(>(0), group.A_i), positive_A_i_share = n == 0 ? missing : count(>(0), group.A_i)/n,
                benchmark_note = PARTY_SIZE_NOTE))
        end
    end
    check("input cabinet chronology unchanged", isequal(periods, chronology_before), true)
    diagnostic = (parties = parties, cabinet_size = DataFrame(cabinet_rows), correlations = DataFrame(correlation_rows),
        size_groups = DataFrame(bin_rows), cabinet_sets = DataFrame(set_rows), period_linkage = links)
    regressions = validate_party_size_regressions(diagnostic)
    return merge(diagnostic, (checks = vcat(DataFrame(checks), regressions),))
end

function party_size_report_latex(d)
    io = IOBuffer()
    println(io, raw"\noindent These are descriptive accounting comparisons, not a causal model of cabinet formation or of party-size effects. Each pooled observation is one election-year party, equally weighted. Cabinet membership uses the existing translated election-year registry. The 5\% benchmark is descriptive only, not an electoral threshold or theoretical cutoff. Since $q_i=513v_i/V$, vote share and $q_i$ have identical correlations. Ratios are dimensionless; $A_i$ is measured in seats.")
    println(io, longtable_latex(d.cabinet_size; caption = "Party size by cabinet participation", label = "tab:report-party-size-cabinet",
        column_spec = "llrrrrr", headers = ("Election", "Ever cabinet", "n", "Mean vote \\%", "Median vote \\%", "Mean q", "Median q"),
        renderers = (r -> r.election, r -> r.ever_in_cabinet ? "Yes" : "No", r -> string(r.n),
            r -> fmtpct(r.mean_vote_share), r -> fmtpct(r.median_vote_share), r -> fmt2(r.mean_q_i), r -> fmt2(r.median_q_i))))
    println(io, longtable_latex(d.correlations; caption = "Party-size associations", label = "tab:report-party-size-correlations",
        column_spec = "lrrrrr", headers = ("Election", "n", "Pearson A", "Pearson A/q", "Spearman A", "Spearman A/q"),
        renderers = (r -> r.election, r -> string(r.n), r -> fmt3(r.pearson_vote_share_A_i), r -> fmt3(r.pearson_vote_share_A_over_q),
            r -> fmt3(r.spearman_vote_share_A_i), r -> fmt3(r.spearman_vote_share_A_over_q))))
    println(io, longtable_latex(d.size_groups; caption = "Descriptive national-vote size groups", label = "tab:report-party-size-bins",
        column_spec = "llrrrr", headers = ("Election", "Vote group", "n", "A mean / median", "A/q mean / median", "Positive A: n (\\%)"),
        renderers = (r -> r.election, r -> CD.latex_escape(r.size_group), r -> string(r.n),
            r -> "$(fmt2(r.mean_A_i)) / $(fmt2(r.median_A_i))", r -> "$(fmt3(r.mean_A_over_q)) / $(fmt3(r.median_A_over_q))",
            r -> "$(r.positive_A_i_count) ($(fmtpct(r.positive_A_i_share)))")))
    large = only(eachrow(d.size_groups[(d.size_groups.election .== "pooled") .& (d.size_groups.size_group_order .== 4), :]))
    below = d.size_groups[(d.size_groups.election .== "pooled") .& (d.size_groups.size_group_order .< 4), :]
    println(io, "Across party-elections, $(large.positive_A_i_count)/$(large.n) parties with at least 5\\% of votes have positive \\(A_i\\), compared with $(sum(below.positive_A_i_count))/$(sum(below.n)) below 5\\%. " *
        "Cabinet participants have larger means and medians in each election, and normalization by \\(q_i\\) retains a positive size association. This is not a monotonic size gradient: the 2022 intermediate-size group (3 to less than 5\\%) is entirely negative, while many tiny parties have small absolute losses.")
    exceptions = d.parties[(d.parties.vote_share .>= Float64(PARTY_SIZE_BENCHMARK)) .& (d.parties.A_i .< 0), :]
    for r in eachrow(exceptions)
        println(io, "The large-party exception $(CD.latex_escape(r.party)) $(r.election_year) has $(fmtpct(r.vote_share))\\% of votes, \\(A_i=$(fmt2(r.A_i))\\) and \\(A_i/q_i=$(fmt3(r.A_over_q))\\).")
    end
    absent = String[]
    for year in (2014, 2018, 2022)
        never = d.parties[(d.parties.election_year .== year) .& .!d.parties.ever_in_cabinet, :]
        sort!(never, [:vote_share, :party]; rev = [true, false])
        r = first(eachrow(never))
        push!(absent, "$(CD.latex_escape(r.party)) $(year) ($(fmtpct(r.vote_share))\\% of votes)")
    end
    println(io, "Large parties also remain outside cabinet: the largest nonparticipant in each election is " * join(absent, "; ") * ".")
    links, sets = d.period_linkage, d.cabinet_sets
    shares = sets.large_party_share_gross_positive_A
    println(io, "\\par All $(nrow(links)) current cabinet observations have positive \\(A_C\\); \\(B_C>0\\) in $(count(>(0), links.B_C)) of them. " *
        "There are $(nrow(sets)) distinct election-year party sets. The nonadjacent 2020.4 and 2021.1 observations share a set, but remain separate chronology rows. " *
        "The existing adjacent 2021.3/2022.1 observation is unchanged. Positive contributions from members with at least 5\\% of votes exceed all negative member contributions in every set; they supply $(fmtpct(minimum(shares)))--$(fmtpct(maximum(shares)))\\% of gross positives. " *
        "The smallest such remaining balance is $(fmt2(minimum(sets.large_positive_minus_all_negative_A))) seats. Net \\(A_C\\) ranges from $(fmt2(minimum(sets.A_C))) to $(fmt2(maximum(sets.A_C))) seats.")
    println(io, raw"\begin{itemize}")
    for year in (2014, 2018, 2022)
        pp = d.parties[(d.parties.election_year .== year) .& d.parties.ever_in_cabinet .& (d.parties.A_i .> 0), :]
        by_A = sort(pp, [:A_i, :party]; rev = [true, false])
        by_frequency = sort(pp, [:cabinet_observation_count, :A_i, :party]; rev = [true, true, false])
        selected = union(Set(first(by_A, min(3, nrow(pp))).party),
            Set(first(by_frequency, min(2, nrow(pp))).party))
        top = by_A[in.(by_A.party, Ref(selected)), :]
        entries = ["$(CD.latex_escape(r.party)): \\(A_i=$(fmt2(r.A_i))\\), $(r.cabinet_observation_count) observations" for r in eachrow(top)]
        println(io, "\\item $(year) recurring positive members: " * join(entries, "; ") * ".")
    end
    println(io, raw"\end{itemize}")
    worst = sets[argmin(sets.A_C), :]
    println(io, "The weakest net total belongs to period(s) $(CD.latex_escape(join(String.(JSON3.read(worst.cabinet_periods)), ", "))): " *
        "\\(A_C=$(fmt2(worst.A_C))\\), with gross positives $(fmt2(worst.gross_positive_A)) and negatives $(fmt2(worst.gross_negative_A)). " *
        "The three largest members by \\(q_i\\) supply as little as $(fmtpct(minimum(sets.top_three_by_q_share_gross_positive_A)))\\% of gross positive \\(A_i\\); the explanation concerns a broader group of relatively large members. " *
        "All distinct-set decompositions and all $(nrow(links)) period links are retained in the machine-readable outputs. The group arithmetic holds fixed the observed party contributions; it is not a simulated seat allocation after removing parties.")
    return String(take!(io))
end

function write_party_size_diagnostic_outputs(output_root, full, diagnostic; write_accounting_base = true)
    artifacts = NamedTuple[]
    function record(relative, data, kind, description)
        path = joinpath(output_root, relative)
        write_csv_file(path, data)
        push!(artifacts, (path = relative, artifact_type = kind, description = description,
            rows = nrow(data), columns = ncol(data), sha256 = sha256_file(path)))
        return path
    end
    if write_accounting_base
        record("raw/party_accounting_all_years.csv", full.parties, "raw", "Exact national party accounting, normalized components and cabinet participation.")
        record("raw/party_district_accounting_all_years.csv", full.cells, "raw", "Complete existing party-district accounting panel.")
        record("raw/district_accounting_all_years.csv", full.districts, "raw", "Existing district accounting weights and closure.")
    end
    specs = ((:cabinet_size, "tables/report/party_size_cabinet_summary.csv"),
        (:correlations, "tables/report/party_size_correlations.csv"),
        (:size_groups, "tables/report/party_size_groups.csv"),
        (:cabinet_sets, "raw/cabinet_party_set_accounting.csv"),
        (:period_linkage, "raw/cabinet_party_set_period_linkage.csv"),
        (:checks, "audit/party_size_diagnostic_checks.csv"))
    disk = Dict{Symbol,DataFrame}()
    for (key, relative) in specs
        path = record(relative, diagnostic[key], startswith(relative, "audit/") ? "audit" : startswith(relative, "raw/") ? "raw" : "table",
            "Party-size diagnostic: $(key); descriptive accounting only.")
        disk[key] = CSV.read(path, DataFrame)
    end
    metadata = DataFrame(key = ["benchmark", "interpretation", "party_unit", "membership", "duration", "negative_contributions", "set_grouping", "correlations", "validation"],
        note = [PARTY_SIZE_NOTE, "Ex post accounting; no causal cabinet-formation or size-effect claim.",
            "All vote-receiving election-year parties including zero-seat parties; pooled rows weighted equally.",
            "Existing translated election-year cabinet party sets; no party identity changes.",
            "Sum days_overlapping_mandate across current observations; inclusive civil days.",
            "gross_negative_A is signed and nonpositive; A_C = gross_positive_A + gross_negative_A.",
            "Set ID = election year plus alphabetically sorted translated members; descriptive grouping never changes chronology.",
            "Pearson and Spearman (average ranks for ties); q_i=513*vote_share has identical correlations; no significance tests.",
            "Exact accounting identities are separate from frozen-data regressions, which fail loudly on discrepancy."])
    record("audit/party_size_diagnostic_metadata.csv", metadata, "audit", "Diagnostic definitions, scope and descriptive-benchmark qualification.")
    # Like the existing report, render only after reloading machine-readable inputs.
    disk[:parties] = CSV.read(joinpath(output_root, "raw/party_accounting_all_years.csv"), DataFrame)
    report_data = (; (key => value for (key, value) in disk)...)
    relative = "latex/report/party_size_diagnostics.tex"
    path = joinpath(output_root, relative)
    mkpath(dirname(path))
    write(path, party_size_report_latex(report_data))
    push!(artifacts, (path = relative, artifact_type = "latex",
        description = "Central analysis report section; never synchronized into the manuscript.",
        rows = nrow(diagnostic.parties), columns = 0, sha256 = sha256_file(path)))
    return artifacts
end
