using Test, CSV, DataFrames
include(joinpath(@__DIR__, "ProseSummaries.jl"))
using .ProseSummaries
const PS_PAPER = normpath(joinpath(@__DIR__, "..", "output", "paper"))
const PS_ACCOUNTING = normpath(joinpath(@__DIR__, "..", "output", "decomposition"))

@testset "Raw summaries preserve aggregations and semantic identity" begin
    sources = load_summary_sources(PS_PAPER, PS_ACCOUNTING)
    data = build_summaries(sources)
    reordered = build_summaries(Dict(key => rows[nrow(rows):-1:1, :] for (key, rows) in sources))
    @test isequal(data, reordered)
    @test allunique(select(data, ProseSummaries.SUMMARY_KEY))
    @test !any(name -> occursin("macro", name) || occursin("display", name), names(data))
    @test all(endswith.(data.source_file, ".csv"))
    value(summary, metric, op) = parse(Float64, only(filter(r -> r.summary == summary &&
        r.metric == metric && r.aggregation == op, data).value))
    @test value("cabinet-periods", "count", "sum") == nrow(sources["periods"])
    @test value("cabinet-periods", "d_C", "count_positive") == count(>(0), sources["periods"].d_C)
    @test value("cabinet-component-signs", "A_C", "count_positive") == count(>(0), sources["linkage"].A_C)
    @test value("seat-winning-k1-negative-within", "A_C", "count_negative") == 3
    @test value("district-magnitude", "S_d", "median") == 10
    @test value("district-magnitude", "S_d", "mean") == 19
    empty = copy(sources); empty["periods"] = sources["periods"][1:0, :]
    empty_result = build_summaries(empty)
    @test only(filter(r -> r.summary == "cabinet-periods" && r.metric == "count", empty_result).value) == "0"
    @test_throws ErrorException build_summaries(sources; specs = [SUMMARY_SPECS[1], SUMMARY_SPECS[1]])
    mktempdir() do temp
        # Use the existing source CSVs, then check serialization without TeX.
        rows = build_summaries(sources)
        path = joinpath(temp, "summaries.csv")
        CSV.write(path, rows)
        @test CSV.read(path, DataFrame; types = Dict(:value => String)) == rows
        @test readdir(temp) == ["summaries.csv"]
    end
end

@testset "Prose observation and independently generated table sources agree" begin
    ideology = CSV.read(joinpath(PS_PAPER, "raw/ideology_k_gap_accounting_both_universes.csv"), DataFrame; types = (i, name) -> name in (:period, :cabinet_period) ? String : nothing)
    summary = CSV.read(joinpath(PS_PAPER, "tables/ideological_universe_comparison.csv"), DataFrame; types = (i, name) -> name in (:period, :cabinet_period) ? String : nothing)
    membership(value) = Set(strip.(split(String(value), ',')))
    for universe in ("seat_winning", "all_parties")
        table_path = universe == "seat_winning" ? "tables/table_accounting_minimal_ideological.csv" :
            "all_parties/tables/table_accounting_minimal_ideological.csv"
        table = CSV.read(joinpath(PS_PAPER, table_path), DataFrame; types = (i, name) -> name in (:period, :cabinet_period) ? String : nothing)
        for row in eachrow(filter(r -> r.ideological_universe == universe && r.k == 0 && r.minimal_inversion, ideology))
            selected = filter(r -> r.election_year == row.election && membership(r.coalition_parties) == membership(row.parties), table)
            @test nrow(selected) == 1
            for metric in (:q_C, :d_C, :A_C, :B_C, :R_C)
                @test isapprox(only(selected[!, metric]), row[metric]; atol = 1e-10, rtol = 0)
            end
        end
    end
    for row in eachrow(summary)
        row.minimal_inversions == 0 && continue
        selected = filter(r -> r.election == row.election && r.ideological_universe == row.ideological_universe &&
            r.k == row.k && r.minimal_inversion, ideology)
        strongest = first(sort(selected, [:vote_share, :party_count, :coalition_id]))
        @test strongest.coalition_label == row.strongest_inversion_coalition
        @test strongest.vote_share == row.strongest_inversion_vote_share
        @test strongest.seats == row.strongest_inversion_seats
    end
    cabinets = CSV.read(joinpath(PS_ACCOUNTING, "raw/accounting_all_inversion_decomposition.csv"), DataFrame; types = (i, name) -> name in (:period, :cabinet_period) ? String : nothing)
    cabinet_table = CSV.read(joinpath(PS_PAPER, "tables/table_observed_inversion_decomposition.csv"), DataFrame; types = (i, name) -> name in (:period, :cabinet_period) ? String : nothing)
    for row in eachrow(filter(r -> r.case_domain == "cabinet", cabinets))
        selected = filter(r -> r.election_year == row.election_year && r.cabinet_period == row.cabinet_period, cabinet_table)
        @test nrow(selected) == 1
        for metric in (:q_C, :d_C, :A_C, :B_C, :R_C)
            @test isapprox(only(selected[!, metric]), row[metric]; atol = 1e-10, rtol = 0)
        end
    end
end

@testset "Ideological component dominance from authoritative stored outputs" begin
    sources = load_summary_sources(PS_PAPER, PS_ACCOUNTING)
    data = build_summaries(sources)
    dominance = filter(r -> r.summary == "ideology-component-dominance", data)
    @test nrow(dominance) == 112
    lookup(u, k, y, metric, op = "count") = only(filter(r -> r.ideological_universe == u &&
        r.k == string(k) && r.election == string(y) && r.metric == metric && r.aggregation == op, dominance).value)
    expected = Dict(
        ("seat_winning", 0) => [(4, 4, 0, 0), (1, 1, 0, 0), (2, 1, 1, 0)],
        ("seat_winning", 1) => [(46, 40, 6, 0), (42, 32, 10, 0), (12, 5, 7, 0)],
        ("all_parties", 0) => [(4, 4, 0, 0), (0, 0, 0, 0), (2, 1, 1, 0)],
        ("all_parties", 1) => [(43, 36, 7, 0), (24, 10, 14, 0), (17, 4, 13, 0)],
    )
    metrics = ("minimal_inversions", "A_C_gt_B_C", "A_C_lt_B_C", "A_C_eq_B_C")
    for ((u, k), yearly) in expected
        totals = Tuple(sum(row[i] for row in yearly) for i in 1:4)
        for (year, counts) in zip((2014, 2018, 2022, "all"), vcat(yearly, [totals]))
            for (metric, count) in zip(metrics, counts)
                @test parse(Int, lookup(u, k, year, metric)) == count
                metric == "minimal_inversions" && continue
                percentage = lookup(u, k, year, metric, "percentage")
                @test counts[1] == 0 ? percentage == "missing" : parse(Float64, percentage) == 100 * count / counts[1]
            end
            group = filter(r -> r.ideological_universe == u && r.k == string(k) && r.election == string(year), dominance)
            @test all(==(counts[1]), group.source_row_count)
        end
    end
    for u in ("seat_winning", "all_parties")
        exception = only(eachrow(filter(r -> r.ideological_universe == u && r.k == 0 &&
            r.minimal_seat_majority && r.inversion && r.B_C > r.A_C, sources["ideology"])))
        @test (exception.election, exception.left_endpoint, exception.right_endpoint) == (2022, "MDB", "UNIÃO")
        @test ismissing(exception.omitted_party)
    end
    checks = audit_component_dominance(sources)
    @test checks.accounting_rows == 31238
    @test checks.domain_groups == 12
    @test checks.max_identity_residual <= 1e-10
    println("Component dominance audits: ", checks)
    # Original prose selections, values, and order must survive the appended rows.
    existing = CSV.read(joinpath(PS_PAPER, "tables/prose_analysis_summaries.csv"), DataFrame; types = Dict(:value => String))
    legacy = filter(r -> r.summary != "ideology-component-dominance", existing)
    @test select(data[1:32, :], names(legacy)) == legacy
end

@testset "Dominance audits reject corrupt accounting and denominators" begin
    sources = load_summary_sources(PS_PAPER, PS_ACCOUNTING)
    i = findfirst(r -> r.ideological_universe == "seat_winning" && r.k == 1 && r.minimal_inversion,
        eachrow(sources["ideology"]))
    for column in (:d_C, :A_C, :B_C)
        bad = deepcopy(sources)
        bad["ideology"][i, column] += 1e-5
        @test_throws r"accounting identity" audit_component_dominance(bad)
    end
    bad = deepcopy(sources); bad["ideology"][i, :A_C] = NaN
    @test_throws r"Nonfinite" audit_component_dominance(bad)
    bad = deepcopy(sources); bad["ideology"][i, :minimal_inversion] = false
    @test_throws r"conjunction" audit_component_dominance(bad)
    bad = deepcopy(sources); bad["summary"][1, :minimal_inversions] += 1
    @test_throws r"denominator disagrees" audit_component_dominance(bad)
    bad = deepcopy(sources); push!(bad["ideology"], bad["ideology"][i, :])
    @test_throws r"Duplicate ideological coalition" audit_component_dominance(bad)
    # Even coordinated denominator drift must fail the explicitly requested 100/46/42/12 gate.
    bad = deepcopy(sources)
    row = bad["ideology"][i, :]
    row.inversion = row.minimal_inversion = false
    j = findfirst(r -> r.ideological_universe == "seat_winning" && r.k == 1 && r.election == row.election,
        eachrow(bad["summary"]))
    bad["summary"][j, :minimal_inversions] -= 1
    @test_throws r"Primary k=1 denominator regression" audit_component_dominance(bad)
end

@testset "Dominance uses strict full-precision comparisons, including real ties" begin
    sources = load_summary_sources(PS_PAPER, PS_ACCOUNTING)
    i = findfirst(r -> r.ideological_universe == "seat_winning" && r.k == 1 && r.minimal_inversion,
        eachrow(sources["ideology"]))
    row = sources["ideology"][i, :]
    half = row.d_C / 2
    @test 0 < nextfloat(half) - half < 1e-10
    specs = filter(s -> s.summary == "ideology-component-dominance" &&
        s.filters.ideological_universe == "seat_winning" && s.filters.k == "1" &&
        !hasproperty(s.filters, :election) && s.aggregation == :count && s.metric != :minimal_inversions, SUMMARY_SPECS)
    original = [parse(Int, r.value) for r in eachrow(build_summaries(sources; specs))]
    old_category = [row.A_C > row.B_C, row.A_C < row.B_C, row.A_C == row.B_C]
    for (A, B, category) in ((half, half, [0, 0, 1]), (nextfloat(half), half, [1, 0, 0]), (half, nextfloat(half), [0, 1, 0]))
        row.A_C, row.B_C = A, B
        result = build_summaries(sources; specs)
        @test parse.(Int, result.value) == original - old_category + category
        @test sum(parse.(Int, result.value)) == 100
    end
end
