using Test
using Processing
using CSV
using DataFrames
using Dates
using JSON3

@testset "Adjacent translated cabinet periods" begin
    # Reordered parties compare as sets; neither gaps nor election boundaries merge.
    sample = DataFrame(
        election_year = [2018, 2018, 2018, 2018, 2022],
        coalition_year = [2021, 2022, 2022, 2022, 2022],
        period = ["a", "b", "c", "d", "e"],
        period_start = Date.( ["2021-12-31", "2022-01-01", "2022-01-03", "2022-01-04", "2022-01-05"]),
        period_end = Date.( ["2021-12-31", "2022-01-01", "2022-01-03", "2022-01-04", "2022-01-05"]),
        period_days = ones(Int, 5), days_overlapping_mandate = ones(Int, 5),
        share_of_mandate = fill(1 / 365, 5),
        parties = ["A, B", "B, A", "A, B", "C", "C"],
        seats = [257, 257, 257, 258, 258], vote_share = fill(0.47, 5),
    )
    original = copy(sample)
    result = Processing.coalesce_adjacent_cabinet_periods(sample;
        expected_merges = Set([(2018, ("a", "b"))]))
    @test isequal(sample, original)
    @test result.period == ["a/b", "c", "d", "e"]
    @test JSON3.read(result.source_periods[1]) == ["a", "b"]
    @test result.period_days[1] == result.days_overlapping_mandate[1] == 2
    @test result.period_start[1] == Date(2021, 12, 31)
    @test result.period_end[1] == Date(2022, 1, 1)
    @test result.seats[1] == 257
    @test_throws r"Unexpected merges" Processing.coalesce_adjacent_cabinet_periods(sample; expected_merges = Set())
    unequal = copy(sample)
    unequal.seats[2] = 258
    @test_throws r"unequal seats" Processing.coalesce_adjacent_cabinet_periods(unequal)
    chain = copy(sample[1:3, :])
    chain.period_start[3] = chain.period_end[3] = Date(2022, 1, 2)
    @test only(Processing.coalesce_adjacent_cabinet_periods(chain).period) == "a/b/c"
end

@testset "Pinned cabinet reporting linkage and independent arithmetic" begin
    root = abspath(@__DIR__, "..", "..", "..")
    paper = joinpath(root, "processing", "Processing", "output", "paper")
    read_csv(path) = CSV.read(path, DataFrame; stringtype=String,
        types=(i,name)->String(name)=="period" ? String : nothing)
    before=read_csv(joinpath(paper,"diagnostics","cabinet_coalitions_before_coalescing.csv"))
    after=read_csv(joinpath(paper,"raw","cabinet_coalition_metrics.csv"))
    calendar=Processing.CabinetRelease.calendar_table()
    @test sum(calendar.days)==4096
    @test sum(after.period_days)+sum(calendar.days[.!calendar.identified])==4096
    recomputed=Processing.coalesce_adjacent_cabinet_periods(before)
    @test names(recomputed)==names(after)
    for col in names(after)
        @test all(isequal.(recomputed[!,col],after[!,col]))
    end
    historical=Processing.CabinetRelease.load_release().periods
    linked=String[]
    party=read_csv(joinpath(paper,"raw","party_seat_differentials_all_years.csv"))
    for row in eachrow(after)
        sources=String.(JSON3.read(row.source_periods)); append!(linked,sources)
        hist=historical[in.(historical.period_id,Ref(Set(sources))),:]
        @test sum(hist.days)==row.period_days
        @test length(unique(hist.administration_id))==1
        @test all((year(t) <= 2018 ? 2014 : year(t) <= 2022 ? 2018 : 2022) == row.election_year for t in hist.start_inclusive)
        @test minimum(hist.start_inclusive)==row.period_start
        @test maximum(hist.end_exclusive)-Day(1)==row.period_end
        members=Set(strip.(split(row.parties,',')))
        selected=party[(party.election_year .== row.election_year) .& in.(party.party,Ref(members)),:]
        @test nrow(selected)==length(members)
        @test sum(selected.votes)==row.votes
        @test sum(selected.seats)==row.seats
        @test row.quota ≈ 513*row.votes/row.national_vote_total
        @test row.seat_diff ≈ row.seats-row.quota
        @test row.coalition_inversion==(2*row.votes < row.national_vote_total && row.seats>=257)
    end
    @test length(linked)==length(unique(linked))
    @test Set(linked)==Set(historical.period_id)
    for y in [2014,2018,2022]
        duration(df)=sum(df.period_days[(df.election_year .== y) .& df.coalition_inversion])
        @test duration(before)==duration(after)
    end
end

@testset "Historical identity is not reporting identity" begin
    sample=DataFrame(election_year=[2018,2018,2018],coalition_year=[2022,2022,2022],
        period=["a","b","c"],source_periods=["[\"H1\"]","[\"H2\"]","[\"H3\"]"],
        administration_id=["A","A","B"],composition_status=fill("identified",3),
        historical_parties=["DEM, PSL","UNIAO","UNIAO"],parties=fill("DEM, PSL",3),
        period_start=Date.(["2022-02-07","2022-02-08","2022-02-09"]),
        period_end=Date.(["2022-02-07","2022-02-08","2022-02-09"]),
        period_days=ones(Int,3),days_overlapping_mandate=ones(Int,3),share_of_mandate=fill(1/1461,3),seats=fill(257,3))
    result=Processing.coalesce_adjacent_cabinet_periods(sample)
    @test result.period==["a/b","c"]
    @test JSON3.read(result.source_periods[1])==["H1","H2"]
    report=Processing.CabinetRelease.translate(["DEM","UNIAO"];election_year=2018,valid_election_parties=["DEM","PSL"])
    @test Set(report.election_party)==Set(["DEM","PSL"])
    @test_throws r"Unmapped" Processing.CabinetRelease.translate(["NEW_UNKNOWN_PARTY"];election_year=2018,valid_election_parties=["NEW_UNKNOWN_PARTY"])
end

@testset "Cabinet bridge does not infer transitions across unknown gaps" begin
    paper=joinpath(@__DIR__, "..", "output", "paper")
    for suffix in ("", "_all_parties")
        bridge=CSV.read(joinpath(paper,"diagnostics","cabinet_interval_chronology$(suffix).csv"),DataFrame;types=Dict(:cabinet_period=>String))
        @test issorted(bridge.period_start)
        for i in 1:nrow(bridge)
            row=bridge[i,:]
            adjacent=i>1 && bridge.period_end[i-1]+Day(1)==row.period_start && bridge.administration_id[i-1]==row.administration_id
            @test (row.transition_status=="identified_adjacent")==adjacent
            if !adjacent
                @test ismissing(row.delta_cabinet_mean_ideology_value_unweighted)
                @test ismissing(row.delta_cabinet_mean_ideology_value_seat_weighted)
                @test ismissing(row.entered_ideology_summary) || isempty(row.entered_ideology_summary)
                @test ismissing(row.left_ideology_summary) || isempty(row.left_ideology_summary)
            end
        end
    end
end

@testset "Set bridges have one row per registry ID and no implied transitions" begin
    paper=joinpath(@__DIR__, "..", "output", "paper")
    expected=Set(Processing.cabinet_set_identity().cabinet_party_set_id)
    for suffix in ("", "_all_parties")
        bridge=CSV.read(joinpath(paper,"tables","table_appendix_cabinet_interval_bridge$(suffix).csv"),DataFrame;types=Dict(:cabinet_period=>String))
        @test Set(bridge.cabinet_party_set_id)==expected
        @test allunique(bridge.cabinet_party_set_id)
        @test all(==("not_applicable_set"), bridge.transition_status)
        @test all(ismissing, bridge.delta_cabinet_mean_ideology_value_unweighted)
        @test all(ismissing, bridge.delta_cabinet_mean_ideology_value_seat_weighted)
    end
end
