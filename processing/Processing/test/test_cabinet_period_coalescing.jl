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

@testset "Current manuscript cabinet coalescing regression" begin
    root = abspath(@__DIR__, "..", "..", "..")
    paper = joinpath(root, "processing", "Processing", "output", "paper")
    read_csv(path) = CSV.read(path, DataFrame; stringtype = String,
        types = (i, name) -> String(name) in ("period", "periodo") ? String : nothing)
    raw = read_csv(joinpath(root, "scraping", "output", "partidos_por_periodo.csv"))
    before = read_csv(joinpath(paper, "diagnostics", "cabinet_coalitions_before_coalescing.csv"))
    after = read_csv(joinpath(paper, "raw", "cabinet_coalition_metrics.csv"))
    row_for(df, period) = only(eachrow(df[(df.election_year .== 2018) .& (df.period .== period), :]))
    left, right = row_for(before, "2021.3"), row_for(before, "2022.1")
    merged = row_for(after, "2021.3/2022.1")
    # Historical labels retain the actual pre/post-fusion boundary.
    pre = raw[raw.periodo .== "2021.3", :]
    post = raw[raw.periodo .== "2022.1", :]
    @test Set(["DEM", "PSL", "PSC"]) ⊆ Set(pre.partido)
    @test !("UNIÃO" in pre.partido)
    @test "UNIÃO" in post.partido
    @test "PSC" in post.partido
    @test isempty(intersect(Set(["DEM", "PSL"]), Set(post.partido)))
    @test only(unique(pre.data_fim)) == Date(2022, 2, 7)
    @test only(unique(post.data_inicio)) == Date(2022, 2, 8)
    translation = read_csv(joinpath(paper, "diagnostics", "cabinet_translation_report.csv"))
    translated_set(period) = Set(translation.election_party[(translation.election_year .== 2018) .& (translation.period .== period)])
    expected_parties = Set(["DEM", "PATRIOTA", "PP", "PR", "PRB", "PSC", "PSD", "PSDB", "PSL"])
    @test translated_set("2021.3") == translated_set("2022.1") == expected_parties
    @test Set(strip.(split(left.parties, ','))) == Set(strip.(split(right.parties, ','))) == expected_parties
    @test left.period_end + Day(1) == right.period_start
    @test left.period_days == 188
    @test right.period_days == 50
    @test merged.period_start == Date(2021, 8, 4)
    @test merged.period_end == Date(2022, 3, 29)
    @test merged.period_days == merged.days_overlapping_mandate == 238
    @test JSON3.read(merged.source_periods) == ["2021.3", "2022.1"]
    @test nrow(before) == 24
    @test nrow(after) == 23
    @test count(before.coalition_inversion) == 5
    @test count(after.coalition_inversion) == 4
    @test Set(zip(after.election_year[after.coalition_inversion], after.period[after.coalition_inversion])) == Set([
        (2014, "2016.2"), (2014, "2017.1"), (2018, "2021.3/2022.1"), (2022, "2023.1"),
    ])
    recomputed = Processing.coalesce_adjacent_cabinet_periods(before;
        expected_merges = Set([(2018, ("2021.3", "2022.1"))]))
    @test isequal(recomputed, after)
    @test count(cell -> length(JSON3.read(cell)) > 1, after.source_periods) == 1
    for column in (:votes, :national_vote_total, :vote_share, :seats, :seat_share,
        :quota, :seat_diff, :required_diff, :representation_ratio, :coalition_inversion)
        @test isequal(merged[column], left[column]) && isequal(left[column], right[column])
    end
    @test merged.vote_share * 100 ≈ 47.246851574312064
    @test merged.seats == 257
    @test merged.quota ≈ 242.3763485762209
    @test merged.seat_diff ≈ 14.6236514237791
    @test merged.representation_ratio ≈ 1.0603344819314346
    for (year, expected_days) in [(2014, 102), (2018, 238), (2022, 255)]
        inversion_days(df) = sum(df.days_overlapping_mandate[(df.election_year .== year) .& df.coalition_inversion])
        @test inversion_days(before) == inversion_days(after) == expected_days
    end
end
