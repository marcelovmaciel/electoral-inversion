using Test
using Dates
using JSON3
using SHA

const _PIN_LINKAGE = Processing.CabinetRelease.default_pin_path()

@testset "Pinned coalition period linkage" begin
    @test isfile(_PIN_LINKAGE)
    calendar = Processing.CabinetRelease.calendar_table(_PIN_LINKAGE)
    periods = Processing.coalitions_by_period_raw(; path = _PIN_LINKAGE)
    windows = Processing.coalition_period_windows(; path = _PIN_LINKAGE)
    @test Set(keys(windows)) == Set(calendar.period)
    @test Set(keys(periods)) == Set(calendar.period[calendar.identified])
    @test Set(calendar.period[.!calendar.identified]) ∩ Set(keys(periods)) == Set{String}()
    for row in eachrow(calendar)
        @test windows[row.period] == (row.start_inclusive, row.end_exclusive - Day(1))
        @test row.days == Dates.value(row.period_end - row.period_start) + 1
    end
    for (start_date, end_date) in [(Date(2025,1,1), Date(2025,12,31)),
                                  (Date(2023,1,1), Date(2026,3,19)),
                                  (Date(2015,1,1), Date(2018,12,31))]
        actual = Processing.coalition_periods_overlapping_window(periods, start_date, end_date; path = _PIN_LINKAGE)
        expected = Set(String(row.period) for row in eachrow(calendar)
                       if row.identified && row.start_inclusive <= end_date && row.end_exclusive > start_date)
        @test Set(keys(actual)) == expected
    end
    @test Processing.coalition_periods_overlapping_year(periods, 2025; path = _PIN_LINKAGE) ==
          Processing.coalition_periods_overlapping_window(periods, Date(2025,1,1), Date(2025,12,31); path = _PIN_LINKAGE)
    @test Set(keys(Processing.coalition_periods_by_label_year(periods, 2025))) ==
          Set(String(row.period) for row in eachrow(calendar) if row.identified && year(row.start_inclusive) == 2025)
    @test isempty(calendar[.!calendar.identified, :])
    @test sum(calendar.provisional_days) == 100
    @test sum(calendar.established_days) == 3996
    @test isempty(Processing.coalition_periods_overlapping_window(Dict{String,Vector{String}}(),
                   Date(2025,1,1), Date(2025,1,2); path = _PIN_LINKAGE))
    @test_throws ErrorException Processing.coalition_periods_overlapping_window(periods, Date(2025,1,2), Date(2025,1,1))
    @test_throws ErrorException Processing.coalition_periods_overlapping_window(Dict("missing" => ["PT"]), Date(2025,1,1), Date(2025,1,2))
    row = first(eachrow(calendar[calendar.identified, :]))
    single = Dict(String(row.period) => periods[row.period])
    @test haskey(Processing.coalition_periods_overlapping_window(single, row.period_end, row.period_end), row.period)
    @test isempty(Processing.coalition_periods_overlapping_window(single, row.end_exclusive, row.end_exclusive))
end

@testset "Cabinet pin fails closed" begin
    original = JSON3.read(read(_PIN_LINKAGE, String), Dict{String,Any})
    mktempdir() do tempdir
        # Each override is a distinct path so no previously validated cache entry is reused.
        cases = [
            ("metadata", pin -> (pin["metadata_sha256"] = repeat("0", 64))),
            ("schema", pin -> (pin["schema_version"] = 2)),
            ("cutoff", pin -> (pin["cutoff_exclusive"] = "2026-03-21")),
            ("version", pin -> (pin["data_version"] = "unreviewed")),
            ("missing_atomic", pin -> delete!(pin["file_hashes"], "atomic_events.csv")),
            ("missing_evidence", pin -> delete!(pin["file_hashes"], "evidence.csv")),
            ("bad_file", pin -> (pin["file_hashes"]["membership.csv"] = repeat("0", 64))),
            ("missing_primary_assumptions", pin -> delete!(pin["file_hashes"], "primary_assumptions.csv")),
        ]
        for (name, mutate!) in cases
            pin = deepcopy(original); mutate!(pin)
            path = joinpath(tempdir, name * ".json")
            write(path, JSON3.write(pin))
            @test_throws ErrorException Processing.CabinetRelease.load_release(path)
        end
        @test_throws ErrorException Processing.CabinetRelease.load_release(joinpath(tempdir, "missing.json"))
        good = joinpath(tempdir, "valid.json")
        write(good, JSON3.write(original))
        @test Processing.CabinetRelease.load_release(good).metadata.data_version == original["data_version"]
    end
end
