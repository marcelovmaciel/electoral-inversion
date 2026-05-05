using Test
using Dates

const _PROCESSING_MODULE_FILE_LINKAGE = joinpath(@__DIR__, "..", "src", "Processing.jl")
const _ROOT_DIR_LINKAGE = abspath(@__DIR__, "..", "..", "..")
const _COALITION_JSON_LINKAGE = joinpath(_ROOT_DIR_LINKAGE, "scraping", "output", "partidos_por_periodo.json")

const _PROCESSING_LOADED_LINKAGE = let
    try
        if !isdefined(Main, :Processing)
            include(_PROCESSING_MODULE_FILE_LINKAGE)
        end
        @eval using .Processing
        true
    catch err
        @test_skip "Não foi possível carregar módulo Processing: $(sprint(showerror, err))"
        false
    end
end

function _period_keys_sorted_by_window(periods::Dict{String,Vector{String}}, windows)
    ks = collect(keys(periods))
    sort!(ks, by = k -> begin
        start_date, _ = windows[k]
        (start_date, Processing.period_sort_key(k))
    end)
    return ks
end

@testset "Coalition Period Linkage" begin
    if !_PROCESSING_LOADED_LINKAGE
        @test_skip "Módulo Processing não carregado; testes de linkage ignorados."
    else
        @test isfile(_COALITION_JSON_LINKAGE)

        periods = Processing.coalitions_by_period_raw(; path = _COALITION_JSON_LINKAGE)
        windows = Processing.coalition_period_windows(; path = _COALITION_JSON_LINKAGE)

        overlap_2025 = Processing.coalition_periods_overlapping_year(
            periods,
            2025;
            path = _COALITION_JSON_LINKAGE,
        )
        label_2025 = Processing.coalition_periods_by_label_year(periods, 2025)
        mandate_2022 = Processing.coalition_periods_overlapping_window(
            periods,
            Date(2023, 1, 1),
            Date(2025, 12, 31);
            path = _COALITION_JSON_LINKAGE,
        )

        @test _period_keys_sorted_by_window(overlap_2025, windows) == ["2023.2", "2025.1"]
        @test sort(collect(keys(label_2025)); by = Processing.period_sort_key) == ["2025.1"]
        @test _period_keys_sorted_by_window(mandate_2022, windows) == ["2023.1", "2023.2", "2025.1"]
        @test length(keys(mandate_2022)) == length(unique(collect(keys(mandate_2022))))
        @test _period_keys_sorted_by_window(mandate_2022, windows) ==
              sort(collect(keys(mandate_2022)); by = k -> begin
                  start_date, _ = windows[k]
                  (start_date, Processing.period_sort_key(k))
              end)
    end
end
