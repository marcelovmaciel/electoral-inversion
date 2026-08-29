using Test
using CSV
using DataFrames
using Dates
using Processing

include(joinpath(@__DIR__, "CoalitionDecomposition.jl"))
using .CoalitionDecomposition

const PROCESSING_ROOT_TEST = normpath(joinpath(@__DIR__, ".."))
const OUTPUT_ROOT_TEST = joinpath(PROCESSING_ROOT_TEST, "output", "decomposition")
const PAPER_ROOT_TEST = joinpath(PROCESSING_ROOT_TEST, "output", "paper")
const TEST_LOG_PATH = joinpath(OUTPUT_ROOT_TEST, "audit", "focused_decomposition_test.log")

function required_csv(path)
    isfile(path) || error("Required focused-test artifact is missing: $(path)")
    data = CSV.read(path, DataFrame)
    for column in (:cabinet_period, :period)
        column in propertynames(data) || continue
        data[!, column] = string.(data[!, column])
    end
    return data
end

function key_set(data, period_column)
    return Set((Int(row.election_year), String(row[period_column])) for row in eachrow(data))
end

function source_row(data, year, period, period_column)
    selected = data[
        (Int.(data.election_year) .== year) .& (String.(data[!, period_column]) .== period),
        :,
    ]
    return only(eachrow(selected))
end

display_milli(value) = parse(Int, replace(String(value), "." => ""))

coalition_periods = required_csv(joinpath(OUTPUT_ROOT_TEST, "raw", "coalition_period_quantities.csv"))
decomposition = required_csv(joinpath(OUTPUT_ROOT_TEST, "raw", "inversion_decomposition.csv"))
party_contributions = required_csv(joinpath(OUTPUT_ROOT_TEST, "raw", "inversion_party_contributions.csv"))
district_contributions = required_csv(joinpath(OUTPUT_ROOT_TEST, "raw", "inversion_district_contributions.csv"))
identity_checks = required_csv(joinpath(OUTPUT_ROOT_TEST, "audit", "decomposition_identity_checks.csv"))
input_manifest = required_csv(joinpath(OUTPUT_ROOT_TEST, "audit", "decomposition_input_manifest.csv"))
ideological = required_csv(joinpath(PAPER_ROOT_TEST, "raw", "ideological_interval_metrics.csv"))
psc_baseline = required_csv(joinpath(PAPER_ROOT_TEST, "raw", "cabinet_coalition_metrics.csv"))

test_result = @testset "PSC-correct five-case coalition decomposition" begin
    expected_keys = Set(EXPECTED_INVERSION_KEYS)
    inversion_periods = coalition_periods[coalition_periods.coalition_inversion .== true, :]
    @test nrow(coalition_periods) == 24
    @test key_set(inversion_periods, :cabinet_period) == expected_keys
    @test key_set(decomposition, :cabinet_period) == expected_keys
    @test nrow(decomposition) == 5
    @test nrow(district_contributions) == 5 * 27
    @test !((2018, "2022.2") in key_set(decomposition, :cabinet_period))

    observed_latex = CoalitionDecomposition.decomposition_latex(decomposition)
    @test occursin("\\begin{tabularx}", observed_latex)
    @test occursin(
        "Election & Period & Days & Vote \\% & Seats & \\(d_C\\) & \\(A_C\\) & \\(B_C\\) & Parties",
        observed_latex,
    )
    @test !occursin("\\(q_C\\)", observed_latex)
    @test !occursin("\\(r_C\\)", observed_latex)
    @test !occursin("Threshold", observed_latex)
    @test occursin("2021.3", observed_latex)
    @test occursin("2022.1", observed_latex)

    # CSV B_C stays at full precision; only its manuscript display is the exact
    # three-decimal residual of the independently rounded d_C and A_C entries.
    raw_B_C = copy(decomposition.B_C)
    for row in eachrow(decomposition)
        displayed = CoalitionDecomposition.closure_preserving_display(
            row.d_C,
            row.A_C,
        )
        @test display_milli(displayed.d_C) ==
            display_milli(displayed.A_C) + display_milli(displayed.B_C)
        @test occursin(
            "$(displayed.d_C) & $(displayed.A_C) & $(displayed.B_C)",
            observed_latex,
        )
    end
    @test decomposition.B_C == raw_B_C
    @test any(abs.(decomposition.B_C .- round.(decomposition.B_C; digits = 3)) .> 1e-12)

    for key in EXPECTED_INVERSION_KEYS
        year, period = key
        coalition = source_row(coalition_periods, year, period, :cabinet_period)
        decomposed = source_row(decomposition, year, period, :cabinet_period)
        baseline = source_row(psc_baseline, year, period, :period)

        parties = sort(strip.(split(String(coalition.coalition_parties), ",")))
        @test parties == EXPECTED_INVERSION_COALITIONS[key]
        @test coalition.vote_share < 0.5
        @test coalition.s_C >= 257
        @test coalition.coalition_inversion
        @test coalition.v_C == baseline.votes
        @test coalition.s_C == baseline.seats
        @test isapprox(coalition.q_C, baseline.quota; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(coalition.d_C, baseline.seat_diff; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(coalition.r_C, baseline.required_diff; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(coalition.R_C, baseline.representation_ratio; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)

        @test isapprox(decomposed.q_C, coalition.q_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(decomposed.d_C, coalition.d_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(decomposed.r_C, coalition.r_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(decomposed.A_C + decomposed.B_C, decomposed.d_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test abs(decomposed.A_plus_B_residual) <= ACCOUNTING_ATOL
        @test abs(decomposed.party_d_residual) <= ACCOUNTING_ATOL

        members = party_contributions[
            (Int.(party_contributions.election_year) .== year) .&
            (String.(party_contributions.cabinet_period) .== period),
            :,
        ]
        @test sort(String.(members.party)) == EXPECTED_INVERSION_COALITIONS[key]
        @test isapprox(sum(members.d_i), decomposed.d_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(sum(members.A_i), decomposed.A_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(sum(members.B_i), decomposed.B_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test all(isapprox.(members.q_times_R_minus_1, members.d_i; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL))
        @test maximum(members.d_i) > 0
        @test minimum(members.d_i) < 0
        if year in (2014, 2018)
            @test all(occursin.("ex post party accounting contribution; joint electoral lists", String.(members.accounting_qualification)))
        end

        districts = district_contributions[
            (Int.(district_contributions.election_year) .== year) .&
            (String.(district_contributions.cabinet_period) .== period),
            :,
        ]
        @test nrow(districts) == 27
        @test length(unique(String.(districts.electoral_unit))) == 27
        @test isapprox(sum(districts.a_Cd), decomposed.A_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test isapprox(sum(districts.b_Cd), decomposed.B_C; atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
        @test all(abs.(districts.b_crosscheck_residual) .<= ACCOUNTING_ATOL)
    end

    cases_2018 = decomposition[decomposition.election_year .== 2018, :]
    @test nrow(cases_2018) == 2
    @test all(cases_2018.s_C .== 257)
    @test all(isapprox.(cases_2018.vote_share_pct, 47.2469; atol = 0.0001, rtol = 0.0))

    @test nrow(identity_checks) == 20
    @test all(identity_checks.exact_pass)
    @test all(String.(identity_checks.status) .== "PASS")
    @test all(abs.(identity_checks.floating_residual) .<= ACCOUNTING_ATOL)
    @test count(endswith.(String.(input_manifest.path), "seats.csv")) == 3
    @test all(input_manifest.bytes .> 0)
    @test all(length.(String.(input_manifest.sha256)) .== 64)

    ideological_regression = validate_ideological_counts(ideological)
    @test collect(zip(
        ideological_regression.election_year,
        ideological_regression.coalition_inversions,
        ideological_regression.minimal_inversions,
    )) == [(2014, 8, 4), (2018, 0, 0), (2022, 6, 2)]
end

mkpath(dirname(TEST_LOG_PATH))
open(TEST_LOG_PATH, "w") do io
    println(io, "Focused PSC-correct coalition decomposition test")
    println(io, "timestamp_utc=$(Dates.now(Dates.UTC))")
    println(io, "julia_version=$(VERSION)")
    println(io, "accounting_atol=$(ACCOUNTING_ATOL)")
    println(io, "accounting_rtol=$(ACCOUNTING_RTOL)")
    println(io, "observed_periods=$(nrow(coalition_periods))")
    println(io, "decomposed_cases=$(nrow(decomposition))")
    println(io, "district_rows=$(nrow(district_contributions))")
    println(io, "party_rows=$(nrow(party_contributions))")
    println(io, "identity_checks=$(nrow(identity_checks))")
    println(io, "expected_keys=$(join([string(year, '/', period) for (year, period) in EXPECTED_INVERSION_KEYS], ','))")
    println(io, "status=PASS")
end

println("Focused decomposition audit log: $(TEST_LOG_PATH)")
