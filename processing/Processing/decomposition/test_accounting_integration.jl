using Test
using CSV
using DataFrames
using Dates
using Statistics

include(joinpath(@__DIR__, "AccountingIntegration.jl"))
using .AccountingIntegration

const ACCOUNTING_INTEGRATION_OUTPUT_ROOT = normpath(
    joinpath(@__DIR__, "..", "output", "decomposition"),
)
const ACCOUNTING_INTEGRATION_TEST_LOG = joinpath(
    ACCOUNTING_INTEGRATION_OUTPUT_ROOT,
    "audit",
    "focused_accounting_integration_test.log",
)

function required_integration_csv(relative_path)
    path = joinpath(ACCOUNTING_INTEGRATION_OUTPUT_ROOT, relative_path)
    isfile(path) || error("Required accounting-integration artifact is missing: $(path)")
    return CSV.read(path, DataFrame)
end

function integration_exact(value)
    pieces = split(strip(String(value)), "//"; limit = 2)
    length(pieces) == 2 || error("Expected exact rational n//d, found $(value).")
    return parse(BigInt, pieces[1]) // parse(BigInt, pieces[2])
end

integration_display_milli(value) = parse(Int, replace(String(value), "." => ""))

focal_integration = required_integration_csv("raw/accounting_focal_case_decomposition.csv")
focal_states = required_integration_csv("raw/accounting_focal_state_contributions.csv")
gross_integration = required_integration_csv(
    "raw/accounting_gross_component_concentration.csv",
)
state_anatomy = required_integration_csv(
    "figure_data/accounting_state_weighting_anatomy.csv",
)
district_weights = required_integration_csv(
    "figure_data/accounting_district_electoral_weight.csv",
)
selected_geography = required_integration_csv(
    "raw/accounting_selected_party_geography.csv",
)
minimal_accounting = required_integration_csv(
    "raw/accounting_minimal_ideological_decomposition.csv",
)
integration_checks_data = required_integration_csv(
    "audit/accounting_integration_checks.csv",
)
coalition_party_contributions = required_integration_csv(
    "raw/coalition_party_contributions.csv",
)
coalition_party_contribution_summary = required_integration_csv(
    "raw/coalition_party_contribution_summary.csv",
)
coalition_party_contribution_focal = required_integration_csv(
    "tables/table_coalition_party_contributions.csv",
)
coalition_party_contribution_checks = required_integration_csv(
    "audit/coalition_party_contribution_checks.csv",
)
coalition_party_named_aggregates = required_integration_csv(
    "raw/coalition_party_contribution_named_aggregates.csv",
)

const EXPECTED_FOCAL_ACCOUNTING_IDS = [
    "cabinet/2014/2016.2",
    "cabinet/2014/2017.1",
    "cabinet/2018/2021.3/2022.1",
    "cabinet/2022/2023.1",
    "ideological/2014/08-20",
    "ideological/2018/03-16",
    "ideological/2022/11-20",
    "ideological/2022/17-23",
]
const EXPECTED_PARTY_CONTRIBUTION_FOCAL_IDS = [
    "cabinet/2014/2016.2",
    "cabinet/2014/2017.1",
    "cabinet/2018/2021.3/2022.1",
    "cabinet/2022/2023.1",
    "ideological/2014/05-16",
    "ideological/2014/08-20",
    "ideological/2014/09-23",
    "ideological/2014/10-24",
    "ideological/2018/03-16",
    "ideological/2022/11-20",
    "ideological/2022/17-23",
]

const EXPECTED_NATIONAL_VALID_VOTES = Dict(
    2014 => 97_355_354,
    2018 => 98_264_190,
    2022 => 109_413_508,
)

accounting_integration_test_result = @testset "Manuscript accounting integration" begin
    @test String.(focal_integration.case_id) == EXPECTED_FOCAL_ACCOUNTING_IDS
    @test Int.(focal_integration.focal_order) == collect(1:8)
    @test nrow(focal_integration) == 8
    @test nrow(focal_states) == 8 * 27
    @test all(eachrow(focal_integration)) do row
        integration_exact(row.A_C_exact) + integration_exact(row.B_C_exact) ==
            integration_exact(row.d_C_exact)
    end
    @test all(eachrow(focal_integration)) do row
        integration_exact(row.d_C_exact) - integration_exact(row.r_C_exact) ==
            integration_exact(row.seat_margin_exact)
    end
    @test all(eachrow(focal_integration)) do row
        integration_exact(row.A_C_exact) - integration_exact(row.r_C_exact) ==
            integration_exact(row.A_minus_r_C_exact)
    end

    for total in eachrow(focal_integration)
        selected = focal_states[String.(focal_states.case_id) .== String(total.case_id), :]
        @test nrow(selected) == 27
        @test length(unique(String.(selected.electoral_unit))) == 27
        @test sum(integration_exact.(selected.a_Cd_exact)) ==
            integration_exact(total.A_C_exact)
        @test sum(integration_exact.(selected.b_Cd_exact)) ==
            integration_exact(total.B_C_exact)
        @test sum(integration_exact.(selected.d_Cd_exact)) ==
            integration_exact(total.d_C_exact)
    end

    @test nrow(gross_integration) == 48
    @test Set(String.(gross_integration.aggregation_level)) == Set(["party", "state"])
    @test Set(String.(gross_integration.component)) == Set(["A", "B", "d"])
    @test all(eachrow(gross_integration)) do row
        integration_exact(row.gross_positive_exact) -
            integration_exact(row.gross_negative_magnitude_exact) ==
            integration_exact(row.net_component_exact)
    end
    @test all((0 .<= gross_integration.cancellation_share) .&
              (gross_integration.cancellation_share .<= 1))
    @test all((0 .<= gross_integration.absolute_hhi) .&
              (gross_integration.absolute_hhi .<= 1))

    @test nrow(state_anatomy) == 8
    @test String.(state_anatomy.case_id) == EXPECTED_FOCAL_ACCOUNTING_IDS
    @test all(eachrow(state_anatomy)) do row
        integration_exact(row.b_positive_eight_seat_exact) +
            integration_exact(row.b_positive_other_exact) -
            integration_exact(row.b_negative_sp_exact) -
            integration_exact(row.b_negative_other_exact) ==
            integration_exact(row.B_C_exact)
    end
    @test all(state_anatomy.b_positive_eight_seat .>= 0)
    @test all(state_anatomy.b_negative_sp .>= 0)
    @test all(state_anatomy.largest_positive_b_Cd .> 0)

    @test nrow(district_weights) == 3 * 27
    @test sort(unique(Int.(district_weights.election_year))) == [2014, 2018, 2022]
    @test all(eachrow(district_weights)) do row
        integration_exact(row.district_electoral_weight_exact) ==
            (BigInt(row.S_d) // BigInt(row.V_d)) /
            (BigInt(row.S) // BigInt(row.V))
    end
    for year in (2014, 2018, 2022)
        selected = district_weights[Int.(district_weights.election_year) .== year, :]
        @test nrow(selected) == 27
        @test only(unique(Int.(selected.V))) == EXPECTED_NATIONAL_VALID_VOTES[year]
        @test only(unique(Int.(selected.S))) == 513
        @test sum(Int.(selected.V_d)) == EXPECTED_NATIONAL_VALID_VOTES[year]
        @test sum(Int.(selected.S_d)) == 513
        @test mean(Int.(selected.S_d)) == 19
        @test median(Int.(selected.S_d)) == 10
        @test count(==(8), Int.(selected.S_d)) == 11
        @test maximum(Int.(selected.S_d)) == 70
        @test String.(selected.electoral_unit[Int.(selected.S_d) .== 70]) == ["SP"]
        sp_weight = only(
            Float64.(selected.district_electoral_weight[
                String.(selected.electoral_unit) .== "SP"
            ]),
        )
        @test sp_weight == minimum(Float64.(selected.district_electoral_weight))
    end
    for electoral_unit in unique(String.(district_weights.electoral_unit))
        selected = district_weights[
            String.(district_weights.electoral_unit) .== electoral_unit,
            :,
        ]
        @test length(unique(Int.(selected.S_d))) == 1
    end

    @test nrow(selected_geography) == 17
    @test Int.(selected_geography.selected_order) == collect(1:17)
    @test all(eachrow(selected_geography)) do row
        integration_exact(row.A_i_exact) + integration_exact(row.B_i_exact) ==
            integration_exact(row.d_i_exact)
    end
    @test all(eachrow(selected_geography)) do row
        integration_exact(row.gross_positive_B_exact) -
            integration_exact(row.gross_negative_B_magnitude_exact) ==
            integration_exact(row.B_i_exact)
    end

    @test nrow(minimal_accounting) == 7
    @test Set(String.(minimal_accounting.case_id)) == Set([
        "ideological/2014/05-16",
        "ideological/2014/08-20",
        "ideological/2014/09-23",
        "ideological/2014/10-24",
        "ideological/2018/03-16",
    "ideological/2022/11-20",
        "ideological/2022/17-23",
    ])
    @test all(Bool.(minimal_accounting.minimal_inversion))
    @test all(minimal_accounting.ideological_universe .== "seat_winning")
    primary_members = coalition_party_contributions[coalition_party_contributions.domain .== "ideological", :]
    @test all(primary_members.party_seats .> 0)
    @test all(primary_members.ideological_universe .== "seat_winning")
    @test all(row.national_vote_total == EXPECTED_NATIONAL_VALID_VOTES[Int(row.election)] for row in eachrow(primary_members))

    required_contribution_columns = Set([
        :election, :domain, :case_identifier, :cabinet_period,
        :coalition_start_party, :coalition_end_party, :party,
        :party_vote_total, :party_vote_share, :party_seats, :party_quota_q_i,
        :party_differential_d_i, :coalition_differential_d_C,
        :contribution_sign, :rank_among_positive_contributors,
        :rank_among_negative_contributors, :A_i, :B_i,
        :party_differential_d_i_exact, :coalition_differential_d_C_exact,
    ])
    @test required_contribution_columns ⊆ Set(propertynames(coalition_party_contributions))
    @test nrow(coalition_party_contributions) == 179
    @test sum(coalition_party_contributions.domain .== "cabinet") == 33
    @test sum(coalition_party_contributions.domain .== "ideological") == 146
    @test length(unique(String.(coalition_party_contributions.case_identifier))) == 16
    @test nrow(coalition_party_contribution_summary) == 16
    @test nrow(coalition_party_contribution_checks) == 16
    @test all(Bool.(coalition_party_contribution_checks.all_checks_pass))
    @test all(Bool.(coalition_party_contribution_checks.sum_party_votes_matches_coalition))
    @test all(Bool.(coalition_party_contribution_checks.sum_party_seats_matches_coalition))
    @test all(Bool.(coalition_party_contribution_checks.exact_differential_closure))
    @test all(Bool.(coalition_party_contribution_checks.coalition_d_C_matches_source))
    @test all(Bool.(coalition_party_contribution_checks.coalition_remains_inversion))

    for total in eachrow(coalition_party_contribution_summary)
        selected = coalition_party_contributions[
            String.(coalition_party_contributions.case_identifier) .==
                String(total.case_identifier),
            :,
        ]
        @test nrow(selected) == Int(total.coalition_party_count)
        @test sum(Int.(selected.party_vote_total)) == Int(total.coalition_vote_total)
        @test sum(Int.(selected.party_seats)) == Int(total.coalition_seats)
        @test sum(integration_exact.(selected.party_differential_d_i_exact)) ==
            integration_exact(total.d_C_exact)
        @test integration_exact(total.gross_positive_party_contribution_exact) -
            integration_exact(total.gross_negative_party_contribution_exact) ==
            integration_exact(total.d_C_exact)

        positive_ranks = sort(collect(skipmissing(
            selected.rank_among_positive_contributors,
        )))
        negative_ranks = sort(collect(skipmissing(
            selected.rank_among_negative_contributors,
        )))
        @test positive_ranks == collect(1:Int(total.positive_party_count))
        @test negative_ranks == collect(1:Int(total.negative_party_count))
        @test count(==("positive"), String.(selected.contribution_sign)) ==
            Int(total.positive_party_count)
        @test count(==("negative"), String.(selected.contribution_sign)) ==
            Int(total.negative_party_count)
    end

    # Reconstruct each historical period's full contribution vector from the
    # same exact election-party accounting used by the coalesced observation.
    originals = required_csv(joinpath(PAPER_ROOT_TEST, "diagnostics",
        "cabinet_coalitions_before_coalescing.csv"))
    originals = originals[(originals.election_year .== 2018) .&
        in.(String.(originals.period), Ref(Set(["2021.3", "2022.1"]))), :]
    @test nrow(originals) == 2
    complete_party = required_csv(joinpath(OUTPUT_ROOT_TEST, "raw",
        "party_accounting_all_years.csv"))
    merged_vector = sort(coalition_party_contributions[
        String.(coalition_party_contributions.case_identifier) .==
            "cabinet/2018/2021.3/2022.1", :], :party)
    @test nrow(merged_vector) == 9
    merged_total = only(eachrow(focal_integration[
        String.(focal_integration.case_id) .== "cabinet/2018/2021.3/2022.1", :]))
    for original in eachrow(originals)
        names = sort(strip.(split(String(original.parties), ",")))
        source_vector = sort(complete_party[(complete_party.election_year .== 2018) .&
            in.(String.(complete_party.party), Ref(Set(names))), :], :party)
        @test String.(merged_vector.party) == String.(source_vector.party) == names
        for (target, source) in ((:party_vote_total, :v_i), (:party_seats, :s_i),
            (:party_quota_q_i_exact, :q_i_exact),
            (:party_differential_d_i_exact, :d_i_exact),
            (:A_i_exact, :A_i_exact), (:B_i_exact, :B_i_exact))
            @test merged_vector[!, target] == source_vector[!, source]
        end
        for (aggregate, component) in ((:A_C_exact, :A_i_exact),
            (:B_C_exact, :B_i_exact), (:d_C_exact, :d_i_exact), (:q_C_exact, :q_i_exact))
            @test integration_exact(merged_total[aggregate]) ==
                sum(integration_exact.(source_vector[!, component]))
        end
        @test merged_total.v_C == original.votes
        @test merged_total.s_C == original.seats
        @test isapprox(merged_total.R_C, original.representation_ratio;
            atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
    end
    @test all(String.(merged_vector.source_periods) .== "[\"2021.3\",\"2022.1\"]")
    @test all(merged_vector.period_days .== 238)
    @test !(:shared_2018_vector_check_applicable in propertynames(coalition_party_contribution_checks))

    @test nrow(coalition_party_contribution_focal) == 11
    @test String.(coalition_party_contribution_focal.case_identifier) ==
        EXPECTED_PARTY_CONTRIBUTION_FOCAL_IDS
    @test Int.(coalition_party_contribution_focal.focal_order) == collect(1:11)
    @test sum(coalition_party_contribution_focal.domain .== "cabinet") == 4
    @test sum(coalition_party_contribution_focal.domain .== "ideological") == 7
    @test all(Bool.(coalition_party_contribution_focal.exact_closure_pass))
    @test all(eachrow(coalition_party_contribution_focal)) do row
        displayed = AccountingIntegration.party_contribution_closure_preserving_display(
            row.d_C,
            row.gross_positive_party_contribution,
        )
        parse(Int, replace(displayed.gross_positive, "." => "")) -
            parse(Int, replace(displayed.gross_negative, "." => "")) ==
            parse(Int, replace(displayed.d_C, "." => ""))
    end

    @test nrow(coalition_party_named_aggregates) == 1
    pp_pl = only(eachrow(coalition_party_named_aggregates))
    pp_pl_members = coalition_party_contributions[
        (String.(coalition_party_contributions.case_identifier) .==
            "ideological/2022/17-23") .&
        in.(String.(coalition_party_contributions.party), Ref(Set(["PL", "PP"]))),
        :,
    ]
    @test integration_exact(pp_pl.combined_d_i_exact) ==
        sum(integration_exact.(pp_pl_members.party_differential_d_i_exact))
    @test integration_exact(pp_pl.combined_d_i_exact) /
        integration_exact(pp_pl.d_C_exact) ==
        integration_exact(pp_pl.share_of_d_C_exact)
    @test round(Float64(pp_pl.share_of_d_C_pct); digits = 2) == 77.56

    contribution_latex =
        AccountingIntegration.coalition_party_contribution_latex(
            coalition_party_contribution_focal,
        )
    @test occursin("\\begin{table}[htbp]", contribution_latex)
    @test occursin("\\caption{Party contributions to coalition differentials}", contribution_latex)
    @test occursin("2021.3/2022.1", contribution_latex)
    @test occursin("\\(d_i\\) is an ex post accounting contribution", contribution_latex)
    @test occursin("Positive and negative party contributions sum to \\(d_C\\)", contribution_latex)
    @test occursin("closure-preserving three-decimal display", contribution_latex)
    @test occursin("joint electoral lists", contribution_latex)
    @test occursin("do not identify party-specific causal effects", contribution_latex)
    @test !occursin("caused the inversion", lowercase(contribution_latex))


    focal_latex = AccountingIntegration.focal_case_latex(focal_integration)
    @test occursin("\\begin{tabularx}", focal_latex)
    @test !occursin("\\(r_C\\)", focal_latex)
    @test !occursin("A_C-r_C", focal_latex)
    @test !occursin("Threshold accounting", focal_latex)
    @test !occursin("accounting-sufficient", lowercase(focal_latex))
    @test !occursin("required to meet threshold", lowercase(focal_latex))

    minimal_latex = AccountingIntegration.minimal_ideological_latex(
        minimal_accounting,
    )
    @test occursin("\\begin{tabular}", minimal_latex)
    @test occursin(
        "Election & Start & End & Parties & Vote \\% & Seats & \\(d_C\\) & \\(A_C\\) & \\(B_C\\)",
        minimal_latex,
    )
    @test !occursin("\\(q_C\\)", minimal_latex)
    @test !occursin("\\(r_C\\)", minimal_latex)
    @test !occursin("Pattern", minimal_latex)

    # CSV B_C stays at full precision; only its manuscript display is the exact
    # three-decimal residual of the independently rounded d_C and A_C entries.
    raw_minimal_B_C = copy(minimal_accounting.B_C)
    for (source, rendered) in (
        (focal_integration, focal_latex),
        (minimal_accounting, minimal_latex),
    )
        for row in eachrow(source)
            displayed = AccountingIntegration.closure_preserving_display(row.d_C, row.A_C)
            @test integration_display_milli(displayed.d_C) ==
                integration_display_milli(displayed.A_C) + integration_display_milli(displayed.B_C)
            @test occursin(
                "$(displayed.d_C) & $(displayed.A_C) & $(displayed.B_C)",
                rendered,
            )
        end
    end
    @test minimal_accounting.B_C == raw_minimal_B_C
    @test any(abs.(minimal_accounting.B_C .- round.(minimal_accounting.B_C; digits = 3)) .> 1e-12)

    @test nrow(integration_checks_data) == 12
    @test all(String.(integration_checks_data.status) .== "PASS")

    for filename in (
        "table_accounting_focal_cases.tex",
        "table_accounting_gross_components.tex",
        "table_accounting_selected_party_geography.tex",
        "table_accounting_minimal_ideological.tex",
        "table_coalition_party_contributions.tex",
    )
        @test isfile(joinpath(ACCOUNTING_INTEGRATION_OUTPUT_ROOT, "latex", filename))
    end
end

mkpath(dirname(ACCOUNTING_INTEGRATION_TEST_LOG))
open(ACCOUNTING_INTEGRATION_TEST_LOG, "w") do io
    println(io, "Focused accounting integration test")
    println(io, "timestamp_utc=$(Dates.now(Dates.UTC))")
    println(io, "julia_version=$(VERSION)")
    println(io, "focal_cases=$(nrow(focal_integration))")
    println(io, "focal_state_rows=$(nrow(focal_states))")
    println(io, "gross_component_rows=$(nrow(gross_integration))")
    println(io, "district_electoral_weight_rows=$(nrow(district_weights))")
    println(io, "selected_party_rows=$(nrow(selected_geography))")
    println(io, "coalition_party_rows=$(nrow(coalition_party_contributions))")
    println(io, "coalition_party_cases=$(nrow(coalition_party_contribution_summary))")
    println(io, "coalition_party_focal_rows=$(nrow(coalition_party_contribution_focal))")
    println(io, "coalition_party_checks=$(nrow(coalition_party_contribution_checks))")
    println(io, "status=PASS")
end

println("Focused accounting-integration audit log: $(ACCOUNTING_INTEGRATION_TEST_LOG)")

# Empirical snapshots belong only to tests; production enforces structural and exact identities.
const BASELINE_CASE_EXPECTATIONS = Dict(
    "cabinet/2014/2016.2" => (A_C = 15.74, B_C = 4.53, d_C = 20.27, r_C = 18.27),
    "cabinet/2014/2017.1" => (A_C = 11.27, B_C = -1.32, d_C = 9.95, r_C = 0.95),
    "cabinet/2018/2021.3/2022.1" => (A_C = 19.07, B_C = -4.45, d_C = 14.62, r_C = 14.62),
    "cabinet/2022/2023.1" => (A_C = 13.20, B_C = -0.99, d_C = 12.20, r_C = 6.20),
)

const BASELINE_PARTY_EXPECTATIONS = [
    (case_id = "cabinet/2014/2016.2", party = "PMDB", A_i = 3.91, B_i = 4.22, d_i = 8.13),
    (case_id = "cabinet/2014/2016.2", party = "PT", A_i = -1.22, B_i = -1.21, d_i = -2.42),
    (case_id = "cabinet/2014/2017.1", party = "PMDB", A_i = 3.91, B_i = 4.22, d_i = 8.13),
    (case_id = "cabinet/2014/2017.1", party = "PSDB", A_i = 0.58, B_i = -5.01, d_i = -4.43),
    (case_id = "cabinet/2018/2021.3/2022.1", party = "PP", A_i = 7.32, B_i = 1.45, d_i = 8.76),
    (case_id = "cabinet/2018/2021.3/2022.1", party = "PSL", A_i = -2.42, B_i = -5.28, d_i = -7.70),
    (case_id = "cabinet/2018/2021.3/2022.1", party = "PSC", A_i = -3.07, B_i = 1.09, d_i = -1.98),
    (case_id = "cabinet/2022/2023.1", party = "UNIÃO", A_i = 9.36, B_i = 1.75, d_i = 11.10),
    (case_id = "cabinet/2022/2023.1", party = "PT", A_i = 9.07, B_i = -2.13, d_i = 6.94),
    (case_id = "cabinet/2022/2023.1", party = "PSOL", A_i = -3.40, B_i = -2.66, d_i = -6.06),
]


@testset "Historical accounting presentation regressions" begin
    @test sum(coalition_party_contributions.domain .== "cabinet") == 33
    @test sum(coalition_party_contribution_focal.domain .== "cabinet") == 4
    baseline = required_integration_csv("raw/accounting_all_inversion_decomposition.csv")
    parties = required_integration_csv("raw/coalition_party_contributions.csv")
    for (case_id, expected) in BASELINE_CASE_EXPECTATIONS
        row = only(eachrow(baseline[baseline.case_id .== case_id, :]))
        for field in (:A_C, :B_C, :d_C, :r_C)
            @test isapprox(row[field], expected[field]; atol = 0.005, rtol = 0)
        end
    end
    for expected in BASELINE_PARTY_EXPECTATIONS
        row = only(eachrow(parties[(parties.case_identifier .== expected.case_id) .&
                                  (parties.party .== expected.party), :]))
        for (field, source) in ((:A_i, :A_i),
                               (:B_i, :B_i),
                               (:d_i, :party_differential_d_i))
            @test isapprox(row[source], expected[field]; atol = 0.005, rtol = 0)
        end
    end
end

@testset "Cabinet district table source and serialization" begin
    table = AccountingIntegration.cabinet_district_concentration(focal_states)
    @test nrow(table) == sum(focal_integration.case_domain .== "cabinet")
    @test table.positive_count .+ table.negative_count == fill(27, nrow(table))
    for row in eachrow(table)
        total = only(eachrow(focal_integration[focal_integration.case_id .== row.case_id, :]))
        @test integration_exact(row.positive_sum_exact) + integration_exact(row.negative_sum_exact) == integration_exact(total.A_C_exact)
    end
    latex = AccountingIntegration.cabinet_district_concentration_latex(table)
    @test length(collect(eachmatch(r"\\\\\s*\n", latex))) == nrow(table) + 1
end
