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
    return CSV.read(path, DataFrame; types = (i, name) -> name in (:period, :cabinet_period) ? String : nothing)
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

current_case_registry = required_integration_csv("raw/inversion_case_registry.csv")
current_cabinet_registry = current_case_registry[current_case_registry.case_domain .== "cabinet", :]
current_cabinet_ids = String.(current_cabinet_registry.case_id)
const CURRENT_CABINET_N = nrow(current_cabinet_registry)
const CURRENT_CABINET_PARTY_N = sum(current_cabinet_registry.coalition_party_count)
const CURRENT_CASE_N = CURRENT_CABINET_N + 12

const EXPECTED_FOCAL_ACCOUNTING_IDS = vcat(current_cabinet_ids, [
    "ideological/2014/08-20",
    "ideological/2018/03-16",
    "ideological/2022/11-20",
    "ideological/2022/17-23",
])
const EXPECTED_PARTY_CONTRIBUTION_FOCAL_IDS = vcat(current_cabinet_ids, [
    "ideological/2014/05-16",
    "ideological/2014/08-20",
    "ideological/2014/09-23",
    "ideological/2014/10-24",
    "ideological/2018/03-16",
    "ideological/2022/11-20",
    "ideological/2022/17-23",
])

const EXPECTED_NATIONAL_VALID_VOTES = Dict(
    2014 => 97_355_354,
    2018 => 98_264_190,
    2022 => 109_413_508,
)

accounting_integration_test_result = @testset "Manuscript accounting integration" begin
    @test String.(focal_integration.case_id) == EXPECTED_FOCAL_ACCOUNTING_IDS
    @test Int.(focal_integration.focal_order) == collect(1:length(EXPECTED_FOCAL_ACCOUNTING_IDS))
    @test nrow(focal_integration) == length(EXPECTED_FOCAL_ACCOUNTING_IDS)
    @test nrow(focal_states) == length(EXPECTED_FOCAL_ACCOUNTING_IDS) * 27
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

    @test nrow(gross_integration) == 6 * length(EXPECTED_FOCAL_ACCOUNTING_IDS)
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

    @test nrow(state_anatomy) == length(EXPECTED_FOCAL_ACCOUNTING_IDS)
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
    @test nrow(coalition_party_contributions) == CURRENT_CABINET_PARTY_N + 146
    @test sum(coalition_party_contributions.domain .== "cabinet") == CURRENT_CABINET_PARTY_N
    @test sum(coalition_party_contributions.domain .== "ideological") == 146
    @test length(unique(String.(coalition_party_contributions.case_identifier))) == CURRENT_CASE_N
    @test nrow(coalition_party_contribution_summary) == CURRENT_CASE_N
    @test nrow(coalition_party_contribution_checks) == CURRENT_CASE_N
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

    # Independently sum the full exact election-party vectors of every current
    # cabinet case; no prior period or historical count is an arithmetic oracle.
    complete_party = required_integration_csv("raw/party_accounting_all_years.csv")
    for total in eachrow(focal_integration[focal_integration.case_domain .== "cabinet", :])
        names = Set(strip.(split(String(total.coalition_parties), ",")))
        source = complete_party[(complete_party.election_year .== total.election_year) .&
            in.(String.(complete_party.party), Ref(names)), :]
        @test Set(String.(source.party)) == names
        @test sum(source.v_i) == total.v_C
        @test sum(source.s_i) == total.s_C
        for (aggregate, component) in ((:A_C_exact, :A_i_exact), (:B_C_exact, :B_i_exact),
                                       (:d_C_exact, :d_i_exact), (:q_C_exact, :q_i_exact))
            @test integration_exact(total[aggregate]) == sum(integration_exact.(source[!, component]))
        end
        @test total.v_C * 2 < total.V && total.s_C >= 257
        @test Dates.value(Date(total.period_end) - Date(total.period_start)) + 1 == total.period_days
    end
    @test !(:shared_2018_vector_check_applicable in propertynames(coalition_party_contribution_checks))

    @test nrow(coalition_party_contribution_focal) == length(EXPECTED_PARTY_CONTRIBUTION_FOCAL_IDS)
    @test String.(coalition_party_contribution_focal.case_identifier) ==
        EXPECTED_PARTY_CONTRIBUTION_FOCAL_IDS
    @test Int.(coalition_party_contribution_focal.focal_order) == collect(1:length(EXPECTED_PARTY_CONTRIBUTION_FOCAL_IDS))
    @test sum(coalition_party_contribution_focal.domain .== "cabinet") == CURRENT_CABINET_N
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
    @test all(occursin(AccountingIntegration.latex_escape(r.cabinet_period), contribution_latex) for r in eachrow(current_cabinet_registry))
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

# Cabinet numerical snapshots were retired with the contemporaneous-affiliation
# release; the complete direct exact-party sums above replace those old cases.

@testset "Cabinet district table source and serialization" begin
    table = AccountingIntegration.cabinet_district_concentration(focal_states)
    @test nrow(table) == sum(focal_integration.case_domain .== "cabinet")
    @test all(table.positive_count .+ table.negative_count .<= 27)
    for row in eachrow(table)
        total = only(eachrow(focal_integration[focal_integration.case_id .== row.case_id, :]))
        @test integration_exact(row.positive_sum_exact) + integration_exact(row.negative_sum_exact) == integration_exact(total.A_C_exact)
    end
    latex = AccountingIntegration.cabinet_district_concentration_latex(table)
    @test length(collect(eachmatch(r"\\\\\s*\n", latex))) == nrow(table) + 1 + Int(isempty(table))
end
