using Test
using CSV
using DataFrames
using Dates

const REPORT_TEST_PROCESSING_ROOT = normpath(joinpath(@__DIR__, ".."))
const REPORT_TEST_OUTPUT_ROOT = joinpath(
    REPORT_TEST_PROCESSING_ROOT,
    "output",
    "decomposition",
)
const REPORT_TEST_LOG_PATH = joinpath(
    REPORT_TEST_OUTPUT_ROOT,
    "audit",
    "focused_intermediate_accounting_report_test.log",
)
const REPORT_ACCOUNTING_ATOL = 1.0e-9
const REPORT_ACCOUNTING_RTOL = 1.0e-12

const EXPECTED_REPORT_PARTIES = Dict(2014 => 32, 2018 => 35, 2022 => 32)
const EXPECTED_REPORT_IDEOLOGICAL_KEYS = Set([
    (2014, 5, 16), (2014, 5, 17), (2014, 7, 20), (2014, 8, 20),
    (2014, 9, 23), (2014, 9, 24), (2014, 10, 24), (2018, 3, 16),
    (2022, 11, 20), (2022, 15, 23), (2022, 16, 23), (2022, 17, 23),
])

const REQUIRED_REPORT_RAW_PATHS = [
    "raw/party_district_accounting_all_years.csv",
    "raw/party_accounting_all_years.csv",
    "raw/district_accounting_all_years.csv",
    "raw/inversion_case_registry.csv",
    "raw/all_inversion_decomposition.csv",
    "raw/all_inversion_party_contributions.csv",
    "raw/all_inversion_district_contributions.csv",
    "raw/all_inversion_party_district_contributions.csv",
    "raw/cabinet_inversion_decomposition.csv",
    "raw/cabinet_inversion_party_contributions.csv",
    "raw/cabinet_inversion_district_contributions.csv",
    "raw/cabinet_inversion_party_district_contributions.csv",
    "raw/ideological_inversion_decomposition.csv",
    "raw/ideological_inversion_party_contributions.csv",
    "raw/ideological_inversion_district_contributions.csv",
    "raw/ideological_inversion_party_district_contributions.csv",
    "raw/all_inversion_contribution_rankings.csv",
]

const REPORT_TABLE_STEMS = [
    "table_year_accounting_closure",
    "table_district_weight_extremes",
    "table_inversion_case_registry",
    "table_all_inversion_decomposition",
    "table_case_component_extremes",
    "table_case_party_vectors",
    "table_case_district_vectors",
    "table_case_party_district_extremes",
    "table_party_district_accounting_2014",
    "table_party_district_accounting_2018",
    "table_party_district_accounting_2022",
]

const REQUIRED_REPORT_TABLE_PATHS = vcat(
    [joinpath("tables", "report", string(stem, ".csv")) for stem in REPORT_TABLE_STEMS],
    [joinpath("tables", "report", "generated_interpretation_source.csv")],
)

const REQUIRED_REPORT_TEX_PATHS = vcat(
    [joinpath("latex", "report", string(stem, ".tex")) for stem in REPORT_TABLE_STEMS],
    [joinpath("latex", "report", "generated_interpretation.tex"),
     joinpath("latex", "report", "party_size_diagnostics.tex")],
)

const REQUIRED_REPORT_AUDIT_PATHS = [
    "audit/intermediate_accounting_identity_checks.csv",
    "audit/intermediate_accounting_input_manifest.csv",
    "audit/intermediate_accounting_generation_checks.csv",
    "audit/intermediate_accounting_report_artifact_manifest.csv",
]

function report_string(value)
    ismissing(value) && return ""
    return strip(string(value))
end

function report_int(value)
    value isa Integer && return Int(value)
    if value isa AbstractFloat
        isinteger(value) || error("Expected integer-valued number, found " * string(value))
        return Int(round(value))
    end
    text = report_string(value)
    parsed = tryparse(Int, text)
    parsed === nothing || return parsed
    parsed_float = tryparse(Float64, text)
    parsed_float === nothing && error("Expected integer value, found " * text)
    isinteger(parsed_float) || error("Expected integer-valued text, found " * text)
    return Int(round(parsed_float))
end

function report_bool(value)
    value isa Bool && return value
    text = lowercase(report_string(value))
    text == "true" && return true
    text == "false" && return false
    error("Expected Boolean value, found " * text)
end

function report_exact(value)
    text = report_string(value)
    pieces = split(text, "//"; limit = 2)
    length(pieces) == 2 || error("Expected exact rational n//d, found " * text)
    numerator_value = parse(BigInt, strip(pieces[1]))
    denominator_value = parse(BigInt, strip(pieces[2]))
    denominator_value == 0 && error("Exact rational has a zero denominator: " * text)
    return numerator_value // denominator_value
end

function report_isapprox(left, right)
    return isapprox(
        Float64(left),
        Float64(right);
        atol = REPORT_ACCOUNTING_ATOL,
        rtol = REPORT_ACCOUNTING_RTOL,
    )
end

function normalize_report_strings!(data::DataFrame)
    identifier_columns = (
        :case_id,
        :source_case_id,
        :case_domain,
        :case_label,
        :cabinet_period,
        :start_party,
        :end_party,
        :coalition_parties,
        :party,
        :electoral_unit,
        :aggregation_level,
        :component,
        :unit_label,
        :value_sign,
        :minimal_status,
        :status,
        :check_name,
        :path,
        :artifact_type,
    )
    for column in identifier_columns
        column in propertynames(data) || continue
        data[!, column] = report_string.(data[!, column])
    end
    return data
end

function require_report_csv(relative_path::AbstractString)
    path = joinpath(REPORT_TEST_OUTPUT_ROOT, relative_path)
    isfile(path) || error("Required intermediate-report CSV is missing: " * path)
    return normalize_report_strings!(CSV.read(path, DataFrame; types = (i, name) -> name in (:period, :cabinet_period) ? String : nothing))
end

function require_columns(data::DataFrame, columns)
    return Set(columns) <= Set(propertynames(data))
end

function report_party_list(value)
    return filter(!isempty, strip.(split(report_string(value), ",")))
end

function normalized_artifact_path(value)
    return replace(report_string(value), '\\' => '/')
end

function artifact_on_disk(relative_path::AbstractString)
    components = filter(!isempty, split(normalized_artifact_path(relative_path), "/"))
    return joinpath(REPORT_TEST_OUTPUT_ROOT, components...)
end

all_required_report_paths = vcat(
    REQUIRED_REPORT_RAW_PATHS,
    REQUIRED_REPORT_TABLE_PATHS,
    REQUIRED_REPORT_TEX_PATHS,
    REQUIRED_REPORT_AUDIT_PATHS,
)
missing_report_paths = filter(
    relative_path -> !isfile(joinpath(REPORT_TEST_OUTPUT_ROOT, relative_path)),
    all_required_report_paths,
)
isempty(missing_report_paths) || error(
    "Missing intermediate-report artifacts: " * join(missing_report_paths, ", "),
)

full_cells = require_report_csv("raw/party_district_accounting_all_years.csv")
full_parties = require_report_csv("raw/party_accounting_all_years.csv")
full_districts = require_report_csv("raw/district_accounting_all_years.csv")
case_registry = require_report_csv("raw/inversion_case_registry.csv")
case_decomposition = require_report_csv("raw/all_inversion_decomposition.csv")
case_parties = require_report_csv("raw/all_inversion_party_contributions.csv")
case_districts = require_report_csv("raw/all_inversion_district_contributions.csv")
case_cells = require_report_csv("raw/all_inversion_party_district_contributions.csv")
cabinet_decomposition = require_report_csv("raw/cabinet_inversion_decomposition.csv")
cabinet_parties = require_report_csv("raw/cabinet_inversion_party_contributions.csv")
cabinet_districts = require_report_csv("raw/cabinet_inversion_district_contributions.csv")
cabinet_cells = require_report_csv("raw/cabinet_inversion_party_district_contributions.csv")
ideological_decomposition = require_report_csv("raw/ideological_inversion_decomposition.csv")
ideological_parties = require_report_csv("raw/ideological_inversion_party_contributions.csv")
ideological_districts = require_report_csv("raw/ideological_inversion_district_contributions.csv")
ideological_cells = require_report_csv("raw/ideological_inversion_party_district_contributions.csv")
rankings = require_report_csv("raw/all_inversion_contribution_rankings.csv")
identity_checks = require_report_csv("audit/intermediate_accounting_identity_checks.csv")
input_manifest = require_report_csv("audit/intermediate_accounting_input_manifest.csv")
generation_checks = require_report_csv("audit/intermediate_accounting_generation_checks.csv")
report_manifest = require_report_csv("audit/intermediate_accounting_report_artifact_manifest.csv")

# Cabinet keys come from direct vote/seat criterion, not an inherited chronology.
report_periods = Processing.cabinet_set_view(require_report_csv("raw/coalition_period_quantities.csv"))
report_inversions = report_periods[(2 .* report_periods.v_C .< report_periods.V) .& (report_periods.s_C .>= 257), :]
const EXPECTED_REPORT_CABINET_KEYS = Set(String.(report_inversions.coalition_id))
const REPORT_CABINET_N = nrow(report_inversions)
const REPORT_CABINET_MEMBERS = sum(report_inversions.coalition_party_count)
const REPORT_CASE_N = REPORT_CABINET_N + 12
const REPORT_PARTY_N = REPORT_CABINET_MEMBERS + 146

report_test_result = @testset "Intermediate party-district accounting report" begin
    @testset "Required artifact boundary" begin
        @test isempty(missing_report_paths)
        @test length(all_required_report_paths) == length(unique(all_required_report_paths))
        @test require_columns(report_manifest, (:path, :artifact_type, :description, :rows, :columns, :sha256))
        manifest_paths = normalized_artifact_path.(report_manifest.path)
        @test length(manifest_paths) == length(unique(manifest_paths))
        @test all(!isempty, manifest_paths)
        @test all(path -> !isabspath(path), manifest_paths)
        @test all(path -> !occursin(r"(^|/)\.\.(/|$)", path), manifest_paths)
        @test all(path -> !occursin("output/paper", lowercase(path)), manifest_paths)
        @test all(path -> !occursin("writing/", lowercase(path)), manifest_paths)
        @test all(path -> isfile(artifact_on_disk(path)), manifest_paths)
        @test all(length.(report_string.(report_manifest.sha256)) .== 64)

        expected_manifest_paths = Set(vcat(
            REQUIRED_REPORT_RAW_PATHS,
            REQUIRED_REPORT_TABLE_PATHS,
            REQUIRED_REPORT_TEX_PATHS,
            REQUIRED_REPORT_AUDIT_PATHS[1:3],
        ))
        @test expected_manifest_paths <= Set(manifest_paths)

        @test require_columns(input_manifest, (:path, :bytes, :sha256))
        @test nrow(input_manifest) > 0
        @test all(input_manifest.bytes .> 0)
        @test all(length.(report_string.(input_manifest.sha256)) .== 64)
        @test length(report_string.(input_manifest.path)) ==
            length(unique(report_string.(input_manifest.path)))
        input_manifest_paths = Set(normalized_artifact_path.(input_manifest.path))
        @test Set([
            "processing/Processing/decomposition/report/intermediate_accounting_report.tex",
            "processing/Processing/decomposition/report/build_report.sh",
        ]) <= input_manifest_paths

        @test nrow(generation_checks) > 0
        @test :status in propertynames(generation_checks)
        @test all(generation_checks.status .== "PASS")
        @test nrow(identity_checks) == 9 + 8 * REPORT_CASE_N
        @test all(report_bool.(identity_checks.exact_pass))
        @test all(identity_checks.status .== "PASS")
        @test all(abs.(Float64.(identity_checks.floating_residual)) .<= REPORT_ACCOUNTING_ATOL)
    end

    @testset "Complete party-district panel and exact closures" begin
        @test nrow(full_cells) == 2_673
        @test require_columns(full_cells, (
            :election_year, :electoral_unit, :party, :v_id, :V_d, :s_id, :S_d,
            :V, :S, :within_district_quota, :national_quota_contribution,
            :a_id, :b_id, :b_id_factored, :d_id,
            :within_district_quota_exact, :national_quota_contribution_exact,
            :a_id_exact, :b_id_exact, :b_id_factored_exact, :d_id_exact,
        ))
        @test nrow(unique(select(full_cells, :election_year, :electoral_unit, :party))) == 2_673

        for year in sort(collect(keys(EXPECTED_REPORT_PARTIES)))
            selected = full_cells[report_int.(full_cells.election_year) .== year, :]
            expected_parties = EXPECTED_REPORT_PARTIES[year]
            @test nrow(selected) == 27 * expected_parties
            @test length(unique(selected.party)) == expected_parties
            @test length(unique(selected.electoral_unit)) == 27
            by_party = combine(groupby(selected, :party), nrow => :cell_count)
            by_district = combine(groupby(selected, :electoral_unit), nrow => :party_count)
            @test all(by_party.cell_count .== 27)
            @test all(by_district.party_count .== expected_parties)
        end

        @test all(eachrow(full_cells)) do row
            within = report_exact(row.within_district_quota_exact)
            national = report_exact(row.national_quota_contribution_exact)
            a_value = report_exact(row.a_id_exact)
            b_value = report_exact(row.b_id_exact)
            b_factored = report_exact(row.b_id_factored_exact)
            d_value = report_exact(row.d_id_exact)
            observed_seats = BigInt(report_int(row.s_id)) // BigInt(1)
            return a_value == observed_seats - within &&
                b_value == within - national &&
                b_value == b_factored &&
                d_value == a_value + b_value &&
                d_value == observed_seats - national
        end
        @test all(eachrow(full_cells)) do row
            return report_isapprox(row.a_id + row.b_id, row.d_id) &&
                report_isapprox(row.b_id, row.b_id_factored) &&
                abs(Float64(row.b_crosscheck_residual)) <= REPORT_ACCOUNTING_ATOL &&
                report_isapprox(row.s_id - row.within_district_quota, row.a_id) &&
                report_isapprox(
                    row.within_district_quota - row.national_quota_contribution,
                    row.b_id,
                )
        end

        @test nrow(full_parties) == 99
        @test require_columns(full_parties, (
            :election_year, :party, :v_i, :s_i, :q_i, :d_i, :A_i, :B_i,
            :q_i_exact, :d_i_exact, :A_i_exact, :B_i_exact,
        ))
        @test nrow(unique(select(full_parties, :election_year, :party))) == 99
        @test all(eachrow(full_parties)) do row
            return report_exact(row.A_i_exact) + report_exact(row.B_i_exact) ==
                report_exact(row.d_i_exact)
        end
        @test all(eachrow(full_parties)) do row
            return report_isapprox(row.A_i + row.B_i, row.d_i) &&
                abs(Float64(row.A_plus_B_residual)) <= REPORT_ACCOUNTING_ATOL
        end

        party_lookup = Dict(
            (report_int(row.election_year), row.party) => row_index for
            (row_index, row) in enumerate(eachrow(full_parties))
        )
        for group in groupby(full_cells, [:election_year, :party])
            key = (report_int(first(group.election_year)), first(group.party))
            @test haskey(party_lookup, key)
            party_row = full_parties[party_lookup[key], :]
            @test sum(report_int.(group.v_id)) == report_int(party_row.v_i)
            @test sum(report_int.(group.s_id)) == report_int(party_row.s_i)
            @test sum(report_exact(value) for value in group.a_id_exact) ==
                report_exact(party_row.A_i_exact)
            @test sum(report_exact(value) for value in group.b_id_exact) ==
                report_exact(party_row.B_i_exact)
            @test sum(report_exact(value) for value in group.d_id_exact) ==
                report_exact(party_row.d_i_exact)
            @test report_isapprox(sum(Float64.(group.a_id)), party_row.A_i)
            @test report_isapprox(sum(Float64.(group.b_id)), party_row.B_i)
            @test report_isapprox(sum(Float64.(group.d_id)), party_row.d_i)
        end

        @test nrow(full_districts) == 81
        @test require_columns(full_districts, (
            :election_year, :electoral_unit, :V_d, :S_d, :V, :S,
            :sum_a_id, :sum_b_id, :expected_sum_b_id,
            :seat_equivalent_weight_gap_exact,
        ))
        @test nrow(unique(select(full_districts, :election_year, :electoral_unit))) == 81
        district_lookup = Dict(
            (report_int(row.election_year), row.electoral_unit) => row_index for
            (row_index, row) in enumerate(eachrow(full_districts))
        )
        for group in groupby(full_cells, [:election_year, :electoral_unit])
            key = (report_int(first(group.election_year)), first(group.electoral_unit))
            @test haskey(district_lookup, key)
            district_row = full_districts[district_lookup[key], :]
            expected_b = (BigInt(report_int(district_row.S_d)) // BigInt(1)) -
                ((BigInt(report_int(district_row.S)) * BigInt(report_int(district_row.V_d))) //
                 BigInt(report_int(district_row.V)))
            @test sum(report_int.(group.v_id)) == report_int(district_row.V_d)
            @test sum(report_int.(group.s_id)) == report_int(district_row.S_d)
            @test sum(report_exact(value) for value in group.a_id_exact) == 0
            @test sum(report_exact(value) for value in group.b_id_exact) == expected_b
            @test report_exact(district_row.seat_equivalent_weight_gap_exact) == expected_b
            @test report_isapprox(sum(Float64.(group.a_id)), district_row.sum_a_id)
            @test report_isapprox(sum(Float64.(group.b_id)), district_row.sum_b_id)
            @test report_isapprox(district_row.sum_b_id, district_row.expected_sum_b_id)
            @test abs(Float64(district_row.b_closure_residual)) <= REPORT_ACCOUNTING_ATOL
        end

        for year in sort(collect(keys(EXPECTED_REPORT_PARTIES)))
            selected_parties = full_parties[report_int.(full_parties.election_year) .== year, :]
            @test sum(report_exact(value) for value in selected_parties.A_i_exact) == 0
            @test sum(report_exact(value) for value in selected_parties.B_i_exact) == 0
            @test sum(report_exact(value) for value in selected_parties.d_i_exact) == 0
            @test report_isapprox(sum(Float64.(selected_parties.A_i)), 0)
            @test report_isapprox(sum(Float64.(selected_parties.B_i)), 0)
            @test report_isapprox(sum(Float64.(selected_parties.d_i)), 0)
        end
    end

    @testset "Cabinet and ideological inversion registry" begin
        @test nrow(case_registry) == REPORT_CASE_N
        @test length(unique(case_registry.case_id)) == REPORT_CASE_N
        cabinets = case_registry[case_registry.case_domain .== "cabinet", :]
        ideological = case_registry[case_registry.case_domain .== "ideological", :]
        @test nrow(cabinets) == REPORT_CABINET_N
        @test all(report_bool(r.compositionally_repeated) == (r.composition_equivalence_count > 1) for r in eachrow(case_registry))
        @test all(Dates.value(Date(r.period_end) - Date(r.period_start)) + 1 == r.period_days for r in eachrow(cabinets))
        @test nrow(ideological) == 12
        @test Set(cabinets.source_case_id) == EXPECTED_REPORT_CABINET_KEYS
        ideological_keys = Set(
            (
                report_int(row.election_year),
                report_int(row.ideology_start_index),
                report_int(row.ideology_end_index),
            ) for row in eachrow(ideological)
        )
        @test ideological_keys == EXPECTED_REPORT_IDEOLOGICAL_KEYS
        @test sum(report_int.(ideological.election_year) .== 2018) == 1
        @test all(ideological.ideological_universe .== "seat_winning")
        @test all(report_bool.(cabinets.observed_coalition))
        @test all(!report_bool(value) for value in cabinets.synthetic_ideological_interval)
        @test all(!report_bool(value) for value in ideological.observed_coalition)
        @test all(report_bool.(ideological.synthetic_ideological_interval))

        expected_minimal_counts = Dict(2014 => 4, 2018 => 1, 2022 => 2)
        for year in sort(collect(keys(expected_minimal_counts)))
            selected = ideological[report_int.(ideological.election_year) .== year, :]
            @test sum(report_bool.(selected.minimal_inversion)) == expected_minimal_counts[year]
        end

        @test all(eachrow(case_registry)) do row
            parties = report_party_list(row.coalition_parties)
            return length(parties) == report_int(row.coalition_party_count) &&
                length(parties) == length(unique(parties)) &&
                report_int(row.v_C) / report_int(row.V) < 0.5 &&
                report_int(row.s_C) >= 257
        end
        @test sum(report_int.(case_registry.coalition_party_count)) == REPORT_PARTY_N
    end

    @testset "All-domain decompositions and linked contribution sums" begin
        @test nrow(case_decomposition) == REPORT_CASE_N
        @test nrow(case_parties) == REPORT_PARTY_N
        @test nrow(case_districts) == 27 * REPORT_CASE_N
        @test nrow(case_cells) == 27 * REPORT_PARTY_N
        @test nrow(cabinet_decomposition) == REPORT_CABINET_N
        @test nrow(ideological_decomposition) == 12
        @test nrow(cabinet_parties) == REPORT_CABINET_MEMBERS
        @test nrow(ideological_parties) == 146
        @test nrow(cabinet_districts) == 27 * REPORT_CABINET_N
        @test nrow(ideological_districts) == 324
        @test nrow(cabinet_cells) == 27 * REPORT_CABINET_MEMBERS
        @test nrow(ideological_cells) == 3_942
        @test nrow(unique(select(case_parties, :case_id, :party))) == REPORT_PARTY_N
        @test nrow(unique(select(case_districts, :case_id, :electoral_unit))) == 27 * REPORT_CASE_N
        @test nrow(unique(select(case_cells, :case_id, :party, :electoral_unit))) == 27 * REPORT_PARTY_N

        cabinet_case_ids = Set(case_registry.case_id[case_registry.case_domain .== "cabinet"])
        ideological_case_ids = Set(case_registry.case_id[case_registry.case_domain .== "ideological"])
        @test Set(cabinet_decomposition.case_id) == cabinet_case_ids
        @test Set(ideological_decomposition.case_id) == ideological_case_ids
        @test Set(cabinet_parties.case_id) == cabinet_case_ids
        @test Set(ideological_parties.case_id) == ideological_case_ids
        @test Set(cabinet_districts.case_id) == cabinet_case_ids
        @test Set(ideological_districts.case_id) == ideological_case_ids
        @test Set(cabinet_cells.case_id) == cabinet_case_ids
        @test Set(ideological_cells.case_id) == ideological_case_ids

        registry_lookup = Dict(
            row.case_id => row_index for
            (row_index, row) in enumerate(eachrow(case_registry))
        )
        decomposition_lookup = Dict(
            row.case_id => row_index for
            (row_index, row) in enumerate(eachrow(case_decomposition))
        )
        full_cell_lookup = Dict(
            (report_int(row.election_year), row.party, row.electoral_unit) => row_index for
            (row_index, row) in enumerate(eachrow(full_cells))
        )
        case_party_lookup = Dict(
            (row.case_id, row.party) => row_index for
            (row_index, row) in enumerate(eachrow(case_parties))
        )
        case_district_lookup = Dict(
            (row.case_id, row.electoral_unit) => row_index for
            (row_index, row) in enumerate(eachrow(case_districts))
        )

        @test Set(keys(registry_lookup)) == Set(keys(decomposition_lookup))
        @test all(eachrow(case_decomposition)) do row
            q_value = report_exact(row.q_C_exact)
            d_value = report_exact(row.d_C_exact)
            r_value = report_exact(row.r_C_exact)
            A_value = report_exact(row.A_C_exact)
            B_value = report_exact(row.B_C_exact)
            expected_q = (BigInt(report_int(row.S)) * BigInt(report_int(row.v_C))) //
                BigInt(report_int(row.V))
            expected_d = (BigInt(report_int(row.s_C)) // BigInt(1)) - expected_q
            expected_r = (BigInt(257) // BigInt(1)) - expected_q
            return q_value == expected_q && d_value == expected_d &&
                r_value == expected_r && A_value + B_value == d_value &&
                report_int(row.v_C) / report_int(row.V) < 0.5 &&
                report_int(row.s_C) >= 257 &&
                report_isapprox(row.A_C + row.B_C, row.d_C) &&
                abs(Float64(row.A_plus_B_residual)) <= REPORT_ACCOUNTING_ATOL &&
                abs(Float64(row.party_d_residual)) <= REPORT_ACCOUNTING_ATOL &&
                abs(Float64(row.district_A_residual)) <= REPORT_ACCOUNTING_ATOL &&
                abs(Float64(row.district_B_residual)) <= REPORT_ACCOUNTING_ATOL &&
                abs(Float64(row.linked_cell_A_residual)) <= REPORT_ACCOUNTING_ATOL &&
                abs(Float64(row.linked_cell_B_residual)) <= REPORT_ACCOUNTING_ATOL
        end

        @test all(eachrow(case_parties)) do row
            return report_isapprox(row.A_i + row.B_i, row.d_i) &&
                abs(Float64(row.A_plus_B_residual)) <= REPORT_ACCOUNTING_ATOL &&
                (ismissing(row.q_times_R_minus_1) ||
                 report_isapprox(row.q_times_R_minus_1, row.d_i))
        end
        @test all(eachrow(case_districts)) do row
            return report_exact(row.a_Cd_exact) + report_exact(row.b_Cd_exact) ==
                report_exact(row.d_Cd_exact) &&
                report_isapprox(row.a_Cd + row.b_Cd, row.d_Cd) &&
                report_isapprox(row.b_Cd, row.b_Cd_factored) &&
                abs(Float64(row.b_crosscheck_residual)) <= REPORT_ACCOUNTING_ATOL
        end
        @test all(eachrow(case_cells)) do row
            return report_exact(row.a_id_exact) + report_exact(row.b_id_exact) ==
                report_exact(row.d_id_exact) &&
                report_isapprox(row.a_id + row.b_id, row.d_id) &&
                report_isapprox(row.b_id, row.b_id_factored) &&
                abs(Float64(row.b_crosscheck_residual)) <= REPORT_ACCOUNTING_ATOL
        end
        @test all(eachrow(case_cells)) do row
            key = (report_int(row.election_year), row.party, row.electoral_unit)
            haskey(full_cell_lookup, key) || return false
            source = full_cells[full_cell_lookup[key], :]
            return report_int(row.v_id) == report_int(source.v_id) &&
                report_int(row.s_id) == report_int(source.s_id) &&
                report_exact(row.a_id_exact) == report_exact(source.a_id_exact) &&
                report_exact(row.b_id_exact) == report_exact(source.b_id_exact) &&
                report_exact(row.d_id_exact) == report_exact(source.d_id_exact)
        end

        for case_id in sort(collect(keys(decomposition_lookup)))
            decomposed = case_decomposition[decomposition_lookup[case_id], :]
            registered = case_registry[registry_lookup[case_id], :]
            member_parties = case_parties[case_parties.case_id .== case_id, :]
            member_districts = case_districts[case_districts.case_id .== case_id, :]
            member_cells = case_cells[case_cells.case_id .== case_id, :]
            expected_party_count = report_int(registered.coalition_party_count)

            @test nrow(member_parties) == expected_party_count
            @test Set(member_parties.party) == Set(report_party_list(registered.coalition_parties))
            @test nrow(member_districts) == 27
            @test length(unique(member_districts.electoral_unit)) == 27
            @test nrow(member_cells) == expected_party_count * 27
            @test all(combine(groupby(member_cells, :party), nrow => :cell_count).cell_count .== 27)

            @test report_isapprox(sum(Float64.(member_parties.A_i)), decomposed.A_C)
            @test report_isapprox(sum(Float64.(member_parties.B_i)), decomposed.B_C)
            @test report_isapprox(sum(Float64.(member_parties.d_i)), decomposed.d_C)
            @test report_isapprox(sum(Float64.(member_districts.a_Cd)), decomposed.A_C)
            @test report_isapprox(sum(Float64.(member_districts.b_Cd)), decomposed.B_C)
            @test report_isapprox(sum(Float64.(member_districts.d_Cd)), decomposed.d_C)
            @test report_isapprox(sum(Float64.(member_cells.a_id)), decomposed.A_C)
            @test report_isapprox(sum(Float64.(member_cells.b_id)), decomposed.B_C)
            @test report_isapprox(sum(Float64.(member_cells.d_id)), decomposed.d_C)

            @test sum(report_exact(value) for value in member_districts.a_Cd_exact) ==
                report_exact(decomposed.A_C_exact)
            @test sum(report_exact(value) for value in member_districts.b_Cd_exact) ==
                report_exact(decomposed.B_C_exact)
            @test sum(report_exact(value) for value in member_cells.a_id_exact) ==
                report_exact(decomposed.A_C_exact)
            @test sum(report_exact(value) for value in member_cells.b_id_exact) ==
                report_exact(decomposed.B_C_exact)
            @test sum(report_exact(value) for value in member_cells.d_id_exact) ==
                report_exact(decomposed.d_C_exact)

            for party_group in groupby(member_cells, :party)
                key = (case_id, first(party_group.party))
                @test haskey(case_party_lookup, key)
                party_row = case_parties[case_party_lookup[key], :]
                @test report_isapprox(sum(Float64.(party_group.a_id)), party_row.A_i)
                @test report_isapprox(sum(Float64.(party_group.b_id)), party_row.B_i)
                @test report_isapprox(sum(Float64.(party_group.d_id)), party_row.d_i)
            end
            for district_group in groupby(member_cells, :electoral_unit)
                key = (case_id, first(district_group.electoral_unit))
                @test haskey(case_district_lookup, key)
                district_row = case_districts[case_district_lookup[key], :]
                @test report_isapprox(sum(Float64.(district_group.a_id)), district_row.a_Cd)
                @test report_isapprox(sum(Float64.(district_group.b_id)), district_row.b_Cd)
                @test report_isapprox(sum(Float64.(district_group.d_id)), district_row.d_Cd)
            end
        end
    end

    @testset "Complete ranking values, extrema, and deterministic ranks" begin
        @test nrow(rankings) == 3 * (REPORT_PARTY_N + 27 * REPORT_CASE_N + 27 * REPORT_PARTY_N)
        @test require_columns(rankings, (
            :case_id, :case_domain, :aggregation_level, :component, :party,
            :electoral_unit, :unit_label, :value, :value_sign,
            :descending_rank, :ascending_rank, :absolute_rank,
            :positive_rank, :negative_rank,
        ))
        ranking_source = Dict{NTuple{5,String},Float64}()
        for row in eachrow(case_parties)
            for component in ("A_i", "B_i", "d_i")
                ranking_source[(row.case_id, "party", component, row.party, "")] =
                    Float64(row[Symbol(component)])
            end
        end
        for row in eachrow(case_districts)
            for component in ("a_Cd", "b_Cd", "d_Cd")
                ranking_source[(row.case_id, "district", component, "", row.electoral_unit)] =
                    Float64(row[Symbol(component)])
            end
        end
        for row in eachrow(case_cells)
            for component in ("a_id", "b_id", "d_id")
                ranking_source[(
                    row.case_id,
                    "party_district",
                    component,
                    row.party,
                    row.electoral_unit,
                )] = Float64(row[Symbol(component)])
            end
        end
        @test length(ranking_source) == 3 * (REPORT_PARTY_N + 27 * REPORT_CASE_N + 27 * REPORT_PARTY_N)
        ranking_keys = [
            (
                row.case_id,
                row.aggregation_level,
                row.component,
                row.party,
                row.electoral_unit,
            ) for row in eachrow(rankings)
        ]
        @test length(ranking_keys) == length(unique(ranking_keys))
        @test Set(ranking_keys) == Set(keys(ranking_source))
        @test all(eachrow(rankings)) do row
            key = (
                row.case_id,
                row.aggregation_level,
                row.component,
                row.party,
                row.electoral_unit,
            )
            expected_sign = row.value > 0 ? "positive" : row.value < 0 ? "negative" : "zero"
            return haskey(ranking_source, key) &&
                report_isapprox(row.value, ranking_source[key]) &&
                row.value_sign == expected_sign
        end

        for group in groupby(rankings, [:case_id, :aggregation_level, :component])
            row_indices = collect(1:nrow(group))
            descending_expected = sort(row_indices; by = index ->
                (-Float64(group.value[index]), group.unit_label[index]))
            ascending_expected = sort(row_indices; by = index ->
                (Float64(group.value[index]), group.unit_label[index]))
            absolute_expected = sort(row_indices; by = index ->
                (-abs(Float64(group.value[index])), -Float64(group.value[index]),
                 group.unit_label[index]))
            descending_actual = sort(row_indices; by = index ->
                report_int(group.descending_rank[index]))
            ascending_actual = sort(row_indices; by = index ->
                report_int(group.ascending_rank[index]))
            absolute_actual = sort(row_indices; by = index ->
                report_int(group.absolute_rank[index]))
            @test descending_actual == descending_expected
            @test ascending_actual == ascending_expected
            @test absolute_actual == absolute_expected
            @test sort(report_int.(group.descending_rank)) == collect(1:nrow(group))
            @test sort(report_int.(group.ascending_rank)) == collect(1:nrow(group))
            @test sort(report_int.(group.absolute_rank)) == collect(1:nrow(group))
            @test Float64(group.value[only(findall(report_int.(group.descending_rank) .== 1))]) ==
                maximum(Float64.(group.value))
            @test Float64(group.value[only(findall(report_int.(group.ascending_rank) .== 1))]) ==
                minimum(Float64.(group.value))
            @test abs(Float64(group.value[only(findall(report_int.(group.absolute_rank) .== 1))])) ==
                maximum(abs.(Float64.(group.value)))

            positive_expected = filter(index -> group.value[index] > 0, descending_expected)
            negative_expected = filter(index -> group.value[index] < 0, ascending_expected)
            positive_actual = sort(
                filter(index -> !ismissing(group.positive_rank[index]), row_indices);
                by = index -> report_int(group.positive_rank[index]),
            )
            negative_actual = sort(
                filter(index -> !ismissing(group.negative_rank[index]), row_indices);
                by = index -> report_int(group.negative_rank[index]),
            )
            @test positive_actual == positive_expected
            @test negative_actual == negative_expected
            @test all(
                ismissing(group.positive_rank[index]) for
                index in row_indices if group.value[index] <= 0
            )
            @test all(
                ismissing(group.negative_rank[index]) for
                index in row_indices if group.value[index] >= 0
            )
        end
    end
end

mkpath(dirname(REPORT_TEST_LOG_PATH))
open(REPORT_TEST_LOG_PATH, "w") do io
    println(io, "Focused intermediate party-district accounting report test")
    println(io, "timestamp_utc=", Dates.now(Dates.UTC))
    println(io, "julia_version=", VERSION)
    println(io, "accounting_atol=", REPORT_ACCOUNTING_ATOL)
    println(io, "accounting_rtol=", REPORT_ACCOUNTING_RTOL)
    println(io, "full_party_district_cells=", nrow(full_cells))
    println(io, "case_registry_rows=", nrow(case_registry))
    println(io, "case_party_rows=", nrow(case_parties))
    println(io, "case_district_rows=", nrow(case_districts))
    println(io, "case_party_district_rows=", nrow(case_cells))
    println(io, "ranking_rows=", nrow(rankings))
    println(io, "identity_checks=", nrow(identity_checks))
    println(io, "status=PASS")
end

println("Focused intermediate accounting report audit log: ", REPORT_TEST_LOG_PATH)
