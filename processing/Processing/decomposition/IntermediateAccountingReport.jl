module IntermediateAccountingReport

using CSV
using DataFrames
using Printf
using SHA
using Statistics
using Dates
using JSON3

import ..CoalitionDecomposition

const CD = CoalitionDecomposition

export EXPECTED_PARTY_DISTRICT_ROWS,
       build_full_accounting_outputs,
       build_inversion_case_registry,
       decompose_case_registry,
       build_case_rankings,
       validate_cabinet_compatibility!,
       build_party_size_diagnostics!,
       validate_party_size_regressions,
       write_party_size_diagnostic_outputs,
       write_intermediate_report_outputs

const EXPECTED_PARTIES_BY_YEAR = Dict(2014 => 32, 2018 => 35, 2022 => 32)
const EXPECTED_PARTY_DISTRICT_ROWS = sum(27 * value for value in values(EXPECTED_PARTIES_BY_YEAR))
const EXPECTED_CABINET_PARTY_DISTRICT_ROWS = 891

require(condition::Bool, message::AbstractString) = condition ? true : error(message)
fmt2(value) = @sprintf("%.2f", Float64(value))
fmt3(value) = @sprintf("%.3f", Float64(value))
fmt4(value) = @sprintf("%.4f", Float64(value))
fmtpct(value) = @sprintf("%.2f", 100 * Float64(value))
exact_text(value) = string(numerator(value), "//", denominator(value))

function ordered_parties(value)
    parties = String.(filter(!isempty, strip.(split(String(value), ","))))
    length(parties) == length(unique(parties)) || error(
        "Coalition contains duplicate party labels: $(value)",
    )
    return parties
end

function require_approx(left, right, label)
    CD.accounting_isapprox(left, right) || error(
        "$(label): values differ (left=$(left), right=$(right), " *
        "atol=$(CD.ACCOUNTING_ATOL), rtol=$(CD.ACCOUNTING_RTOL)).",
    )
    return true
end

function sign_pattern(A, B)
    A > 0 && B > 0 && return "A+, B+: reinforcing accounting components"
    A > 0 && B < 0 && return "A+, B-: B offsets part of A"
    A < 0 && B > 0 && return "A-, B+: B more than offsets A"
    A < 0 && B < 0 && return "A-, B-: both components negative"
    A == 0 && return "A=0"
    return "B=0"
end

"""
    build_full_accounting_outputs(accounting_by_year)

Persistable views of the exact party-by-district panels already produced by
`build_year_accounting`. The returned cell table includes every explicit zero
cell. Exact rational strings accompany decimal columns used by tables.
"""
function build_full_accounting_outputs(accounting_by_year::AbstractDict)
    cell_rows = NamedTuple[]
    party_rows = NamedTuple[]
    district_rows = NamedTuple[]
    validation_rows = NamedTuple[]

    for year in sort(collect(keys(accounting_by_year)))
        accounting = accounting_by_year[year]
        expected_parties = EXPECTED_PARTIES_BY_YEAR[Int(year)]
        nrow(accounting.party) == expected_parties || error(
            "$(year): expected $(expected_parties) parties, found $(nrow(accounting.party)).",
        )
        nrow(accounting.panel) == 27 * expected_parties || error(
            "$(year): complete panel has the wrong number of cells.",
        )
        length(unique(String.(accounting.panel.district))) == 27 || error(
            "$(year): complete panel does not contain 27 electoral units.",
        )

        party_lookup = Dict(String(row.party) => row for row in eachrow(accounting.party))
        for row in eachrow(accounting.panel)
            aggregate = party_lookup[String(row.party)]
            cell_d_exact = row.a_exact + row.b_exact
            expected_cell_d = CD.exact_fraction(row.seats, 1) - row.national_quota_contribution_exact
            cell_d_exact == expected_cell_d || error(
                "$(year)/$(row.district)/$(row.party): a_id + b_id != cell differential.",
            )
            row.b_exact == row.b_factored_exact || error(
                "$(year)/$(row.district)/$(row.party): defining and factored b_id differ.",
            )
            push!(cell_rows, (
                election_year = Int(year),
                electoral_unit = String(row.district),
                party = String(row.party),
                v_id = Int(row.votes),
                V_d = Int(row.district_votes),
                district_party_vote_share = row.votes / row.district_votes,
                s_id = Int(row.seats),
                S_d = Int(row.district_seats),
                district_party_seat_share = row.seats / row.district_seats,
                v_i = Int(aggregate.votes),
                V = Int(accounting.national_votes),
                national_party_vote_share = Float64(aggregate.vote_share),
                national_cell_vote_share = row.votes / accounting.national_votes,
                s_i = Int(aggregate.seats),
                S = Int(accounting.national_seats),
                national_party_seat_share = Float64(aggregate.seat_share),
                district_vote_weight = row.district_votes / accounting.national_votes,
                district_seat_weight = row.district_seats / accounting.national_seats,
                district_weight_gap = row.district_seats / accounting.national_seats -
                    row.district_votes / accounting.national_votes,
                within_district_quota = Float64(row.within_quota_exact),
                national_quota_contribution = Float64(row.national_quota_contribution_exact),
                a_id = Float64(row.a_exact),
                b_id = Float64(row.b_exact),
                b_id_factored = Float64(row.b_factored_exact),
                b_crosscheck_residual = Float64(row.b_exact - row.b_factored_exact),
                d_id = Float64(cell_d_exact),
                q_i = Float64(aggregate.quota_exact),
                R_i = aggregate.R_exact === missing ? missing : Float64(aggregate.R_exact),
                d_i = Float64(aggregate.d_exact),
                A_i = Float64(aggregate.A_exact),
                B_i = Float64(aggregate.B_exact),
                within_district_quota_exact = exact_text(row.within_quota_exact),
                national_quota_contribution_exact = exact_text(row.national_quota_contribution_exact),
                a_id_exact = exact_text(row.a_exact),
                b_id_exact = exact_text(row.b_exact),
                b_id_factored_exact = exact_text(row.b_factored_exact),
                d_id_exact = exact_text(cell_d_exact),
                accounting_qualification = CD.qualification_for_year(Int(year)),
            ))
        end

        for row in eachrow(accounting.party)
            residual = row.A_exact + row.B_exact - row.d_exact
            residual == 0 || error("$(year)/$(row.party): A_i + B_i != d_i exactly.")
            push!(party_rows, (
                election_year = Int(year),
                party = String(row.party),
                v_i = Int(row.votes),
                V = Int(accounting.national_votes),
                vote_share = Float64(row.vote_share),
                s_i = Int(row.seats),
                S = Int(accounting.national_seats),
                seat_share = Float64(row.seat_share),
                q_i = Float64(row.quota_exact),
                R_i = row.R_exact === missing ? missing : Float64(row.R_exact),
                d_i = Float64(row.d_exact),
                A_i = Float64(row.A_exact),
                B_i = Float64(row.B_exact),
                A_plus_B_residual = Float64(residual),
                q_i_exact = exact_text(row.quota_exact),
                d_i_exact = exact_text(row.d_exact),
                A_i_exact = exact_text(row.A_exact),
                B_i_exact = exact_text(row.B_exact),
                accounting_qualification = CD.qualification_for_year(Int(year)),
            ))
        end

        for group in groupby(accounting.panel, :district)
            district = String(first(group.district))
            V_d = Int(first(group.district_votes))
            S_d = Int(first(group.district_seats))
            sum_a = sum(group.a_exact)
            sum_b = sum(group.b_exact)
            expected_b = CD.exact_fraction(S_d, 1) -
                CD.exact_product_ratio(accounting.national_seats, V_d, accounting.national_votes)
            sum_a == 0 || error("$(year)/$(district): sum_i a_id != 0.")
            sum_b == expected_b || error("$(year)/$(district): sum_i b_id closure failed.")
            push!(district_rows, (
                election_year = Int(year),
                electoral_unit = district,
                V_d = V_d,
                S_d = S_d,
                V = Int(accounting.national_votes),
                S = Int(accounting.national_seats),
                district_vote_weight = V_d / accounting.national_votes,
                district_seat_weight = S_d / accounting.national_seats,
                district_weight_gap = S_d / accounting.national_seats -
                    V_d / accounting.national_votes,
                seat_equivalent_weight_gap = Float64(expected_b),
                valid_votes_per_seat = V_d / S_d,
                sum_a_id = Float64(sum_a),
                sum_b_id = Float64(sum_b),
                expected_sum_b_id = Float64(expected_b),
                b_closure_residual = Float64(sum_b - expected_b),
                seat_equivalent_weight_gap_exact = exact_text(expected_b),
            ))
        end

        for (check_name, exact_value) in (
            ("complete-system sum_i A_i = 0", sum(accounting.party.A_exact)),
            ("complete-system sum_i B_i = 0", sum(accounting.party.B_exact)),
            ("complete-system sum_i d_i = 0", sum(accounting.party.d_exact)),
        )
            exact_value == 0 || error("$(year): $(check_name) failed.")
            push!(validation_rows, (
                scope = "year",
                case_id = missing,
                election_year = Int(year),
                check_name = check_name,
                exact_pass = true,
                floating_residual = Float64(exact_value),
                atol = CD.ACCOUNTING_ATOL,
                rtol = CD.ACCOUNTING_RTOL,
                status = "PASS",
            ))
        end
    end

    cells = DataFrame(cell_rows)
    parties = DataFrame(party_rows)
    districts = DataFrame(district_rows)
    validations = DataFrame(validation_rows)
    sort!(cells, [:election_year, :electoral_unit, :party])
    sort!(parties, [:election_year, :party])
    sort!(districts, [:election_year, :electoral_unit])
    nrow(cells) == EXPECTED_PARTY_DISTRICT_ROWS || error(
        "Complete all-year party-district panel has $(nrow(cells)) rows, not $(EXPECTED_PARTY_DISTRICT_ROWS).",
    )
    nrow(parties) == sum(values(EXPECTED_PARTIES_BY_YEAR)) || error(
        "Complete party accounting output has the wrong row count.",
    )
    nrow(districts) == 81 || error("Complete district accounting output must contain 81 rows.")
    return (cells = cells, parties = parties, districts = districts, validations = validations)
end

function build_inversion_case_registry(
    coalition_periods::DataFrame,
    ideological_intervals::DataFrame,
    accounting_by_year::AbstractDict,
)
    rows = NamedTuple[]
    cabinets = coalition_periods[Bool.(coalition_periods.coalition_inversion), :]
    cabinet_keys = [
        (Int(row.election_year), String(row.cabinet_period)) for row in eachrow(cabinets)
    ]
    cabinet_keys == CD.EXPECTED_INVERSION_KEYS || error(
        "Cabinet inversion registry changed: $(cabinet_keys).",
    )
    for (case_order, row) in enumerate(eachrow(cabinets))
        year = Int(row.election_year)
        parties = ordered_parties(row.coalition_parties)
        push!(rows, (
            case_id = "cabinet/$(row.coalition_id)",
            source_case_id = String(row.coalition_id),
            case_domain = "cabinet",
            ideological_universe = "not_applicable",
            k = missing,
            gap_count = missing,
            case_order = case_order,
            election_year = year,
            case_label = String(row.cabinet_period),
            cabinet_period = String(row.cabinet_period),
            source_periods = String(row.source_periods),
            period_start = row.period_start,
            period_end = row.period_end,
            period_days = Int(row.period_days),
            ideology_start_index = missing,
            ideology_end_index = missing,
            start_party = missing,
            end_party = missing,
            coalition_parties = String(row.coalition_parties),
            coalition_party_count = length(parties),
            minimal_inversion = missing,
            minimal_status = "not applicable",
            observed_coalition = true,
            synthetic_ideological_interval = false,
            v_C = Int(row.v_C),
            V = Int(row.V),
            vote_share = Float64(row.vote_share),
            s_C = Int(row.s_C),
            S = Int(row.S),
            seat_share = Float64(row.seat_share),
            q_C = Float64(row.q_C),
            d_C = Float64(row.d_C),
            r_C = Float64(row.r_C),
            R_C = Float64(row.R_C),
            source_registry = "PSC-correct observed cabinet reconstruction",
        ))
    end

    ideological = ideological_intervals[Bool.(ideological_intervals.coalition_inversion), :]
    for (case_order, row) in enumerate(eachrow(ideological))
        year = Int(row.election_year)
        accounting = accounting_by_year[year]
        parties = ordered_parties(row.parties)
        Int(row.interval_size) == length(parties) || error(
            "$(year)/$(row.start_index)-$(row.end_index): interval size differs from membership.",
        )
        minimal = Bool(row.minimal_connected_inversion)
        push!(rows, (
            case_id = @sprintf("ideological/%d/%02d-%02d", year, row.start_index, row.end_index),
            source_case_id = "$(year)/$(row.start_index)-$(row.end_index)",
            case_domain = "ideological",
            ideological_universe = String(row.ideological_universe),
            k = 0,
            gap_count = 0,
            case_order = case_order,
            election_year = year,
            case_label = "$(row.start_index)-$(row.end_index) $(row.start_party)-$(row.end_party)",
            cabinet_period = missing,
            source_periods = missing,
            period_start = missing,
            period_end = missing,
            period_days = missing,
            ideology_start_index = Int(row.start_index),
            ideology_end_index = Int(row.end_index),
            start_party = String(row.start_party),
            end_party = String(row.end_party),
            coalition_parties = String(row.parties),
            coalition_party_count = length(parties),
            minimal_inversion = minimal,
            minimal_status = minimal ? "endpoint-minimal" : "nonminimal",
            observed_coalition = false,
            synthetic_ideological_interval = true,
            v_C = Int(row.votes),
            V = Int(row.national_vote_total),
            vote_share = Float64(row.vote_share),
            s_C = Int(row.seats),
            S = Int(accounting.national_seats),
            seat_share = Float64(row.seat_share),
            q_C = Float64(row.quota),
            d_C = Float64(row.seat_diff),
            r_C = Float64(row.required_diff),
            R_C = Float64(row.representation_ratio),
            source_registry = "validated contiguous ideological intervals",
        ))
    end

    registry = DataFrame(rows)
    registry[!, :composition_equivalence_group] = [
        "$(row.election_year):" * join(sort(ordered_parties(row.coalition_parties)), "|") for
        row in eachrow(registry)
    ]
    group_sizes = combine(
        groupby(registry, :composition_equivalence_group),
        nrow => :composition_equivalence_count,
    )
    registry = leftjoin(registry, group_sizes; on = :composition_equivalence_group)
    registry[!, :compositionally_repeated] = registry.composition_equivalence_count .> 1
    sort!(registry, [:case_domain, :election_year, :case_order])

    nrow(registry) == nrow(cabinets) + nrow(ideological) || error("Combined registry cardinality mismatch.")
    sum(registry.case_domain .== "cabinet") == 4 || error("Combined registry lost cabinet cases.")
    all(registry.vote_share .< 0.5) && all(registry.s_C .>= 257) || error("Registry contains a non-inversion.")
    repeated = registry[registry.compositionally_repeated, :]
    require(
        isempty(repeated),
        "Composition-equivalence audit changed; cabinet observations are already coalesced.",
    )
    return registry
end

function case_metadata(row)
    return (
        case_id = String(row.case_id),
        source_case_id = String(row.source_case_id),
        case_domain = String(row.case_domain),
        ideological_universe = String(row.ideological_universe),
        k = row.k,
        gap_count = row.gap_count,
        case_order = Int(row.case_order),
        election_year = Int(row.election_year),
        case_label = String(row.case_label),
        cabinet_period = row.cabinet_period,
        source_periods = row.source_periods,
        period_start = row.period_start,
        period_end = row.period_end,
        period_days = row.period_days,
        ideology_start_index = row.ideology_start_index,
        ideology_end_index = row.ideology_end_index,
        start_party = row.start_party,
        end_party = row.end_party,
        coalition_parties = String(row.coalition_parties),
        coalition_party_count = Int(row.coalition_party_count),
        minimal_inversion = row.minimal_inversion,
        minimal_status = String(row.minimal_status),
        observed_coalition = Bool(row.observed_coalition),
        synthetic_ideological_interval = Bool(row.synthetic_ideological_interval),
        composition_equivalence_group = String(row.composition_equivalence_group),
        composition_equivalence_count = Int(row.composition_equivalence_count),
        compositionally_repeated = Bool(row.compositionally_repeated),
    )
end

"""
    decompose_case_registry(registry, accounting_by_year)

Apply the same exact party/district accounting to all four cabinet and all
registered ideological inversion coalitions. Ideological membership is read
directly from the validated interval output; no coalition is reconstructed by
hand.
"""
function decompose_case_registry(registry::DataFrame, accounting_by_year::AbstractDict)
    decomposition_rows = NamedTuple[]
    party_rows = NamedTuple[]
    district_rows = NamedTuple[]
    party_district_rows = NamedTuple[]
    validation_rows = NamedTuple[]

    for case in eachrow(registry)
        metadata = case_metadata(case)
        year = Int(case.election_year)
        accounting = accounting_by_year[year]
        ordered = ordered_parties(case.coalition_parties)
        member_set = Set(ordered)
        available = Set(String.(accounting.party.party))
        missing_parties = setdiff(member_set, available)
        isempty(missing_parties) || error(
            "$(case.case_id): parties absent from accounting panel: " *
            join(sort(collect(missing_parties)), ", "),
        )
        case.vote_share < 0.5 || error("$(case.case_id): inversion is not below 50% of votes.")
        case.s_C >= accounting.seat_majority_threshold || error(
            "$(case.case_id): inversion does not reach the seat-majority threshold.",
        )

        party_lookup = Dict(String(row.party) => row for row in eachrow(accounting.party))
        member_rows = [party_lookup[party] for party in ordered]
        v_C = sum(Int(row.votes) for row in member_rows)
        s_C = sum(Int(row.seats) for row in member_rows)
        q_exact = CD.exact_product_ratio(accounting.national_seats, v_C, accounting.national_votes)
        d_exact = CD.exact_fraction(s_C, 1) - q_exact
        r_exact = CD.exact_fraction(accounting.seat_majority_threshold, 1) - q_exact
        R_value = s_C / Float64(q_exact)

        v_C == Int(case.v_C) || error("$(case.case_id): coalition votes differ from source registry.")
        s_C == Int(case.s_C) || error("$(case.case_id): coalition seats differ from source registry.")
        accounting.national_votes == Int(case.V) || error(
            "$(case.case_id): national vote denominator differs from source registry.",
        )
        accounting.national_seats == Int(case.S) || error(
            "$(case.case_id): national seat total differs from source registry.",
        )
        require_approx(v_C / accounting.national_votes, case.vote_share, "$(case.case_id) vote share")
        require_approx(s_C / accounting.national_seats, case.seat_share, "$(case.case_id) seat share")
        require_approx(q_exact, case.q_C, "$(case.case_id) q_C")
        require_approx(d_exact, case.d_C, "$(case.case_id) d_C")
        require_approx(r_exact, case.r_C, "$(case.case_id) r_C")
        require_approx(R_value, case.R_C, "$(case.case_id) R_C")

        A_exact = sum(row.A_exact for row in member_rows)
        B_exact = sum(row.B_exact for row in member_rows)
        party_d_exact = sum(row.d_exact for row in member_rows)
        A_exact + B_exact == d_exact || error("$(case.case_id): A_C + B_C != d_C exactly.")
        party_d_exact == d_exact || error("$(case.case_id): sum_i d_i != d_C exactly.")

        case_district_A = CD.Rat(0)
        case_district_B = CD.Rat(0)
        districts = sort(unique(String.(accounting.panel.district)))
        for district in districts
            values = CD.exact_coalition_district(accounting, sort(collect(member_set)), district)
            case_district_A += values.a_exact
            case_district_B += values.b_exact
            push!(district_rows, merge(metadata, (
                electoral_unit = district,
                v_Cd = values.v_Cd,
                V_d = values.V_d,
                district_coalition_vote_share = values.v_Cd / values.V_d,
                s_Cd = values.s_Cd,
                S_d = values.S_d,
                district_coalition_seat_share = values.s_Cd / values.S_d,
                within_district_quota = Float64(values.within_quota_exact),
                national_quota_contribution = Float64(values.national_contribution_exact),
                a_Cd = Float64(values.a_exact),
                b_Cd = Float64(values.b_exact),
                b_Cd_factored = Float64(values.b_factored_exact),
                b_crosscheck_residual = Float64(values.b_exact - values.b_factored_exact),
                d_Cd = Float64(values.a_exact + values.b_exact),
                a_Cd_exact = exact_text(values.a_exact),
                b_Cd_exact = exact_text(values.b_exact),
                d_Cd_exact = exact_text(values.a_exact + values.b_exact),
            )))
        end
        case_district_A == A_exact || error("$(case.case_id): sum_d a_Cd != A_C exactly.")
        case_district_B == B_exact || error("$(case.case_id): sum_d b_Cd != B_C exactly.")

        linked_A = CD.Rat(0)
        linked_B = CD.Rat(0)
        for (party_order, party_name) in enumerate(ordered)
            member = party_lookup[party_name]
            push!(party_rows, merge(metadata, (
                coalition_party_order = party_order,
                party = party_name,
                v_i = Int(member.votes),
                V = Int(accounting.national_votes),
                vote_share = Float64(member.vote_share),
                s_i = Int(member.seats),
                S = Int(accounting.national_seats),
                seat_share = Float64(member.seat_share),
                q_i = Float64(member.quota_exact),
                R_i = member.R_exact === missing ? missing : Float64(member.R_exact),
                d_i = Float64(member.d_exact),
                A_i = Float64(member.A_exact),
                B_i = Float64(member.B_exact),
                A_plus_B_residual = Float64(member.A_exact + member.B_exact - member.d_exact),
                q_times_R_minus_1 = member.R_exact === missing ? missing :
                    Float64(member.quota_exact * (member.R_exact - CD.exact_fraction(1, 1))),
                accounting_qualification = CD.qualification_for_year(year),
            )))

            member_cells = accounting.panel[accounting.panel.party .== party_name, :]
            nrow(member_cells) == 27 || error("$(case.case_id)/$(party_name): expected 27 cells.")
            for cell in eachrow(member_cells)
                d_cell_exact = cell.a_exact + cell.b_exact
                linked_A += cell.a_exact
                linked_B += cell.b_exact
                push!(party_district_rows, merge(metadata, (
                    coalition_party_order = party_order,
                    party = party_name,
                    electoral_unit = String(cell.district),
                    v_id = Int(cell.votes),
                    V_d = Int(cell.district_votes),
                    district_party_vote_share = cell.votes / cell.district_votes,
                    s_id = Int(cell.seats),
                    S_d = Int(cell.district_seats),
                    district_party_seat_share = cell.seats / cell.district_seats,
                    district_vote_weight = cell.district_votes / accounting.national_votes,
                    district_seat_weight = cell.district_seats / accounting.national_seats,
                    district_weight_gap = cell.district_seats / accounting.national_seats -
                        cell.district_votes / accounting.national_votes,
                    within_district_quota = Float64(cell.within_quota_exact),
                    national_quota_contribution = Float64(cell.national_quota_contribution_exact),
                    a_id = Float64(cell.a_exact),
                    b_id = Float64(cell.b_exact),
                    b_id_factored = Float64(cell.b_factored_exact),
                    b_crosscheck_residual = Float64(cell.b_exact - cell.b_factored_exact),
                    d_id = Float64(d_cell_exact),
                    a_id_exact = exact_text(cell.a_exact),
                    b_id_exact = exact_text(cell.b_exact),
                    d_id_exact = exact_text(d_cell_exact),
                    accounting_qualification = CD.qualification_for_year(year),
                )))
            end
        end
        linked_A == A_exact || error("$(case.case_id): linked member cells do not sum to A_C.")
        linked_B == B_exact || error("$(case.case_id): linked member cells do not sum to B_C.")

        push!(decomposition_rows, merge(metadata, (
            v_C = v_C,
            V = Int(accounting.national_votes),
            vote_share = v_C / accounting.national_votes,
            vote_share_pct = 100 * v_C / accounting.national_votes,
            s_C = s_C,
            S = Int(accounting.national_seats),
            seat_share = s_C / accounting.national_seats,
            q_C = Float64(q_exact),
            d_C = Float64(d_exact),
            r_C = Float64(r_exact),
            R_C = Float64(R_value),
            A_C = Float64(A_exact),
            B_C = Float64(B_exact),
            A_share_of_d_C = Float64(A_exact / d_exact),
            B_share_of_d_C = Float64(B_exact / d_exact),
            dominant_absolute_component = abs(A_exact) >= abs(B_exact) ? "A_C" : "B_C",
            component_sign_pattern = sign_pattern(A_exact, B_exact),
            A_plus_B_residual = Float64(A_exact + B_exact - d_exact),
            party_d_residual = Float64(party_d_exact - d_exact),
            district_A_residual = Float64(case_district_A - A_exact),
            district_B_residual = Float64(case_district_B - B_exact),
            linked_cell_A_residual = Float64(linked_A - A_exact),
            linked_cell_B_residual = Float64(linked_B - B_exact),
            q_C_exact = exact_text(q_exact),
            d_C_exact = exact_text(d_exact),
            r_C_exact = exact_text(r_exact),
            A_C_exact = exact_text(A_exact),
            B_C_exact = exact_text(B_exact),
            accounting_qualification = CD.qualification_for_year(year),
            interpretation = "accounting identity; B combines district seat weights, valid-vote weights, and coalition vote geography",
        )))

        checks = (
            ("A_C + B_C = d_C", A_exact + B_exact - d_exact),
            ("sum_i d_i = d_C", party_d_exact - d_exact),
            ("sum_i A_i = A_C", sum(row.A_exact for row in member_rows) - A_exact),
            ("sum_i B_i = B_C", sum(row.B_exact for row in member_rows) - B_exact),
            ("sum_d a_Cd = A_C", case_district_A - A_exact),
            ("sum_d b_Cd = B_C", case_district_B - B_exact),
            ("sum_id a_id = A_C", linked_A - A_exact),
            ("sum_id b_id = B_C", linked_B - B_exact),
        )
        for (check_name, residual) in checks
            residual == 0 || error("$(case.case_id): $(check_name) failed exactly.")
            push!(validation_rows, (
                scope = "case",
                case_id = String(case.case_id),
                election_year = year,
                check_name = check_name,
                exact_pass = true,
                floating_residual = Float64(residual),
                atol = CD.ACCOUNTING_ATOL,
                rtol = CD.ACCOUNTING_RTOL,
                status = "PASS",
            ))
        end
    end

    decomposition = DataFrame(decomposition_rows)
    party_contributions = DataFrame(party_rows)
    district_contributions = DataFrame(district_rows)
    party_district_contributions = DataFrame(party_district_rows)
    validations = DataFrame(validation_rows)
    sort!(decomposition, [:case_domain, :election_year, :case_order])
    sort!(party_contributions, [:case_domain, :election_year, :case_order, :coalition_party_order])
    sort!(district_contributions, [:case_domain, :election_year, :case_order, :electoral_unit])
    sort!(party_district_contributions,
        [:case_domain, :election_year, :case_order, :coalition_party_order, :electoral_unit])

    nrow(decomposition) == nrow(registry) || error("Case decomposition lost registry rows.")
    nrow(party_contributions) == sum(registry.coalition_party_count) || error("Incomplete party vectors.")
    nrow(party_district_contributions[party_district_contributions.case_domain .== "cabinet", :]) ==
        EXPECTED_CABINET_PARTY_DISTRICT_ROWS || error("Cabinet member-cell output changed.")
    nrow(party_district_contributions) == 27 * nrow(party_contributions) || error("Incomplete party-district vectors.")
    nrow(district_contributions) == nrow(registry) * 27 || error("Incomplete coalition-district vectors.")
    return (
        decomposition = decomposition,
        party_contributions = party_contributions,
        district_contributions = district_contributions,
        party_district_contributions = party_district_contributions,
        validations = validations,
    )
end


function _reference_frame(reference, name::Symbol)
    reference isa DataFrame && name == :decomposition && return reference
    hasproperty(reference, name) || error(
        "Cabinet compatibility reference has no $(name) table.",
    )
    return getproperty(reference, name)
end

"""
    validate_cabinet_compatibility!(cases, cabinet_reference)

Require the cabinet slice of the expanded registry-derived accounting to reproduce
the already validated four-case decomposition. Failure stops report generation;
success returns `true`.
"""
function validate_cabinet_compatibility!(cases, cabinet_reference)
    expanded = cases.decomposition[cases.decomposition.case_domain .== "cabinet", :]
    reference = _reference_frame(cabinet_reference, :decomposition)
    nrow(expanded) == 4 || error("Expanded cabinet decomposition must contain four cases.")
    nrow(reference) == 4 || error("Validated cabinet reference must contain four cases.")

    expanded_lookup = Dict(String(row.source_case_id) => row for row in eachrow(expanded))
    reference_lookup = Dict(String(row.coalition_id) => row for row in eachrow(reference))
    Set(keys(expanded_lookup)) == Set(keys(reference_lookup)) || error(
        "Expanded and validated cabinet decomposition case registries differ.",
    )
    for case_id in sort(collect(keys(reference_lookup)))
        left = expanded_lookup[case_id]
        right = reference_lookup[case_id]
        sort(ordered_parties(left.coalition_parties)) ==
            sort(ordered_parties(right.coalition_parties)) || error(
                "$(case_id): cabinet composition differs from validated decomposition.",
            )
        Int(left.s_C) == Int(right.s_C) || error("$(case_id): cabinet seats differ.")
        for column in (:vote_share, :q_C, :d_C, :r_C, :R_C, :A_C, :B_C)
            require_approx(left[column], right[column], "$(case_id) cabinet $(column)")
        end
    end

    if hasproperty(cabinet_reference, :party_contributions)
        expanded_party = cases.party_contributions[
            cases.party_contributions.case_domain .== "cabinet", :,
        ]
        reference_party = _reference_frame(cabinet_reference, :party_contributions)
        nrow(expanded_party) == nrow(reference_party) || error(
            "Expanded and validated cabinet party outputs have different row counts.",
        )
        left_lookup = Dict(
            (String(row.source_case_id), String(row.party)) => row for
            row in eachrow(expanded_party)
        )
        right_lookup = Dict(
            (String(row.coalition_id), String(row.party)) => row for
            row in eachrow(reference_party)
        )
        Set(keys(left_lookup)) == Set(keys(right_lookup)) || error(
            "Expanded and validated cabinet party-output keys differ.",
        )
        for key in keys(right_lookup)
            left = left_lookup[key]
            right = right_lookup[key]
            Int(left.v_i) == Int(right.v_i) || error("$(key): cabinet party votes differ.")
            Int(left.s_i) == Int(right.s_i) || error("$(key): cabinet party seats differ.")
            for column in (:q_i, :R_i, :d_i, :A_i, :B_i)
                if ismissing(left[column]) || ismissing(right[column])
                    ismissing(left[column]) && ismissing(right[column]) || error(
                        "$(key): cabinet party $(column) missingness differs.",
                    )
                else
                    require_approx(left[column], right[column], "$(key) cabinet party $(column)")
                end
            end
        end
    end

    if hasproperty(cabinet_reference, :district_contributions)
        expanded_district = cases.district_contributions[
            cases.district_contributions.case_domain .== "cabinet", :,
        ]
        reference_district = _reference_frame(cabinet_reference, :district_contributions)
        nrow(expanded_district) == nrow(reference_district) || error(
            "Expanded and validated cabinet district outputs have different row counts.",
        )
        left_lookup = Dict(
            (String(row.source_case_id), String(row.electoral_unit)) => row for
            row in eachrow(expanded_district)
        )
        right_lookup = Dict(
            (String(row.coalition_id), String(row.electoral_unit)) => row for
            row in eachrow(reference_district)
        )
        Set(keys(left_lookup)) == Set(keys(right_lookup)) || error(
            "Expanded and validated cabinet district-output keys differ.",
        )
        for key in keys(right_lookup)
            left = left_lookup[key]
            right = right_lookup[key]
            for column in (:v_Cd, :V_d, :s_Cd, :S_d)
                Int(left[column]) == Int(right[column]) || error(
                    "$(key): cabinet district $(column) differs.",
                )
            end
            for column in (:a_Cd, :b_Cd, :b_Cd_factored, :b_crosscheck_residual)
                require_approx(left[column], right[column], "$(key) cabinet district $(column)")
            end
        end
    end
    return true
end

function _ranking_metadata(row)
    return (
        case_id = String(row.case_id), source_case_id = String(row.source_case_id),
        case_domain = String(row.case_domain), case_order = Int(row.case_order),
        election_year = Int(row.election_year), case_label = String(row.case_label),
        cabinet_period = row.cabinet_period,
        source_periods = row.source_periods,
        period_start = row.period_start,
        period_end = row.period_end,
        period_days = row.period_days,
        ideology_start_index = row.ideology_start_index,
        ideology_end_index = row.ideology_end_index,
        minimal_inversion = row.minimal_inversion,
        minimal_status = String(row.minimal_status),
    )
end

function _append_ranking_rows!(rows, source::DataFrame, level::String, components)
    for row in eachrow(source)
        metadata = _ranking_metadata(row)
        party = level in ("party", "party_district") ? String(row.party) : missing
        electoral_unit = level in ("district", "party_district") ?
            String(row.electoral_unit) : missing
        unit_label = level == "party" ? String(party) :
            level == "district" ? String(electoral_unit) : "$(party)/$(electoral_unit)"
        for (component, column) in components
            value = Float64(row[column])
            push!(rows, merge(metadata, (
                aggregation_level = level, component = component, party = party,
                electoral_unit = electoral_unit, unit_label = unit_label, value = value,
                value_sign = value > 0 ? "positive" : value < 0 ? "negative" : "zero",
            )))
        end
    end
    return rows
end

"""
    build_case_rankings(cases)

Build complete deterministic rankings for party, district, and party--district
A/B/d contributions. Exact ties are broken by the displayed unit identifier.
"""
function build_case_rankings(cases)
    rows = NamedTuple[]
    _append_ranking_rows!(rows, cases.party_contributions, "party",
        (("A_i", :A_i), ("B_i", :B_i), ("d_i", :d_i)))
    _append_ranking_rows!(rows, cases.district_contributions, "district",
        (("a_Cd", :a_Cd), ("b_Cd", :b_Cd), ("d_Cd", :d_Cd)))
    _append_ranking_rows!(rows, cases.party_district_contributions, "party_district",
        (("a_id", :a_id), ("b_id", :b_id), ("d_id", :d_id)))
    rankings = DataFrame(rows)
    n = nrow(rankings)
    descending_rank = zeros(Int, n)
    ascending_rank = zeros(Int, n)
    absolute_rank = zeros(Int, n)
    positive_rank = Vector{Union{Missing,Int}}(missing, n)
    negative_rank = Vector{Union{Missing,Int}}(missing, n)

    for group in groupby(rankings, [:case_id, :aggregation_level, :component])
        indices = collect(parentindices(group)[1])
        descending = sort(indices; by = i -> (-rankings.value[i], rankings.unit_label[i]))
        ascending = sort(indices; by = i -> (rankings.value[i], rankings.unit_label[i]))
        absolute = sort(indices; by = i ->
            (-abs(rankings.value[i]), -rankings.value[i], rankings.unit_label[i]))
        for (rank, index) in enumerate(descending)
            descending_rank[index] = rank
        end
        for (rank, index) in enumerate(ascending)
            ascending_rank[index] = rank
        end
        for (rank, index) in enumerate(absolute)
            absolute_rank[index] = rank
        end
        for (rank, index) in enumerate(filter(i -> rankings.value[i] > 0, descending))
            positive_rank[index] = rank
        end
        for (rank, index) in enumerate(filter(i -> rankings.value[i] < 0, ascending))
            negative_rank[index] = rank
        end
    end
    rankings[!, :descending_rank] = descending_rank
    rankings[!, :ascending_rank] = ascending_rank
    rankings[!, :absolute_rank] = absolute_rank
    rankings[!, :positive_rank] = positive_rank
    rankings[!, :negative_rank] = negative_rank

    expected_rows = 3 * (nrow(cases.party_contributions) +
        nrow(cases.district_contributions) + nrow(cases.party_district_contributions))
    nrow(rankings) == expected_rows || error("Full contribution ranking row count changed.")
    all(rankings.descending_rank .> 0) || error("A descending rank was not assigned.")
    all(rankings.ascending_rank .> 0) || error("An ascending rank was not assigned.")
    all(rankings.absolute_rank .> 0) || error("An absolute rank was not assigned.")

    level_order = Dict("party" => 1, "district" => 2, "party_district" => 3)
    component_order = Dict("A_i" => 1, "B_i" => 2, "d_i" => 3,
        "a_Cd" => 1, "b_Cd" => 2, "d_Cd" => 3,
        "a_id" => 1, "b_id" => 2, "d_id" => 3)
    rankings[!, :_level_order] = [level_order[String(x)] for x in rankings.aggregation_level]
    rankings[!, :_component_order] = [component_order[String(x)] for x in rankings.component]
    sort!(rankings, [:case_domain, :election_year, :case_order, :_level_order,
        :_component_order, :descending_rank])
    select!(rankings, Not([:_level_order, :_component_order]))
    return rankings
end

function sha256_file(path::AbstractString)
    return open(path, "r") do io
        bytes2hex(SHA.sha256(io))
    end
end

function write_csv_file(path::AbstractString, data::DataFrame)
    mkpath(dirname(path))
    CSV.write(path, data; quotestrings = true)
    return path
end

function reload_csv(path::AbstractString)
    data = CSV.read(path, DataFrame)
    for column in (
        :case_id, :source_case_id, :case_domain, :case_label, :cabinet_period,
        :minimal_status, :party, :electoral_unit, :aggregation_level, :component,
        :unit_label, :value_sign, :coalition_parties,
    )
        column in propertynames(data) || continue
        data[!, column] = [ismissing(value) ? missing : string(value) for value in data[!, column]]
    end
    return data
end

function display_case(row)
    domain = String(row.case_domain) == "cabinet" ? "Cabinet" : "Ideological"
    return "$(domain) $(row.election_year)/$(row.case_label)"
end

function build_year_closure_table(cells::DataFrame, parties::DataFrame, districts::DataFrame)
    rows = NamedTuple[]
    for year in sort(unique(Int.(cells.election_year)))
        cell = cells[Int.(cells.election_year) .== year, :]
        party = parties[Int.(parties.election_year) .== year, :]
        district = districts[Int.(districts.election_year) .== year, :]
        push!(rows, (
            election_year = year,
            parties = nrow(party),
            electoral_units = nrow(district),
            party_district_cells = nrow(cell),
            V = only(unique(Int.(cell.V))),
            S = only(unique(Int.(cell.S))),
            sum_A_i = sum(Float64.(party.A_i)),
            sum_B_i = sum(Float64.(party.B_i)),
            sum_d_i = sum(Float64.(party.d_i)),
            max_abs_b_crosscheck_residual = maximum(abs.(Float64.(cell.b_crosscheck_residual))),
            max_abs_district_b_closure_residual = maximum(abs.(Float64.(district.b_closure_residual))),
            status = "PASS",
        ))
    end
    return DataFrame(rows)
end

function build_district_weight_extremes_table(districts::DataFrame; count::Int = 3)
    rows = NamedTuple[]
    for year in sort(unique(Int.(districts.election_year)))
        selected = districts[Int.(districts.election_year) .== year, :]
        high = sort(selected, [:district_weight_gap, :electoral_unit]; rev = [true, false])
        low = sort(selected, [:district_weight_gap, :electoral_unit]; rev = [false, false])
        for (direction, data) in (("largest positive", high), ("most negative", low))
            for rank in 1:min(count, nrow(data))
                row = data[rank, :]
                push!(rows, (
                    election_year = year, direction = direction, rank = rank,
                    electoral_unit = String(row.electoral_unit), V_d = Int(row.V_d),
                    S_d = Int(row.S_d), district_vote_weight = Float64(row.district_vote_weight),
                    district_seat_weight = Float64(row.district_seat_weight),
                    district_weight_gap = Float64(row.district_weight_gap),
                    seat_equivalent_weight_gap = Float64(row.seat_equivalent_weight_gap),
                ))
            end
        end
    end
    return DataFrame(rows)
end

function build_registry_table(registry::DataFrame)
    rows = NamedTuple[]
    for row in eachrow(registry)
        push!(rows, (
            case_id = String(row.case_id), case_domain = String(row.case_domain),
            election_year = Int(row.election_year), case_order = Int(row.case_order),
            case_label = String(row.case_label), case_display = display_case(row),
            coalition_party_count = Int(row.coalition_party_count),
            coalition_parties = String(row.coalition_parties),
            minimal_status = String(row.minimal_status), vote_share = Float64(row.vote_share),
            s_C = Int(row.s_C), compositionally_repeated = Bool(row.compositionally_repeated),
            source_registry = String(row.source_registry),
        ))
    end
    return DataFrame(rows)
end

function build_decomposition_table(decomposition::DataFrame)
    rows = NamedTuple[]
    for row in eachrow(decomposition)
        push!(rows, (
            case_id = String(row.case_id), case_domain = String(row.case_domain),
            election_year = Int(row.election_year), case_order = Int(row.case_order),
            case_label = String(row.case_label), case_display = display_case(row),
            minimal_status = String(row.minimal_status), vote_share = Float64(row.vote_share),
            s_C = Int(row.s_C), q_C = Float64(row.q_C), d_C = Float64(row.d_C),
            r_C = Float64(row.r_C), R_C = Float64(row.R_C), A_C = Float64(row.A_C),
            B_C = Float64(row.B_C), A_share_of_d_C = Float64(row.A_share_of_d_C),
            B_share_of_d_C = Float64(row.B_share_of_d_C),
            dominant_absolute_component = String(row.dominant_absolute_component),
            component_sign_pattern = String(row.component_sign_pattern),
        ))
    end
    return DataFrame(rows)
end

function rank_extreme(rankings::DataFrame, case_id, level, component, rank_column)
    mask = (String.(rankings.case_id) .== String(case_id)) .&
        (String.(rankings.aggregation_level) .== String(level)) .&
        (String.(rankings.component) .== String(component))
    candidates = rankings[mask, :]
    values = candidates[!, rank_column]
    selected = candidates[coalesce.(values .== 1, false), :]
    nrow(selected) <= 1 || error(
        "$(case_id)/$(level)/$(component): rank $(rank_column)=1 is not unique.",
    )
    return nrow(selected) == 0 ? nothing : selected[1, :]
end

function build_component_extremes_table(decomposition::DataFrame, rankings::DataFrame)
    rows = NamedTuple[]
    specs = (
        ("party", "A_i"), ("party", "B_i"), ("party", "d_i"),
        ("district", "a_Cd"), ("district", "b_Cd"), ("district", "d_Cd"),
    )
    for case in eachrow(decomposition)
        for (level, component) in specs
            positive = rank_extreme(rankings, case.case_id, level, component, :positive_rank)
            negative = rank_extreme(rankings, case.case_id, level, component, :negative_rank)
            push!(rows, (
                case_id = String(case.case_id), case_domain = String(case.case_domain),
                election_year = Int(case.election_year), case_order = Int(case.case_order),
                case_label = String(case.case_label), case_display = display_case(case),
                aggregation_level = level, component = component,
                largest_positive_unit = positive === nothing ? missing : String(positive.unit_label),
                largest_positive_value = positive === nothing ? missing : Float64(positive.value),
                largest_negative_unit = negative === nothing ? missing : String(negative.unit_label),
                largest_negative_value = negative === nothing ? missing : Float64(negative.value),
            ))
        end
    end
    return DataFrame(rows)
end

function build_party_vectors_table(parties::DataFrame)
    rows = NamedTuple[]
    for row in eachrow(parties)
        push!(rows, (
            case_id = String(row.case_id), case_domain = String(row.case_domain),
            election_year = Int(row.election_year), case_order = Int(row.case_order),
            case_label = String(row.case_label), case_display = display_case(row),
            coalition_party_order = Int(row.coalition_party_order), party = String(row.party),
            v_i = Int(row.v_i), s_i = Int(row.s_i), q_i = Float64(row.q_i),
            R_i = ismissing(row.R_i) ? missing : Float64(row.R_i), d_i = Float64(row.d_i),
            A_i = Float64(row.A_i), B_i = Float64(row.B_i),
            accounting_qualification = String(row.accounting_qualification),
        ))
    end
    return DataFrame(rows)
end

function build_district_vectors_table(districts::DataFrame)
    rows = NamedTuple[]
    for row in eachrow(districts)
        push!(rows, (
            case_id = String(row.case_id), case_domain = String(row.case_domain),
            election_year = Int(row.election_year), case_order = Int(row.case_order),
            case_label = String(row.case_label), case_display = display_case(row),
            electoral_unit = String(row.electoral_unit), v_Cd = Int(row.v_Cd),
            V_d = Int(row.V_d), s_Cd = Int(row.s_Cd), S_d = Int(row.S_d),
            a_Cd = Float64(row.a_Cd), b_Cd = Float64(row.b_Cd), d_Cd = Float64(row.d_Cd),
        ))
    end
    return DataFrame(rows)
end

function build_cell_extremes_table(decomposition::DataFrame, rankings::DataFrame)
    rows = NamedTuple[]
    for case in eachrow(decomposition), component in ("a_id", "b_id", "d_id")
        positive = rank_extreme(rankings, case.case_id, "party_district", component, :positive_rank)
        negative = rank_extreme(rankings, case.case_id, "party_district", component, :negative_rank)
        push!(rows, (
            case_id = String(case.case_id), case_domain = String(case.case_domain),
            election_year = Int(case.election_year), case_order = Int(case.case_order),
            case_label = String(case.case_label), case_display = display_case(case),
            component = component,
            largest_positive_party = positive === nothing ? missing : String(positive.party),
            largest_positive_electoral_unit = positive === nothing ? missing : String(positive.electoral_unit),
            largest_positive_value = positive === nothing ? missing : Float64(positive.value),
            largest_negative_party = negative === nothing ? missing : String(negative.party),
            largest_negative_electoral_unit = negative === nothing ? missing : String(negative.electoral_unit),
            largest_negative_value = negative === nothing ? missing : Float64(negative.value),
        ))
    end
    return DataFrame(rows)
end

function build_interpretation_source(decomposition::DataFrame, rankings::DataFrame)
    rows = NamedTuple[]
    for case in eachrow(decomposition)
        party_positive = rank_extreme(rankings, case.case_id, "party", "d_i", :positive_rank)
        party_negative = rank_extreme(rankings, case.case_id, "party", "d_i", :negative_rank)
        district_A = rank_extreme(rankings, case.case_id, "district", "a_Cd", :absolute_rank)
        district_B = rank_extreme(rankings, case.case_id, "district", "b_Cd", :absolute_rank)
        cell_A = rank_extreme(rankings, case.case_id, "party_district", "a_id", :absolute_rank)
        cell_B = rank_extreme(rankings, case.case_id, "party_district", "b_id", :absolute_rank)
        push!(rows, (
            case_id = String(case.case_id), case_domain = String(case.case_domain),
            election_year = Int(case.election_year), case_order = Int(case.case_order),
            case_label = String(case.case_label), case_display = display_case(case),
            minimal_status = String(case.minimal_status), d_C = Float64(case.d_C),
            A_C = Float64(case.A_C), B_C = Float64(case.B_C),
            component_sign_pattern = String(case.component_sign_pattern),
            largest_positive_party_d = party_positive === nothing ? missing : String(party_positive.unit_label),
            largest_positive_party_d_value = party_positive === nothing ? missing : Float64(party_positive.value),
            largest_negative_party_d = party_negative === nothing ? missing : String(party_negative.unit_label),
            largest_negative_party_d_value = party_negative === nothing ? missing : Float64(party_negative.value),
            largest_absolute_A_district = district_A === nothing ? missing : String(district_A.unit_label),
            largest_absolute_A_district_value = district_A === nothing ? missing : Float64(district_A.value),
            largest_absolute_B_district = district_B === nothing ? missing : String(district_B.unit_label),
            largest_absolute_B_district_value = district_B === nothing ? missing : Float64(district_B.value),
            largest_absolute_a_cell = cell_A === nothing ? missing : String(cell_A.unit_label),
            largest_absolute_a_cell_value = cell_A === nothing ? missing : Float64(cell_A.value),
            largest_absolute_b_cell = cell_B === nothing ? missing : String(cell_B.unit_label),
            largest_absolute_b_cell_value = cell_B === nothing ? missing : Float64(cell_B.value),
        ))
    end
    return DataFrame(rows)
end

tex(value) = ismissing(value) ? "--" : CD.latex_escape(value)
texint(value) = ismissing(value) ? "--" : string(Int(value))
texnum(value) = ismissing(value) ? "--" : fmt3(value)
texnum2(value) = ismissing(value) ? "--" : fmt2(value)
texshare(value) = ismissing(value) ? "--" : fmtpct(value)

function longtable_latex(
    data::DataFrame;
    caption::String,
    label::String,
    column_spec::String,
    headers,
    renderers,
    landscape::Bool = false,
    font_size::String = "scriptsize",
    notes::String = "",
)
    length(headers) == length(renderers) || error("LaTeX header/renderer lengths differ.")
    io = IOBuffer()
    landscape && println(io, "\\begin{landscape}")
    println(io, "\\begingroup")
    println(io, "\\$(font_size)")
    println(io, "\\setlength{\\tabcolsep}{3.2pt}")
    println(io, "\\begin{longtable}{$(column_spec)}")
    println(io, "\\caption{$(caption)}\\label{$(label)} \\\\")
    println(io, "\\toprule")
    println(io, join(headers, " & ") * " \\\\")
    println(io, "\\midrule")
    println(io, "\\endfirsthead")
    println(io, "\\multicolumn{$(length(headers))}{l}{\\footnotesize $(caption) (continued)} \\\\")
    println(io, "\\toprule")
    println(io, join(headers, " & ") * " \\\\")
    println(io, "\\midrule")
    println(io, "\\endhead")
    println(io, "\\midrule")
    println(io, "\\multicolumn{$(length(headers))}{r}{\\footnotesize Continued on next page} \\\\")
    println(io, "\\endfoot")
    println(io, "\\bottomrule")
    println(io, "\\endlastfoot")
    for row in eachrow(data)
        cells = [renderer(row) for renderer in renderers]
        println(io, join(cells, " & ") * " \\\\")
    end
    println(io, "\\end{longtable}")
    if !isempty(notes)
        println(io, "\\noindent\\footnotesize\\textit{Notes:} $(notes)")
    end
    println(io, "\\endgroup")
    landscape && println(io, "\\end{landscape}")
    return String(take!(io))
end

function generated_interpretation_latex(
    source::DataFrame,
    cells::DataFrame,
    district_weight_extremes::DataFrame,
)
    io = IOBuffer()
    cabinet = source[String.(source.case_domain) .== "cabinet", :]
    ideological = source[String.(source.case_domain) .== "ideological", :]
    election_years = sort(unique(Int.(cells.election_year)))
    println(io,
        "The complete accounting base contains \\textbf{$(nrow(cells))} party--district " *
        "cells. The linked inversion registry contains \\textbf{$(nrow(cabinet))} observed " *
        "cabinet cases and \\textbf{$(nrow(ideological))} ideological-interval cases."
    )

    zero_ideological_years = [
        year for year in election_years if
        sum(Int.(ideological.election_year) .== year) == 0 &&
        sum(Int.(cabinet.election_year) .== year) > 0
    ]
    for year in zero_ideological_years
        cabinet_count = sum(Int.(cabinet.election_year) .== year)
        println(io,
            "The absence of an ideological entry in $(year) is substantive: the validated " *
            "ideological registry contains zero inversions for that election, while the " *
            "corrected cabinet registry contains $(cabinet_count)."
        )
    end

    cabinet_positive_A = sum(Float64.(cabinet.A_C) .> 0)
    cabinet_offsets = sum((Float64.(cabinet.A_C) .> 0) .& (Float64.(cabinet.B_C) .< 0))
    if cabinet_positive_A == nrow(cabinet)
        println(io,
            "All $(nrow(cabinet)) cabinet cases have positive \\(A_C\\). In " *
            "$(cabinet_offsets) of $(nrow(cabinet)), negative \\(B_C\\) offsets part of " *
            "that within-district accounting advantage; in the remaining " *
            "$(nrow(cabinet) - cabinet_offsets), the two components reinforce one another."
        )
    else
        println(io,
            "Among the $(nrow(cabinet)) cabinet cases, \\(A_C\\) is positive in " *
            "$(cabinet_positive_A), and negative \\(B_C\\) offsets positive \\(A_C\\) " *
            "in $(cabinet_offsets)."
        )
    end

    for year in sort(unique(Int.(ideological.election_year)))
        data = ideological[Int.(ideological.election_year) .== year, :]
        positive_A = sum(Float64.(data.A_C) .> 0)
        negative_B = sum(Float64.(data.B_C) .< 0)
        positive_B = sum(Float64.(data.B_C) .> 0)
        if positive_A == nrow(data)
            println(io,
                "All $(nrow(data)) ideological inversions in $(year) have positive " *
                "\\(A_C\\); \\(B_C\\) is negative and offsets part of \\(A_C\\) in " *
                "$(negative_B) of $(nrow(data))."
            )
        else
            println(io,
                "Among the $(nrow(data)) ideological inversions in $(year), \\(A_C\\) is " *
                "positive in $(positive_A) and negative in $(nrow(data) - positive_A)."
            )
        end
        if positive_B == nrow(data)
            println(io,
                "For $(year), \\(B_C\\) is positive in all $(nrow(data)) ideological " *
                "inversions."
            )
        end
        overcoming = data[
            (Float64.(data.A_C) .< 0) .& (Float64.(data.B_C) .> 0) .&
            (Float64.(data.d_C) .> 0),
            :,
        ]
        if nrow(overcoming) == 1
            exceptional = only(eachrow(overcoming))
            println(io,
                "The $(tex(exceptional.case_label)) interval is the exceptional $(year) " *
                "case with \\(A_C<0\\) and \\(B_C>0\\): the between-district weighting " *
                "component more than overcomes the negative within-district component."
            )
        elseif nrow(overcoming) > 1
            labels = join(tex.(String.(overcoming.case_label)), ", ")
            println(io,
                "The $(labels) intervals have \\(A_C<0\\) and \\(B_C>0\\), with the " *
                "between-district component more than overcoming the within-district deficit."
            )
        end
    end

    positive_units = Dict{Int,String}()
    negative_units = Dict{Int,String}()
    for year in election_years
        high = district_weight_extremes[
            (Int.(district_weight_extremes.election_year) .== year) .&
            (String.(district_weight_extremes.direction) .== "largest positive") .&
            (Int.(district_weight_extremes.rank) .== 1),
            :,
        ]
        low = district_weight_extremes[
            (Int.(district_weight_extremes.election_year) .== year) .&
            (String.(district_weight_extremes.direction) .== "most negative") .&
            (Int.(district_weight_extremes.rank) .== 1),
            :,
        ]
        nrow(high) == 1 || error("$(year): expected one maximum positive district weight gap.")
        nrow(low) == 1 || error("$(year): expected one most negative district weight gap.")
        positive_units[year] = String(high.electoral_unit[1])
        negative_units[year] = String(low.electoral_unit[1])
    end
    year_text = join(string.(election_years), ", ")
    if length(unique(values(positive_units))) == 1 && length(unique(values(negative_units))) == 1
        positive_unit = only(unique(values(positive_units)))
        negative_unit = only(unique(values(negative_units)))
        println(io,
            "The district-weight audit shows a recurring geographic contrast: " *
            "$(tex(positive_unit)) has the maximum positive seat--valid-vote weight gap, and " *
            "$(tex(negative_unit)) the most negative gap, in every election ($(year_text)). " *
            "By the factored identity for \\(b_{id}\\), positive-vote cells in the first " *
            "unit have positive \\(b_{id}\\), while those in the second have negative " *
            "\\(b_{id}\\); the magnitude still depends on each party's local vote share."
        )
    else
        details = join([
            "$(year): $(tex(positive_units[year])) / $(tex(negative_units[year]))" for
            year in election_years
        ], "; ")
        println(io,
            "The maximum positive and most negative district weight gaps vary by election " *
            "(positive / negative: $(details)). These signs structure \\(b_{id}\\), while " *
            "party vote geography determines its magnitude."
        )
    end

    duplicate_groups = [
        group for group in groupby(cabinet, [:election_year, :A_C, :B_C, :d_C]) if nrow(group) > 1
    ]
    for group in duplicate_groups
        labels = join(tex.(String.(group.case_label)), " and ")
        println(io,
            "The $(labels) cabinet periods share the same accounting vector. They remain " *
            "separate cabinet-period observations, but their duplicated numerical " *
            "decomposition is not independent evidence."
        )
    end

    println(io, "\\begin{description}[style=nextline,leftmargin=3em,labelindent=0pt]")
    for row in eachrow(source)
        positive = ismissing(row.largest_positive_party_d) ? "none" :
            "$(tex(row.largest_positive_party_d)) ($(fmt2(row.largest_positive_party_d_value)))"
        negative = ismissing(row.largest_negative_party_d) ? "none" :
            "$(tex(row.largest_negative_party_d)) ($(fmt2(row.largest_negative_party_d_value)))"
        println(io, "\\item[\\textbf{$(tex(row.case_display))}]")
        println(io,
            "The differential is \\(d_C=$(fmt2(row.d_C))\\), with " *
            "\\(A_C=$(fmt2(row.A_C))\\) and \\(B_C=$(fmt2(row.B_C))\\): " *
            "$(tex(row.component_sign_pattern)). The largest positive and negative party " *
            "\\(d_i\\) entries are $(positive) and $(negative). The largest absolute " *
            "district entries are $(tex(row.largest_absolute_A_district)) for \\(a_{Cd}\\) " *
            "($(fmt2(row.largest_absolute_A_district_value))) and " *
            "$(tex(row.largest_absolute_B_district)) for \\(b_{Cd}\\) " *
            "($(fmt2(row.largest_absolute_B_district_value))). At party--district level, " *
            "the largest absolute cells are $(tex(row.largest_absolute_a_cell)) for " *
            "\\(a_{id}\\) ($(fmt2(row.largest_absolute_a_cell_value))) and " *
            "$(tex(row.largest_absolute_b_cell)) for \\(b_{id}\\) " *
            "($(fmt2(row.largest_absolute_b_cell_value)))."
        )
    end
    println(io, "\\end{description}")
    println(io,
        "The complete vectors below are required to judge concentration and offsetting. " *
        "An extreme cell is a lead for substantive investigation, not evidence that the " *
        "identified party or district caused the inversion."
    )
    return String(take!(io))
end

function year_closure_latex(data::DataFrame)
    return longtable_latex(data;
        caption = "Election-year accounting base and closure",
        label = "tab:intermediate-year-closure", column_spec = "rrrrrrrrrrl",
        headers = ("Year", "Parties", "Units", "Cells", "\\(V\\)", "\\(S\\)",
            "\\(\\sum A_i\\)", "\\(\\sum B_i\\)", "\\(\\sum d_i\\)",
            "Max residual", "Status"),
        renderers = (r -> texint(r.election_year), r -> texint(r.parties),
            r -> texint(r.electoral_units), r -> texint(r.party_district_cells),
            r -> texint(r.V), r -> texint(r.S), r -> texnum(r.sum_A_i),
            r -> texnum(r.sum_B_i), r -> texnum(r.sum_d_i),
            r -> texnum(r.max_abs_b_crosscheck_residual), r -> tex(r.status)),
        notes = "All complete-system sums are exact zero internally. Displayed residuals are decimal audit products.")
end

function district_weight_latex(data::DataFrame)
    return longtable_latex(data;
        caption = "District seat--valid-vote weight extremes",
        label = "tab:intermediate-district-weight", column_spec = "llrlrrrrr",
        headers = ("Year", "Direction", "Rank", "Unit", "\\(V_d\\)", "\\(S_d\\)",
            "Vote wt.", "Seat wt.", "Seat-eq. gap"),
        renderers = (r -> texint(r.election_year), r -> tex(r.direction), r -> texint(r.rank),
            r -> tex(r.electoral_unit), r -> texint(r.V_d), r -> texint(r.S_d),
            r -> texnum(r.district_vote_weight), r -> texnum(r.district_seat_weight),
            r -> texnum(r.seat_equivalent_weight_gap)),
        notes = "The seat-equivalent gap is \\(S_d-SV_d/V\\). It is a weighting diagnostic, not a pure-malapportionment estimate.")
end

function registry_latex(data::DataFrame)
    return longtable_latex(data;
        caption = "Observed-cabinet and ideological inversion case registry",
        label = "tab:intermediate-case-registry",
        column_spec = "lllrrlL{9.0cm}l", landscape = true,
        headers = ("Domain", "Year", "Case", "Vote \\%", "Seats", "Parties",
            "Composition", "Minimality"),
        renderers = (r -> tex(r.case_domain), r -> texint(r.election_year),
            r -> tex(r.case_label), r -> texshare(r.vote_share), r -> texint(r.s_C),
            r -> texint(r.coalition_party_count), r -> tex(r.coalition_parties),
            r -> tex(r.minimal_status)),
        notes = "Cabinet cases are observed reconstructions. Ideological cases are synthetic connected intervals and may overlap or nest.")
end

function decomposition_report_latex(data::DataFrame)
    return longtable_latex(data;
        caption = "Accounting decomposition of all cabinet and ideological inversion cases",
        label = "tab:intermediate-all-decomposition", column_spec = "lllrrrrrrrrL{4.0cm}",
        headers = ("Domain", "Year", "Case", "Vote \\%", "Seats", "\\(q_C\\)",
            "\\(d_C\\)", "\\(r_C\\)", "\\(R_C\\)", "\\(A_C\\)", "\\(B_C\\)",
            "Sign pattern"),
        renderers = (r -> tex(r.case_domain), r -> texint(r.election_year),
            r -> tex(r.case_label), r -> texshare(r.vote_share), r -> texint(r.s_C),
            r -> texnum2(r.q_C), r -> texnum2(r.d_C), r -> texnum2(r.r_C),
            r -> texnum(r.R_C), r -> texnum2(r.A_C), r -> texnum2(r.B_C),
            r -> tex(r.component_sign_pattern)),
        landscape = true,
        notes = "\\(d_C=A_C+B_C\\) is an accounting identity. \\(B_C\\) combines district seat weights, valid-vote weights, and coalition vote geography.")
end

function component_extremes_latex(data::DataFrame)
    return longtable_latex(data;
        caption = "Largest positive and negative party and district contributions by case",
        label = "tab:intermediate-component-extremes",
        column_spec = "lllL{2.2cm}lL{3.0cm}rL{3.0cm}r", landscape = true,
        headers = ("Domain", "Year", "Case", "Level", "Component", "Largest positive",
            "Value", "Most negative", "Value"),
        renderers = (r -> tex(r.case_domain), r -> texint(r.election_year),
            r -> tex(r.case_label), r -> tex(r.aggregation_level), r -> tex(r.component),
            r -> tex(r.largest_positive_unit), r -> texnum2(r.largest_positive_value),
            r -> tex(r.largest_negative_unit), r -> texnum2(r.largest_negative_value)),
        notes = "Extremes are selected from deterministic full rankings. They are descriptive accounting contributions.")
end

function party_vectors_latex(data::DataFrame)
    return longtable_latex(data;
        caption = "Complete member-party accounting vectors for inversion cases",
        label = "tab:intermediate-party-vectors", column_spec = "lllL{2.8cm}rrrrrr",
        headers = ("Domain", "Year", "Case", "Party", "Votes", "Seats", "\\(q_i\\)",
            "\\(A_i\\)", "\\(B_i\\)", "\\(d_i\\)"),
        renderers = (r -> tex(r.case_domain), r -> texint(r.election_year),
            r -> tex(r.case_label), r -> tex(r.party), r -> texint(r.v_i), r -> texint(r.s_i),
            r -> texnum2(r.q_i), r -> texnum2(r.A_i), r -> texnum2(r.B_i), r -> texnum2(r.d_i)),
        landscape = true,
        notes = "Party entries are ex post accounting attributions. Joint lists in 2014/2018 and federation context in 2022 preclude autonomous party-mechanism interpretations.")
end

function district_vectors_latex(data::DataFrame)
    return longtable_latex(data;
        caption = "Complete district accounting vectors for inversion cases",
        label = "tab:intermediate-district-vectors", column_spec = "lllL{1.4cm}rrrrrrr",
        headers = ("Domain", "Year", "Case", "Unit", "\\(v_{Cd}\\)", "\\(V_d\\)",
            "\\(s_{Cd}\\)", "\\(S_d\\)", "\\(a_{Cd}\\)", "\\(b_{Cd}\\)", "\\(d_{Cd}\\)"),
        renderers = (r -> tex(r.case_domain), r -> texint(r.election_year),
            r -> tex(r.case_label), r -> tex(r.electoral_unit), r -> texint(r.v_Cd),
            r -> texint(r.V_d), r -> texint(r.s_Cd), r -> texint(r.S_d),
            r -> texnum2(r.a_Cd), r -> texnum2(r.b_Cd), r -> texnum2(r.d_Cd)),
        landscape = true,
        notes = "District rows sum to the case-level \\(A_C\\), \\(B_C\\), and \\(d_C\\) values.")
end

function cell_extremes_latex(data::DataFrame)
    return longtable_latex(data;
        caption = "Largest positive and negative party--district cells by inversion case",
        label = "tab:intermediate-cell-extremes",
        column_spec = "lll l L{2.7cm}l r L{2.7cm}l r", landscape = true,
        headers = ("Domain", "Year", "Case", "Component", "Positive party", "Unit", "Value",
            "Negative party", "Unit", "Value"),
        renderers = (r -> tex(r.case_domain), r -> texint(r.election_year), r -> tex(r.case_label),
            r -> tex(r.component), r -> tex(r.largest_positive_party),
            r -> tex(r.largest_positive_electoral_unit), r -> texnum2(r.largest_positive_value),
            r -> tex(r.largest_negative_party), r -> tex(r.largest_negative_electoral_unit),
            r -> texnum2(r.largest_negative_value)),
        notes = "A party--district extreme is a descriptive accounting lead. Repeated cells across nested ideological cases are not independent observations.")
end

function full_cell_latex(data::DataFrame, year::Int)
    return longtable_latex(data;
        caption = "Complete party--district accounting panel, $(year)",
        label = "tab:intermediate-party-district-$(year)",
        column_spec = "llrrrrrrrrr", landscape = true, font_size = "tiny",
        headers = ("Unit", "Party", "\\(v_{id}\\)", "\\(V_d\\)", "\\(s_{id}\\)",
            "\\(S_d\\)", "Within quota", "National term", "\\(a_{id}\\)",
            "\\(b_{id}\\)", "\\(d_{id}\\)"),
        renderers = (r -> tex(r.electoral_unit), r -> tex(r.party), r -> texint(r.v_id),
            r -> texint(r.V_d), r -> texint(r.s_id), r -> texint(r.S_d),
            r -> texnum(r.within_district_quota), r -> texnum(r.national_quota_contribution),
            r -> texnum(r.a_id), r -> texnum(r.b_id), r -> texnum(r.d_id)),
        notes = "Every election-year party appears in every electoral unit; absent combinations are explicit zero cells. \\(d_{id}=a_{id}+b_{id}\\).")
end

"""
    write_intermediate_report_outputs(output_root, full, registry, cases, rankings;
                                      input_manifest)

Write all numerical intermediaries first, reload them from CSV, derive report
CSV tables, reload those tables, and only then render the thirteen LaTeX
fragments consumed by the standalone wrapper. Every path stays below
`output_root`; this function has no manuscript or paper-output synchronization
code.
"""
function write_intermediate_report_outputs(
    output_root::AbstractString,
    full,
    registry::DataFrame,
    cases,
    rankings::DataFrame;
    input_manifest,
    party_size_diagnostics,
)
    raw_dir = joinpath(output_root, "raw")
    table_dir = joinpath(output_root, "tables", "report")
    latex_dir = joinpath(output_root, "latex", "report")
    audit_dir = joinpath(output_root, "audit")
    foreach(mkpath, (raw_dir, table_dir, latex_dir, audit_dir))

    artifacts = NamedTuple[]
    function record_csv(relative_path, data, artifact_type, description)
        path = joinpath(output_root, relative_path)
        write_csv_file(path, data)
        push!(artifacts, (
            path = relative_path, artifact_type = artifact_type,
            description = description, rows = nrow(data), columns = length(names(data)),
            bytes = filesize(path), sha256 = sha256_file(path),
        ))
        return path
    end
    function record_tex(relative_path, contents, description, rows, columns)
        path = joinpath(output_root, relative_path)
        mkpath(dirname(path))
        open(path, "w") do io
            write(io, contents)
        end
        push!(artifacts, (
            path = relative_path, artifact_type = "latex", description = description,
            rows = rows, columns = columns, bytes = filesize(path), sha256 = sha256_file(path),
        ))
        return path
    end

    source_paths = Dict{Symbol,String}()
    source_paths[:cells] = record_csv(
        "raw/party_district_accounting_all_years.csv", full.cells, "raw",
        "Complete explicit-zero party-by-district a_id/b_id accounting panel for all election years.")
    source_paths[:parties] = record_csv(
        "raw/party_accounting_all_years.csv", full.parties, "raw",
        "All-year national party A_i/B_i/d_i accounting output.")
    source_paths[:districts] = record_csv(
        "raw/district_accounting_all_years.csv", full.districts, "raw",
        "All-year district seat/valid-vote weighting and closure output.")
    for artifact in write_party_size_diagnostic_outputs(
        output_root, full, party_size_diagnostics; write_accounting_base = false,
    )
        push!(artifacts, (
            path = artifact.path, artifact_type = artifact.artifact_type,
            description = artifact.description, rows = artifact.rows, columns = artifact.columns,
            bytes = filesize(joinpath(output_root, artifact.path)), sha256 = artifact.sha256,
        ))
    end
    source_paths[:registry] = record_csv(
        "raw/inversion_case_registry.csv", registry, "raw",
        "Combined cabinet and ideological inversion registry.")
    source_paths[:decomposition] = record_csv(
        "raw/all_inversion_decomposition.csv", cases.decomposition, "raw",
        "Combined registry-derived exact-audited coalition decomposition.")
    source_paths[:case_parties] = record_csv(
        "raw/all_inversion_party_contributions.csv", cases.party_contributions, "raw",
        "Combined full member-party accounting vectors for all inversion cases.")
    source_paths[:case_districts] = record_csv(
        "raw/all_inversion_district_contributions.csv", cases.district_contributions, "raw",
        "Combined district accounting vectors for all inversion cases.")
    source_paths[:case_cells] = record_csv(
        "raw/all_inversion_party_district_contributions.csv",
        cases.party_district_contributions, "raw",
        "Combined case-linked member-party-by-district accounting cells.")
    source_paths[:rankings] = record_csv(
        "raw/all_inversion_contribution_rankings.csv", rankings, "raw",
        "Complete deterministic party, district, and cell rankings for A/B/d components.")

    for domain in ("cabinet", "ideological")
        decomposition = cases.decomposition[cases.decomposition.case_domain .== domain, :]
        parties = cases.party_contributions[cases.party_contributions.case_domain .== domain, :]
        districts = cases.district_contributions[cases.district_contributions.case_domain .== domain, :]
        cells = cases.party_district_contributions[
            cases.party_district_contributions.case_domain .== domain, :,
        ]
        record_csv("raw/$(domain)_inversion_decomposition.csv", decomposition, "raw",
            "$(uppercasefirst(domain))-only coalition decomposition.")
        record_csv("raw/$(domain)_inversion_party_contributions.csv", parties, "raw",
            "$(uppercasefirst(domain))-only member-party accounting vectors.")
        record_csv("raw/$(domain)_inversion_district_contributions.csv", districts, "raw",
            "$(uppercasefirst(domain))-only district accounting vectors.")
        record_csv("raw/$(domain)_inversion_party_district_contributions.csv", cells, "raw",
            "$(uppercasefirst(domain))-only linked member-party-by-district cells.")
    end

    validations = vcat(full.validations, cases.validations; cols = :union)
    nrow(validations) == nrow(full.validations) + nrow(cases.validations) || error("Identity audit cardinality mismatch.")
    source_paths[:validations] = record_csv(
        "audit/intermediate_accounting_identity_checks.csv", validations, "audit",
        "Exact complete-system and registry-derived accounting identity checks.")
    input_manifest isa DataFrame || error("input_manifest must be a DataFrame.")
    nrow(input_manifest) > 0 || error("Intermediate accounting input manifest is empty.")
    record_csv(
        "audit/intermediate_accounting_input_manifest.csv", input_manifest, "audit",
        "SHA-256 provenance for every corrected-baseline and source-code input.")

    # From this point onward, all empirical table rows come from reloaded CSVs.
    cells = reload_csv(source_paths[:cells])
    parties = reload_csv(source_paths[:parties])
    districts = reload_csv(source_paths[:districts])
    registry_disk = reload_csv(source_paths[:registry])
    decomposition = reload_csv(source_paths[:decomposition])
    case_parties = reload_csv(source_paths[:case_parties])
    case_districts = reload_csv(source_paths[:case_districts])
    ranking_disk = reload_csv(source_paths[:rankings])

    nrow(cells) == EXPECTED_PARTY_DISTRICT_ROWS || error("Reloaded full cell panel changed.")
    nrow(case_parties) == sum(registry_disk.coalition_party_count) || error("Reloaded case-party output lost registry members.")
    nrow(case_districts) == 27 * nrow(registry_disk) || error("Reloaded case-district output lost registered districts.")
    nrow(ranking_disk) == nrow(rankings) || error("Reloaded ranking source cardinality changed.")

    table_paths = Dict{Symbol,String}()
    table_data = Dict{Symbol,DataFrame}()
    table_data[:interpretation] = build_interpretation_source(decomposition, ranking_disk)
    table_data[:closure] = build_year_closure_table(cells, parties, districts)
    table_data[:district_weights] = build_district_weight_extremes_table(districts)
    table_data[:registry] = build_registry_table(registry_disk)
    table_data[:decomposition] = build_decomposition_table(decomposition)
    table_data[:component_extremes] = build_component_extremes_table(decomposition, ranking_disk)
    table_data[:party_vectors] = build_party_vectors_table(case_parties)
    table_data[:district_vectors] = build_district_vectors_table(case_districts)
    table_data[:cell_extremes] = build_cell_extremes_table(decomposition, ranking_disk)
    for year in (2014, 2018, 2022)
        table_data[Symbol("cells_$(year)")] = cells[Int.(cells.election_year) .== year, :]
    end

    table_specs = (
        (:interpretation, "generated_interpretation_source.csv",
            "Case-level source for procedurally generated substantive interpretation."),
        (:closure, "table_year_accounting_closure.csv",
            "Election-year complete-panel and closure summary."),
        (:district_weights, "table_district_weight_extremes.csv",
            "Largest positive and negative district seat/valid-vote weight gaps."),
        (:registry, "table_inversion_case_registry.csv",
            "Compact cabinet and ideological inversion registry."),
        (:decomposition, "table_all_inversion_decomposition.csv",
            "Registry-derived coalition accounting decomposition table."),
        (:component_extremes, "table_case_component_extremes.csv",
            "Party and district positive/negative component extremes."),
        (:party_vectors, "table_case_party_vectors.csv",
            "Complete member-party vectors for all inversion cases."),
        (:district_vectors, "table_case_district_vectors.csv",
            "Complete district vectors for all inversion cases."),
        (:cell_extremes, "table_case_party_district_extremes.csv",
            "Party-by-district positive/negative cell extremes."),
        (:cells_2014, "table_party_district_accounting_2014.csv",
            "Complete 2014 party-by-district accounting panel."),
        (:cells_2018, "table_party_district_accounting_2018.csv",
            "Complete 2018 party-by-district accounting panel."),
        (:cells_2022, "table_party_district_accounting_2022.csv",
            "Complete 2022 party-by-district accounting panel."),
    )
    for (key, filename, description) in table_specs
        table_paths[key] = record_csv(
            joinpath("tables", "report", filename), table_data[key], "table", description,
        )
    end

    # Reload table CSVs before any LaTeX rendering.
    table_disk = Dict(key => reload_csv(path) for (key, path) in table_paths)
    latex_specs = (
        ("generated_interpretation.tex",
            generated_interpretation_latex(table_disk[:interpretation], cells, table_disk[:district_weights]),
            "Procedurally generated descriptive interpretation linked to case outputs.",
            nrow(table_disk[:interpretation]), length(names(table_disk[:interpretation]))),
        ("table_year_accounting_closure.tex", year_closure_latex(table_disk[:closure]),
            "Reusable LaTeX election-year accounting closure table.",
            nrow(table_disk[:closure]), length(names(table_disk[:closure]))),
        ("table_district_weight_extremes.tex", district_weight_latex(table_disk[:district_weights]),
            "Reusable LaTeX district-weight extremes table.",
            nrow(table_disk[:district_weights]), length(names(table_disk[:district_weights]))),
        ("table_inversion_case_registry.tex", registry_latex(table_disk[:registry]),
            "Reusable LaTeX inversion registry table.",
            nrow(table_disk[:registry]), length(names(table_disk[:registry]))),
        ("table_all_inversion_decomposition.tex",
            decomposition_report_latex(table_disk[:decomposition]),
            "Reusable LaTeX registry-derived decomposition table.",
            nrow(table_disk[:decomposition]), length(names(table_disk[:decomposition]))),
        ("table_case_component_extremes.tex",
            component_extremes_latex(table_disk[:component_extremes]),
            "Reusable LaTeX party/district component-extremes table.",
            nrow(table_disk[:component_extremes]), length(names(table_disk[:component_extremes]))),
        ("table_case_party_vectors.tex", party_vectors_latex(table_disk[:party_vectors]),
            "Reusable LaTeX complete party-vector table.",
            nrow(table_disk[:party_vectors]), length(names(table_disk[:party_vectors]))),
        ("table_case_district_vectors.tex", district_vectors_latex(table_disk[:district_vectors]),
            "Reusable LaTeX complete district-vector table.",
            nrow(table_disk[:district_vectors]), length(names(table_disk[:district_vectors]))),
        ("table_case_party_district_extremes.tex",
            cell_extremes_latex(table_disk[:cell_extremes]),
            "Reusable LaTeX party-by-district cell-extremes table.",
            nrow(table_disk[:cell_extremes]), length(names(table_disk[:cell_extremes]))),
        ("table_party_district_accounting_2014.tex",
            full_cell_latex(table_disk[:cells_2014], 2014),
            "Reusable LaTeX complete 2014 party-by-district longtable.",
            nrow(table_disk[:cells_2014]), length(names(table_disk[:cells_2014]))),
        ("table_party_district_accounting_2018.tex",
            full_cell_latex(table_disk[:cells_2018], 2018),
            "Reusable LaTeX complete 2018 party-by-district longtable.",
            nrow(table_disk[:cells_2018]), length(names(table_disk[:cells_2018]))),
        ("table_party_district_accounting_2022.tex",
            full_cell_latex(table_disk[:cells_2022], 2022),
            "Reusable LaTeX complete 2022 party-by-district longtable.",
            nrow(table_disk[:cells_2022]), length(names(table_disk[:cells_2022]))),
    )
    latex_paths = [joinpath(output_root, "latex", "report", "party_size_diagnostics.tex")]
    for (filename, contents, description, rows, columns) in latex_specs
        relative = joinpath("latex", "report", filename)
        push!(latex_paths, record_tex(relative, contents, description, rows, columns))
    end
    length(latex_paths) == 13 || error("Standalone report requires exactly thirteen LaTeX fragments.")

    generation_checks = DataFrame([
        (check_name = "full party-district rows", observed = nrow(cells),
            expected = EXPECTED_PARTY_DISTRICT_ROWS, status = "PASS"),
        (check_name = "all inversion cases", observed = nrow(decomposition),
            expected = nrow(registry), status = "PASS"),
        (check_name = "all case-party rows", observed = nrow(case_parties),
            expected = sum(registry.coalition_party_count), status = "PASS"),
        (check_name = "all case-district rows", observed = nrow(case_districts),
            expected = 27 * nrow(registry), status = "PASS"),
        (check_name = "all case party-district rows",
            observed = nrow(reload_csv(source_paths[:case_cells])),
            expected = 27 * sum(registry.coalition_party_count), status = "PASS"),
        (check_name = "full ranking rows", observed = nrow(ranking_disk),
            expected = nrow(rankings), status = "PASS"),
        (check_name = "exact identity checks", observed = nrow(validations),
            expected = nrow(full.validations) + nrow(cases.validations), status = "PASS"),
        (check_name = "generated LaTeX fragments", observed = length(latex_paths),
            expected = 13, status = "PASS"),
    ])
    all(generation_checks.observed .== generation_checks.expected) || error(
        "One or more intermediate-accounting generation cardinalities failed.",
    )
    record_csv(
        "audit/intermediate_accounting_generation_checks.csv", generation_checks, "audit",
        "Report-output cardinality and generation-boundary checks.")

    manifest = DataFrame(artifacts)
    sort!(manifest, :path)
    all(startswith.(String.(manifest.path), Ref("raw/")) .|
        startswith.(String.(manifest.path), Ref("tables/report/")) .|
        startswith.(String.(manifest.path), Ref("latex/report/")) .|
        startswith.(String.(manifest.path), Ref("audit/"))) || error(
            "Intermediate report manifest contains a path outside the isolated output tree.",
        )
    manifest_path = joinpath(audit_dir, "intermediate_accounting_report_artifact_manifest.csv")
    CSV.write(manifest_path, manifest; quotestrings = true)
    return manifest
end

include(joinpath(@__DIR__, "PartySizeDiagnostics.jl"))

end # module IntermediateAccountingReport
