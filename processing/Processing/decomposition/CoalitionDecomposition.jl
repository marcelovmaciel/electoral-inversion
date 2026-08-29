module CoalitionDecomposition

using CSV
using DataFrames
using Dates
using Printf
using SHA

import ..Processing
import ..Processing.AnalysisRunnerCore as ARC

export ACCOUNTING_ATOL,
       ACCOUNTING_RTOL,
       EXPECTED_INVERSION_KEYS,
       EXPECTED_INVERSION_COALITIONS,
       build_year_accounting,
       recompute_coalition_periods,
       decompose_inversions,
       write_decomposition_outputs,
       validate_ideological_counts

const ACCOUNTING_ATOL = 1.0e-9
const ACCOUNTING_RTOL = 1.0e-12
const EXPECTED_TOTAL_SEATS = 513
const EXPECTED_INVERSION_KEYS = [
    (2014, "2016.2"),
    (2014, "2017.1"),
    (2018, "2021.3"),
    (2018, "2022.1"),
    (2022, "2023.1"),
]
const EXPECTED_INVERSION_COALITIONS = Dict(
    (2014, "2016.2") => sort(["PCdoB", "PDT", "PMDB", "PR", "PSD", "PT", "PTB"]),
    (2014, "2017.1") => sort(["DEM", "PMDB", "PP", "PPS", "PSB", "PSD", "PSDB", "PV"]),
    (2018, "2021.3") => sort(["DEM", "PATRIOTA", "PP", "PR", "PRB", "PSC", "PSD", "PSDB", "PSL"]),
    (2018, "2022.1") => sort(["DEM", "PATRIOTA", "PP", "PR", "PRB", "PSC", "PSD", "PSDB", "PSL"]),
    (2022, "2023.1") => sort(["MDB", "PCdoB", "PDT", "PSB", "PSD", "PSOL", "PT", "REDE", "UNIÃO"]),
)

const Rat = Rational{BigInt}

bigint(value::Integer) = BigInt(value)
exact_fraction(numerator::Integer, denominator::Integer) = bigint(numerator) // bigint(denominator)
exact_product_ratio(left::Integer, right::Integer, denominator::Integer) =
    (bigint(left) * bigint(right)) // bigint(denominator)

function require(condition::Bool, message::AbstractString)
    condition || error(message)
    return true
end

function accounting_isapprox(left, right)
    return isapprox(Float64(left), Float64(right); atol = ACCOUNTING_ATOL, rtol = ACCOUNTING_RTOL)
end

function require_approx(left, right, label::AbstractString)
    accounting_isapprox(left, right) || error(
        "$(label): values differ (left=$(left), right=$(right), " *
        "atol=$(ACCOUNTING_ATOL), rtol=$(ACCOUNTING_RTOL)).",
    )
    return true
end

function split_parties(value)
    parties = sort(String.(filter(!isempty, strip.(split(String(value), ",")))))
    length(parties) == length(unique(parties)) || error(
        "Coalition contains duplicate party labels: $(value)",
    )
    return parties
end

function canonical_party(raw_party, year::Integer)
    raw = strip(String(raw_party))
    canonical = Processing.canonical_party(raw; year = Int(year), strict = false)
    canonical == Processing.UNKNOWN_PARTY && error(
        "Unmapped federal-deputy party label for $(year): $(raw)",
    )
    return canonical
end

function canonicalize_party_column!(data::DataFrame, year::Integer)
    raw_values = String.(data.SG_PARTIDO)
    mapping = Dict(raw => canonical_party(raw, year) for raw in unique(raw_values))
    data[!, :SG_PARTIDO] = [mapping[raw] for raw in raw_values]
    return data
end

function load_district_votes(year::Integer, path::AbstractString)
    required_names = Set([
        "DS_CARGO",
        "SG_UF",
        "SG_PARTIDO",
        "QT_VOTOS_NOMINAIS_VALIDOS",
        "QT_VOTOS_NOMINAIS",
        "QT_TOTAL_VOTOS_LEG_VALIDOS",
        "QT_VOTOS_LEGENDA_VALIDOS",
        "QT_VOTOS_LEGENDA",
        "QT_VOTOS_NOMINAIS_CONVR_LEG",
        "QT_VOTOS_NOM_CONVR_LEG_VALIDOS",
        "QT_VOTOS",
        "TOTAL_VOTOS",
        "QT_VOTOS_VALIDOS",
    ])
    data = CSV.read(
        path,
        DataFrame;
        select = (_, name) -> String(name) in required_names,
    )
    Processing.upper_strip!(data, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", data)
    nrow(data) > 0 || error("No federal-deputy vote rows for $(year): $(path)")

    # The office filter intentionally precedes strict party canonicalization.
    # This brackets the unrelated PCA14 non-federal row without weakening the
    # canonicalizer for any row used by the decomposition.
    Processing.stringify!(data, :SG_UF)
    Processing.stringify!(data, :SG_PARTIDO)
    canonicalize_party_column!(data, year)

    vote_kwargs = ARC.vote_kwargs_for_year(year)
    nominal_col, legend_col, total_col, scheme = Processing.detect_vote_cols(data; vote_kwargs...)
    valid_votes = if scheme == :total
        total_col === nothing && error("No total-vote column detected for $(year).")
        Processing.to_int.(data[!, total_col])
    else
        nominal_col === nothing && error("No nominal-vote column detected for $(year).")
        legend_col === nothing && error("No legend-vote column detected for $(year).")
        Processing.to_int.(data[!, nominal_col]) .+ Processing.to_int.(data[!, legend_col])
    end
    all(valid_votes .>= 0) || error("Negative federal-deputy votes detected for $(year).")

    district_votes = DataFrame(
        district = String.(data.SG_UF),
        party = String.(data.SG_PARTIDO),
        votes = Int.(valid_votes),
    )
    district_votes = combine(groupby(district_votes, [:district, :party]), :votes => sum => :votes)
    sort!(district_votes, [:district, :party])
    return district_votes
end

function load_district_seats(year::Integer, path::AbstractString)
    columns = [:DS_CARGO, :SG_UF, :SG_PARTIDO, :DS_SIT_TOT_TURNO]
    data = CSV.read(path, DataFrame; select = columns, normalizenames = true)
    Processing.upper_strip!(data, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", data)
    nrow(data) > 0 || error("No federal-deputy candidate rows for $(year): $(path)")

    Processing.stringify!(data, :SG_UF)
    Processing.stringify!(data, :SG_PARTIDO)
    canonicalize_party_column!(data, year)
    winner_status = uppercase.(strip.(String.(data.DS_SIT_TOT_TURNO)))
    winners = in.(winner_status, Ref(Processing.WINNER_STATUSES))

    district_seats = DataFrame(
        district = String.(data.SG_UF),
        party = String.(data.SG_PARTIDO),
        seats = Int.(winners),
    )
    district_seats = combine(groupby(district_seats, [:district, :party]), :seats => sum => :seats)
    filter!(row -> row.seats > 0, district_seats)
    sort!(district_seats, [:district, :party])
    return district_seats
end

function load_apportioned_district_seats(year::Integer, path::AbstractString)
    columns = [:DS_CARGO, :SG_UF, :QT_VAGA]
    data = CSV.read(path, DataFrame; select = columns, normalizenames = true)
    Processing.upper_strip!(data, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", data)
    nrow(data) > 0 || error("No federal-deputy apportionment rows for $(year): $(path)")

    Processing.stringify!(data, :SG_UF)
    apportioned = DataFrame(
        district = String.(data.SG_UF),
        apportioned_seats = Processing.to_int.(data.QT_VAGA),
    )
    apportioned = combine(
        groupby(apportioned, :district),
        :apportioned_seats => sum => :apportioned_seats,
    )
    sort!(apportioned, :district)
    nrow(apportioned) == 27 || error(
        "$(year): seat-apportionment file has $(nrow(apportioned)) federal-deputy units, not 27.",
    )
    sum(apportioned.apportioned_seats) == EXPECTED_TOTAL_SEATS || error(
        "$(year): seat-apportionment file does not sum to $(EXPECTED_TOTAL_SEATS).",
    )
    return apportioned
end

function complete_panel(votes::DataFrame, seats::DataFrame)
    vote_districts = Set(String.(votes.district))
    seat_districts = Set(String.(seats.district))
    vote_districts == seat_districts || error(
        "Vote and seat electoral-unit sets differ: votes=$(sort(collect(vote_districts))), " *
        "seats=$(sort(collect(seat_districts))).",
    )
    districts = sort(collect(vote_districts))
    parties = sort(collect(union(Set(String.(votes.party)), Set(String.(seats.party)))))
    length(districts) == 27 || error(
        "Expected Brazil's 27 federal-deputy electoral units; found $(length(districts)): " *
        join(districts, ", "),
    )

    vote_lookup = Dict(
        (String(row.district), String(row.party)) => Int(row.votes) for row in eachrow(votes)
    )
    seat_lookup = Dict(
        (String(row.district), String(row.party)) => Int(row.seats) for row in eachrow(seats)
    )
    rows = NamedTuple[]
    for district in districts, party in parties
        push!(rows, (
            district = district,
            party = party,
            votes = get(vote_lookup, (district, party), 0),
            seats = get(seat_lookup, (district, party), 0),
        ))
    end
    panel = DataFrame(rows)
    nrow(panel) == length(districts) * length(parties) || error(
        "Incomplete party-by-district panel.",
    )
    return panel
end

"""
    build_year_accounting(year, vote_path, candidate_path, apportionment_path;
                          expected_national_votes)

Load the same federal-deputy vote components and winner statuses used by the
validated paper runner, aggregate them by state, complete the party-by-state
panel with explicit zeroes, and compute the exact-rational accounting terms.
"""
function build_year_accounting(
    year::Integer,
    vote_path::AbstractString,
    candidate_path::AbstractString;
    apportionment_path::Union{Nothing,AbstractString} = nothing,
    expected_national_votes::Integer,
    expected_total_seats::Integer = EXPECTED_TOTAL_SEATS,
)
    district_votes = load_district_votes(year, vote_path)
    district_seats = load_district_seats(year, candidate_path)
    panel = complete_panel(district_votes, district_seats)

    national_votes = sum(panel.votes)
    national_seats = sum(panel.seats)
    national_votes == expected_national_votes || error(
        "$(year) national vote denominator changed: $(national_votes) != $(expected_national_votes).",
    )
    national_seats == expected_total_seats || error(
        "$(year) Chamber seats changed: $(national_seats) != $(expected_total_seats).",
    )

    district_totals = combine(
        groupby(panel, :district),
        :votes => sum => :district_votes,
        :seats => sum => :district_seats,
    )
    all(district_totals.district_votes .> 0) || error("$(year) contains a zero-vote electoral unit.")
    all(district_totals.district_seats .> 0) || error("$(year) contains a zero-seat electoral unit.")
    apportionment_path === nothing && error(
        "$(year): independent seats.csv input is required for district-magnitude validation.",
    )
    apportioned = load_apportioned_district_seats(year, apportionment_path)
    candidate_lookup = Dict(
        String(row.district) => Int(row.district_seats) for row in eachrow(district_totals)
    )
    apportioned_lookup = Dict(
        String(row.district) => Int(row.apportioned_seats) for row in eachrow(apportioned)
    )
    keys(candidate_lookup) == keys(apportioned_lookup) || error(
        "$(year): candidate and seats.csv electoral-unit sets differ.",
    )
    for district in sort(collect(keys(candidate_lookup)))
        candidate_lookup[district] == apportioned_lookup[district] || error(
            "$(year)/$(district): elected-candidate count $(candidate_lookup[district]) " *
            "differs from seats.csv magnitude $(apportioned_lookup[district]).",
        )
    end
    district_vote_lookup = Dict(String(row.district) => Int(row.district_votes) for row in eachrow(district_totals))
    district_seat_lookup = Dict(String(row.district) => Int(row.district_seats) for row in eachrow(district_totals))

    panel[!, :district_votes] = [district_vote_lookup[String(value)] for value in panel.district]
    panel[!, :district_seats] = [district_seat_lookup[String(value)] for value in panel.district]
    within_quota_exact = Rat[]
    national_quota_contribution_exact = Rat[]
    a_exact = Rat[]
    b_exact = Rat[]
    b_factored_exact = Rat[]
    for row in eachrow(panel)
        within_quota = exact_product_ratio(row.district_seats, row.votes, row.district_votes)
        national_contribution = exact_product_ratio(national_seats, row.votes, national_votes)
        a_value = exact_fraction(row.seats, 1) - within_quota
        b_value = within_quota - national_contribution
        b_factored = exact_fraction(national_seats, 1) *
            (exact_fraction(row.district_seats, national_seats) -
             exact_fraction(row.district_votes, national_votes)) *
            exact_fraction(row.votes, row.district_votes)
        b_value == b_factored || error(
            "$(year)/$(row.district)/$(row.party): defining and factored B terms differ.",
        )
        push!(within_quota_exact, within_quota)
        push!(national_quota_contribution_exact, national_contribution)
        push!(a_exact, a_value)
        push!(b_exact, b_value)
        push!(b_factored_exact, b_factored)
    end
    panel[!, :within_quota_exact] = within_quota_exact
    panel[!, :national_quota_contribution_exact] = national_quota_contribution_exact
    panel[!, :a_exact] = a_exact
    panel[!, :b_exact] = b_exact
    panel[!, :b_factored_exact] = b_factored_exact

    party = combine(
        groupby(panel, :party),
        :votes => sum => :votes,
        :seats => sum => :seats,
        :a_exact => sum => :A_exact,
        :b_exact => sum => :B_exact,
    )
    quota_exact = Rat[]
    differential_exact = Rat[]
    ratio_exact = Union{Missing,Rat}[]
    for row in eachrow(party)
        quota = exact_product_ratio(national_seats, row.votes, national_votes)
        differential = exact_fraction(row.seats, 1) - quota
        row.A_exact + row.B_exact == differential || error(
            "$(year)/$(row.party): A_i + B_i != d_i.",
        )
        ratio = row.votes == 0 ? missing : exact_fraction(row.seats, 1) / quota
        if ratio !== missing
            quota * (ratio - exact_fraction(1, 1)) == differential || error(
                "$(year)/$(row.party): q_i(R_i - 1) != d_i.",
            )
        elseif row.seats > 0
            error("$(year)/$(row.party) has positive seats and zero valid votes.")
        end
        push!(quota_exact, quota)
        push!(differential_exact, differential)
        push!(ratio_exact, ratio)
    end
    party[!, :quota_exact] = quota_exact
    party[!, :d_exact] = differential_exact
    party[!, :R_exact] = ratio_exact
    party[!, :vote_share] = Float64.(party.votes ./ national_votes)
    party[!, :seat_share] = Float64.(party.seats ./ national_seats)
    party[!, :quota] = Float64.(party.quota_exact)
    party[!, :d_i] = Float64.(party.d_exact)
    party[!, :A_i] = Float64.(party.A_exact)
    party[!, :B_i] = Float64.(party.B_exact)
    party[!, :R_i] = [value === missing ? missing : Float64(value) for value in party.R_exact]
    sort!(party, :party)

    sum(party.A_exact) == 0 || error("$(year): complete-system sum(A_i) != 0.")
    sum(party.B_exact) == 0 || error("$(year): complete-system sum(B_i) != 0.")
    sum(party.d_exact) == 0 || error("$(year): complete-system sum(d_i) != 0.")
    for group in groupby(panel, :district)
        sum(group.a_exact) == 0 || error(
            "$(year)/$(first(group.district)): district sum(a_id) != 0.",
        )
        expected_b_sum = exact_fraction(first(group.district_seats), 1) -
            exact_product_ratio(national_seats, first(group.district_votes), national_votes)
        sum(group.b_exact) == expected_b_sum || error(
            "$(year)/$(first(group.district)): district sum(b_id) closure failed.",
        )
    end

    return (
        year = Int(year),
        panel = panel,
        party = party,
        district = district_totals,
        national_votes = Int(national_votes),
        national_seats = Int(national_seats),
        seat_majority_threshold = fld(Int(national_seats), 2) + 1,
    )
end

function validate_party_baseline!(accounting, baseline::DataFrame)
    rows = baseline[Int.(baseline.election_year) .== accounting.year, :]
    nrow(rows) == nrow(accounting.party) || error(
        "$(accounting.year): district reconstruction has $(nrow(accounting.party)) parties, " *
        "baseline has $(nrow(rows)).",
    )
    baseline_parties = Set(String.(rows.party))
    panel_parties = Set(String.(accounting.party.party))
    baseline_parties == panel_parties || error(
        "$(accounting.year): district and national party sets differ.",
    )
    lookup = Dict(String(row.party) => row for row in eachrow(rows))
    for row in eachrow(accounting.party)
        source = lookup[String(row.party)]
        row.votes == source.votes || error("$(accounting.year)/$(row.party): vote total differs from baseline.")
        row.seats == source.seats || error("$(accounting.year)/$(row.party): seat total differs from baseline.")
        require_approx(row.quota, source.quota, "$(accounting.year)/$(row.party) q_i baseline")
        require_approx(row.d_i, source.seat_diff, "$(accounting.year)/$(row.party) d_i baseline")
        if row.R_i !== missing
            require_approx(row.R_i, source.representation_ratio, "$(accounting.year)/$(row.party) R_i baseline")
        end
    end
    return true
end

function majority_status(vote_majority::Bool, seat_majority::Bool)
    vote_majority && seat_majority && return "votes+seats"
    vote_majority && return "votes_only"
    seat_majority && return "seats_only"
    return "neither"
end

"""
    recompute_coalition_periods(observed, accounting_by_year; party_baseline)

Recompute every observed cabinet-period quantity from district-reconstructed
party totals, then compare it with the frozen PSC-correct paper output.
"""
function recompute_coalition_periods(
    observed::DataFrame,
    accounting_by_year::AbstractDict;
    party_baseline::Union{Nothing,DataFrame} = nothing,
)
    party_baseline === nothing || foreach(
        accounting -> validate_party_baseline!(accounting, party_baseline),
        values(accounting_by_year),
    )
    rows = NamedTuple[]
    for source in eachrow(observed)
        year = Int(source.election_year)
        accounting = accounting_by_year[year]
        parties = split_parties(source.parties)
        available = Set(String.(accounting.party.party))
        missing_parties = setdiff(Set(parties), available)
        isempty(missing_parties) || error(
            "$(year)/$(source.period): coalition parties absent from election panel: " *
            join(sort(collect(missing_parties)), ", "),
        )
        members = accounting.party[in.(String.(accounting.party.party), Ref(Set(parties))), :]
        coalition_votes = sum(members.votes)
        coalition_seats = sum(members.seats)
        quota_exact = exact_product_ratio(accounting.national_seats, coalition_votes, accounting.national_votes)
        differential_exact = exact_fraction(coalition_seats, 1) - quota_exact
        required_exact = exact_fraction(accounting.seat_majority_threshold, 1) - quota_exact
        vote_share = coalition_votes / accounting.national_votes
        seat_share = coalition_seats / accounting.national_seats
        ratio = coalition_seats / Float64(quota_exact)
        vote_majority = vote_share >= 0.5
        seat_majority = coalition_seats >= accounting.seat_majority_threshold
        inversion = vote_share < 0.5 && seat_majority
        canonical_parties = join(parties, ", ")

        canonical_parties == join(split_parties(source.parties), ", ") || error(
            "$(year)/$(source.period): coalition composition normalization changed.",
        )
        coalition_votes == Int(source.votes) || error("$(year)/$(source.period): v_C differs from PSC baseline.")
        coalition_seats == Int(source.seats) || error("$(year)/$(source.period): s_C differs from PSC baseline.")
        accounting.national_votes == Int(source.national_vote_total) || error(
            "$(year)/$(source.period): national vote denominator differs from PSC baseline.",
        )
        require_approx(vote_share, source.vote_share, "$(year)/$(source.period) vote share baseline")
        require_approx(quota_exact, source.quota, "$(year)/$(source.period) q_C baseline")
        require_approx(differential_exact, source.seat_diff, "$(year)/$(source.period) d_C baseline")
        require_approx(required_exact, source.required_diff, "$(year)/$(source.period) r_C baseline")
        require_approx(ratio, source.representation_ratio, "$(year)/$(source.period) R_C baseline")
        inversion == Bool(source.coalition_inversion) || error(
            "$(year)/$(source.period): inversion classification differs from PSC baseline.",
        )

        push!(rows, (
            coalition_id = "$(year)/$(source.period)",
            election_year = year,
            coalition_year = Int(source.coalition_year),
            cabinet_period = string(source.period),
            period_start = Date(string(source.period_start)),
            period_end = Date(string(source.period_end)),
            period_days = Int(source.period_days),
            days_overlapping_mandate = Int(source.days_overlapping_mandate),
            coalition_parties = canonical_parties,
            coalition_party_count = length(parties),
            v_C = Int(coalition_votes),
            V = Int(accounting.national_votes),
            vote_share = Float64(vote_share),
            s_C = Int(coalition_seats),
            S = Int(accounting.national_seats),
            seat_share = Float64(seat_share),
            q_C = Float64(quota_exact),
            d_C = Float64(differential_exact),
            r_C = Float64(required_exact),
            R_C = Float64(ratio),
            vote_majority = Bool(vote_majority),
            seat_majority = Bool(seat_majority),
            majority_status = majority_status(vote_majority, seat_majority),
            coalition_inversion = Bool(inversion),
        ))
    end
    result = DataFrame(rows)
    sort!(result, [:election_year, :period_start, :cabinet_period])

    inversion_keys = [
        (Int(row.election_year), String(row.cabinet_period)) for
        row in eachrow(result[result.coalition_inversion .== true, :])
    ]
    inversion_keys == EXPECTED_INVERSION_KEYS || error(
        "Observed inversion registry changed: expected $(EXPECTED_INVERSION_KEYS), found $(inversion_keys).",
    )
    for key in EXPECTED_INVERSION_KEYS
        row = only(eachrow(result[
            (result.election_year .== key[1]) .& (result.cabinet_period .== key[2]),
            :,
        ]))
        split_parties(row.coalition_parties) == EXPECTED_INVERSION_COALITIONS[key] || error(
            "$(key[1])/$(key[2]): coalition composition differs from corrected cabinet reconstruction.",
        )
    end
    return result
end

function exact_coalition_district(accounting, parties::Vector{String}, district::String)
    district_rows = accounting.panel[accounting.panel.district .== district, :]
    member_mask = in.(String.(district_rows.party), Ref(Set(parties)))
    members = district_rows[member_mask, :]
    v_Cd = sum(members.votes)
    s_Cd = sum(members.seats)
    V_d = first(district_rows.district_votes)
    S_d = first(district_rows.district_seats)
    within_quota = exact_product_ratio(S_d, v_Cd, V_d)
    national_contribution = exact_product_ratio(accounting.national_seats, v_Cd, accounting.national_votes)
    a_value = exact_fraction(s_Cd, 1) - within_quota
    b_value = within_quota - national_contribution
    b_factored = exact_fraction(accounting.national_seats, 1) *
        (exact_fraction(S_d, accounting.national_seats) -
         exact_fraction(V_d, accounting.national_votes)) *
        exact_fraction(v_Cd, V_d)
    b_value == b_factored || error("$(accounting.year)/$(district): coalition B_Cd cross-check failed.")
    sum(members.a_exact) == a_value || error("$(accounting.year)/$(district): party sum a_id != a_Cd.")
    sum(members.b_exact) == b_value || error("$(accounting.year)/$(district): party sum b_id != b_Cd.")
    return (
        v_Cd = Int(v_Cd),
        s_Cd = Int(s_Cd),
        V_d = Int(V_d),
        S_d = Int(S_d),
        within_quota_exact = within_quota,
        national_contribution_exact = national_contribution,
        a_exact = a_value,
        b_exact = b_value,
        b_factored_exact = b_factored,
    )
end

function qualification_for_year(year::Integer)
    year in (2014, 2018) && return "ex post party accounting contribution; joint electoral lists"
    return "ex post party accounting contribution; party/federation allocation context"
end

"""
    decompose_inversions(coalition_periods, accounting_by_year)

Calculate A_C and B_C for exactly the five corrected observed inversions, plus
the full member-party d_i vector and state-level contributions. All internal
identities are checked with exact rational arithmetic; decimal residuals are
also checked under ACCOUNTING_ATOL/ACCOUNTING_RTOL for output regressions.
"""
function decompose_inversions(coalition_periods::DataFrame, accounting_by_year::AbstractDict)
    inversion_rows = coalition_periods[coalition_periods.coalition_inversion .== true, :]
    keys = [(Int(row.election_year), String(row.cabinet_period)) for row in eachrow(inversion_rows)]
    keys == EXPECTED_INVERSION_KEYS || error(
        "Decomposition case registry must be exactly $(EXPECTED_INVERSION_KEYS); found $(keys).",
    )

    decomposition_rows = NamedTuple[]
    party_rows = NamedTuple[]
    district_rows = NamedTuple[]
    validation_rows = NamedTuple[]

    for coalition in eachrow(inversion_rows)
        year = Int(coalition.election_year)
        period = String(coalition.cabinet_period)
        key = (year, period)
        accounting = accounting_by_year[year]
        parties = split_parties(coalition.coalition_parties)
        parties == EXPECTED_INVERSION_COALITIONS[key] || error(
            "$(year)/$(period): decomposition coalition differs from corrected reconstruction.",
        )
        coalition.vote_share < 0.5 || error("$(year)/$(period): decomposed coalition is not below 50% of votes.")
        coalition.s_C >= accounting.seat_majority_threshold || error(
            "$(year)/$(period): decomposed coalition lacks a Chamber seat majority.",
        )

        case_district_exact = NamedTuple[]
        for district in sort(unique(String.(accounting.panel.district)))
            values = exact_coalition_district(accounting, parties, district)
            push!(case_district_exact, merge((district = district,), values))
            push!(district_rows, (
                coalition_id = String(coalition.coalition_id),
                election_year = year,
                cabinet_period = period,
                coalition_parties = String(coalition.coalition_parties),
                electoral_unit = district,
                v_Cd = values.v_Cd,
                V_d = values.V_d,
                s_Cd = values.s_Cd,
                S_d = values.S_d,
                within_district_quota = Float64(values.within_quota_exact),
                national_quota_contribution = Float64(values.national_contribution_exact),
                a_Cd = Float64(values.a_exact),
                b_Cd = Float64(values.b_exact),
                b_Cd_factored = Float64(values.b_factored_exact),
                b_crosscheck_residual = Float64(values.b_exact - values.b_factored_exact),
            ))
        end
        A_exact = sum(row.a_exact for row in case_district_exact)
        B_exact = sum(row.b_exact for row in case_district_exact)
        q_exact = exact_product_ratio(accounting.national_seats, coalition.v_C, accounting.national_votes)
        d_exact = exact_fraction(coalition.s_C, 1) - q_exact
        r_exact = exact_fraction(accounting.seat_majority_threshold, 1) - q_exact
        A_exact + B_exact == d_exact || error("$(year)/$(period): A_C + B_C != d_C exactly.")

        members = accounting.party[in.(String.(accounting.party.party), Ref(Set(parties))), :]
        sum(members.d_exact) == d_exact || error("$(year)/$(period): sum_i d_i != d_C exactly.")
        sum(members.A_exact) == A_exact || error("$(year)/$(period): sum_i A_i != A_C exactly.")
        sum(members.B_exact) == B_exact || error("$(year)/$(period): sum_i B_i != B_C exactly.")

        complement = accounting.party[.!in.(String.(accounting.party.party), Ref(Set(parties))), :]
        sum(complement.d_exact) == -d_exact || error("$(year)/$(period): complement d identity failed.")
        sum(complement.A_exact) == -A_exact || error("$(year)/$(period): complement A identity failed.")
        sum(complement.B_exact) == -B_exact || error("$(year)/$(period): complement B identity failed.")

        require_approx(q_exact, coalition.q_C, "$(year)/$(period) q_C coalition output")
        require_approx(d_exact, coalition.d_C, "$(year)/$(period) d_C coalition output")
        require_approx(r_exact, coalition.r_C, "$(year)/$(period) r_C coalition output")
        require_approx(A_exact + B_exact, coalition.d_C, "$(year)/$(period) decimal A+B=d")
        require_approx(sum(Float64.(members.d_exact)), coalition.d_C, "$(year)/$(period) decimal sum(d_i)=d_C")

        for member in eachrow(members)
            push!(party_rows, (
                coalition_id = String(coalition.coalition_id),
                election_year = year,
                cabinet_period = period,
                coalition_parties = String(coalition.coalition_parties),
                party = String(member.party),
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
                q_times_R_minus_1 = member.R_exact === missing ? missing :
                    Float64(member.quota_exact * (member.R_exact - exact_fraction(1, 1))),
                accounting_qualification = qualification_for_year(year),
            ))
        end

        push!(decomposition_rows, (
            coalition_id = String(coalition.coalition_id),
            election_year = year,
            coalition_year = Int(coalition.coalition_year),
            cabinet_period = period,
            period_start = coalition.period_start,
            period_end = coalition.period_end,
            period_days = Int(coalition.period_days),
            coalition_parties = String(coalition.coalition_parties),
            coalition_party_count = Int(coalition.coalition_party_count),
            vote_share = Float64(coalition.vote_share),
            vote_share_pct = 100 * Float64(coalition.vote_share),
            s_C = Int(coalition.s_C),
            q_C = Float64(q_exact),
            d_C = Float64(d_exact),
            r_C = Float64(r_exact),
            R_C = Float64(coalition.R_C),
            A_C = Float64(A_exact),
            B_C = Float64(B_exact),
            A_plus_B_residual = Float64(A_exact + B_exact - d_exact),
            party_d_residual = Float64(sum(members.d_exact) - d_exact),
            accounting_tolerance_atol = ACCOUNTING_ATOL,
            accounting_tolerance_rtol = ACCOUNTING_RTOL,
            interpretation = "accounting identity; B_C combines district seat weights, valid-vote weights, and coalition vote geography",
        ))
        for (check_name, residual) in (
            ("A_C + B_C = d_C", Float64(A_exact + B_exact - d_exact)),
            ("sum_i d_i = d_C", Float64(sum(members.d_exact) - d_exact)),
            ("sum_i A_i = A_C", Float64(sum(members.A_exact) - A_exact)),
            ("sum_i B_i = B_C", Float64(sum(members.B_exact) - B_exact)),
        )
            push!(validation_rows, (
                coalition_id = String(coalition.coalition_id),
                election_year = year,
                cabinet_period = period,
                check_name = check_name,
                exact_pass = residual == 0.0,
                floating_residual = residual,
                atol = ACCOUNTING_ATOL,
                rtol = ACCOUNTING_RTOL,
                status = "PASS",
            ))
        end
    end

    decomposition = DataFrame(decomposition_rows)
    party_contributions = DataFrame(party_rows)
    district_contributions = DataFrame(district_rows)
    validations = DataFrame(validation_rows)
    sort!(decomposition, [:election_year, :period_start, :cabinet_period])
    sort!(party_contributions, [:election_year, :cabinet_period, :d_i], rev = [false, false, true])
    sort!(district_contributions, [:election_year, :cabinet_period, :electoral_unit])

    extreme_rows = NamedTuple[]
    for group in groupby(party_contributions, [:coalition_id, :election_year, :cabinet_period])
        positive = group[argmax(group.d_i), :]
        negative = group[argmin(group.d_i), :]
        positive.d_i > 0 || error("$(positive.coalition_id): no positive party d_i contribution.")
        negative.d_i < 0 || error("$(negative.coalition_id): no negative party d_i contribution.")
        push!(extreme_rows, (
            coalition_id = String(positive.coalition_id),
            election_year = Int(positive.election_year),
            cabinet_period = String(positive.cabinet_period),
            largest_positive_party = String(positive.party),
            largest_positive_d_i = Float64(positive.d_i),
            largest_negative_party = String(negative.party),
            largest_negative_d_i = Float64(negative.d_i),
            accounting_qualification = qualification_for_year(Int(positive.election_year)),
        ))
    end
    party_extremes = DataFrame(extreme_rows)
    sort!(party_extremes, [:election_year, :cabinet_period])

    component_rows = NamedTuple[]
    for row in eachrow(decomposition)
        for (component, value) in (("A_C", row.A_C), ("B_C", row.B_C), ("d_C", row.d_C))
            push!(component_rows, (
                coalition_id = String(row.coalition_id),
                election_year = Int(row.election_year),
                cabinet_period = String(row.cabinet_period),
                component = component,
                seats = Float64(value),
            ))
        end
    end
    component_figure_data = DataFrame(component_rows)

    return (
        decomposition = decomposition,
        party_contributions = party_contributions,
        party_extremes = party_extremes,
        district_contributions = district_contributions,
        component_figure_data = component_figure_data,
        validations = validations,
    )
end

function latex_escape(value)
    replacements = Dict(
        '\\' => "\\textbackslash{}",
        '&' => "\\&",
        '%' => "\\%",
        '$' => "\\\$",
        '#' => "\\#",
        '_' => "\\_",
        '{' => "\\{",
        '}' => "\\}",
        '~' => "\\textasciitilde{}",
        '^' => "\\textasciicircum{}",
    )
    io = IOBuffer()
    for character in string(value)
        print(io, get(replacements, character, string(character)))
    end
    return String(take!(io))
end

fmt2(value) = @sprintf("%.2f", Float64(value))

function decomposition_latex(data::DataFrame)
    io = IOBuffer()
    println(io, "\\begin{landscape}")
    println(io, "\\begin{table}[p]")
    println(io, "\\centering")
    println(io, "\\caption{Accounting decomposition of the five observed cabinet inversions}")
    println(io, "\\label{tab:inversion-decomposition}")
    println(io, "\\scriptsize")
    println(io, "\\setlength{\\tabcolsep}{3.2pt}")
    println(io, "\\begin{tabular}{llrrrrrrr}")
    println(io, "\\toprule")
    println(io, "Election & Period & Vote \\% & Seats & \\(q_C\\) & \\(d_C\\) & \\(r_C\\) & \\(A_C\\) & \\(B_C\\) \\\\")
    println(io, "\\midrule")
    for row in eachrow(data)
        println(io,
            "$(row.election_year) & $(latex_escape(row.cabinet_period)) & " *
            "$(fmt2(row.vote_share_pct)) & $(row.s_C) & $(fmt2(row.q_C)) & " *
            "$(fmt2(row.d_C)) & $(fmt2(row.r_C)) & $(fmt2(row.A_C)) & $(fmt2(row.B_C)) \\\\",
        )
    end
    println(io, "\\bottomrule")
    println(io, "\\end{tabular}")
    println(io, "\\begin{minipage}{0.94\\linewidth}")
    println(io, "\\footnotesize Notes: \\(A_C\\) is the within-district allocation component. \\(B_C\\) is the between-district seat--vote weighting component; it combines district seat weights, turnout and valid-vote differences, and coalition vote geography. The equality \\(d_C=A_C+B_C\\) is an accounting identity, not a causal decomposition.")
    println(io, "\\end{minipage}")
    println(io, "\\end{table}")
    println(io, "\\end{landscape}")
    return String(take!(io))
end

function party_extremes_latex(data::DataFrame)
    io = IOBuffer()
    println(io, "\\begin{tabular}{lllrlr}")
    println(io, "\\toprule")
    println(io, "Election & Period & Largest positive & \\(d_i\\) & Largest negative & \\(d_i\\) \\\\")
    println(io, "\\midrule")
    for row in eachrow(data)
        println(io,
            "$(row.election_year) & $(latex_escape(row.cabinet_period)) & " *
            "$(latex_escape(row.largest_positive_party)) & $(fmt2(row.largest_positive_d_i)) & " *
            "$(latex_escape(row.largest_negative_party)) & $(fmt2(row.largest_negative_d_i)) \\\\",
        )
    end
    println(io, "\\bottomrule")
    println(io, "\\end{tabular}")
    return String(take!(io))
end

function sha256_file(path::AbstractString)
    return open(path, "r") do io
        bytes2hex(SHA.sha256(io))
    end
end

function write_csv(path::AbstractString, data::DataFrame)
    mkpath(dirname(path))
    CSV.write(path, data; quotestrings = true)
    return path
end

"""
    write_decomposition_outputs(output_root, coalition_periods, outputs)

Write machine-readable intermediaries first. The LaTeX assets are then created
only from CSVs read back from disk, preserving the pipeline boundary between
calculation and presentation.
"""
function write_decomposition_outputs(output_root::AbstractString, coalition_periods::DataFrame, outputs)
    raw_dir = joinpath(output_root, "raw")
    tables_dir = joinpath(output_root, "tables")
    figure_dir = joinpath(output_root, "figure_data")
    latex_dir = joinpath(output_root, "latex")
    audit_dir = joinpath(output_root, "audit")
    foreach(mkpath, (raw_dir, tables_dir, figure_dir, latex_dir, audit_dir))

    artifacts = NamedTuple[]
    function record_csv(relative_path, data, artifact_type, description)
        path = joinpath(output_root, relative_path)
        write_csv(path, data)
        push!(artifacts, (
            path = relative_path,
            artifact_type = artifact_type,
            description = description,
            rows = nrow(data),
            columns = length(names(data)),
            sha256 = sha256_file(path),
        ))
        return path
    end

    record_csv(
        "raw/coalition_period_quantities.csv",
        coalition_periods,
        "raw",
        "PSC-correct coalition quantities for every observed cabinet period.",
    )
    record_csv(
        "raw/inversion_decomposition.csv",
        outputs.decomposition,
        "raw",
        "Exact-audited A_C/B_C decomposition for the five observed inversions.",
    )
    record_csv(
        "raw/inversion_party_contributions.csv",
        outputs.party_contributions,
        "raw",
        "Full party-level d_i, A_i, and B_i vectors for the five observed inversions.",
    )
    record_csv(
        "raw/inversion_district_contributions.csv",
        outputs.district_contributions,
        "raw",
        "State-level a_Cd and b_Cd contributions for the five observed inversions.",
    )
    decomposition_table_path = record_csv(
        "tables/table_observed_inversion_decomposition.csv",
        outputs.decomposition,
        "table",
        "Manuscript summary of coalition quantities and decomposition components.",
    )
    extremes_table_path = record_csv(
        "tables/table_inversion_party_contribution_extremes.csv",
        outputs.party_extremes,
        "table",
        "Largest positive and negative party accounting contributions by inversion.",
    )
    record_csv(
        "figure_data/inversion_decomposition_components.csv",
        outputs.component_figure_data,
        "figure_data",
        "Long-form A_C, B_C, and d_C values for decomposition visualization.",
    )
    record_csv(
        "audit/decomposition_identity_checks.csv",
        outputs.validations,
        "audit",
        "Exact and floating-point decomposition identity results.",
    )

    decomposition_from_csv = CSV.read(decomposition_table_path, DataFrame)
    extremes_from_csv = CSV.read(extremes_table_path, DataFrame)
    latex_assets = (
        (
            "latex/table_observed_inversion_decomposition.tex",
            decomposition_latex(decomposition_from_csv),
            "Landscape manuscript table for the five-case accounting decomposition.",
            nrow(decomposition_from_csv),
            9,
        ),
        (
            "latex/table_inversion_party_contribution_extremes.tex",
            party_extremes_latex(extremes_from_csv),
            "Compact manuscript table of party contribution extremes.",
            nrow(extremes_from_csv),
            6,
        ),
    )
    for (relative_path, contents, description, rows, columns) in latex_assets
        path = joinpath(output_root, relative_path)
        mkpath(dirname(path))
        open(path, "w") do io
            write(io, contents)
        end
        push!(artifacts, (
            path = relative_path,
            artifact_type = "latex",
            description = description,
            rows = rows,
            columns = columns,
            sha256 = sha256_file(path),
        ))
    end

    manifest = DataFrame(artifacts)
    sort!(manifest, :path)
    manifest_path = joinpath(output_root, "artifact_manifest.csv")
    CSV.write(manifest_path, manifest)
    return manifest
end

function validate_ideological_counts(intervals::DataFrame)
    expected = Dict(2014 => (8, 4), 2018 => (0, 0), 2022 => (6, 2))
    rows = NamedTuple[]
    for year in sort(collect(keys(expected)))
        selected = intervals[Int.(intervals.election_year) .== year, :]
        inversions = sum(Bool.(selected.coalition_inversion))
        minimal = sum(Bool.(selected.minimal_connected_inversion))
        (inversions, minimal) == expected[year] || error(
            "Ideological regression changed for $(year): expected $(expected[year]), " *
            "found $((inversions, minimal)).",
        )
        push!(rows, (
            election_year = year,
            coalition_inversions = inversions,
            minimal_inversions = minimal,
            expected_coalition_inversions = expected[year][1],
            expected_minimal_inversions = expected[year][2],
            status = "PASS",
        ))
    end
    return DataFrame(rows)
end

end
