using Pkg

processing_root = normpath(joinpath(@__DIR__, ".."))
Pkg.activate(processing_root)

using CSV
using DataFrames

include(joinpath(processing_root, "src", "Processing.jl"))
using .Processing
import .Processing.AnalysisRunnerCore as ARC

# -----------------------------------------------------------------------------
# Simple visible paths
# -----------------------------------------------------------------------------

repo_root = normpath(joinpath(@__DIR__, "..", "..", ".."))
data_root = joinpath(repo_root, "data", "raw", "electionsBR")
coalition_json_path = joinpath(repo_root, "scraping", "output", "partidos_por_periodo.json")
classification_root_dir = joinpath(repo_root, "scrape_classification", "output")

party_mun_zone_2014_path = joinpath(data_root, "2014", "party_mun_zone.csv")
candidate_2014_path = joinpath(data_root, "2014", "candidate.csv")

party_mun_zone_2018_path = joinpath(data_root, "2018", "party_mun_zone.csv")
candidate_2018_path = joinpath(data_root, "2018", "candidate.csv")

party_mun_zone_2022_path = joinpath(data_root, "2022", "party_mun_zone.csv")
candidate_2022_path = joinpath(data_root, "2022", "candidate.csv")

expected_total_seats = 513

Processing.set_root!(data_root)
Processing.set_coalition_path!(coalition_json_path)

# -----------------------------------------------------------------------------
# Small visible overrides for the few labels the shared alias table does not yet
# cover in the exact way needed here.
# -----------------------------------------------------------------------------

electoral_party_overrides = Dict(
    2014 => Dict(
        "PATRIOTA" => "PEN",
    ),
    2018 => Dict{String,String}(),
    2022 => Dict(
        "AGIR" => "AGIR",
    ),
)

ideology_raw_overrides = Dict(
    2014 => Dict{String,String}(),
    2018 => Dict{String,String}(),
    2022 => Dict(
        "AGIR" => "AGIR",
        "CDD" => "CIDADANIA",
        "PROGRE" => "PP",
    ),
)

ideology_label_overrides = Dict(
    2014 => Dict(
        "AVANTE" => "PT DO B",
        "DC" => "PSDC",
        "MDB" => "PMDB",
        "PATRIOTA" => "PEN",
        "PODE" => "PTN",
    ),
    2018 => Dict{String,String}(),
    2022 => Dict{String,String}(),
)

ideology_drop_labels = Dict(
    2014 => Set(["NOVO", "PMB", "REDE"]),
    2018 => Set{String}(),
    2022 => Set{String}(),
)

# -----------------------------------------------------------------------------
# Small helper functions only. The main script below stays top-to-bottom.
# -----------------------------------------------------------------------------

function print_block(title)
    println()
    println(repeat("=", 100))
    println(title)
    println(repeat("=", 100))
end

function show_table(df::DataFrame)
    show(stdout, MIME("text/plain"), df; allrows = true, allcols = true, truncate = 0)
    println()
end

function majority_status(vote_majority::Bool, seat_majority::Bool)
    if vote_majority && seat_majority
        return "votes+seats"
    elseif seat_majority
        return "seats_only"
    elseif vote_majority
        return "votes_only"
    else
        return "neither"
    end
end

function canonical_party_for_election(raw_party, year; overrides = Dict{String,String}())
    raw_text = strip(String(raw_party))
    normalized = Processing.normalize_party(raw_text)

    if haskey(overrides, normalized)
        return overrides[normalized]
    end

    canonical = Processing.canonical_party(raw_text; year = year, strict = false)
    canonical != Processing.UNKNOWN_PARTY && return canonical

    error("Party alias not mapped for election year $(year): $(raw_text)")
end

function canonicalize_party_column!(df::DataFrame, year; col = :SG_PARTIDO, overrides = Dict{String,String}())
    raw_values = String.(df[!, col])
    unique_raw_values = unique(raw_values)

    mapped = Dict{String,String}()
    for raw_value in unique_raw_values
        mapped[raw_value] = canonical_party_for_election(raw_value, year; overrides = overrides)
    end

    df[!, col] = [mapped[raw_value] for raw_value in raw_values]
    return df
end

function load_votes_for_year(year, path; overrides = Dict{String,String}())
    needed_columns = Set([
        "DS_CARGO",
        "SG_UF",
        "SG_PARTIDO",
        "QT_VOTOS_NOMINAIS_VALIDOS",
        "QT_VOTOS_NOMINAIS",
        "QT_TOTAL_VOTOS_LEG_VALIDOS",
        "QT_VOTOS_LEGENDA_VALIDOS",
        "QT_VOTOS_LEGENDA",
        "QT_VOTOS",
        "TOTAL_VOTOS",
        "QT_VOTOS_VALIDOS",
    ])

    pmz = CSV.read(path, DataFrame; select = (index, name) -> String(name) in needed_columns)

    Processing.upper_strip!(pmz, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", pmz)

    Processing.stringify!(pmz, :SG_UF)
    Processing.stringify!(pmz, :SG_PARTIDO)
    canonicalize_party_column!(pmz, year; col = :SG_PARTIDO, overrides = overrides)

    vote_kwargs = ARC.vote_kwargs_for_year(year)
    nom_col, leg_col, total_col, scheme = Processing.detect_vote_cols(pmz; vote_kwargs...)

    votes = if scheme == :total
        total_col === nothing && error("Could not detect a total vote column for year $(year).")
        Processing.to_int.(pmz[!, total_col])
    else
        nom_col === nothing && error("Could not detect a nominal vote column for year $(year).")
        leg_col === nothing && error("Could not detect a legend vote column for year $(year).")
        Processing.to_int.(pmz[!, nom_col]) .+ Processing.to_int.(pmz[!, leg_col])
    end

    national_votes = combine(
        groupby(DataFrame(SG_PARTIDO = pmz.SG_PARTIDO, votes = votes), :SG_PARTIDO),
        :votes => sum => :valid_total,
    )

    sort!(national_votes, :valid_total, rev = true)
    return national_votes
end

function load_seats_for_year(year, path; overrides = Dict{String,String}())
    needed_columns = [
        :ANO_ELEICAO,
        :NR_TURNO,
        :SG_UF,
        :CD_CARGO,
        :DS_CARGO,
        :SG_PARTIDO,
        :DS_SIT_TOT_TURNO,
    ]

    candidates = CSV.read(path, DataFrame; select = needed_columns, normalizenames = true)

    Processing.upper_strip!(candidates, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", candidates)

    Processing.stringify!(candidates, :SG_UF)
    Processing.stringify!(candidates, :SG_PARTIDO)
    canonicalize_party_column!(candidates, year; col = :SG_PARTIDO, overrides = overrides)

    winner_status = uppercase.(strip.(String.(candidates.DS_SIT_TOT_TURNO)))
    candidates[!, :WINNER] = in.(winner_status, Ref(Processing.WINNER_STATUSES))

    national_seats = combine(
        groupby(candidates, :SG_PARTIDO),
        :WINNER => (winners -> sum(Int.(winners))) => :total_seats,
    )

    filter!(row -> row.total_seats > 0, national_seats)
    sort!(national_seats, :total_seats, rev = true)

    total_seats_found = sum(national_seats.total_seats)
    total_seats_found == expected_total_seats || error(
        "Expected $(expected_total_seats) seats for year $(year), found $(total_seats_found).",
    )

    return national_seats
end

function summarize_coalition(summary_df::DataFrame, parties::Vector{String})
    party_set = Set(parties)
    mask = in.(summary_df.SG_PARTIDO, Ref(party_set))

    total_votes = sum(summary_df.valid_total)
    total_seats = sum(summary_df.total_seats)

    coalition_votes = sum(summary_df.valid_total[mask])
    coalition_seats = sum(summary_df.total_seats[mask])
    vote_share = coalition_votes / total_votes
    quota = vote_share * total_seats
    seat_diff = coalition_seats - quota

    vote_majority = vote_share > 0.5
    seat_majority = coalition_seats > total_seats / 2
    candidate_inversion = seat_majority && !vote_majority

    return (
        votes = coalition_votes,
        vote_share = vote_share,
        seats = coalition_seats,
        quota = quota,
        seat_diff = seat_diff,
        vote_majority = vote_majority,
        seat_majority = seat_majority,
        majority_status = majority_status(vote_majority, seat_majority),
        candidate_inversion = candidate_inversion,
    )
end

function build_observed_coalition_table(summary_df::DataFrame, election_year, coalition_periods, coalition_windows)
    valid_election_parties = String.(summary_df.SG_PARTIDO)
    rows = NamedTuple[]

    for period in sort(collect(keys(coalition_periods)); by = Processing.period_sort_key)
        coalition_year = parse(Int, first(split(period, ".")))
        raw_parties = coalition_periods[period]

        cabinet_parties = Processing.canonicalize_parties(raw_parties; year = coalition_year, strict = true)
        election_space_parties = Processing.cabinet_parties_in_election_space(
            cabinet_parties;
            election_year = election_year,
            valid_election_parties = valid_election_parties,
        )

        metrics = summarize_coalition(summary_df, election_space_parties)
        period_start, period_end = coalition_windows[period]

        push!(rows, (
            coalition_year = coalition_year,
            period = period,
            period_start = period_start === nothing ? "" : string(period_start),
            period_end = period_end === nothing ? "" : string(period_end),
            parties = join(election_space_parties, ", "),
            votes = Int(metrics.votes),
            vote_share = metrics.vote_share,
            seats = Int(metrics.seats),
            quota = metrics.quota,
            seat_diff = metrics.seat_diff,
            vote_majority = metrics.vote_majority,
            seat_majority = metrics.seat_majority,
            majority_status = metrics.majority_status,
            candidate_inversion = metrics.candidate_inversion,
        ))
    end

    return DataFrame(rows)
end

function build_ideology_order(year, summary_df, classification_2023, classification_2025)
    source_df, source_name = if year == 2022
        (classification_2025, "2025")
    else
        (classification_2023, "2023")
    end

    raw_overrides = get(ideology_raw_overrides, year, Dict{String,String}())
    label_overrides = get(ideology_label_overrides, year, Dict{String,String}())
    drop_labels = get(ideology_drop_labels, year, Set{String}())

    rows = NamedTuple[]
    for row in eachrow(source_df)
        raw_party = strip(String(row.party_name_raw))
        normalized_raw = Processing.normalize_party(raw_party)

        translated_party = Processing.canonical_party(raw_party; year = year, strict = false)
        if translated_party == Processing.UNKNOWN_PARTY
            haskey(raw_overrides, normalized_raw) || continue
            translated_party = raw_overrides[normalized_raw]
        end

        translated_party = get(label_overrides, translated_party, translated_party)
        translated_party in drop_labels && continue

        push!(rows, (
            SG_PARTIDO = translated_party,
            ordinal_position = Int(row.ordinal_position),
            classification_label = String(coalesce(row.classification_label, "")),
            ideology_value_numeric = row.ideology_value_numeric,
            ideology_source = source_name,
            source_party_raw = raw_party,
        ))
    end

    ideology_df = DataFrame(rows)
    duplicate_counts = combine(groupby(ideology_df, :SG_PARTIDO), nrow => :n)
    if any(duplicate_counts.n .> 1)
        duplicate_parties = duplicate_counts.SG_PARTIDO[duplicate_counts.n .> 1]
        error("Duplicate ideology rows after translation for year $(year): $(join(duplicate_parties, ", "))")
    end

    ideology_df = innerjoin(ideology_df, select(summary_df, :SG_PARTIDO), on = :SG_PARTIDO)
    sort!(ideology_df, :ordinal_position)

    missing_parties = setdiff(sort(String.(summary_df.SG_PARTIDO)), sort(String.(ideology_df.SG_PARTIDO)))
    isempty(missing_parties) || error(
        "Parties without ideology position for year $(year): $(join(missing_parties, ", "))",
    )

    return ideology_df
end

function build_ideology_sweep_table(summary_df::DataFrame, ideology_df::DataFrame)
    ordered = innerjoin(
        ideology_df,
        select(summary_df, :SG_PARTIDO, :valid_total, :total_seats),
        on = :SG_PARTIDO,
    )
    sort!(ordered, :ordinal_position)

    total_votes = sum(ordered.valid_total)
    total_seats = sum(ordered.total_seats)

    rows = NamedTuple[]
    for start_index in 1:nrow(ordered)
        parties = String[]
        coalition_votes = 0.0
        coalition_seats = 0
        end_index = nothing

        for current_index in start_index:nrow(ordered)
            push!(parties, ordered.SG_PARTIDO[current_index])
            coalition_votes += ordered.valid_total[current_index]
            coalition_seats += ordered.total_seats[current_index]

            # For this sweep I stop at the first seat majority, because the
            # inversion question here is "seat majority without vote majority".
            if coalition_seats > total_seats / 2
                end_index = current_index
                break
            end
        end

        vote_share = coalition_votes / total_votes
        quota = vote_share * total_seats
        seat_diff = coalition_seats - quota
        vote_majority = vote_share > 0.5
        seat_majority = coalition_seats > total_seats / 2
        candidate_inversion = seat_majority && !vote_majority

        push!(rows, (
            start_party = ordered.SG_PARTIDO[start_index],
            end_party = end_index === nothing ? missing : ordered.SG_PARTIDO[end_index],
            parties = join(parties, ", "),
            votes = Int(round(coalition_votes)),
            vote_share = vote_share,
            seats = coalition_seats,
            quota = quota,
            seat_diff = seat_diff,
            vote_majority = vote_majority,
            seat_majority = seat_majority,
            majority_status = majority_status(vote_majority, seat_majority),
            candidate_inversion = candidate_inversion,
            reached_seat_majority = seat_majority,
            note = seat_majority ? "first seat majority" : "never reaches seat majority",
        ))
    end

    return DataFrame(rows)
end

# =============================================================================
# PART 1. LOAD DATA
# =============================================================================

print_block("PART 1. LOAD DATA")

println("repo_root: ", repo_root)
println("data_root: ", data_root)
println("coalition_json_path: ", coalition_json_path)
println("classification_root_dir: ", classification_root_dir)

println()
println("Loading ideology / party-classification data...")
classification_2023 = Processing.load_party_classification(2023; root_dir = classification_root_dir)
classification_2025 = Processing.load_party_classification(2025; root_dir = classification_root_dir)

println("classification_2023 rows: ", nrow(classification_2023))
println("classification_2025 rows: ", nrow(classification_2025))
println("classification_2023 parties: ", join(String.(classification_2023.party_name_raw), ", "))
println("classification_2025 parties: ", join(String.(classification_2025.party_name_raw), ", "))

println()
println("Loading electoral data for 2014...")
votes_2014 = load_votes_for_year(2014, party_mun_zone_2014_path; overrides = electoral_party_overrides[2014])
seats_2014 = load_seats_for_year(2014, candidate_2014_path; overrides = electoral_party_overrides[2014])
println("2014 vote parties ($(nrow(votes_2014))): ", join(String.(votes_2014.SG_PARTIDO), ", "))
println("2014 seat parties ($(nrow(seats_2014))): ", join(String.(seats_2014.SG_PARTIDO), ", "))
println("2014 total valid votes: ", sum(votes_2014.valid_total))
println("2014 total seats: ", sum(seats_2014.total_seats))

println()
println("Loading electoral data for 2018...")
votes_2018 = load_votes_for_year(2018, party_mun_zone_2018_path; overrides = electoral_party_overrides[2018])
seats_2018 = load_seats_for_year(2018, candidate_2018_path; overrides = electoral_party_overrides[2018])
println("2018 vote parties ($(nrow(votes_2018))): ", join(String.(votes_2018.SG_PARTIDO), ", "))
println("2018 seat parties ($(nrow(seats_2018))): ", join(String.(seats_2018.SG_PARTIDO), ", "))
println("2018 total valid votes: ", sum(votes_2018.valid_total))
println("2018 total seats: ", sum(seats_2018.total_seats))

println()
println("Loading electoral data for 2022...")
votes_2022 = load_votes_for_year(2022, party_mun_zone_2022_path; overrides = electoral_party_overrides[2022])
seats_2022 = load_seats_for_year(2022, candidate_2022_path; overrides = electoral_party_overrides[2022])
println("2022 vote parties ($(nrow(votes_2022))): ", join(String.(votes_2022.SG_PARTIDO), ", "))
println("2022 seat parties ($(nrow(seats_2022))): ", join(String.(seats_2022.SG_PARTIDO), ", "))
println("2022 total valid votes: ", sum(votes_2022.valid_total))
println("2022 total seats: ", sum(seats_2022.total_seats))

println()
println("Loading coalition data...")
coalition_periods_raw = Processing.coalitions_by_period_raw(; path = coalition_json_path)
coalition_windows = Processing.coalition_period_windows(; path = coalition_json_path)

coalitions_2015 = Processing.coalitions_by_year(coalition_periods_raw, 2015; path = coalition_json_path)
coalitions_2016 = Processing.coalitions_by_year(coalition_periods_raw, 2016; path = coalition_json_path)
coalitions_2017 = Processing.coalitions_by_year(coalition_periods_raw, 2017; path = coalition_json_path)
coalitions_2018_mandate = Processing.coalitions_by_year(coalition_periods_raw, 2018; path = coalition_json_path)
coalitions_for_2014_election = merge(coalitions_2015, coalitions_2016, coalitions_2017, coalitions_2018_mandate)

coalitions_2019 = Processing.coalitions_by_year(coalition_periods_raw, 2019; path = coalition_json_path)
coalitions_2020 = Processing.coalitions_by_year(coalition_periods_raw, 2020; path = coalition_json_path)
coalitions_2021 = Processing.coalitions_by_year(coalition_periods_raw, 2021; path = coalition_json_path)
coalitions_2022_mandate = Processing.coalitions_by_year(coalition_periods_raw, 2022; path = coalition_json_path)
coalitions_for_2018_election = merge(coalitions_2019, coalitions_2020, coalitions_2021, coalitions_2022_mandate)

coalitions_2023 = Processing.coalitions_by_year(coalition_periods_raw, 2023; path = coalition_json_path)
coalitions_2024 = Processing.coalitions_by_year(coalition_periods_raw, 2024; path = coalition_json_path)
coalitions_2025 = Processing.coalitions_by_year(coalition_periods_raw, 2025; path = coalition_json_path)
coalitions_for_2022_election = merge(coalitions_2023, coalitions_2024, coalitions_2025)

println("Coalition periods linked to the 2014 election: ", join(sort(collect(keys(coalitions_for_2014_election)); by = Processing.period_sort_key), ", "))
println("Coalition periods linked to the 2018 election: ", join(sort(collect(keys(coalitions_for_2018_election)); by = Processing.period_sort_key), ", "))
println("Coalition periods linked to the 2022 election: ", join(sort(collect(keys(coalitions_for_2022_election)); by = Processing.period_sort_key), ", "))

party_summary_2014 = Processing.party_summary(votes_2014, seats_2014; expected_total_seats = expected_total_seats)
party_summary_2018 = Processing.party_summary(votes_2018, seats_2018; expected_total_seats = expected_total_seats)
party_summary_2022 = Processing.party_summary(votes_2022, seats_2022; expected_total_seats = expected_total_seats)

loaded = Dict(
    2014 => (summary = party_summary_2014, coalitions = coalitions_for_2014_election),
    2018 => (summary = party_summary_2018, coalitions = coalitions_for_2018_election),
    2022 => (summary = party_summary_2022, coalitions = coalitions_for_2022_election),
)

# =============================================================================
# PART 2. ANALYZE SEAT DIFFERENTIALS
# =============================================================================

print_block("PART 2. ANALYZE SEAT DIFFERENTIALS")

year = 2014
summary_df = sort(copy(loaded[year].summary), :seat_diff, rev = true)
coalition_periods = loaded[year].coalitions

print_block("YEAR $(year)")
println("Total valid votes: ", sum(summary_df.valid_total))
println("Total seats: ", sum(summary_df.total_seats))
println("Parties in summary: ", join(sort(String.(summary_df.SG_PARTIDO)), ", "))

println()
println("A. Party-level seat differentials")
party_table = select(
    summary_df,
    :SG_PARTIDO => :party,
    :valid_total => :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :total_seats => :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
)
show_table(party_table)

println()
println("B. Observed coalition seat differentials")
observed_table = build_observed_coalition_table(summary_df, year, coalition_periods, coalition_windows)
observed_table = select(
    observed_table,
    :coalition_year,
    :period,
    :period_start,
    :period_end,
    :parties,
    :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
    :vote_majority,
    :seat_majority,
    :majority_status,
    :candidate_inversion,
)
show_table(observed_table)

println()
println("C. Minimal ideology-based coalitions, sweeping left to right")
ideology_df = build_ideology_order(year, summary_df, classification_2023, classification_2025)
println("Ideology source used for $(year): ", first(ideology_df.ideology_source))

ideology_order_table = select(
    ideology_df,
    :ordinal_position,
    :SG_PARTIDO => :party,
    :classification_label => :label,
    :ideology_value_numeric => ByRow(x -> x === missing ? missing : round(Float64(x), digits = 2)) => :score,
    :source_party_raw,
)

println("Ordered parties from left to right:")
show_table(ideology_order_table)

println("Sweep rule used below: start at each party and stop at the first seat majority.")
ideology_sweep_table = build_ideology_sweep_table(summary_df, ideology_df)
ideology_sweep_table = select(
    ideology_sweep_table,
    :start_party,
    :end_party,
    :parties,
    :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
    :vote_majority,
    :seat_majority,
    :majority_status,
    :candidate_inversion,
    :reached_seat_majority,
    :note,
)
show_table(ideology_sweep_table)

year = 2018

summary_df = sort(copy(loaded[year].summary), :seat_diff, rev = true)
coalition_periods = loaded[year].coalitions

print_block("YEAR $(year)")
println("Total valid votes: ", sum(summary_df.valid_total))
println("Total seats: ", sum(summary_df.total_seats))
println("Parties in summary: ", join(sort(String.(summary_df.SG_PARTIDO)), ", "))

println()
println("A. Party-level seat differentials")
party_table = select(
    summary_df,
    :SG_PARTIDO => :party,
    :valid_total => :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :total_seats => :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
)
show_table(party_table)

println()
println("B. Observed coalition seat differentials")
observed_table = build_observed_coalition_table(summary_df, year, coalition_periods, coalition_windows)
observed_table = select(
    observed_table,
    :coalition_year,
    :period,
    :period_start,
    :period_end,
    :parties,
    :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
    :vote_majority,
    :seat_majority,
    :majority_status,
    :candidate_inversion,
)
show_table(observed_table)

println()
println("C. Minimal ideology-based coalitions, sweeping left to right")
ideology_df = build_ideology_order(year, summary_df, classification_2023, classification_2025)
println("Ideology source used for $(year): ", first(ideology_df.ideology_source))

ideology_order_table = select(
    ideology_df,
    :ordinal_position,
    :SG_PARTIDO => :party,
    :classification_label => :label,
    :ideology_value_numeric => ByRow(x -> x === missing ? missing : round(Float64(x), digits = 2)) => :score,
    :source_party_raw,
)

println("Ordered parties from left to right:")
show_table(ideology_order_table)

println("Sweep rule used below: start at each party and stop at the first seat majority.")
ideology_sweep_table = build_ideology_sweep_table(summary_df, ideology_df)
ideology_sweep_table = select(
    ideology_sweep_table,
    :start_party,
    :end_party,
    :parties,
    :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
    :vote_majority,
    :seat_majority,
    :majority_status,
    :candidate_inversion,
    :reached_seat_majority,
    :note,
)
show_table(ideology_sweep_table)

year = 2022

summary_df = sort(copy(loaded[year].summary), :seat_diff, rev = true)
coalition_periods = loaded[year].coalitions

print_block("YEAR $(year)")
println("Total valid votes: ", sum(summary_df.valid_total))
println("Total seats: ", sum(summary_df.total_seats))
println("Parties in summary: ", join(sort(String.(summary_df.SG_PARTIDO)), ", "))

println()
println("A. Party-level seat differentials")
party_table = select(
    summary_df,
    :SG_PARTIDO => :party,
    :valid_total => :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :total_seats => :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
)
show_table(party_table)

println()
println("B. Observed coalition seat differentials")
observed_table = build_observed_coalition_table(summary_df, year, coalition_periods, coalition_windows)
observed_table = select(
    observed_table,
    :coalition_year,
    :period,
    :period_start,
    :period_end,
    :parties,
    :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
    :vote_majority,
    :seat_majority,
    :majority_status,
    :candidate_inversion,
)
show_table(observed_table)

println()
println("C. Minimal ideology-based coalitions, sweeping left to right")
ideology_df = build_ideology_order(year, summary_df, classification_2023, classification_2025)
println("Ideology source used for $(year): ", first(ideology_df.ideology_source))

ideology_order_table = select(
    ideology_df,
    :ordinal_position,
    :SG_PARTIDO => :party,
    :classification_label => :label,
    :ideology_value_numeric => ByRow(x -> x === missing ? missing : round(Float64(x), digits = 2)) => :score,
    :source_party_raw,
)

println("Ordered parties from left to right:")
show_table(ideology_order_table)

println("Sweep rule used below: start at each party and stop at the first seat majority.")
ideology_sweep_table = build_ideology_sweep_table(summary_df, ideology_df)
ideology_sweep_table = select(
    ideology_sweep_table,
    :start_party,
    :end_party,
    :parties,
    :votes,
    :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
    :seats,
    :quota => ByRow(x -> round(x, digits = 2)) => :quota,
    :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
    :vote_majority,
    :seat_majority,
    :majority_status,
    :candidate_inversion,
    :reached_seat_majority,
    :note,
)
show_table(ideology_sweep_table)
