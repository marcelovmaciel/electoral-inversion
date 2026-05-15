using Pkg

# =============================================================================
# BLOCK 0. PROJECT ACTIVATION AND IMPORTS
# =============================================================================

running_dir = @__DIR__
processing_root = normpath(joinpath(running_dir, ".."))
repo_root = normpath(joinpath(processing_root, "..", ".."))

Pkg.activate(processing_root)

using CSV
using DataFrames
using Dates
using Statistics

include(joinpath(processing_root, "src", "Processing.jl"))
using .Processing
import .Processing.AnalysisRunnerCore as ARC

println("Paper-oriented Processing runner")
println("running_dir: ", running_dir)
println("processing_root: ", processing_root)
println("repo_root: ", repo_root)

# =============================================================================
# BLOCK 1. PATHS AND CONFIGURATION
# =============================================================================

data_root = joinpath(repo_root, "data", "raw", "electionsBR")
coalition_json_path = joinpath(repo_root, "scraping", "output", "partidos_por_periodo.json")
classification_root_dir = joinpath(repo_root, "scrape_classification", "output")
party_alias_path = joinpath(processing_root, "data", "party_aliases.csv")
cabinet_crosswalk_path = joinpath(processing_root, "data", "cabinet_to_election_party_crosswalk.csv")

paper_output_root = joinpath(processing_root, "output", "paper")
raw_dir = joinpath(paper_output_root, "raw")
diagnostics_dir = joinpath(paper_output_root, "diagnostics")
tables_dir = joinpath(paper_output_root, "tables")
figure_data_dir = joinpath(paper_output_root, "figure_data")
artifact_manifest_path = joinpath(paper_output_root, "artifact_manifest.csv")

analysis_years = [2014, 2018, 2022]
expected_total_seats = 513
seat_majority_threshold = 257
ALLOW_OVERWRITE = false

party_mun_zone_paths = Dict(
    2014 => joinpath(data_root, "2014", "party_mun_zone.csv"),
    2018 => joinpath(data_root, "2018", "party_mun_zone.csv"),
    2022 => joinpath(data_root, "2022", "party_mun_zone.csv"),
)
candidate_paths = Dict(
    2014 => joinpath(data_root, "2014", "candidate.csv"),
    2018 => joinpath(data_root, "2018", "candidate.csv"),
    2022 => joinpath(data_root, "2022", "candidate.csv"),
)

Processing.set_root!(data_root)
Processing.set_coalition_path!(coalition_json_path)

mkpath(raw_dir)
mkpath(diagnostics_dir)
mkpath(tables_dir)
mkpath(figure_data_dir)

println("data_root: ", data_root)
println("coalition_json_path: ", coalition_json_path)
println("classification_root_dir: ", classification_root_dir)
println("paper_output_root: ", paper_output_root)
println("ALLOW_OVERWRITE: ", ALLOW_OVERWRITE)

# =============================================================================
# BLOCK 2. SMALL HELPERS
# =============================================================================

artifact_records = NamedTuple[]

electoral_party_overrides = Dict(
    2014 => Dict{String,String}(),
    2018 => Dict{String,String}(),
    2022 => Dict{String,String}(),
)

ideology_raw_overrides = Dict(
    2014 => Dict{String,String}(),
    2018 => Dict{String,String}(),
    2022 => Dict{String,String}(),
)

ideology_label_overrides = Dict(
    2014 => Dict(
        "AVANTE" => "PT DO B",
        "DC" => "PSDC",
        "MDB" => "PMDB",
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

function print_block(title)
    println()
    println(repeat("=", 100))
    println(title)
    println(repeat("=", 100))
end

function show_table(df)
    show(stdout, MIME("text/plain"), df; allrows = true, allcols = true, truncate = 0)
    println()
end

function write_paper_csv(path, df; allow_overwrite = ALLOW_OVERWRITE)
    if isfile(path) && !allow_overwrite
        error("Output exists and ALLOW_OVERWRITE is false: $(path)")
    end
    mkpath(dirname(path))
    CSV.write(path, df)
    return path
end

function record_artifact!(path, artifact_type, description, df)
    push!(artifact_records, (
        path = relpath(path, paper_output_root),
        artifact_type = String(artifact_type),
        description = String(description),
        rows = nrow(df),
        columns = length(names(df)),
    ))
    return path
end

function write_artifact_csv(path, df, artifact_type, description; allow_overwrite = ALLOW_OVERWRITE)
    write_paper_csv(path, df; allow_overwrite = allow_overwrite)
    record_artifact!(path, artifact_type, description, df)
    return path
end

function preflight_table()
    rows = NamedTuple[]
    push!(rows, (item = "repo root", path = repo_root, kind = "dir", exists = isdir(repo_root), essential = true))
    push!(rows, (item = "processing root", path = processing_root, kind = "dir", exists = isdir(processing_root), essential = true))
    push!(rows, (item = "data root", path = data_root, kind = "dir", exists = isdir(data_root), essential = true))
    push!(rows, (item = "coalition JSON", path = coalition_json_path, kind = "file", exists = isfile(coalition_json_path), essential = true))
    push!(rows, (item = "classification root", path = classification_root_dir, kind = "dir", exists = isdir(classification_root_dir), essential = true))
    push!(rows, (item = "party aliases", path = party_alias_path, kind = "file", exists = isfile(party_alias_path), essential = true))
    push!(rows, (item = "cabinet crosswalk", path = cabinet_crosswalk_path, kind = "file", exists = isfile(cabinet_crosswalk_path), essential = true))
    for year in analysis_years
        push!(rows, (item = "$(year) party_mun_zone.csv", path = party_mun_zone_paths[year], kind = "file", exists = isfile(party_mun_zone_paths[year]), essential = true))
        push!(rows, (item = "$(year) candidate.csv", path = candidate_paths[year], kind = "file", exists = isfile(candidate_paths[year]), essential = true))
    end
    return DataFrame(rows)
end

function validate_total_seats!(df, year)
    seat_col = hasproperty(df, :total_seats) ? :total_seats : :seats
    total_seats = sum(Int.(coalesce.(df[!, seat_col], 0)))
    total_seats == expected_total_seats || error("Expected $(expected_total_seats) seats for $(year), found $(total_seats).")
    return true
end

function validate_seat_diff_sum!(df, year)
    seat_diff_sum = sum(Float64.(coalesce.(df.seat_diff, 0.0)))
    isapprox(seat_diff_sum, 0.0; atol = 1e-6) || error("Seat differentials do not sum to zero for $(year): $(seat_diff_sum).")
    return true
end

function majority_status(vote_majority, seat_majority)
    if vote_majority && seat_majority
        return "votes+seats"
    elseif vote_majority
        return "votes_only"
    elseif seat_majority
        return "seats_only"
    end
    return "neither"
end

pct(x; digits = 2) = round(100 * Float64(x), digits = digits)
display_round(x; digits = 2) = round(Float64(x), digits = digits)

function vcat_or_empty(tables)
    isempty(tables) && return DataFrame()
    return vcat(tables...; cols = :union)
end

function mapping_source(raw_party, year; overrides = Dict{String,String}())
    normalized = Processing.normalize_party(raw_party)
    haskey(overrides, normalized) && return "local_override"
    canonical = Processing.canonical_party(raw_party; year = year, strict = false)
    canonical == Processing.UNKNOWN_PARTY && return "unmapped"
    return "shared_alias"
end

function party_mapping_table(raw_values, year; source, context = "", overrides = Dict{String,String}())
    rows = NamedTuple[]
    for raw in sort(unique(String.(raw_values)))
        normalized = Processing.normalize_party(raw)
        canonical = haskey(overrides, normalized) ? overrides[normalized] : Processing.canonical_party(raw; year = year, strict = false)
        push!(rows, (
            source = source,
            election_year = Int(year),
            context = context,
            raw_party = raw,
            normalized_party = normalized,
            canonical_party = canonical,
            mapping_source = mapping_source(raw, year; overrides = overrides),
        ))
    end
    return DataFrame(rows)
end

function append_mapping!(target, raw_values, year; source, context = "", overrides = Dict{String,String}())
    push!(target, party_mapping_table(raw_values, year; source = source, context = context, overrides = overrides))
    return target
end

function canonical_party_for_election(raw_party, year; overrides = Dict{String,String}())
    raw_text = strip(String(raw_party))
    normalized = Processing.normalize_party(raw_text)
    haskey(overrides, normalized) && return overrides[normalized]
    canonical = Processing.canonical_party(raw_text; year = year, strict = false)
    canonical != Processing.UNKNOWN_PARTY && return canonical
    error("Party alias not mapped for election year $(year): $(raw_text)")
end

function canonicalize_party_column!(df, year; col = :SG_PARTIDO, overrides = Dict{String,String}())
    raw_values = String.(df[!, col])
    mapped = Dict(raw => canonical_party_for_election(raw, year; overrides = overrides) for raw in unique(raw_values))
    df[!, col] = [mapped[raw] for raw in raw_values]
    return df
end

function inclusive_days(start_date::Date, end_date::Date)
    end_date < start_date && error("Invalid date interval: $(start_date) > $(end_date).")
    return Dates.value(end_date - start_date) + 1
end

function overlap_days(start_a::Date, end_a::Date, start_b::Date, end_b::Date)
    start_overlap = max(start_a, start_b)
    end_overlap = min(end_a, end_b)
    end_overlap < start_overlap && return 0
    return inclusive_days(start_overlap, end_overlap)
end

function summarize_coalition(summary_df, parties)
    party_set = Set(String.(parties))
    available = Set(String.(summary_df.SG_PARTIDO))
    missing_parties = setdiff(sort(collect(party_set)), sort(collect(available)))
    isempty(missing_parties) || error("Coalition party absent from election summary: $(join(missing_parties, ", "))")
    mask = in.(String.(summary_df.SG_PARTIDO), Ref(party_set))
    total_votes = sum(summary_df.valid_total)
    coalition_votes = sum(summary_df.valid_total[mask])
    coalition_seats = sum(summary_df.total_seats[mask])
    vote_share = coalition_votes / total_votes
    seat_share = coalition_seats / expected_total_seats
    quota = expected_total_seats * vote_share
    seat_diff = coalition_seats - quota
    vote_majority = vote_share > 0.5
    seat_majority = coalition_seats >= seat_majority_threshold
    return (
        votes = Int(coalition_votes),
        vote_share = Float64(vote_share),
        seats = Int(coalition_seats),
        seat_share = Float64(seat_share),
        quota = Float64(quota),
        seat_diff = Float64(seat_diff),
        vote_majority = Bool(vote_majority),
        seat_majority = Bool(seat_majority),
        majority_status = majority_status(vote_majority, seat_majority),
        coalition_inversion = Bool(seat_majority && !vote_majority),
    )
end

# =============================================================================
# BLOCK 3. PREFLIGHT CHECKS
# =============================================================================

print_block("BLOCK 3. PREFLIGHT CHECKS")
preflight_checks = preflight_table()
show_table(preflight_checks)
write_artifact_csv(joinpath(diagnostics_dir, "preflight_checks.csv"), preflight_checks, "diagnostic", "Required input files and directories for the paper runner.")
missing_essential = preflight_checks[(preflight_checks.essential .== true) .& (preflight_checks.exists .== false), :]
nrow(missing_essential) == 0 || error("Preflight failed; missing required input(s): $(join(String.(missing_essential.path), ", "))")

# =============================================================================
# BLOCK 4. MAPPING AND VOTE-COLUMN DIAGNOSTICS
# =============================================================================

print_block("BLOCK 4. MAPPING AND VOTE-COLUMN DIAGNOSTICS")
vote_mapping_tables = DataFrame[]
seat_mapping_tables = DataFrame[]
coalition_mapping_tables = DataFrame[]
ideology_mapping_tables = DataFrame[]
cabinet_translation_tables = DataFrame[]
vote_consistency_tables = DataFrame[]

function selected_vote_component_diagnostic(pmz, year)
    vote_kwargs = ARC.vote_kwargs_for_year(year)
    nom_col, leg_col, total_col, scheme = Processing.detect_vote_cols(pmz; vote_kwargs...)
    expected_nom = year == 2014 ? "QT_VOTOS_NOMINAIS" : "QT_VOTOS_NOMINAIS_VALIDOS"
    expected_leg = year == 2014 ? "QT_VOTOS_LEGENDA" : "QT_TOTAL_VOTOS_LEG_VALIDOS"
    String(nom_col) == expected_nom || error("Wrong nominal vote column for $(year): selected $(nom_col), expected $(expected_nom).")
    String(leg_col) == expected_leg || error("Wrong legend vote column for $(year): selected $(leg_col), expected $(expected_leg).")
    if year in (2018, 2022)
        String(leg_col) != "QT_VOTOS_LEGENDA_VALIDOS" || error("Regressed to QT_VOTOS_LEGENDA_VALIDOS for $(year); expected QT_TOTAL_VOTOS_LEG_VALIDOS.")
    end
    selected_sum = scheme == :total ? sum(Processing.to_int.(pmz[!, total_col])) : sum(Processing.to_int.(pmz[!, nom_col]) .+ Processing.to_int.(pmz[!, leg_col]))
    rows = NamedTuple[]
    direct_legend_col = Processing.pick_col(pmz, "QT_VOTOS_LEGENDA_VALIDOS")
    total_legend_col = Processing.pick_col(pmz, "QT_TOTAL_VOTOS_LEG_VALIDOS")
    converted_col = nothing
    for candidate_col in ["QT_VOTOS_NOMINAIS_CONVR_LEG", "QT_VOTOS_NOM_CONVR_LEG_VALIDOS"]
        converted_col = Processing.pick_col(pmz, candidate_col)
        converted_col === nothing || break
    end
    if direct_legend_col !== nothing && total_legend_col !== nothing && converted_col !== nothing
        direct_sum = sum(Processing.to_int.(pmz[!, direct_legend_col]))
        total_legend_sum = sum(Processing.to_int.(pmz[!, total_legend_col]))
        converted_sum = sum(Processing.to_int.(pmz[!, converted_col]))
        push!(rows, (election_year = Int(year), selected_nominal_col = String(nom_col), selected_legend_col = String(leg_col), selected_total_col = total_col === nothing ? "" : String(total_col), selected_scheme = String(scheme), check_name = "total_legend_equals_direct_plus_converted", lhs_column = String(total_legend_col), lhs_sum = total_legend_sum, rhs_columns = string(direct_legend_col, " + ", converted_col), rhs_sum = direct_sum + converted_sum, difference = total_legend_sum - direct_sum - converted_sum, selected_component_sum = selected_sum))
    end
    if nom_col !== nothing && leg_col !== nothing && direct_legend_col !== nothing && direct_legend_col != leg_col
        selected_sum_int = sum(Processing.to_int.(pmz[!, nom_col]) .+ Processing.to_int.(pmz[!, leg_col]))
        legacy_sum = sum(Processing.to_int.(pmz[!, nom_col]) .+ Processing.to_int.(pmz[!, direct_legend_col]))
        push!(rows, (election_year = Int(year), selected_nominal_col = String(nom_col), selected_legend_col = String(leg_col), selected_total_col = total_col === nothing ? "" : String(total_col), selected_scheme = String(scheme), check_name = "selected_components_vs_direct_legend_components", lhs_column = string(nom_col, " + ", leg_col), lhs_sum = selected_sum_int, rhs_columns = string(nom_col, " + ", direct_legend_col), rhs_sum = legacy_sum, difference = selected_sum_int - legacy_sum, selected_component_sum = selected_sum))
    end
    if isempty(rows)
        push!(rows, (election_year = Int(year), selected_nominal_col = String(nom_col), selected_legend_col = String(leg_col), selected_total_col = total_col === nothing ? "" : String(total_col), selected_scheme = String(scheme), check_name = "selected_components_available", lhs_column = "", lhs_sum = missing, rhs_columns = "", rhs_sum = missing, difference = missing, selected_component_sum = selected_sum))
    end
    return DataFrame(rows)
end

function inspect_vote_inputs_for_year(year, path)
    needed_columns = Set(["DS_CARGO", "SG_UF", "SG_PARTIDO", "QT_VOTOS_NOMINAIS_VALIDOS", "QT_VOTOS_NOMINAIS", "QT_TOTAL_VOTOS_LEG_VALIDOS", "QT_VOTOS_LEGENDA_VALIDOS", "QT_VOTOS_LEGENDA", "QT_VOTOS_NOMINAIS_CONVR_LEG", "QT_VOTOS_NOM_CONVR_LEG_VALIDOS", "QT_VOTOS", "TOTAL_VOTOS", "QT_VOTOS_VALIDOS"])
    pmz = CSV.read(path, DataFrame; select = (index, name) -> String(name) in needed_columns)
    Processing.upper_strip!(pmz, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", pmz)
    Processing.stringify!(pmz, :SG_PARTIDO)
    append_mapping!(vote_mapping_tables, pmz.SG_PARTIDO, year; source = "votes", context = basename(path), overrides = electoral_party_overrides[year])
    push!(vote_consistency_tables, selected_vote_component_diagnostic(pmz, year))
    return pmz
end

function inspect_seat_inputs_for_year(year, path)
    needed_columns = [:ANO_ELEICAO, :NR_TURNO, :SG_UF, :CD_CARGO, :DS_CARGO, :SG_PARTIDO, :DS_SIT_TOT_TURNO]
    candidates = CSV.read(path, DataFrame; select = needed_columns, normalizenames = true)
    Processing.upper_strip!(candidates, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", candidates)
    Processing.stringify!(candidates, :SG_PARTIDO)
    append_mapping!(seat_mapping_tables, candidates.SG_PARTIDO, year; source = "seats", context = basename(path), overrides = electoral_party_overrides[year])
    return candidates
end

inspect_vote_inputs_for_year(2014, party_mun_zone_paths[2014])
inspect_vote_inputs_for_year(2018, party_mun_zone_paths[2018])
inspect_vote_inputs_for_year(2022, party_mun_zone_paths[2022])
inspect_seat_inputs_for_year(2014, candidate_paths[2014])
inspect_seat_inputs_for_year(2018, candidate_paths[2018])
inspect_seat_inputs_for_year(2022, candidate_paths[2022])
vote_column_consistency = vcat_or_empty(vote_consistency_tables)
println("Vote-column consistency:")
show_table(vote_column_consistency)

# =============================================================================
# BLOCK 5. LOAD IDEOLOGY CLASSIFICATIONS
# =============================================================================

print_block("BLOCK 5. LOAD IDEOLOGY CLASSIFICATIONS")
classification_2023 = Processing.load_party_classification(2023; root_dir = classification_root_dir)
classification_2025 = Processing.load_party_classification(2025; root_dir = classification_root_dir)
println("Classification rule: 2014 -> 2023; 2018 -> 2023; 2022 -> 2025.")
println("classification_2023 parties: ", join(String.(classification_2023.party_name_raw), ", "))
println("classification_2025 parties: ", join(String.(classification_2025.party_name_raw), ", "))

# =============================================================================
# BLOCK 6. LOAD ELECTION DATA EXPLICITLY BY YEAR
# =============================================================================

print_block("BLOCK 6. LOAD ELECTION DATA EXPLICITLY BY YEAR")

function load_votes_for_year(year, path; overrides = Dict{String,String}())
    needed_columns = Set(["DS_CARGO", "SG_UF", "SG_PARTIDO", "QT_VOTOS_NOMINAIS_VALIDOS", "QT_VOTOS_NOMINAIS", "QT_TOTAL_VOTOS_LEG_VALIDOS", "QT_VOTOS_LEGENDA_VALIDOS", "QT_VOTOS_LEGENDA", "QT_VOTOS_NOMINAIS_CONVR_LEG", "QT_VOTOS_NOM_CONVR_LEG_VALIDOS", "QT_VOTOS", "TOTAL_VOTOS", "QT_VOTOS_VALIDOS"])
    pmz = CSV.read(path, DataFrame; select = (index, name) -> String(name) in needed_columns)
    Processing.upper_strip!(pmz, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", pmz)
    Processing.stringify!(pmz, :SG_UF)
    Processing.stringify!(pmz, :SG_PARTIDO)
    canonicalize_party_column!(pmz, year; col = :SG_PARTIDO, overrides = overrides)
    vote_kwargs = ARC.vote_kwargs_for_year(year)
    nom_col, leg_col, total_col, scheme = Processing.detect_vote_cols(pmz; vote_kwargs...)
    votes = if scheme == :total
        total_col === nothing && error("Could not detect total vote column for $(year).")
        Processing.to_int.(pmz[!, total_col])
    else
        nom_col === nothing && error("Could not detect nominal vote column for $(year).")
        leg_col === nothing && error("Could not detect legend vote column for $(year).")
        Processing.to_int.(pmz[!, nom_col]) .+ Processing.to_int.(pmz[!, leg_col])
    end
    national_votes = combine(groupby(DataFrame(SG_PARTIDO = pmz.SG_PARTIDO, valid_total = votes), :SG_PARTIDO), :valid_total => sum => :valid_total)
    sort!(national_votes, :valid_total, rev = true)
    return national_votes
end

function load_seats_for_year(year, path; overrides = Dict{String,String}())
    needed_columns = [:ANO_ELEICAO, :NR_TURNO, :SG_UF, :CD_CARGO, :DS_CARGO, :SG_PARTIDO, :DS_SIT_TOT_TURNO]
    candidates = CSV.read(path, DataFrame; select = needed_columns, normalizenames = true)
    Processing.upper_strip!(candidates, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", candidates)
    Processing.stringify!(candidates, :SG_UF)
    Processing.stringify!(candidates, :SG_PARTIDO)
    canonicalize_party_column!(candidates, year; col = :SG_PARTIDO, overrides = overrides)
    winner_status = uppercase.(strip.(String.(candidates.DS_SIT_TOT_TURNO)))
    candidates[!, :WINNER] = in.(winner_status, Ref(Processing.WINNER_STATUSES))
    national_seats = combine(groupby(candidates, :SG_PARTIDO), :WINNER => (w -> sum(Int.(w))) => :total_seats)
    filter!(row -> row.total_seats > 0, national_seats)
    sort!(national_seats, :total_seats, rev = true)
    sum(national_seats.total_seats) == expected_total_seats || error("Expected $(expected_total_seats) seats for $(year), found $(sum(national_seats.total_seats)).")
    return national_seats
end

function print_loaded_year(year, votes, seats, summary)
    println()
    println("Election $(year)")
    println("number of vote parties: ", nrow(votes))
    println("number of seat parties: ", nrow(seats))
    println("total valid votes: ", sum(votes.valid_total))
    println("total seats: ", sum(seats.total_seats))
    println("party list: ", join(sort(String.(summary.SG_PARTIDO)), ", "))
    validate_total_seats!(summary, year)
end

votes_2014 = load_votes_for_year(2014, party_mun_zone_paths[2014]; overrides = electoral_party_overrides[2014])
seats_2014 = load_seats_for_year(2014, candidate_paths[2014]; overrides = electoral_party_overrides[2014])
party_summary_2014 = Processing.party_summary(votes_2014, seats_2014; expected_total_seats = expected_total_seats)
print_loaded_year(2014, votes_2014, seats_2014, party_summary_2014)

votes_2018 = load_votes_for_year(2018, party_mun_zone_paths[2018]; overrides = electoral_party_overrides[2018])
seats_2018 = load_seats_for_year(2018, candidate_paths[2018]; overrides = electoral_party_overrides[2018])
party_summary_2018 = Processing.party_summary(votes_2018, seats_2018; expected_total_seats = expected_total_seats)
print_loaded_year(2018, votes_2018, seats_2018, party_summary_2018)

votes_2022 = load_votes_for_year(2022, party_mun_zone_paths[2022]; overrides = electoral_party_overrides[2022])
seats_2022 = load_seats_for_year(2022, candidate_paths[2022]; overrides = electoral_party_overrides[2022])
party_summary_2022 = Processing.party_summary(votes_2022, seats_2022; expected_total_seats = expected_total_seats)
print_loaded_year(2022, votes_2022, seats_2022, party_summary_2022)

# =============================================================================
# BLOCK 7. COALITION PERIOD LINKAGE
# =============================================================================

print_block("BLOCK 7. COALITION PERIOD LINKAGE")
coalition_periods_raw = Processing.coalitions_by_period_raw(; path = coalition_json_path)
coalition_windows = Processing.coalition_period_windows(; path = coalition_json_path)
mandate_2014 = ARC.mandate_window(2014)
mandate_2018 = ARC.mandate_window(2018)
mandate_2022 = ARC.mandate_window(2022)
coalitions_for_2014_election = Processing.coalition_periods_overlapping_window(coalition_periods_raw, mandate_2014.start_date, mandate_2014.end_date; path = coalition_json_path)
coalitions_for_2018_election = Processing.coalition_periods_overlapping_window(coalition_periods_raw, mandate_2018.start_date, mandate_2018.end_date; path = coalition_json_path)
coalitions_for_2022_election = Processing.coalition_periods_overlapping_window(coalition_periods_raw, mandate_2022.start_date, mandate_2022.end_date; path = coalition_json_path)

function coalition_period_linkage_table()
    rows = NamedTuple[]
    for year in analysis_years
        mandate = ARC.mandate_window(year)
        for period in sort(collect(keys(coalition_periods_raw)); by = Processing.period_sort_key)
            period_start, period_end = coalition_windows[period]
            period_start === nothing && error("Coalition period $(period) has no start date.")
            period_end === nothing && error("Coalition period $(period) has no end date.")
            selected = period_start <= mandate.end_date && period_end >= mandate.start_date
            push!(rows, (election_year = Int(year), mandate_start = mandate.start_date, mandate_end = mandate.end_date, period_label = period, period_start = period_start, period_end = period_end, selected_by_overlap = selected, raw_parties = join(coalition_periods_raw[period], ", ")))
        end
    end
    return DataFrame(rows)
end

coalition_period_linkage = coalition_period_linkage_table()
show_table(coalition_period_linkage[coalition_period_linkage.selected_by_overlap .== true, :])
write_artifact_csv(joinpath(diagnostics_dir, "coalition_period_linkage.csv"), coalition_period_linkage, "diagnostic", "Coalition periods linked to elections by inclusive mandate-window overlap.")
println("2014 election periods: ", join(sort(collect(keys(coalitions_for_2014_election)); by = Processing.period_sort_key), ", "))
println("2018 election periods: ", join(sort(collect(keys(coalitions_for_2018_election)); by = Processing.period_sort_key), ", "))
println("2022 election periods: ", join(sort(collect(keys(coalitions_for_2022_election)); by = Processing.period_sort_key), ", "))

# =============================================================================
# BLOCK 8. PARTY-LEVEL SEAT DIFFERENTIALS
# =============================================================================

print_block("BLOCK 8. PARTY-LEVEL SEAT DIFFERENTIALS")

function party_seat_differentials(year, summary)
    df = select(summary, :SG_PARTIDO => :party, :valid_total => :votes, :vote_share, :total_seats => :seats, :seat_share, :quota, :seat_diff)
    df[!, :election_year] = fill(Int(year), nrow(df))
    sort!(df, [:election_year, :seat_diff], rev = [false, true])
    validate_total_seats!(df, year)
    validate_seat_diff_sum!(df, year)
    return df
end

party_seat_differentials_2014 = party_seat_differentials(2014, party_summary_2014)
party_seat_differentials_2018 = party_seat_differentials(2018, party_summary_2018)
party_seat_differentials_2022 = party_seat_differentials(2022, party_summary_2022)
party_seat_differentials_all_years = vcat(party_seat_differentials_2014, party_seat_differentials_2018, party_seat_differentials_2022; cols = :union)
for year_df in [party_seat_differentials_2014, party_seat_differentials_2018, party_seat_differentials_2022]
    year = first(year_df.election_year)
    println("$(year): seats=$(sum(year_df.seats)), sum(seat_diff)=$(sum(year_df.seat_diff))")
end
write_artifact_csv(joinpath(raw_dir, "party_seat_differentials_2014.csv"), party_seat_differentials_2014, "raw", "Party-level vote shares, seat shares, quotas, and seat differentials for 2014.")
write_artifact_csv(joinpath(raw_dir, "party_seat_differentials_2018.csv"), party_seat_differentials_2018, "raw", "Party-level vote shares, seat shares, quotas, and seat differentials for 2018.")
write_artifact_csv(joinpath(raw_dir, "party_seat_differentials_2022.csv"), party_seat_differentials_2022, "raw", "Party-level vote shares, seat shares, quotas, and seat differentials for 2022.")
write_artifact_csv(joinpath(raw_dir, "party_seat_differentials_all_years.csv"), party_seat_differentials_all_years, "raw", "Party-level vote shares, seat shares, quotas, and seat differentials for all elections.")

function over_under_table(all_df; n_each = 5)
    rows = NamedTuple[]
    for year in analysis_years
        df = all_df[all_df.election_year .== year, :]
        for row in eachrow(first(sort(df, :seat_diff, rev = true), min(n_each, nrow(df))))
            push!(rows, (election_year = year, direction = "overrepresented", party = row.party, votes = row.votes, seats = row.seats, vote_share_pct = pct(row.vote_share), seat_share_pct = pct(row.seat_share), quota_display = display_round(row.quota), seat_diff_display = display_round(row.seat_diff)))
        end
        for row in eachrow(first(sort(df, :seat_diff), min(n_each, nrow(df))))
            push!(rows, (election_year = year, direction = "underrepresented", party = row.party, votes = row.votes, seats = row.seats, vote_share_pct = pct(row.vote_share), seat_share_pct = pct(row.seat_share), quota_display = display_round(row.quota), seat_diff_display = display_round(row.seat_diff)))
        end
    end
    return DataFrame(rows)
end

table_02_party_over_underrepresentation = over_under_table(party_seat_differentials_all_years)
write_artifact_csv(joinpath(tables_dir, "table_02_party_over_underrepresentation.csv"), table_02_party_over_underrepresentation, "table", "Top overrepresented and underrepresented parties by election.")

# =============================================================================
# BLOCK 9. OBSERVED CABINET-PERIOD COALITIONS
# =============================================================================

print_block("BLOCK 9. OBSERVED CABINET-PERIOD COALITIONS")

function cabinet_translation_report_for_period(raw_parties; election_year, coalition_year, period, valid_election_parties)
    crosswalk = Processing.load_cabinet_to_election_crosswalk(cabinet_crosswalk_path)
    valid_labels = Set(String.(valid_election_parties))
    rows = NamedTuple[]
    append_mapping!(coalition_mapping_tables, raw_parties, coalition_year; source = "coalitions", context = period)
    canonicalized = Processing.canonicalize_parties(raw_parties; year = coalition_year, strict = true, with_mapping = true)
    for row in eachrow(canonicalized.mapping)
        cabinet_party = String(row.canonical)
        cabinet_norm = Processing.normalize_party(cabinet_party)
        mask = [crosswalk.election_year[i] == Int(election_year) && crosswalk.cabinet_party_norm[i] == cabinet_norm for i in eachindex(crosswalk.election_year)]
        mapped = sort(unique(String.(crosswalk.election_party[mask])))
        notes = join(sort(unique(String.(crosswalk.notes[mask]))), " | ")
        mapping_type = if !isempty(mapped)
            length(mapped) > 1 ? "crosswalk_expansion" : only(mapped) == cabinet_party ? "crosswalk_passthrough" : "crosswalk_rename"
        elseif cabinet_party in valid_labels
            mapped = [cabinet_party]
            "label_passthrough"
        else
            mapped = String[]
            "unmapped"
        end
        if isempty(mapped)
            push!(rows, (election_year = Int(election_year), period = String(period), coalition_year = Int(coalition_year), cabinet_party_raw = String(row.alias_raw), cabinet_party_normalized = String(row.alias_norm), cabinet_party_canonical = cabinet_party, mapping_type = mapping_type, mapped_party_count = 0, election_party = "", notes = "No crosswalk row and no same-label election party."))
        else
            for election_party in mapped
                push!(rows, (election_year = Int(election_year), period = String(period), coalition_year = Int(coalition_year), cabinet_party_raw = String(row.alias_raw), cabinet_party_normalized = String(row.alias_norm), cabinet_party_canonical = cabinet_party, mapping_type = mapping_type, mapped_party_count = length(mapped), election_party = election_party, notes = notes))
            end
        end
    end
    return DataFrame(rows)
end

function build_observed_coalition_table(summary_df, election_year, coalition_periods)
    valid_election_parties = String.(summary_df.SG_PARTIDO)
    mandate = ARC.mandate_window(election_year)
    rows = NamedTuple[]
    for period in sort(collect(keys(coalition_periods)); by = Processing.period_sort_key)
        coalition_year = parse(Int, first(split(period, ".")))
        raw_parties = coalition_periods[period]
        translation_report = cabinet_translation_report_for_period(raw_parties; election_year = election_year, coalition_year = coalition_year, period = period, valid_election_parties = valid_election_parties)
        push!(cabinet_translation_tables, translation_report)
        unmapped = translation_report[translation_report.mapping_type .== "unmapped", :]
        nrow(unmapped) == 0 || error("Cabinet party without election-space mapping for $(election_year) $(period): " * join(String.(unmapped.cabinet_party_canonical), ", "))
        election_space_parties = sort(unique(String.(translation_report.election_party[translation_report.election_party .!= ""])))
        isempty(election_space_parties) && error("No election-space parties after cabinet translation for $(election_year) $(period).")
        metrics = summarize_coalition(summary_df, election_space_parties)
        period_start, period_end = coalition_windows[period]
        period_start === nothing && error("Missing start date for coalition period $(period).")
        period_end === nothing && error("Missing end date for coalition period $(period).")
        days = inclusive_days(period_start, period_end)
        overlap = overlap_days(period_start, period_end, mandate.start_date, mandate.end_date)
        push!(rows, merge((election_year = Int(election_year), coalition_year = coalition_year, period = period, period_start = period_start, period_end = period_end, period_days = days, days_overlapping_mandate = overlap, share_of_mandate = overlap / mandate.total_days, parties = join(election_space_parties, ", ")), metrics))
    end
    result = DataFrame(rows)
    sort!(result, [:election_year, :period])
    return result
end

observed_cabinet_coalitions_2014 = build_observed_coalition_table(party_summary_2014, 2014, coalitions_for_2014_election)
observed_cabinet_coalitions_2018 = build_observed_coalition_table(party_summary_2018, 2018, coalitions_for_2018_election)
observed_cabinet_coalitions_2022 = build_observed_coalition_table(party_summary_2022, 2022, coalitions_for_2022_election)
observed_cabinet_coalitions_all_years = vcat(observed_cabinet_coalitions_2014, observed_cabinet_coalitions_2018, observed_cabinet_coalitions_2022; cols = :union)
observed_cabinet_inversions_only = observed_cabinet_coalitions_all_years[observed_cabinet_coalitions_all_years.coalition_inversion .== true, :]
observed_cabinet_duration_summary = combine(groupby(observed_cabinet_coalitions_all_years, :election_year), :period => length => :n_periods, :coalition_inversion => (x -> sum(Int.(x))) => :n_inversion_periods, :days_overlapping_mandate => sum => :covered_days, [:coalition_inversion, :days_overlapping_mandate] => ((inv, days) -> sum(days[Bool.(inv)])) => :inversion_days, :share_of_mandate => sum => :covered_share_of_mandate)
show_table(select(observed_cabinet_coalitions_all_years, :election_year, :period, :period_start, :period_end, :period_days, :days_overlapping_mandate, :parties, :vote_share, :seats, :seat_diff, :majority_status, :coalition_inversion))
write_artifact_csv(joinpath(raw_dir, "observed_cabinet_coalitions_2014.csv"), observed_cabinet_coalitions_2014, "raw", "Observed cabinet-period coalition metrics for the 2014 election.")
write_artifact_csv(joinpath(raw_dir, "observed_cabinet_coalitions_2018.csv"), observed_cabinet_coalitions_2018, "raw", "Observed cabinet-period coalition metrics for the 2018 election.")
write_artifact_csv(joinpath(raw_dir, "observed_cabinet_coalitions_2022.csv"), observed_cabinet_coalitions_2022, "raw", "Observed cabinet-period coalition metrics for the 2022 election.")
write_artifact_csv(joinpath(raw_dir, "observed_cabinet_coalitions_all_years.csv"), observed_cabinet_coalitions_all_years, "raw", "Observed cabinet-period coalition metrics for all elections.")
write_artifact_csv(joinpath(raw_dir, "observed_cabinet_inversions_only.csv"), observed_cabinet_inversions_only, "raw", "Observed cabinet-period coalition inversions only.")
write_artifact_csv(joinpath(raw_dir, "observed_cabinet_duration_summary.csv"), observed_cabinet_duration_summary, "raw", "Observed cabinet coverage and inversion duration summary.")

function observed_display_table(df)
    return select(df, :election_year, :period, :period_start, :period_end, :period_days, :days_overlapping_mandate, :share_of_mandate => ByRow(pct) => :share_of_mandate_pct, :parties, :votes, :vote_share => ByRow(pct) => :vote_share_pct, :seats, :seat_share => ByRow(pct) => :seat_share_pct, :quota => ByRow(display_round) => :quota_display, :seat_diff => ByRow(display_round) => :seat_diff_display, :majority_status, :coalition_inversion)
end

table_03_observed_cabinet_coalitions = observed_display_table(observed_cabinet_coalitions_all_years)
table_04_observed_cabinet_inversions_only = observed_display_table(observed_cabinet_inversions_only)
write_artifact_csv(joinpath(tables_dir, "table_03_observed_cabinet_coalitions.csv"), table_03_observed_cabinet_coalitions, "table", "Observed cabinet-period coalitions with rounded display columns.")
write_artifact_csv(joinpath(tables_dir, "table_04_observed_cabinet_inversions_only.csv"), table_04_observed_cabinet_inversions_only, "table", "Observed cabinet-period coalition inversions only with rounded display columns.")
observed_keys = Set(zip(observed_cabinet_inversions_only.election_year, observed_cabinet_inversions_only.period))
for expected_key in [(2014, "2016.2"), (2014, "2017.1"), (2022, "2023.1")]
    expected_key in observed_keys || error("Missing expected observed cabinet inversion: $(expected_key)")
end
nrow(observed_cabinet_inversions_only[observed_cabinet_inversions_only.election_year .== 2018, :]) == 0 || error("2018 should have no observed cabinet inversion.")
row_2016_2 = only(eachrow(observed_cabinet_inversions_only[(observed_cabinet_inversions_only.election_year .== 2014) .& (observed_cabinet_inversions_only.period .== "2016.2"), :]))
row_2016_2.period_days <= 2 || error("2014 period 2016.2 should be ultra-short, found $(row_2016_2.period_days) days.")
println("Observed cabinet pattern validated: 2014/2016.2, 2014/2017.1, 2022/2023.1 inversions; no 2018 observed inversion.")

# =============================================================================
# BLOCK 10. IDEOLOGY ORDERING
# =============================================================================

print_block("BLOCK 10. IDEOLOGY ORDERING")

function build_ideology_order(year, summary_df, classification_2023, classification_2025)
    source_df, source_name = year == 2022 ? (classification_2025, "2025") : (classification_2023, "2023")
    raw_overrides = get(ideology_raw_overrides, year, Dict{String,String}())
    label_overrides = get(ideology_label_overrides, year, Dict{String,String}())
    drop_labels = get(ideology_drop_labels, year, Set{String}())
    rows = NamedTuple[]
    mapping_rows = NamedTuple[]
    for row in eachrow(source_df)
        raw_party = strip(String(row.party_name_raw))
        normalized_raw = Processing.normalize_party(raw_party)
        translated_party = Processing.canonical_party(raw_party; year = year, strict = false)
        source = translated_party == Processing.UNKNOWN_PARTY ? "unmapped" : "shared_alias"
        if translated_party == Processing.UNKNOWN_PARTY
            haskey(raw_overrides, normalized_raw) || continue
            translated_party = raw_overrides[normalized_raw]
            source = "local_raw_override"
        end
        canonical_before_label_override = translated_party
        final_party = get(label_overrides, translated_party, translated_party)
        label_override_applied = final_party != translated_party
        dropped = final_party in drop_labels
        push!(mapping_rows, (source = "ideology", election_year = Int(year), context = source_name, raw_party = raw_party, normalized_party = normalized_raw, canonical_party = canonical_before_label_override, final_party = final_party, mapping_source = source, local_label_override_applied = label_override_applied, dropped_before_join = dropped))
        dropped && continue
        push!(rows, (SG_PARTIDO = final_party, ordinal_position = Int(row.ordinal_position), classification_label = String(coalesce(row.classification_label, "")), ideology_value_numeric = row.ideology_value_numeric, ideology_source = source_name, source_party_raw = raw_party))
    end
    ideology_df = DataFrame(rows)
    duplicate_counts = combine(groupby(ideology_df, :SG_PARTIDO), nrow => :n)
    if any(duplicate_counts.n .> 1)
        duplicate_parties = duplicate_counts.SG_PARTIDO[duplicate_counts.n .> 1]
        error("Duplicate translated parties in ideology order for $(year): $(join(duplicate_parties, ", "))")
    end
    summary_parties = sort(String.(summary_df.SG_PARTIDO))
    ideology_df = innerjoin(ideology_df, select(summary_df, :SG_PARTIDO), on = :SG_PARTIDO)
    sort!(ideology_df, :ordinal_position)
    ordered_parties = String.(ideology_df.SG_PARTIDO)
    missing_parties = setdiff(summary_parties, sort(ordered_parties))
    isempty(missing_parties) || error("Seat-winning/summary party missing from ideology ordering for $(year): $(join(missing_parties, ", "))")
    length(unique(ordered_parties)) == length(ordered_parties) || error("Duplicate party after ideology translation for $(year).")
    sort(ordered_parties) == summary_parties || error("Ideology order does not cover summary parties exactly once for $(year).")
    mapping_df = DataFrame(mapping_rows)
    unexplained_drops = mapping_df[(mapping_df.dropped_before_join .== true) .& in.(mapping_df.final_party, Ref(Set(summary_parties))), :]
    nrow(unexplained_drops) == 0 || error("Dropped a summary party in ideology mapping for $(year): $(join(String.(unexplained_drops.final_party), ", "))")
    push!(ideology_mapping_tables, mapping_df)
    return ideology_df
end

function ideology_order_output(year, ideology_df)
    out = select(ideology_df, :ordinal_position, :SG_PARTIDO => :party, :classification_label, :ideology_value_numeric, :ideology_source, :source_party_raw)
    out[!, :election_year] = fill(Int(year), nrow(out))
    sort!(out, [:election_year, :ordinal_position])
    return out
end

ideology_order_2014_base = build_ideology_order(2014, party_summary_2014, classification_2023, classification_2025)
ideology_order_2018_base = build_ideology_order(2018, party_summary_2018, classification_2023, classification_2025)
ideology_order_2022_base = build_ideology_order(2022, party_summary_2022, classification_2023, classification_2025)
ideology_order_2014 = ideology_order_output(2014, ideology_order_2014_base)
ideology_order_2018 = ideology_order_output(2018, ideology_order_2018_base)
ideology_order_2022 = ideology_order_output(2022, ideology_order_2022_base)
ideology_order_all_years = vcat(ideology_order_2014, ideology_order_2018, ideology_order_2022; cols = :union)
println("2014 ideology order: ", join(String.(ideology_order_2014.party), ", "))
println("2018 ideology order: ", join(String.(ideology_order_2018.party), ", "))
println("2022 ideology order: ", join(String.(ideology_order_2022.party), ", "))
write_artifact_csv(joinpath(raw_dir, "ideology_order_2014.csv"), ideology_order_2014, "raw", "Ideology order used for 2014 election analysis.")
write_artifact_csv(joinpath(raw_dir, "ideology_order_2018.csv"), ideology_order_2018, "raw", "Ideology order used for 2018 election analysis.")
write_artifact_csv(joinpath(raw_dir, "ideology_order_2022.csv"), ideology_order_2022, "raw", "Ideology order used for 2022 election analysis.")
write_artifact_csv(joinpath(raw_dir, "ideology_order_all_years.csv"), ideology_order_all_years, "raw", "Ideology order used for all election analyses.")

# =============================================================================
# BLOCK 11. IDEOLOGICALLY CONTIGUOUS NO-GAP COALITIONS
# =============================================================================

print_block("BLOCK 11. IDEOLOGICALLY CONTIGUOUS NO-GAP COALITIONS")

function build_ideological_intervals(year, summary_df, ideology_df)
    raw = Processing.ideological_interval_coalitions(summary_df, ideology_df)
    expected_rows = nrow(ideology_df) * (nrow(ideology_df) + 1) ÷ 2
    nrow(raw) == expected_rows || error("Ideological interval count mismatch for $(year): expected $(expected_rows), found $(nrow(raw)).")
    raw[!, :election_year] = fill(Int(year), nrow(raw))
    raw[!, :interval_size] = Int.(raw.n_parties)
    raw[!, :coalition_inversion] = Bool.(raw.weak_inversion)
    party_seats = Dict(String(row.SG_PARTIDO) => Int(row.total_seats) for row in eachrow(summary_df))
    raw[!, :left_removed_seats] = [row.seats - party_seats[String(row.start_party)] for row in eachrow(raw)]
    raw[!, :right_removed_seats] = [row.seats - party_seats[String(row.end_party)] for row in eachrow(raw)]
    raw[!, :minimal_ideological_interval_inversion] = raw.coalition_inversion .& (raw.left_removed_seats .< seat_majority_threshold) .& (raw.right_removed_seats .< seat_majority_threshold)
    out = select(raw, :election_year, :start_index, :end_index, :start_party, :end_party, :parties, :interval_size, :votes, :vote_share, :seats, :seat_share, :quota, :seat_diff, :vote_majority, :seat_majority, :majority_status, :coalition_inversion, :left_removed_seats, :right_removed_seats, :minimal_ideological_interval_inversion, :old_sweep_equivalent)
    sort!(out, [:election_year, :start_index, :end_index])
    return out
end

function build_legacy_first_majority_sweep(year, summary_df, ideology_df)
    ordered = innerjoin(ideology_df, select(summary_df, :SG_PARTIDO, :valid_total, :total_seats), on = :SG_PARTIDO)
    sort!(ordered, :ordinal_position)
    total_votes = sum(ordered.valid_total)
    rows = NamedTuple[]
    for start_index in 1:nrow(ordered)
        parties = String[]
        coalition_votes = 0
        coalition_seats = 0
        end_index = missing
        for current_index in start_index:nrow(ordered)
            push!(parties, ordered.SG_PARTIDO[current_index])
            coalition_votes += ordered.valid_total[current_index]
            coalition_seats += ordered.total_seats[current_index]
            if coalition_seats >= seat_majority_threshold
                end_index = current_index
                break
            end
        end
        vote_share = coalition_votes / total_votes
        seat_share = coalition_seats / expected_total_seats
        quota = expected_total_seats * vote_share
        seat_diff = coalition_seats - quota
        vote_majority = vote_share > 0.5
        seat_majority = coalition_seats >= seat_majority_threshold
        push!(rows, (election_year = Int(year), start_index = Int(start_index), end_index = end_index, start_party = ordered.SG_PARTIDO[start_index], end_party = end_index === missing ? missing : ordered.SG_PARTIDO[end_index], parties = join(parties, ", "), votes = Int(coalition_votes), vote_share = Float64(vote_share), seats = Int(coalition_seats), seat_share = Float64(seat_share), quota = Float64(quota), seat_diff = Float64(seat_diff), vote_majority = Bool(vote_majority), seat_majority = Bool(seat_majority), majority_status = majority_status(vote_majority, seat_majority), coalition_inversion = Bool(seat_majority && !vote_majority), reached_seat_majority = Bool(seat_majority), diagnostic_label = "legacy first-majority sweep"))
    end
    return DataFrame(rows)
end

ideological_intervals_2014 = build_ideological_intervals(2014, party_summary_2014, ideology_order_2014_base)
ideological_intervals_2018 = build_ideological_intervals(2018, party_summary_2018, ideology_order_2018_base)
ideological_intervals_2022 = build_ideological_intervals(2022, party_summary_2022, ideology_order_2022_base)
ideological_intervals_all_years = vcat(ideological_intervals_2014, ideological_intervals_2018, ideological_intervals_2022; cols = :union)
ideological_interval_inversions_only = ideological_intervals_all_years[ideological_intervals_all_years.coalition_inversion .== true, :]
minimal_ideological_interval_inversions = ideological_intervals_all_years[ideological_intervals_all_years.minimal_ideological_interval_inversion .== true, :]
legacy_first_majority_sweeps = vcat(build_legacy_first_majority_sweep(2014, party_summary_2014, ideology_order_2014_base), build_legacy_first_majority_sweep(2018, party_summary_2018, ideology_order_2018_base), build_legacy_first_majority_sweep(2022, party_summary_2022, ideology_order_2022_base); cols = :union)
write_artifact_csv(joinpath(raw_dir, "ideological_intervals_2014.csv"), ideological_intervals_2014, "raw", "All contiguous ideological intervals for 2014.")
write_artifact_csv(joinpath(raw_dir, "ideological_intervals_2018.csv"), ideological_intervals_2018, "raw", "All contiguous ideological intervals for 2018.")
write_artifact_csv(joinpath(raw_dir, "ideological_intervals_2022.csv"), ideological_intervals_2022, "raw", "All contiguous ideological intervals for 2022.")
write_artifact_csv(joinpath(raw_dir, "ideological_intervals_all_years.csv"), ideological_intervals_all_years, "raw", "All contiguous ideological intervals for all elections.")
write_artifact_csv(joinpath(raw_dir, "ideological_interval_inversions_only.csv"), ideological_interval_inversions_only, "raw", "Contiguous ideological interval coalition inversions only.")
write_artifact_csv(joinpath(raw_dir, "minimal_ideological_interval_inversions.csv"), minimal_ideological_interval_inversions, "raw", "Endpoint-minimal contiguous ideological interval inversions.")
write_artifact_csv(joinpath(raw_dir, "legacy_first_majority_sweeps.csv"), legacy_first_majority_sweeps, "raw", "Legacy first-seat-majority sweep diagnostic, not the main ideological result.")

function interval_summary_table(df)
    result = combine(groupby(df, :election_year), nrow => :all_intervals, :seat_majority => (x -> sum(Int.(x))) => :seat_majority_intervals, :coalition_inversion => (x -> sum(Int.(x))) => :coalition_inversions, :minimal_ideological_interval_inversion => (x -> sum(Int.(x))) => :minimal_inversions)
    sort!(result, :election_year)
    return result
end

table_05_ideological_interval_summary_by_election = interval_summary_table(ideological_intervals_all_years)
table_06_minimal_ideological_interval_inversions = select(minimal_ideological_interval_inversions, :election_year, :start_party, :end_party, :parties, :interval_size, :votes, :vote_share => ByRow(pct) => :vote_share_pct, :seats, :seat_share => ByRow(pct) => :seat_share_pct, :quota => ByRow(display_round) => :quota_display, :seat_diff => ByRow(display_round) => :seat_diff_display, :majority_status)
write_artifact_csv(joinpath(tables_dir, "table_05_ideological_interval_summary_by_election.csv"), table_05_ideological_interval_summary_by_election, "table", "Ideological interval counts and inversion counts by election.")
write_artifact_csv(joinpath(tables_dir, "table_06_minimal_ideological_interval_inversions.csv"), table_06_minimal_ideological_interval_inversions, "table", "Endpoint-minimal ideological interval inversions with rounded display columns.")
for (year, expected_minimal) in [(2014, 4), (2018, 0), (2022, 2)]
    interval_df = ideological_intervals_all_years[ideological_intervals_all_years.election_year .== year, :]
    inversion_count = sum(Int.(interval_df.coalition_inversion))
    minimal_count = sum(Int.(interval_df.minimal_ideological_interval_inversion))
    year == 2018 && inversion_count != 0 && error("2018 should have no ideological interval inversions.")
    year in (2014, 2022) && inversion_count == 0 && error("$(year) should have ideological interval inversions.")
    minimal_count == expected_minimal || error("Expected $(expected_minimal) minimal ideological interval inversions for $(year), found $(minimal_count).")
end
pp_pl_2022 = minimal_ideological_interval_inversions[(minimal_ideological_interval_inversions.election_year .== 2022) .& (minimal_ideological_interval_inversions.start_party .== "PP") .& (minimal_ideological_interval_inversions.end_party .== "PL"), :]
println("2022 PP-PL minimal ideological inversion present: ", nrow(pp_pl_2022) > 0)
show_table(table_05_ideological_interval_summary_by_election)

# =============================================================================
# BLOCK 12. PAPER TABLES
# =============================================================================

print_block("BLOCK 12. PAPER TABLES")
table_01_definitions_classification_rules = DataFrame([
    (item = "seat_majority_threshold", value = string(seat_majority_threshold), note = "Seat majority in a 513-seat Chamber."),
    (item = "vote_majority", value = "vote_share > 0.5", note = "Strict vote-majority rule."),
    (item = "seat_majority", value = "seats >= 257", note = "Absolute Chamber seat majority."),
    (item = "coalition_inversion", value = "seat_majority && !vote_majority", note = "Seat majority without national vote majority."),
    (item = "2014 classification", value = "2023", note = "Paper rule for ideology source year."),
    (item = "2018 classification", value = "2023", note = "Paper rule for ideology source year."),
    (item = "2022 classification", value = "2025", note = "Paper rule for ideology source year."),
    (item = "2014 vote columns", value = "QT_VOTOS_NOMINAIS + QT_VOTOS_LEGENDA", note = "Legacy TSE file schema."),
    (item = "2018 vote columns", value = "QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS", note = "Includes converted nominal-to-legend votes."),
    (item = "2022 vote columns", value = "QT_VOTOS_NOMINAIS_VALIDOS + QT_TOTAL_VOTOS_LEG_VALIDOS", note = "Includes converted nominal-to-legend votes."),
])
table_07_audit_vote_columns_crosswalk = select(vote_column_consistency, :election_year, :selected_nominal_col, :selected_legend_col, :selected_scheme, :check_name, :difference, :selected_component_sum)
write_artifact_csv(joinpath(tables_dir, "table_01_definitions_classification_rules.csv"), table_01_definitions_classification_rules, "table", "Definitions, thresholds, ideology classification rules, and vote-column rules.")
write_artifact_csv(joinpath(tables_dir, "table_07_audit_vote_columns_crosswalk.csv"), table_07_audit_vote_columns_crosswalk, "table", "Audit table for selected vote columns and converted legend-vote checks.")

# =============================================================================
# BLOCK 13. FIGURE-INPUT DATA
# =============================================================================

print_block("BLOCK 13. FIGURE-INPUT DATA")
party_vote_share_vs_seat_share = select(party_seat_differentials_all_years, :election_year, :party, :votes, :vote_share, :seats, :seat_share, :quota, :seat_diff)
observed_coalition_timeline = select(observed_cabinet_coalitions_all_years, :election_year, :period, :period_start, :period_end, :period_days, :days_overlapping_mandate, :share_of_mandate, :vote_share, :seat_share, :seats, :seat_diff, :majority_status, :coalition_inversion)
ideological_interval_heatmap = select(ideological_intervals_all_years, :election_year, :start_index, :end_index, :start_party, :end_party, :interval_size, :vote_share, :seat_share, :seats, :seat_diff, :majority_status, :coalition_inversion, :minimal_ideological_interval_inversion)
write_artifact_csv(joinpath(figure_data_dir, "party_vote_share_vs_seat_share.csv"), party_vote_share_vs_seat_share, "figure_data", "Party vote share versus seat share figure input.")
write_artifact_csv(joinpath(figure_data_dir, "observed_coalition_timeline.csv"), observed_coalition_timeline, "figure_data", "Observed cabinet coalition timeline figure input.")
write_artifact_csv(joinpath(figure_data_dir, "ideological_interval_heatmap.csv"), ideological_interval_heatmap, "figure_data", "Ideological interval heatmap figure input.")

# =============================================================================
# BLOCK 14. DIAGNOSTICS
# =============================================================================

print_block("BLOCK 14. DIAGNOSTICS")
party_mapping_votes = vcat_or_empty(vote_mapping_tables)
party_mapping_seats = vcat_or_empty(seat_mapping_tables)
party_mapping_coalitions = vcat_or_empty(coalition_mapping_tables)
party_mapping_ideology = vcat_or_empty(ideology_mapping_tables)
cabinet_translation_report = vcat_or_empty(cabinet_translation_tables)
audit_appendix_vote_columns_and_crosswalk = copy(table_07_audit_vote_columns_crosswalk)
write_artifact_csv(joinpath(diagnostics_dir, "party_mapping_votes.csv"), party_mapping_votes, "diagnostic", "Raw-to-canonical party mapping for vote inputs.")
write_artifact_csv(joinpath(diagnostics_dir, "party_mapping_seats.csv"), party_mapping_seats, "diagnostic", "Raw-to-canonical party mapping for seat inputs.")
write_artifact_csv(joinpath(diagnostics_dir, "party_mapping_coalitions.csv"), party_mapping_coalitions, "diagnostic", "Raw-to-canonical party mapping for coalition inputs.")
write_artifact_csv(joinpath(diagnostics_dir, "party_mapping_ideology.csv"), party_mapping_ideology, "diagnostic", "Raw-to-canonical party mapping for ideology inputs.")
write_artifact_csv(joinpath(diagnostics_dir, "vote_column_consistency.csv"), vote_column_consistency, "diagnostic", "Vote-column selection and consistency checks.")
write_artifact_csv(joinpath(diagnostics_dir, "cabinet_translation_report.csv"), cabinet_translation_report, "diagnostic", "Cabinet-party to election-party translation report.")
write_artifact_csv(joinpath(diagnostics_dir, "audit_appendix_vote_columns_and_crosswalk.csv"), audit_appendix_vote_columns_and_crosswalk, "diagnostic", "Appendix-ready vote-column and crosswalk audit extract.")
println("Diagnostics written. Preflight and coalition-period linkage were written in their own blocks.")

# =============================================================================
# BLOCK 15. ARTIFACT MANIFEST
# =============================================================================

print_block("BLOCK 15. ARTIFACT MANIFEST")
artifact_manifest = DataFrame(artifact_records)
manifest_row = (path = relpath(artifact_manifest_path, paper_output_root), artifact_type = "manifest", description = "Manifest of every generated paper-runner artifact.", rows = nrow(artifact_manifest) + 1, columns = length(names(artifact_manifest)))
push!(artifact_records, manifest_row)
artifact_manifest = DataFrame(artifact_records)
write_paper_csv(artifact_manifest_path, artifact_manifest)
manifest_check = CSV.read(artifact_manifest_path, DataFrame)
nrow(manifest_check) == nrow(artifact_manifest) || error("Artifact manifest row count mismatch after write.")
Set(String.(manifest_check.path)) == Set(String.(artifact_manifest.path)) || error("Artifact manifest path set mismatch after write.")
println("Artifact manifest:")
show_table(artifact_manifest)
println()
println("Validated empirical pattern:")
println("- Total seats are 513 in 2014, 2018, and 2022.")
println("- Party-level sum(seat_diff) is approximately zero by year.")
println("- Observed cabinet inversions include 2014/2016.2, 2014/2017.1, and 2022/2023.1.")
println("- 2018 has no observed cabinet inversion.")
println("- Ideological interval inversions exist in 2014 and 2022, not in 2018.")
println("- Minimal ideological interval inversions counts match: 2014=4, 2018=0, 2022=2.")
