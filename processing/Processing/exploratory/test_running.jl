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
audit_root_dir = joinpath(@__DIR__, "audit")
audit_out_dir = joinpath(audit_root_dir, "out")
audit_findings_dir = joinpath(audit_root_dir, "findings")
party_alias_path = joinpath(processing_root, "data", "party_aliases.csv")
cabinet_crosswalk_path = joinpath(processing_root, "data", "cabinet_to_election_party_crosswalk.csv")

party_mun_zone_2014_path = joinpath(data_root, "2014", "party_mun_zone.csv")
candidate_2014_path = joinpath(data_root, "2014", "candidate.csv")

party_mun_zone_2018_path = joinpath(data_root, "2018", "party_mun_zone.csv")
candidate_2018_path = joinpath(data_root, "2018", "candidate.csv")

party_mun_zone_2022_path = joinpath(data_root, "2022", "party_mun_zone.csv")
candidate_2022_path = joinpath(data_root, "2022", "candidate.csv")

analysis_years = [2014, 2018, 2022]
party_mun_zone_paths = Dict(
    2014 => party_mun_zone_2014_path,
    2018 => party_mun_zone_2018_path,
    2022 => party_mun_zone_2022_path,
)
candidate_paths = Dict(
    2014 => candidate_2014_path,
    2018 => candidate_2018_path,
    2022 => candidate_2022_path,
)

function configured_expected_total_seats(; cargo = "DEPUTADO FEDERAL")
    metadata_value = Processing.expected_total_seats_for_cargo(cargo)
    raw_value = strip(get(ENV, "EXPECTED_TOTAL_SEATS", ""))
    if isempty(raw_value)
        metadata_value === nothing && error("No metadata seat expectation for cargo $(cargo).")
        return metadata_value
    end

    parsed_value = tryparse(Int, raw_value)
    parsed_value === nothing && error("EXPECTED_TOTAL_SEATS must be an integer, got '$(raw_value)'.")
    metadata_value !== nothing && parsed_value != metadata_value && println(
        "WARNING: EXPECTED_TOTAL_SEATS=$(parsed_value) differs from metadata value $(metadata_value) for $(cargo).",
    )
    return parsed_value
end

expected_total_seats = configured_expected_total_seats()

Processing.set_root!(data_root)
Processing.set_coalition_path!(coalition_json_path)

# -----------------------------------------------------------------------------
# Small visible overrides for the few labels the shared alias table does not yet
# cover in the exact way needed here.
# -----------------------------------------------------------------------------

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

function write_audit_csv(filename, df::DataFrame)
    mkpath(audit_out_dir)
    path = joinpath(audit_out_dir, filename)
    CSV.write(path, df)
    return path
end

function run_preflight_checks()
    rows = NamedTuple[]
    push!(rows, (item = "processing_root", path = processing_root, kind = "dir", exists = isdir(processing_root)))
    push!(rows, (item = "data_root", path = data_root, kind = "dir", exists = isdir(data_root)))
    push!(rows, (item = "coalition_json", path = coalition_json_path, kind = "file", exists = isfile(coalition_json_path)))
    push!(rows, (item = "classification_root", path = classification_root_dir, kind = "dir", exists = isdir(classification_root_dir)))
    push!(rows, (item = "party_aliases", path = party_alias_path, kind = "file", exists = isfile(party_alias_path)))
    push!(rows, (item = "cabinet_crosswalk", path = cabinet_crosswalk_path, kind = "file", exists = isfile(cabinet_crosswalk_path)))

    for year in analysis_years
        push!(rows, (item = "party_mun_zone_$(year)", path = party_mun_zone_paths[year], kind = "file", exists = isfile(party_mun_zone_paths[year])))
        push!(rows, (item = "candidate_$(year)", path = candidate_paths[year], kind = "file", exists = isfile(candidate_paths[year])))
    end

    df = DataFrame(rows)
    write_audit_csv("preflight_checks.csv", df)
    show_table(df)

    missing = df[.!df.exists, :]
    isempty(missing.path) || error("Preflight failed; missing required paths: $(join(missing.path, ", "))")
    return df
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
        canonical = if haskey(overrides, normalized)
            overrides[normalized]
        else
            Processing.canonical_party(raw; year = year, strict = false)
        end
        push!(rows, (
            source = source,
            election_year = year,
            context = context,
            raw_party = raw,
            normalized_party = normalized,
            canonical_party = canonical,
            mapping_source = mapping_source(raw, year; overrides = overrides),
        ))
    end
    return DataFrame(rows)
end

function append_mapping!(target::Vector{DataFrame}, raw_values, year; source, context = "", overrides = Dict{String,String}())
    push!(target, party_mapping_table(raw_values, year; source = source, context = context, overrides = overrides))
end

function vcat_or_empty(tables::Vector{DataFrame})
    isempty(tables) && return DataFrame()
    return vcat(tables...; cols = :union)
end

function mandate_coalitions_for_election(coalition_periods_raw, election_year, coalition_json_path)
    window = ARC.mandate_window(election_year)
    return Processing.coalition_periods_overlapping_window(
        coalition_periods_raw,
        window.start_date,
        window.end_date;
        path = coalition_json_path,
    )
end

function print_mandate_linkage_diagnostic(election_year, coalition_periods, coalition_windows)
    window = ARC.mandate_window(election_year)
    rows = NamedTuple[]

    for period in sort(collect(keys(coalition_periods)); by = Processing.period_sort_key)
        period_start, period_end = coalition_windows[period]
        selected_by_date_overlap = period_start <= window.end_date && period_end >= window.start_date
        push!(rows, (
            election_year = election_year,
            mandate_window = string(window.start_date, " to ", window.end_date),
            period = period,
            period_start = period_start,
            period_end = period_end,
            selected_by_date_overlap = selected_by_date_overlap,
            raw_parties = join(coalition_periods[period], ", "),
        ))
    end

    show_table(DataFrame(rows))
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

function selected_vote_component_diagnostic(pmz::DataFrame, year)
    vote_kwargs = ARC.vote_kwargs_for_year(year)
    nom_col, leg_col, total_col, scheme = Processing.detect_vote_cols(pmz; vote_kwargs...)

    selected_sum = if scheme == :total
        total_col === nothing ? missing : sum(Processing.to_int.(pmz[!, total_col]))
    else
        nom_col === nothing || leg_col === nothing ? missing :
            sum(Processing.to_int.(pmz[!, nom_col]) .+ Processing.to_int.(pmz[!, leg_col]))
    end

    rows = NamedTuple[]
    direct_legend_col = Processing.pick_col(pmz, "QT_VOTOS_LEGENDA_VALIDOS")
    total_legend_col = Processing.pick_col(pmz, "QT_TOTAL_VOTOS_LEG_VALIDOS")
    converted_cols = [
        "QT_VOTOS_NOMINAIS_CONVR_LEG",
        "QT_VOTOS_NOM_CONVR_LEG_VALIDOS",
    ]
    converted_col = nothing
    for candidate_col in converted_cols
        converted_col = Processing.pick_col(pmz, candidate_col)
        converted_col === nothing || break
    end

    if direct_legend_col !== nothing && total_legend_col !== nothing && converted_col !== nothing
        direct_sum = sum(Processing.to_int.(pmz[!, direct_legend_col]))
        total_legend_sum = sum(Processing.to_int.(pmz[!, total_legend_col]))
        converted_sum = sum(Processing.to_int.(pmz[!, converted_col]))
        push!(rows, (
            election_year = year,
            selected_nominal_col = nom_col === nothing ? "" : String(nom_col),
            selected_legend_col = leg_col === nothing ? "" : String(leg_col),
            selected_total_col = total_col === nothing ? "" : String(total_col),
            selected_scheme = String(scheme),
            check_name = "total_legend_equals_direct_plus_converted",
            lhs_column = String(total_legend_col),
            lhs_sum = total_legend_sum,
            rhs_columns = string(direct_legend_col, " + ", converted_col),
            rhs_sum = direct_sum + converted_sum,
            difference = total_legend_sum - direct_sum - converted_sum,
            selected_component_sum = selected_sum,
        ))
    end

    if nom_col !== nothing && leg_col !== nothing && direct_legend_col !== nothing && direct_legend_col != leg_col
        selected_sum_int = sum(Processing.to_int.(pmz[!, nom_col]) .+ Processing.to_int.(pmz[!, leg_col]))
        legacy_sum = sum(Processing.to_int.(pmz[!, nom_col]) .+ Processing.to_int.(pmz[!, direct_legend_col]))
        push!(rows, (
            election_year = year,
            selected_nominal_col = String(nom_col),
            selected_legend_col = String(leg_col),
            selected_total_col = total_col === nothing ? "" : String(total_col),
            selected_scheme = String(scheme),
            check_name = "selected_components_vs_direct_legend_components",
            lhs_column = string(nom_col, " + ", leg_col),
            lhs_sum = selected_sum_int,
            rhs_columns = string(nom_col, " + ", direct_legend_col),
            rhs_sum = legacy_sum,
            difference = selected_sum_int - legacy_sum,
            selected_component_sum = selected_sum,
        ))
    end

    if isempty(rows)
        push!(rows, (
            election_year = year,
            selected_nominal_col = nom_col === nothing ? "" : String(nom_col),
            selected_legend_col = leg_col === nothing ? "" : String(leg_col),
            selected_total_col = total_col === nothing ? "" : String(total_col),
            selected_scheme = String(scheme),
            check_name = "no_component_total_available",
            lhs_column = "",
            lhs_sum = missing,
            rhs_columns = "",
            rhs_sum = missing,
            difference = missing,
            selected_component_sum = selected_sum,
        ))
    end

    return DataFrame(rows)
end

vote_mapping_tables = DataFrame[]
seat_mapping_tables = DataFrame[]
vote_consistency_tables = DataFrame[]

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
        "QT_VOTOS_NOMINAIS_CONVR_LEG",
        "QT_VOTOS_NOM_CONVR_LEG_VALIDOS",
        "QT_VOTOS",
        "TOTAL_VOTOS",
        "QT_VOTOS_VALIDOS",
    ])

    pmz = CSV.read(path, DataFrame; select = (index, name) -> String(name) in needed_columns)

    Processing.upper_strip!(pmz, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", pmz)

    Processing.stringify!(pmz, :SG_UF)
    Processing.stringify!(pmz, :SG_PARTIDO)
    append_mapping!(vote_mapping_tables, pmz.SG_PARTIDO, year; source = "votes", context = basename(path), overrides = overrides)
    push!(vote_consistency_tables, selected_vote_component_diagnostic(pmz, year))
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
    append_mapping!(seat_mapping_tables, candidates.SG_PARTIDO, year; source = "seats", context = basename(path), overrides = overrides)
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

coalition_mapping_tables = DataFrame[]
cabinet_translation_tables = DataFrame[]

function cabinet_translation_report_for_period(
    raw_parties::Vector{String};
    election_year,
    coalition_year,
    period,
    valid_election_parties,
)
    crosswalk = Processing.load_cabinet_to_election_crosswalk(cabinet_crosswalk_path)
    valid_labels = Set(String.(valid_election_parties))
    rows = NamedTuple[]

    append_mapping!(
        coalition_mapping_tables,
        raw_parties,
        coalition_year;
        source = "coalitions",
        context = period,
    )

    canonicalized = Processing.canonicalize_parties(
        raw_parties;
        year = coalition_year,
        strict = true,
        with_mapping = true,
    )

    for row in eachrow(canonicalized.mapping)
        cabinet_party = String(row.canonical)
        cabinet_norm = Processing.normalize_party(cabinet_party)
        mask = [
            crosswalk.election_year[i] == Int(election_year) &&
            crosswalk.cabinet_party_norm[i] == cabinet_norm
            for i in eachindex(crosswalk.election_year)
        ]
        mapped = sort(unique(String.(crosswalk.election_party[mask])))
        notes = join(sort(unique(String.(crosswalk.notes[mask]))), " | ")

        mapping_type = if !isempty(mapped)
            length(mapped) > 1 ? "crosswalk_expansion" :
                only(mapped) == cabinet_party ? "crosswalk_passthrough" : "crosswalk_rename"
        elseif cabinet_party in valid_labels
            mapped = [cabinet_party]
            "label_passthrough"
        else
            mapped = String[]
            "unmapped"
        end

        for election_party in mapped
            push!(rows, (
                election_year = Int(election_year),
                period = String(period),
                coalition_year = Int(coalition_year),
                cabinet_party_raw = String(row.alias_raw),
                cabinet_party_normalized = String(row.alias_norm),
                cabinet_party_canonical = cabinet_party,
                mapping_type = mapping_type,
                mapped_party_count = length(mapped),
                election_party = election_party,
                notes = notes,
            ))
        end

        isempty(mapped) && push!(rows, (
            election_year = Int(election_year),
            period = String(period),
            coalition_year = Int(coalition_year),
            cabinet_party_raw = String(row.alias_raw),
            cabinet_party_normalized = String(row.alias_norm),
            cabinet_party_canonical = cabinet_party,
            mapping_type = mapping_type,
            mapped_party_count = 0,
            election_party = "",
            notes = "No crosswalk row and no same-label election party.",
        ))
    end

    return DataFrame(rows)
end

function build_observed_coalition_table(summary_df::DataFrame, election_year, coalition_periods, coalition_windows)
    valid_election_parties = String.(summary_df.SG_PARTIDO)
    rows = NamedTuple[]

    for period in sort(collect(keys(coalition_periods)); by = Processing.period_sort_key)
        coalition_year = parse(Int, first(split(period, ".")))
        raw_parties = coalition_periods[period]

        translation_report = cabinet_translation_report_for_period(
            raw_parties;
            election_year = election_year,
            coalition_year = coalition_year,
            period = period,
            valid_election_parties = valid_election_parties,
        )
        push!(cabinet_translation_tables, translation_report)
        unmapped = translation_report[translation_report.mapping_type .== "unmapped", :]
        nrow(unmapped) == 0 || error(
            "Cabinet party without election-space mapping for $(election_year) $(period): " *
            join(String.(unmapped.cabinet_party_canonical), ", "),
        )
        election_space_parties = sort(unique(String.(translation_report.election_party[translation_report.election_party .!= ""])))
        isempty(election_space_parties) && error("No election-space parties after cabinet translation for $(election_year) $(period).")

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

ideology_mapping_tables = DataFrame[]

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

        push!(mapping_rows, (
            source = "ideology",
            election_year = year,
            context = source_name,
            raw_party = raw_party,
            normalized_party = normalized_raw,
            canonical_party = canonical_before_label_override,
            final_party = final_party,
            mapping_source = source,
            local_label_override_applied = label_override_applied,
            dropped_before_join = dropped,
        ))

        dropped && continue

        push!(rows, (
            SG_PARTIDO = final_party,
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

    push!(ideology_mapping_tables, DataFrame(mapping_rows))
    return ideology_df
end

function build_ideology_sweep_table_legacy(summary_df::DataFrame, ideology_df::DataFrame)
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

function compare_legacy_sweep_to_intervals(legacy_sweep_table::DataFrame, interval_table::DataFrame)
    rows = NamedTuple[]
    reached = legacy_sweep_table[legacy_sweep_table.reached_seat_majority .== true, :]

    for legacy_row in eachrow(reached)
        matches = interval_table[
            (interval_table.start_party .== legacy_row.start_party) .&
            (interval_table.old_sweep_equivalent .== true),
            :,
        ]

        matched_exactly = false
        interval_end_party = missing
        interval_parties = missing
        interval_votes = missing
        interval_seats = missing
        interval_weak_inversion = missing

        if nrow(matches) == 1
            match_row = first(eachrow(matches))
            interval_end_party = match_row.end_party
            interval_parties = match_row.parties
            interval_votes = match_row.votes
            interval_seats = match_row.seats
            interval_weak_inversion = match_row.weak_inversion
            matched_exactly =
                legacy_row.end_party == match_row.end_party &&
                legacy_row.parties == match_row.parties &&
                legacy_row.votes == match_row.votes &&
                legacy_row.seats == match_row.seats &&
                legacy_row.candidate_inversion == match_row.weak_inversion
        end

        push!(rows, (
            start_party = legacy_row.start_party,
            legacy_end_party = legacy_row.end_party,
            interval_end_party = interval_end_party,
            legacy_parties = legacy_row.parties,
            interval_parties = interval_parties,
            legacy_votes = legacy_row.votes,
            interval_votes = interval_votes,
            legacy_seats = legacy_row.seats,
            interval_seats = interval_seats,
            legacy_candidate_inversion = legacy_row.candidate_inversion,
            interval_weak_inversion = interval_weak_inversion,
            matched_exactly = matched_exactly,
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
println("party_alias_path: ", party_alias_path)
println("cabinet_crosswalk_path: ", cabinet_crosswalk_path)
println("expected_total_seats: ", expected_total_seats, " (metadata default, override with ENV[\"EXPECTED_TOTAL_SEATS\"])")

println()
println("Preflight checks...")
run_preflight_checks()

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

coalitions_for_2014_election = mandate_coalitions_for_election(coalition_periods_raw, 2014, coalition_json_path)
coalitions_for_2018_election = mandate_coalitions_for_election(coalition_periods_raw, 2018, coalition_json_path)
coalitions_for_2022_election = mandate_coalitions_for_election(coalition_periods_raw, 2022, coalition_json_path)

println("Coalition periods linked to the 2014 election: ", join(sort(collect(keys(coalitions_for_2014_election)); by = Processing.period_sort_key), ", "))
println("Coalition periods linked to the 2018 election: ", join(sort(collect(keys(coalitions_for_2018_election)); by = Processing.period_sort_key), ", "))
println("Coalition periods linked to the 2022 election: ", join(sort(collect(keys(coalitions_for_2022_election)); by = Processing.period_sort_key), ", "))

print_block("COALITION PERIOD LINKAGE DIAGNOSTIC")
println("Mandate linkage uses inclusive date-window overlap, not YYYY label-prefix selection.")
print_mandate_linkage_diagnostic(2014, coalitions_for_2014_election, coalition_windows)
print_mandate_linkage_diagnostic(2018, coalitions_for_2018_election, coalition_windows)
print_mandate_linkage_diagnostic(2022, coalitions_for_2022_election, coalition_windows)

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

party_result_tables = DataFrame[]
observed_result_tables = DataFrame[]
ideology_order_result_tables = DataFrame[]
ideology_sweep_result_tables = DataFrame[]
ideology_interval_result_tables = DataFrame[]
ideology_interval_minimal_majority_result_tables = DataFrame[]
ideology_interval_inversion_result_tables = DataFrame[]
ideology_interval_minimal_inversion_result_tables = DataFrame[]
ideology_interval_old_sweep_comparison_tables = DataFrame[]

for year in analysis_years
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
    party_table[!, :election_year] = fill(year, nrow(party_table))
    push!(party_result_tables, party_table)
    show_table(select(party_table, Not(:election_year)))

    println()
    println("B. Observed coalition seat differentials")
    observed_table = build_observed_coalition_table(summary_df, year, coalition_periods, coalition_windows)
    observed_table[!, :election_year] = fill(year, nrow(observed_table))
    push!(observed_result_tables, observed_table)
    observed_display_table = select(
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
    show_table(observed_display_table)

    println()
    println("C. Ideology-contiguous coalitions: all no-gap intervals")
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
    ideology_order_table[!, :election_year] = fill(year, nrow(ideology_order_table))
    push!(ideology_order_result_tables, ideology_order_table)

    println("Ordered parties from left to right:")
    show_table(select(ideology_order_table, Not(:election_year)))

    interval_table = Processing.ideological_interval_coalitions(summary_df, ideology_df)
    interval_table[!, :election_year] = fill(year, nrow(interval_table))
    push!(ideology_interval_result_tables, interval_table)

    minimal_majority_table = interval_table[interval_table.minimal_seat_majority .== true, :]
    weak_inversion_table = interval_table[interval_table.weak_inversion .== true, :]
    minimal_inversion_table = interval_table[interval_table.minimal_inversion .== true, :]

    push!(ideology_interval_minimal_majority_result_tables, minimal_majority_table)
    push!(ideology_interval_inversion_result_tables, weak_inversion_table)
    push!(ideology_interval_minimal_inversion_result_tables, minimal_inversion_table)

    println("Minimal seat-majority intervals:")
    show_table(select(
        minimal_majority_table,
        :start_index,
        :end_index,
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
        :weak_inversion,
    ))

    println("Weak inversions:")
    show_table(select(
        weak_inversion_table,
        :start_index,
        :end_index,
        :start_party,
        :end_party,
        :parties,
        :votes,
        :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
        :seats,
        :quota => ByRow(x -> round(x, digits = 2)) => :quota,
        :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
        :minimal_seat_majority,
        :minimal_inversion,
    ))

    println("Minimal inversions:")
    show_table(select(
        minimal_inversion_table,
        :start_index,
        :end_index,
        :start_party,
        :end_party,
        :parties,
        :votes,
        :vote_share => ByRow(x -> round(100 * x, digits = 2)) => :vote_share_pct,
        :seats,
        :quota => ByRow(x -> round(x, digits = 2)) => :quota,
        :seat_diff => ByRow(x -> round(x, digits = 2)) => :seat_diff,
    ))

    ideology_sweep_table = build_ideology_sweep_table_legacy(summary_df, ideology_df)
    ideology_sweep_table[!, :election_year] = fill(year, nrow(ideology_sweep_table))
    push!(ideology_sweep_result_tables, ideology_sweep_table)

    old_sweep_comparison_table = compare_legacy_sweep_to_intervals(ideology_sweep_table, interval_table)
    old_sweep_comparison_table[!, :election_year] = fill(year, nrow(old_sweep_comparison_table))
    push!(ideology_interval_old_sweep_comparison_tables, old_sweep_comparison_table)

    println("Legacy first-seat-majority sweep comparison:")
    show_table(select(
        old_sweep_comparison_table,
        :election_year,
        :start_party,
        :legacy_end_party,
        :interval_end_party,
        :legacy_parties,
        :interval_parties,
        :legacy_votes,
        :interval_votes,
        :legacy_seats,
        :interval_seats,
        :legacy_candidate_inversion,
        :interval_weak_inversion,
        :matched_exactly,
    ))
end

print_block("AUDIT DIAGNOSTIC OUTPUTS")
println("Raw -> normalized -> canonical party mappings for vote inputs")
show_table(vcat_or_empty(vote_mapping_tables))
println("Raw -> normalized -> canonical party mappings for seat inputs")
show_table(vcat_or_empty(seat_mapping_tables))
println("Raw -> normalized -> canonical party mappings for coalition inputs")
show_table(vcat_or_empty(coalition_mapping_tables))
println("Raw -> normalized -> canonical party mappings for ideology inputs")
show_table(vcat_or_empty(ideology_mapping_tables))
println("Vote component consistency and selected vote columns")
show_table(vcat_or_empty(vote_consistency_tables))
println("Cabinet-to-election translation report")
show_table(vcat_or_empty(cabinet_translation_tables))

diagnostic_paths = [
    write_audit_csv("party_mapping_votes.csv", vcat_or_empty(vote_mapping_tables)),
    write_audit_csv("party_mapping_seats.csv", vcat_or_empty(seat_mapping_tables)),
    write_audit_csv("party_mapping_coalitions.csv", vcat_or_empty(coalition_mapping_tables)),
    write_audit_csv("party_mapping_ideology.csv", vcat_or_empty(ideology_mapping_tables)),
    write_audit_csv("vote_column_consistency.csv", vcat_or_empty(vote_consistency_tables)),
    write_audit_csv("cabinet_translation_report.csv", vcat_or_empty(cabinet_translation_tables)),
    write_audit_csv("party_tables_after_refactor.csv", vcat_or_empty(party_result_tables)),
    write_audit_csv("observed_coalitions_after_refactor.csv", vcat_or_empty(observed_result_tables)),
    write_audit_csv("ideology_order_after_refactor.csv", vcat_or_empty(ideology_order_result_tables)),
    write_audit_csv("ideology_sweeps_after_refactor.csv", vcat_or_empty(ideology_sweep_result_tables)),
    write_audit_csv("ideology_interval_coalitions_after_refactor.csv", vcat_or_empty(ideology_interval_result_tables)),
    write_audit_csv("ideology_interval_minimal_majorities_after_refactor.csv", vcat_or_empty(ideology_interval_minimal_majority_result_tables)),
    write_audit_csv("ideology_interval_inversions_after_refactor.csv", vcat_or_empty(ideology_interval_inversion_result_tables)),
    write_audit_csv("ideology_interval_minimal_inversions_after_refactor.csv", vcat_or_empty(ideology_interval_minimal_inversion_result_tables)),
    write_audit_csv("ideology_interval_old_sweep_comparison_after_refactor.csv", vcat_or_empty(ideology_interval_old_sweep_comparison_tables)),
]
println("Wrote audit diagnostics:")
for path in diagnostic_paths
    println(path)
end
