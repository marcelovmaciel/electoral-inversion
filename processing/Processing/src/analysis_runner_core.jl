module AnalysisRunnerCore

using CSV
using DataFrames
using Dates
using Statistics

using ..Processing: get_coalition_path,
                    get_root,
                    cabinet_coalition_metrics_for_periods,
                    coalitions_by_period_raw,
                    coalition_periods_overlapping_window,
                    coalition_period_windows,
                    mandate_id_for_election_year,
                    get_agg_party_seats,
                    national_party_valid_votes,
                    party_summary,
                    period_sort_key,
                    set_coalition_path!,
                    set_root!

export SUPPORTED_YEARS,
       coalition_years_for_election,
       vote_kwargs_for_year,
       mandate_window,
       load_inputs,
       compute_seat_differentials,
       compute_inversion_tables,
       compute_coalition_stability,
       summarize_mandate_stability,
       self_check,
       has_cached_outputs,
       read_cached_outputs,
       write_outputs,
       consolidate_results,
       write_consolidated_outputs,
       summarize_year_result

const SUPPORTED_YEARS = [2014, 2018, 2022]
const EXPECTED_TOTAL_SEATS = 513

const COALITION_YEARS_BY_ELECTION = Dict(
    2014 => [2015, 2016, 2017, 2018],
    2018 => [2019, 2020, 2021, 2022],
    2022 => [2023, 2024, 2025],
)

const MANDATE_WINDOWS_BY_ELECTION = Dict(
    2014 => (start_date = Date(2015, 1, 1), end_date = Date(2018, 12, 31)),
    2018 => (start_date = Date(2019, 1, 1), end_date = Date(2022, 12, 31)),
    # The current analysis intentionally covers the available 2023-2025 portion
    # of the 2022 mandate. Do not extend this through 2026 without making that
    # a deliberate analysis configuration.
    2022 => (start_date = Date(2023, 1, 1), end_date = Date(2025, 12, 31)),
)

function validate_supported_year(year::Integer)
    if !(year in SUPPORTED_YEARS)
        error("Ano não suportado: $year. Anos válidos: $(join(SUPPORTED_YEARS, ", ")).")
    end
    return Int(year)
end

function coalition_years_for_election(year::Integer)
    y = validate_supported_year(year)
    return copy(get(COALITION_YEARS_BY_ELECTION, y) do
        error("Mapeamento de anos de coalizão ausente para $y.")
    end)
end

function vote_kwargs_for_year(year::Integer)
    y = validate_supported_year(year)
    if y == 2014
        return (nom_col = "QT_VOTOS_NOMINAIS", leg_col = "QT_VOTOS_LEGENDA")
    elseif y in (2018, 2022)
        return (
            nom_col = "QT_VOTOS_NOMINAIS_VALIDOS",
            leg_col = "QT_TOTAL_VOTOS_LEG_VALIDOS",
        )
    end
    return NamedTuple()
end

function mandate_window(election_year::Integer)
    y = validate_supported_year(election_year)
    window = get(MANDATE_WINDOWS_BY_ELECTION, y) do
        error("Janela de mandato ausente para $y.")
    end
    start_date = window.start_date
    end_date = window.end_date
    total_days = Dates.value(end_date - start_date) + 1
    return (start_date = start_date, end_date = end_date, total_days = total_days)
end

function inclusive_days(start_date::Date, end_date::Date)
    end_date < start_date && error("Intervalo inválido: $start_date > $end_date.")
    return Dates.value(end_date - start_date) + 1
end

function overlap_days(start_a::Date, end_a::Date, start_b::Date, end_b::Date)
    start_overlap = max(start_a, start_b)
    end_overlap = min(end_a, end_b)
    end_overlap < start_overlap && return 0
    return Dates.value(end_overlap - start_overlap) + 1
end

function load_inputs(year::Integer;
                     root::Union{Nothing,AbstractString} = nothing,
                     coalition_path::Union{Nothing,AbstractString} = nothing,
                     cargo::AbstractString = "DEPUTADO FEDERAL",
                     vote_kwargs::Union{Nothing,NamedTuple} = nothing)
    y = validate_supported_year(year)

    root === nothing || set_root!(String(root))
    coalition_path === nothing || set_coalition_path!(String(coalition_path))

    resolved_vote_kwargs = vote_kwargs === nothing ? vote_kwargs_for_year(y) : vote_kwargs

    votes = national_party_valid_votes(y; cargo = cargo, resolved_vote_kwargs...)
    seats = get_agg_party_seats(y; cargo = cargo, expected_total_seats = EXPECTED_TOTAL_SEATS)

    return (
        year = y,
        cargo = String(cargo),
        root = get_root(),
        coalition_path = get_coalition_path(),
        coalition_years = coalition_years_for_election(y),
        votes = votes,
        seats = seats,
    )
end

function compute_seat_differentials(data;
                                    expected_total_seats::Int = EXPECTED_TOTAL_SEATS)
    seat_differentials = party_summary(
        data.votes,
        data.seats;
        expected_total_seats = expected_total_seats,
    )
    sort!(seat_differentials, :SG_PARTIDO)
    return seat_differentials
end

function empty_inversion_table()
    return DataFrame(
        coalition_col = String[],
        V_base_share = Float64[],
        V_out_share = Float64[],
        S_base_share = Float64[],
        S_out_share = Float64[],
        seatdiff_base = Float64[],
        inversion = Bool[],
        election_year = Int[],
        coalition_year = Int[],
    )
end

function normalize_inversion_table(df::DataFrame)
    nrow(df) == 0 && return empty_inversion_table()

    required = [:coalition_col, :V_base_share, :V_out_share, :S_base_share, :S_out_share, :seatdiff_base, :inversion]
    for col in required
        if !hasproperty(df, col)
            error("Tabela de inversão sem coluna obrigatória: $col.")
        end
    end

    return DataFrame(
        coalition_col = string.(df.coalition_col),
        V_base_share = Float64.(coalesce.(df.V_base_share, 0.0)),
        V_out_share = Float64.(coalesce.(df.V_out_share, 0.0)),
        S_base_share = Float64.(coalesce.(df.S_base_share, 0.0)),
        S_out_share = Float64.(coalesce.(df.S_out_share, 0.0)),
        seatdiff_base = Float64.(coalesce.(df.seatdiff_base, 0.0)),
        inversion = Bool.(coalesce.(df.inversion, false)),
    )
end

function compute_inversion_tables(seat_differentials::DataFrame;
                                  year::Integer,
                                  coalition_years::Union{Nothing,AbstractVector{<:Integer}} = nothing)
    y = validate_supported_year(year)
    mandate_id = mandate_id_for_election_year(y)
    periods_raw = coalitions_by_period_raw()

    periods = if coalition_years === nothing
        mandate = mandate_window(y)
        coalition_periods_overlapping_window(
            periods_raw,
            mandate.start_date,
            mandate.end_date,
        )
    else
        selected = Dict{String,Vector{String}}()
        for coalition_year in Int.(coalition_years)
            merge!(
                selected,
                coalition_periods_overlapping_window(
                    periods_raw,
                    Date(coalition_year, 1, 1),
                    Date(coalition_year, 12, 31),
                ),
            )
        end
        selected
    end

    raw_table = cabinet_coalition_metrics_for_periods(
        seat_differentials,
        periods;
        mandate_id = mandate_id,
        election_year = y,
    )
    out = normalize_inversion_table(raw_table)
    out[!, :election_year] = fill(y, nrow(out))
    if hasproperty(raw_table, :coalition_year)
        out[!, :coalition_year] = Int.(raw_table.coalition_year)
    else
        out[!, :coalition_year] = Int[]
    end
    sort!(out, [:coalition_year, :coalition_col])
    return out
end

function empty_coalition_stability_table()
    return DataFrame(
        election_year = Int[],
        coalition_year = Int[],
        coalition_col = String[],
        inversion = Bool[],
        period_start = Date[],
        period_end = Date[],
        period_days = Int[],
        days_in_mandate = Int[],
        years_in_mandate_equiv = Float64[],
        share_of_mandate = Float64[],
        inversion_days = Int[],
    )
end

function compute_coalition_stability(inversion_tables::DataFrame;
                                     election_year::Integer,
                                     coalition_path::AbstractString = get_coalition_path(),
                                     denominator::Symbol = :full_mandate)
    denominator == :full_mandate || error("Denominador não suportado: $denominator")

    y = validate_supported_year(election_year)
    nrow(inversion_tables) == 0 && return empty_coalition_stability_table()

    for col in [:coalition_col, :inversion]
        hasproperty(inversion_tables, col) || error("compute_coalition_stability: coluna ausente: $col")
    end

    mandate = mandate_window(y)
    windows = coalition_period_windows(; path = coalition_path)

    rows = NamedTuple[]
    seen_inversion = Dict{String,Bool}()
    for row in eachrow(inversion_tables)
        label = string(row.coalition_col)
        inversion = Bool(coalesce(row.inversion, false))

        if haskey(seen_inversion, label)
            seen_inversion[label] == inversion || error(
                "compute_coalition_stability: inversão inconsistente para o período $label.",
            )
            continue
        end
        seen_inversion[label] = inversion

        haskey(windows, label) || error("compute_coalition_stability: período sem janela no JSON: $label")
        period_start, period_end = windows[label]
        if period_start === nothing || period_end === nothing
            error("compute_coalition_stability: período $label sem data_inicio/data_fim.")
        end

        p_start = period_start::Date
        p_end = period_end::Date
        days_period = inclusive_days(p_start, p_end)
        days_mandate = overlap_days(p_start, p_end, mandate.start_date, mandate.end_date)
        share_mandate = days_mandate / max(mandate.total_days, 1)

        parsed_year = tryparse(Int, first(split(label, ".")))
        parsed_year === nothing && error(
            "compute_coalition_stability: não foi possível inferir coalition_year de $label.",
        )
        coalition_year = parsed_year::Int

        push!(rows, (
            election_year = y,
            coalition_year = coalition_year,
            coalition_col = label,
            inversion = inversion,
            period_start = p_start,
            period_end = p_end,
            period_days = days_period,
            days_in_mandate = days_mandate,
            years_in_mandate_equiv = days_mandate / 365.25,
            share_of_mandate = share_mandate,
            inversion_days = inversion ? days_mandate : 0,
        ))
    end

    stability = DataFrame(rows)
    sort!(stability, :coalition_col, by = period_sort_key)
    return stability
end

function empty_mandate_stability_summary()
    return DataFrame(
        election_year = Int[],
        mandate_start = Date[],
        mandate_end = Date[],
        mandate_total_days = Int[],
        covered_days = Int[],
        coverage_share_of_mandate = Float64[],
        inversion_true_days = Int[],
        inversion_true_share_of_mandate = Float64[],
        non_inversion_days = Int[],
        non_inversion_share_of_mandate = Float64[],
        n_periods = Int[],
    )
end

function summarize_mandate_stability(stability_table::DataFrame;
                                     election_year::Integer,
                                     denominator::Symbol = :full_mandate)
    denominator == :full_mandate || error("Denominador não suportado: $denominator")

    y = validate_supported_year(election_year)
    mandate = mandate_window(y)

    if nrow(stability_table) == 0
        return DataFrame([(
            election_year = y,
            mandate_start = mandate.start_date,
            mandate_end = mandate.end_date,
            mandate_total_days = mandate.total_days,
            covered_days = 0,
            coverage_share_of_mandate = 0.0,
            inversion_true_days = 0,
            inversion_true_share_of_mandate = 0.0,
            non_inversion_days = 0,
            non_inversion_share_of_mandate = 0.0,
            n_periods = 0,
        )])
    end

    for col in [:days_in_mandate, :inversion_days]
        hasproperty(stability_table, col) || error("summarize_mandate_stability: coluna ausente: $col")
    end

    covered_days = sum(Int.(coalesce.(stability_table.days_in_mandate, 0)))
    inversion_true_days = sum(Int.(coalesce.(stability_table.inversion_days, 0)))

    covered_days <= mandate.total_days || error(
        "summarize_mandate_stability: cobertura excede mandato (covered_days=$covered_days, total=$(mandate.total_days)).",
    )
    inversion_true_days <= covered_days || error(
        "summarize_mandate_stability: inversion_true_days > covered_days ($inversion_true_days > $covered_days).",
    )

    non_inversion_days = covered_days - inversion_true_days

    return DataFrame([(
        election_year = y,
        mandate_start = mandate.start_date,
        mandate_end = mandate.end_date,
        mandate_total_days = mandate.total_days,
        covered_days = covered_days,
        coverage_share_of_mandate = covered_days / max(mandate.total_days, 1),
        inversion_true_days = inversion_true_days,
        inversion_true_share_of_mandate = inversion_true_days / max(mandate.total_days, 1),
        non_inversion_days = non_inversion_days,
        non_inversion_share_of_mandate = non_inversion_days / max(mandate.total_days, 1),
        n_periods = nrow(stability_table),
    )])
end

function check_row_sums(df::DataFrame; atol::Float64 = 1e-8)
    nrow(df) == 0 && return (true, "Sem linhas de coalizão.")

    bad_keys = String[]
    for row in eachrow(df)
        vote_sum = row.V_base_share + row.V_out_share
        seat_sum = row.S_base_share + row.S_out_share
        if !isapprox(vote_sum, 1.0; atol = atol) || !isapprox(seat_sum, 1.0; atol = atol)
            push!(bad_keys, String(row.coalition_col))
        end
    end

    if isempty(bad_keys)
        return (true, "Todas as linhas de coalizão fecham em 1.")
    end

    sample = join(bad_keys[1:min(end, 5)], ", ")
    return (false, "Linhas com soma inconsistente (amostra): $sample")
end

function check_bool_col(df::DataFrame, col::Symbol)
    if !hasproperty(df, col)
        return (false, "Coluna $col ausente.")
    end
    return (true, "Coluna $col presente.")
end

function build_check_row(year::Integer, check::AbstractString, ok::Bool, detail::AbstractString)
    return (year = Int(year), check = String(check), ok = Bool(ok), detail = String(detail))
end

function self_check(year::Integer,
                    data,
                    seat_differentials::DataFrame,
                    inversion_tables::DataFrame;
                    expected_total_seats::Int = EXPECTED_TOTAL_SEATS,
                    atol::Float64 = 1e-8)
    y = validate_supported_year(year)
    checks = NamedTuple[]

    total_seats = sum(Int.(coalesce.(data.seats.total_seats, 0)))
    push!(
        checks,
        build_check_row(
            y,
            "total_seats_expected",
            total_seats == expected_total_seats,
            "total_seats=$total_seats esperado=$expected_total_seats",
        ),
    )

    vote_share_sum = sum(Float64.(coalesce.(seat_differentials.vote_share, 0.0)))
    push!(
        checks,
        build_check_row(
            y,
            "vote_share_sum_1",
            isapprox(vote_share_sum, 1.0; atol = atol),
            "sum(vote_share)=$vote_share_sum",
        ),
    )

    seat_share_sum = sum(Float64.(coalesce.(seat_differentials.seat_share, 0.0)))
    push!(
        checks,
        build_check_row(
            y,
            "seat_share_sum_1",
            isapprox(seat_share_sum, 1.0; atol = atol),
            "sum(seat_share)=$seat_share_sum",
        ),
    )

    seat_diff_sum = sum(Float64.(coalesce.(seat_differentials.seat_diff, 0.0)))
    push!(
        checks,
        build_check_row(
            y,
            "seat_diff_sum_0",
            isapprox(seat_diff_sum, 0.0; atol = atol),
            "sum(seat_diff)=$seat_diff_sum",
        ),
    )

    n_unique_parties = length(unique(String.(seat_differentials.SG_PARTIDO)))
    n_parties = nrow(seat_differentials)
    push!(
        checks,
        build_check_row(
            y,
            "party_key_unique",
            n_unique_parties == n_parties,
            "parties_unicas=$n_unique_parties linhas=$n_parties",
        ),
    )

    row_sum_ok, row_sum_detail = check_row_sums(inversion_tables; atol = atol)
    push!(checks, build_check_row(y, "coalition_rows_sum_1", row_sum_ok, row_sum_detail))

    col_ok, col_detail = check_bool_col(inversion_tables, :inversion)
    push!(checks, build_check_row(y, "inversion_col_present", col_ok, col_detail))

    return DataFrame(checks)
end

function year_output_paths(year::Integer; outdir::AbstractString)
    y = Int(year)
    year_dir = joinpath(outdir, "year_$(y)")
    return (
        year_dir = year_dir,
        seat_differentials = joinpath(year_dir, "seat_differentials.csv"),
        inversion_tables = joinpath(year_dir, "inversion_tables.csv"),
        coalition_stability = joinpath(year_dir, "coalition_stability.csv"),
        mandate_stability_summary = joinpath(year_dir, "mandate_stability_summary.csv"),
        self_checks = joinpath(year_dir, "self_checks.csv"),
    )
end

function has_cached_outputs(year::Integer; outdir::AbstractString)
    paths = year_output_paths(year; outdir = outdir)
    return isfile(paths.seat_differentials) && isfile(paths.inversion_tables)
end

function write_dataframe(df::DataFrame,
                         path::AbstractString;
                         allow_overwrite::Bool = false)
    # Não sobrescrevemos cache por padrão. Só liberamos com flag explícita.
    if isfile(path) && !allow_overwrite
        error("Arquivo já existe e sobrescrita está desabilitada: $path")
    end
    CSV.write(path, df)
    return path
end

function read_cached_outputs(year::Integer;
                             outdir::AbstractString,
                             coalition_path::Union{Nothing,AbstractString} = nothing)
    paths = year_output_paths(year; outdir = outdir)
    if !has_cached_outputs(year; outdir = outdir)
        error("Cache incompleto para o ano $year em $(paths.year_dir).")
    end

    seat_differentials = CSV.read(paths.seat_differentials, DataFrame)
    raw_inversion_tables = CSV.read(paths.inversion_tables, DataFrame)
    inversion_tables = normalize_inversion_table(raw_inversion_tables)
    if hasproperty(raw_inversion_tables, :election_year)
        inversion_tables[!, :election_year] = Int.(coalesce.(raw_inversion_tables.election_year, year))
    else
        inversion_tables[!, :election_year] = fill(Int(year), nrow(inversion_tables))
    end
    if hasproperty(raw_inversion_tables, :coalition_year)
        parsed_years = Int[]
        for (idx, label) in enumerate(inversion_tables.coalition_col)
            val = raw_inversion_tables.coalition_year[idx]
            if val === missing || val === nothing
                parsed = tryparse(Int, first(split(string(label), ".")))
                parsed === nothing && error("read_cached_outputs: não foi possível inferir coalition_year de $label")
                push!(parsed_years, parsed)
            else
                push!(parsed_years, Int(val))
            end
        end
        inversion_tables[!, :coalition_year] = parsed_years
    else
        parsed_years = Int[]
        for label in inversion_tables.coalition_col
            parsed = tryparse(Int, first(split(string(label), ".")))
            parsed === nothing && error("read_cached_outputs: não foi possível inferir coalition_year de $label")
            push!(parsed_years, parsed)
        end
        inversion_tables[!, :coalition_year] = parsed_years
    end
    coal_path = coalition_path === nothing ? get_coalition_path() : String(coalition_path)
    coalition_stability = if isfile(paths.coalition_stability)
        stability = CSV.read(paths.coalition_stability, DataFrame)
        if hasproperty(stability, :coalition_col)
            stability[!, :coalition_col] = string.(stability.coalition_col)
        end
        stability
    else
        compute_coalition_stability(
            inversion_tables;
            election_year = year,
            coalition_path = coal_path,
        )
    end
    mandate_stability_summary = isfile(paths.mandate_stability_summary) ?
                                CSV.read(paths.mandate_stability_summary, DataFrame) :
                                summarize_mandate_stability(
                                    coalition_stability;
                                    election_year = year,
                                )
    self_checks = isfile(paths.self_checks) ? CSV.read(paths.self_checks, DataFrame) : DataFrame()

    return (
        year = Int(year),
        seat_differentials = seat_differentials,
        inversion_tables = inversion_tables,
        coalition_stability = coalition_stability,
        mandate_stability_summary = mandate_stability_summary,
        self_checks = self_checks,
        paths = paths,
    )
end

function write_outputs(results,
                       year::Integer;
                       outdir::AbstractString,
                       allow_overwrite::Bool = false,
                       write_checks::Bool = true)
    y = validate_supported_year(year)
    paths = year_output_paths(y; outdir = outdir)
    mkpath(paths.year_dir)

    write_dataframe(results.seat_differentials, paths.seat_differentials; allow_overwrite = allow_overwrite)
    write_dataframe(results.inversion_tables, paths.inversion_tables; allow_overwrite = allow_overwrite)
    if hasproperty(results, :coalition_stability)
        write_dataframe(results.coalition_stability, paths.coalition_stability; allow_overwrite = allow_overwrite)
    end
    if hasproperty(results, :mandate_stability_summary)
        write_dataframe(
            results.mandate_stability_summary,
            paths.mandate_stability_summary;
            allow_overwrite = allow_overwrite,
        )
    end

    if write_checks && hasproperty(results, :self_checks)
        write_dataframe(results.self_checks, paths.self_checks; allow_overwrite = allow_overwrite)
    end

    return paths
end

function empty_seat_differentials_with_year()
    return DataFrame(
        SG_PARTIDO = String[],
        valid_total = Int[],
        total_seats = Int[],
        vote_share = Float64[],
        seat_share = Float64[],
        quota = Float64[],
        seat_diff = Float64[],
        election_year = Int[],
    )
end

function empty_self_checks()
    return DataFrame(year = Int[], check = String[], ok = Bool[], detail = String[])
end

function summarize_inversion_by_coalition(inversion_tables::DataFrame)
    if nrow(inversion_tables) == 0
        return DataFrame(
            election_year = Int[],
            coalition_col = String[],
            coalition_years = String[],
            n_rows = Int[],
            n_inversions = Int[],
            seatdiff_base_total = Float64[],
            V_base_share_mean = Float64[],
            S_base_share_mean = Float64[],
        )
    end

    grouped = groupby(inversion_tables, [:election_year, :coalition_col])
    summary = combine(
        grouped,
        :coalition_year => (x -> join(string.(sort(unique(Int.(x)))), ";")) => :coalition_years,
        nrow => :n_rows,
        :inversion => (x -> sum(Int.(Bool.(coalesce.(x, false))))) => :n_inversions,
        :seatdiff_base => (x -> sum(Float64.(coalesce.(x, 0.0)))) => :seatdiff_base_total,
        :V_base_share => (x -> mean(Float64.(coalesce.(x, 0.0)))) => :V_base_share_mean,
        :S_base_share => (x -> mean(Float64.(coalesce.(x, 0.0)))) => :S_base_share_mean,
    )
    sort!(summary, [:election_year, :coalition_col])
    return summary
end

function consolidate_results(year_results::Vector)
    seat_parts = DataFrame[]
    inversion_parts = DataFrame[]
    stability_parts = DataFrame[]
    mandate_summary_parts = DataFrame[]
    check_parts = DataFrame[]

    for result in year_results
        year = Int(result.year)

        seat = copy(result.seat_differentials)
        seat[!, :election_year] = fill(year, nrow(seat))
        push!(seat_parts, seat)

        inversion = copy(result.inversion_tables)
        if !hasproperty(inversion, :election_year)
            inversion[!, :election_year] = fill(year, nrow(inversion))
        end
        push!(inversion_parts, inversion)

        stability = hasproperty(result, :coalition_stability) ?
                    copy(result.coalition_stability) :
                    empty_coalition_stability_table()
        if nrow(stability) > 0
            if !hasproperty(stability, :election_year)
                stability[!, :election_year] = fill(year, nrow(stability))
            end
            push!(stability_parts, stability)
        end

        mandate_summary = hasproperty(result, :mandate_stability_summary) ?
                          copy(result.mandate_stability_summary) :
                          empty_mandate_stability_summary()
        if nrow(mandate_summary) > 0
            if !hasproperty(mandate_summary, :election_year)
                mandate_summary[!, :election_year] = fill(year, nrow(mandate_summary))
            end
            push!(mandate_summary_parts, mandate_summary)
        end

        checks = hasproperty(result, :self_checks) ? copy(result.self_checks) : empty_self_checks()
        if nrow(checks) > 0
            if !hasproperty(checks, :year)
                checks[!, :year] = fill(year, nrow(checks))
            end
            push!(check_parts, checks)
        end
    end

    seat_differentials = isempty(seat_parts) ? empty_seat_differentials_with_year() : vcat(seat_parts...)
    inversion_tables = isempty(inversion_parts) ? empty_inversion_table() : vcat(inversion_parts...)
    coalition_stability = isempty(stability_parts) ? empty_coalition_stability_table() : vcat(stability_parts...)
    mandate_stability_summary = isempty(mandate_summary_parts) ? empty_mandate_stability_summary() : vcat(mandate_summary_parts...)
    self_checks = isempty(check_parts) ? empty_self_checks() : vcat(check_parts...)
    inversion_by_coalition = summarize_inversion_by_coalition(inversion_tables)

    return (
        seat_differentials = seat_differentials,
        inversion_tables = inversion_tables,
        coalition_stability = coalition_stability,
        mandate_stability_summary = mandate_stability_summary,
        inversion_by_coalition = inversion_by_coalition,
        self_checks = self_checks,
    )
end

function write_consolidated_outputs(consolidated;
                                    outdir::AbstractString,
                                    allow_overwrite::Bool = false)
    mkpath(outdir)

    seat_path = joinpath(outdir, "seat_differentials_all_years.csv")
    inversion_path = joinpath(outdir, "inversion_tables_all_years.csv")
    stability_path = joinpath(outdir, "coalition_stability_all_years.csv")
    mandate_summary_path = joinpath(outdir, "mandate_stability_summary_all_years.csv")
    coalition_path = joinpath(outdir, "inversion_by_coalition.csv")
    checks_path = joinpath(outdir, "self_checks_all_years.csv")

    write_dataframe(consolidated.seat_differentials, seat_path; allow_overwrite = allow_overwrite)
    write_dataframe(consolidated.inversion_tables, inversion_path; allow_overwrite = allow_overwrite)
    write_dataframe(consolidated.coalition_stability, stability_path; allow_overwrite = allow_overwrite)
    write_dataframe(consolidated.mandate_stability_summary, mandate_summary_path; allow_overwrite = allow_overwrite)
    write_dataframe(consolidated.inversion_by_coalition, coalition_path; allow_overwrite = allow_overwrite)
    write_dataframe(consolidated.self_checks, checks_path; allow_overwrite = allow_overwrite)

    return (
        seat_differentials = seat_path,
        inversion_tables = inversion_path,
        coalition_stability = stability_path,
        mandate_stability_summary = mandate_summary_path,
        inversion_by_coalition = coalition_path,
        self_checks = checks_path,
    )
end

function summarize_year_result(year::Integer;
                               status::AbstractString,
                               source::AbstractString,
                               seat_differentials::DataFrame = DataFrame(),
                               inversion_tables::DataFrame = DataFrame(),
                               coalition_stability::DataFrame = DataFrame(),
                               mandate_stability_summary::DataFrame = DataFrame(),
                               self_checks::DataFrame = DataFrame(),
                               error_message::AbstractString = "")
    seat_rows = nrow(seat_differentials)
    inversion_rows = nrow(inversion_tables)
    stability_rows = nrow(coalition_stability)
    seat_diff_sum = hasproperty(seat_differentials, :seat_diff) ?
                    sum(Float64.(coalesce.(seat_differentials.seat_diff, 0.0))) : 0.0
    seat_diff_near_zero = isapprox(seat_diff_sum, 0.0; atol = 1e-6)

    coverage_share = hasproperty(mandate_stability_summary, :coverage_share_of_mandate) && nrow(mandate_stability_summary) > 0 ?
                     Float64(coalesce(mandate_stability_summary.coverage_share_of_mandate[1], 0.0)) : 0.0
    inversion_share = hasproperty(mandate_stability_summary, :inversion_true_share_of_mandate) && nrow(mandate_stability_summary) > 0 ?
                      Float64(coalesce(mandate_stability_summary.inversion_true_share_of_mandate[1], 0.0)) : 0.0

    check_failures = 0
    if hasproperty(self_checks, :ok)
        check_failures = sum(.!Bool.(coalesce.(self_checks.ok, false)))
    end

    return (
        year = Int(year),
        status = String(status),
        source = String(source),
        seat_rows = seat_rows,
        inversion_rows = inversion_rows,
        stability_rows = stability_rows,
        seat_diff_sum = seat_diff_sum,
        seat_diff_near_zero = seat_diff_near_zero,
        coverage_share_of_mandate = coverage_share,
        inversion_true_share_of_mandate = inversion_share,
        check_failures = check_failures,
        error_message = String(error_message),
    )
end

end
