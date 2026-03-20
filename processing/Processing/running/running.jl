#!/usr/bin/env julia

using Pkg

const RUNNING_DIR = @__DIR__
const PROCESSING_DIR = normpath(joinpath(RUNNING_DIR, ".."))
const REPO_ROOT = normpath(joinpath(PROCESSING_DIR, "..", ".."))

Pkg.activate(PROCESSING_DIR; io = devnull)

import Processing.AnalysisRunnerCore as ARC

using CSV
using DataFrames

const LOG_LEVEL_ORDER = Dict(
    "debug" => 10,
    "info" => 20,
    "warn" => 30,
    "error" => 40,
)

function default_root()
    path = joinpath(REPO_ROOT, "data", "raw", "electionsBR")
    return normpath(path)
end

function default_coalition_path()
    path = joinpath(REPO_ROOT, "scraping", "output", "partidos_por_periodo.json")
    return normpath(path)
end

function default_outdir()
    path = joinpath(PROCESSING_DIR, "output", "running")
    return normpath(path)
end

function usage()
    return """
    Uso:
      julia --project=processing/Processing processing/Processing/running/running.jl [opções]

    Opções:
      --years 2014,2018,2022     Anos a executar (default: 2014,2018,2022)
      --outdir PATH              Diretório de saída (default: processing/Processing/output/running)
      --root PATH                Diretório raiz dos dados eleitorais
      --coalition-path PATH      Caminho para partidos_por_periodo.json
      --recompute                Recalcula e permite sobrescrever outputs existentes
      --use-cache                Usa cache quando outputs por ano já existem (default)
      --no-cache                 Ignora cache (não sobrescreve sem --recompute)
      --self-check               Executa checks de consistência (default)
      --no-self-check            Não executa checks de consistência
      --log-level LEVEL          debug|info|warn|error (default: info)
      -h, --help                 Mostra esta ajuda
    """
end

function parse_years(raw::AbstractString)
    cleaned = replace(raw, " " => "")
    parts = split(cleaned, ",")
    years = Int[]
    for part in parts
        isempty(part) && continue
        parsed = tryparse(Int, part)
        parsed === nothing && error("Ano inválido em --years: '$part'.")
        push!(years, parsed)
    end
    isempty(years) && error("Lista de anos vazia em --years.")
    return sort(unique(years))
end

function take_value(args::Vector{String}, i::Int, flag::AbstractString)
    if i + 1 > length(args)
        error("Flag $flag requer um valor.")
    end
    return args[i + 1], i + 1
end

function parse_args(args::Vector{String})
    config = Dict{Symbol,Any}(
        :years => copy(ARC.SUPPORTED_YEARS),
        :outdir => default_outdir(),
        :root => default_root(),
        :coalition_path => default_coalition_path(),
        :recompute => false,
        :use_cache => true,
        :self_check => true,
        :log_level => "info",
        :help => false,
    )

    i = 1
    while i <= length(args)
        arg = args[i]

        if arg == "--help" || arg == "-h"
            config[:help] = true
        elseif startswith(arg, "--years=")
            config[:years] = parse_years(split(arg, "=", limit = 2)[2])
        elseif arg == "--years"
            value, i = take_value(args, i, "--years")
            config[:years] = parse_years(value)
        elseif startswith(arg, "--outdir=")
            config[:outdir] = normpath(split(arg, "=", limit = 2)[2])
        elseif arg == "--outdir"
            value, i = take_value(args, i, "--outdir")
            config[:outdir] = normpath(value)
        elseif startswith(arg, "--root=")
            config[:root] = normpath(split(arg, "=", limit = 2)[2])
        elseif arg == "--root"
            value, i = take_value(args, i, "--root")
            config[:root] = normpath(value)
        elseif startswith(arg, "--coalition-path=")
            config[:coalition_path] = normpath(split(arg, "=", limit = 2)[2])
        elseif arg == "--coalition-path"
            value, i = take_value(args, i, "--coalition-path")
            config[:coalition_path] = normpath(value)
        elseif arg == "--recompute"
            config[:recompute] = true
        elseif arg == "--use-cache"
            config[:use_cache] = true
        elseif arg == "--no-cache"
            config[:use_cache] = false
        elseif arg == "--self-check"
            config[:self_check] = true
        elseif arg == "--no-self-check"
            config[:self_check] = false
        elseif startswith(arg, "--log-level=")
            config[:log_level] = lowercase(split(arg, "=", limit = 2)[2])
        elseif arg == "--log-level"
            value, i = take_value(args, i, "--log-level")
            config[:log_level] = lowercase(value)
        else
            error("Flag desconhecida: $arg")
        end

        i += 1
    end

    haskey(LOG_LEVEL_ORDER, config[:log_level]) || error("log-level inválido: $(config[:log_level])")
    for year in config[:years]
        ARC.validate_supported_year(year)
    end

    return config
end

function should_log(current_level::AbstractString, msg_level::AbstractString)
    return LOG_LEVEL_ORDER[msg_level] >= LOG_LEVEL_ORDER[current_level]
end

function log_msg(config, level::AbstractString, message::AbstractString)
    lvl = lowercase(level)
    should_log(config[:log_level], lvl) || return
    println("[$(uppercase(lvl))] $(message)")
end

function run_self_check_if_needed(config, year::Int, data, seat_differentials, inversion_tables)
    if !config[:self_check]
        return DataFrame()
    end
    return ARC.self_check(year, data, seat_differentials, inversion_tables)
end

function run_year(config, year::Int)
    outdir = config[:outdir]
    recompute = config[:recompute]
    use_cache = config[:use_cache]

    if use_cache && !recompute && ARC.has_cached_outputs(year; outdir = outdir)
        cached = ARC.read_cached_outputs(
            year;
            outdir = outdir,
            coalition_path = config[:coalition_path],
        )
        summary = ARC.summarize_year_result(
            year;
            status = "ok",
            source = "cache",
            seat_differentials = cached.seat_differentials,
            inversion_tables = cached.inversion_tables,
            coalition_stability = cached.coalition_stability,
            mandate_stability_summary = cached.mandate_stability_summary,
            self_checks = cached.self_checks,
        )
        return (
            ok = true,
            result = (
                year = year,
                seat_differentials = cached.seat_differentials,
                inversion_tables = cached.inversion_tables,
                coalition_stability = cached.coalition_stability,
                mandate_stability_summary = cached.mandate_stability_summary,
                self_checks = cached.self_checks,
            ),
            summary = summary,
            paths = cached.paths,
        )
    end

    if !recompute && !use_cache && ARC.has_cached_outputs(year; outdir = outdir)
        error(
            "Outputs de $year já existem em $outdir. Use --recompute para sobrescrever " *
            "ou --use-cache para reutilizar.",
        )
    end

    data = ARC.load_inputs(
        year;
        root = config[:root],
        coalition_path = config[:coalition_path],
    )
    seat_differentials = ARC.compute_seat_differentials(data)
    inversion_tables = ARC.compute_inversion_tables(seat_differentials; year = year)
    coalition_stability = ARC.compute_coalition_stability(
        inversion_tables;
        election_year = year,
        coalition_path = config[:coalition_path],
    )
    mandate_stability_summary = ARC.summarize_mandate_stability(
        coalition_stability;
        election_year = year,
    )
    checks = run_self_check_if_needed(config, year, data, seat_differentials, inversion_tables)

    result = (
        year = year,
        seat_differentials = seat_differentials,
        inversion_tables = inversion_tables,
        coalition_stability = coalition_stability,
        mandate_stability_summary = mandate_stability_summary,
        self_checks = checks,
    )

    paths = ARC.write_outputs(
        result,
        year;
        outdir = outdir,
        allow_overwrite = recompute,
        write_checks = config[:self_check],
    )

    summary = ARC.summarize_year_result(
        year;
        status = "ok",
        source = "computed",
        seat_differentials = seat_differentials,
        inversion_tables = inversion_tables,
        coalition_stability = coalition_stability,
        mandate_stability_summary = mandate_stability_summary,
        self_checks = checks,
    )

    return (ok = true, result = result, summary = summary, paths = paths)
end

function write_run_summary(df::DataFrame; outdir::AbstractString, allow_overwrite::Bool)
    path = joinpath(outdir, "run_summary.csv")
    if isfile(path) && !allow_overwrite
        error("Arquivo já existe e não pode ser sobrescrito sem --recompute: $path")
    end
    CSV.write(path, df)
    return path
end

function print_year_summary(summary_df::DataFrame)
    println()
    println("Resumo final por ano")
    println("====================")
    show(stdout, MIME("text/plain"), summary_df)
    println()
end

function main(args::Vector{String})
    config = parse_args(args)
    if config[:help]
        println(usage())
        return 0
    end

    mkpath(config[:outdir])

    log_msg(config, "info", "Runner iniciado para anos: $(join(config[:years], ", ")).")
    log_msg(config, "debug", "root=$(config[:root])")
    log_msg(config, "debug", "coalition_path=$(config[:coalition_path])")
    log_msg(config, "debug", "outdir=$(config[:outdir])")

    year_runs = NamedTuple[]

    for year in config[:years]
        log_msg(config, "info", "Processando ano $year...")
        run_data = try
            run_year(config, year)
        catch err
            message = sprint(showerror, err)
            log_msg(config, "error", "Falha no ano $year: $message")

            summary = ARC.summarize_year_result(
                year;
                status = "error",
                source = "failed",
                error_message = message,
            )

            (ok = false, result = nothing, summary = summary, paths = nothing)
        end
        push!(year_runs, run_data)
    end

    summary_rows = [run.summary for run in year_runs]
    summary_df = DataFrame(summary_rows)
    sort!(summary_df, :year)
    print_year_summary(summary_df)

    ok_runs = [run for run in year_runs if run.ok]
    if !isempty(ok_runs)
        results = [run.result for run in ok_runs]
        consolidated = ARC.consolidate_results(results)

        try
            paths = ARC.write_consolidated_outputs(
                consolidated;
                outdir = config[:outdir],
                allow_overwrite = config[:recompute],
            )
            log_msg(config, "info", "Consolidados salvos em $(config[:outdir]).")
            log_msg(config, "debug", "inversion_by_coalition=$(paths.inversion_by_coalition)")
        catch err
            log_msg(config, "warn", "Consolidados não foram salvos: $(sprint(showerror, err))")
        end

        try
            run_summary_path = write_run_summary(
                summary_df;
                outdir = config[:outdir],
                allow_overwrite = config[:recompute],
            )
            log_msg(config, "debug", "run_summary=$(run_summary_path)")
        catch err
            log_msg(config, "warn", "Resumo CSV não foi salvo: $(sprint(showerror, err))")
        end
    end

    failed = [run for run in year_runs if !run.ok]
    return isempty(failed) ? 0 : 1
end

exit(main(ARGS))
