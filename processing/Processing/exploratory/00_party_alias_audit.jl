"""
Manual alias audit for party-name harmonization.

Run in Emacs Julia REPL:
  ] activate processing/Processing
  using Processing
  include("exploratory/00_party_alias_audit.jl")

Output:
  exploratory/out/party_alias_audit/party_alias_report.csv
"""

using Processing
using CSV
using DataFrames
using JSON3

const REPO_ROOT = abspath(joinpath(@__DIR__, "..", "..", ".."))
const YEARS = (2014, 2018, 2022)
const OUT_DIR = joinpath(@__DIR__, "out", "party_alias_audit")
const OUT_PATH = joinpath(OUT_DIR, "party_alias_report.csv")

_period_year(period)::Union{Missing,Int} = begin
    raw = strip(String(period))
    isempty(raw) && return missing
    head = first(split(raw, "."))
    parsed = tryparse(Int, head)
    parsed === nothing ? missing : parsed
end

function _push_rows!(rows::Vector{NamedTuple}, source::String, year::Union{Missing,Int}, aliases)
    seen = Set{String}()
    for alias_any in aliases
        alias_raw = strip(String(alias_any))
        isempty(alias_raw) && continue
        alias_raw in seen && continue
        push!(seen, alias_raw)

        year_kw = year === missing ? missing : Int(year)
        push!(rows, (
            source = source,
            year = year,
            alias_raw = alias_raw,
            alias_norm = Processing.normalize_party(alias_raw),
            canonical = Processing.canonical_party(alias_raw; year = year_kw, strict = false),
        ))
    end
    return rows
end

function _tse_rows!(rows::Vector{NamedTuple})
    for year in YEARS
        for file in ("party_mun_zone.csv", "legends.csv")
            path = joinpath(REPO_ROOT, "data", "raw", "electionsBR", string(year), file)
            isfile(path) || continue

            tbl = CSV.File(path; select = (i, name) -> name in ("SG_PARTIDO", "NM_PARTIDO"))
            names_in_file = Set(String.(propertynames(tbl)))

            if "SG_PARTIDO" in names_in_file
                _push_rows!(rows, "tse_$(replace(file, ".csv" => ""))_SG_PARTIDO", year, getproperty.(tbl, :SG_PARTIDO))
            end
            if "NM_PARTIDO" in names_in_file
                _push_rows!(rows, "tse_$(replace(file, ".csv" => ""))_NM_PARTIDO", year, getproperty.(tbl, :NM_PARTIDO))
            end
        end
    end
    return rows
end

function _coalition_rows!(rows::Vector{NamedTuple})
    json_path = joinpath(REPO_ROOT, "scraping", "output", "partidos_por_periodo.json")
    csv_path = joinpath(REPO_ROOT, "scraping", "output", "partidos_por_periodo.csv")

    if isfile(json_path)
        payload = JSON3.read(read(json_path, String))
        for (period, info) in pairs(payload)
            parties = haskey(info, "partidos") ? info["partidos"] : Any[]
            _push_rows!(rows, "coalition_json", _period_year(period), parties)
        end
        return rows
    end

    isfile(csv_path) || error("Arquivo de coalizão não encontrado (JSON/CSV).")
    coalition = CSV.read(csv_path, DataFrame)
    hasproperty(coalition, :periodo) || error("CSV de coalizão sem coluna 'periodo'.")
    hasproperty(coalition, :partido) || error("CSV de coalizão sem coluna 'partido'.")

    for g in groupby(coalition, :periodo)
        _push_rows!(rows, "coalition_csv", _period_year(g.periodo[1]), g.partido)
    end
    return rows
end

function _classification_rows!(rows::Vector{NamedTuple})
    base_root = joinpath(REPO_ROOT, "scrape_classification", "output")
    for year in (2023, 2025)
        base = joinpath(base_root, "classificacao_$(year)")
        isdir(base) || continue

        json_path = joinpath(base, "party_ordinal_classificacao.json")
        csv_path_2023 = joinpath(base, "party_ordinal_classificacao.csv")
        csv_path_2025 = joinpath(base, "party_classificacao_2025.csv")

        if isfile(json_path)
            payload = JSON3.read(read(json_path, String))
            aliases = String[]
            for item in payload
                haskey(item, "party_name_raw") || continue
                push!(aliases, String(item["party_name_raw"]))
            end
            _push_rows!(rows, "classification_json", year, aliases)
        end

        csv_path = isfile(csv_path_2023) ? csv_path_2023 :
                   isfile(csv_path_2025) ? csv_path_2025 :
                   ""
        if !isempty(csv_path)
            df = CSV.read(csv_path, DataFrame)
            if hasproperty(df, :party_name_raw)
                _push_rows!(rows, "classification_csv", year, df.party_name_raw)
            elseif hasproperty(df, :Partido)
                _push_rows!(rows, "classification_csv", year, df.Partido)
            end
        end
    end

    return rows
end

function run_party_alias_audit(; out_path::AbstractString = OUT_PATH)
    rows = NamedTuple[]
    _tse_rows!(rows)
    _coalition_rows!(rows)
    _classification_rows!(rows)

    report = DataFrame(rows)
    report = unique(report)

    by_norm = combine(
        groupby(report, :alias_norm),
        :canonical => (x -> length(unique(String.(x)))) => :n_canonical_by_norm,
    )
    report = leftjoin(report, by_norm, on = :alias_norm)
    report[!, :has_year_conflict] = report.n_canonical_by_norm .> 1

    sort!(report, [:source, :year, :alias_norm, :alias_raw])

    mkpath(dirname(out_path))
    CSV.write(out_path, report)

    unknown = count(==(Processing.UNKNOWN_PARTY), report.canonical)
    println("party_alias_report rows: ", nrow(report))
    println("unknown aliases: ", unknown)
    println("output: ", out_path)
    return report
end

# Default behavior when this file is included manually.
run_party_alias_audit()
