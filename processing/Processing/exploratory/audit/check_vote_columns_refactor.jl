using CSV
using DataFrames
using Processing
using Processing.AnalysisRunnerCore: vote_kwargs_for_year

const REPO_ROOT = abspath(joinpath(@__DIR__, "../../../.."))
const RAW_ROOT = joinpath(REPO_ROOT, "data", "raw", "electionsBR")
const YEARS = (2014, 2018, 2022)
const EXPECTED_COMPONENTS = Dict(
    2014 => ("QT_VOTOS_NOMINAIS", "QT_VOTOS_LEGENDA"),
    2018 => ("QT_VOTOS_NOMINAIS_VALIDOS", "QT_TOTAL_VOTOS_LEG_VALIDOS"),
    2022 => ("QT_VOTOS_NOMINAIS_VALIDOS", "QT_TOTAL_VOTOS_LEG_VALIDOS"),
)
const EXPECTED_CONVERTED_DELTAS = Dict(2018 => 206_687, 2022 => 123_458)
const EXPECTED_PARTY_DELTAS = Dict(
    2018 => Dict(
        "PT" => 124_647,
        "PP" => 37_558,
        "PSL" => 15_444,
        "PPL" => 12_095,
        "PRB" => 10_490,
        "PSOL" => 3_274,
        "PPS" => 2_544,
        "MDB" => 635,
    ),
    2022 => Dict(
        "PL" => 85_911,
        "PODE" => 29_923,
        "PSB" => 6_799,
        "PTB" => 587,
        "PDT" => 207,
        "PSC" => 31,
    ),
)
const CONVERTED_NOMINAL_TO_LEGEND_COLUMNS = [
    "QT_VOTOS_NOMINAIS_CONVR_LEG",
    "QT_VOTOS_NOM_CONVR_LEG_VALIDOS",
]

function party_mun_zone_path(year::Integer)
    return joinpath(RAW_ROOT, string(year), "party_mun_zone.csv")
end

function load_federal_deputy_party_mun_zone(year::Integer)
    path = party_mun_zone_path(year)
    if !isfile(path)
        println("SKIP year $year: missing $path")
        return nothing
    end

    df = CSV.read(path, DataFrame)
    Processing.upper_strip!(df, :DS_CARGO)
    filter!(row -> row.DS_CARGO == "DEPUTADO FEDERAL", df)
    return df
end

function colsum(df::DataFrame, col::AbstractString)
    return sum(Processing.to_int.(df[!, col]))
end

function party_converted_deltas(df::DataFrame, converted_col::AbstractString)
    tmp = DataFrame(
        SG_PARTIDO = String.(df.SG_PARTIDO),
        delta = Processing.to_int.(df[!, converted_col]),
    )
    deltas = combine(groupby(tmp, :SG_PARTIDO), :delta => sum => :delta)
    filter!(row -> row.delta != 0, deltas)
    sort!(deltas, :delta, rev = true)
    return deltas
end

function check_first_in_df_priority()
    df = DataFrame(
        QT_VOTOS_LEGENDA_VALIDOS = [1],
        QT_TOTAL_VOTOS_LEG_VALIDOS = [2],
    )
    selected = Processing.first_in_df(
        df,
        ["QT_TOTAL_VOTOS_LEG_VALIDOS", "QT_VOTOS_LEGENDA_VALIDOS"],
    )
    selected == "QT_TOTAL_VOTOS_LEG_VALIDOS" || error(
        "first_in_df did not respect candidate priority: selected $selected",
    )
    println("first_in_df priority check: selected $selected")
end

function check_year(year::Integer)
    df = load_federal_deputy_party_mun_zone(year)
    df === nothing && return nothing

    kwargs = vote_kwargs_for_year(year)
    nom_col, leg_col, total_col, scheme = Processing.detect_vote_cols(df; kwargs...)
    expected_nom, expected_leg = EXPECTED_COMPONENTS[year]

    nom_col == expected_nom || error("Year $year selected nominal column $nom_col, expected $expected_nom")
    leg_col == expected_leg || error("Year $year selected legend column $leg_col, expected $expected_leg")

    new_total = colsum(df, nom_col) + colsum(df, leg_col)
    println(
        "year $year selected nominal=$nom_col legend=$leg_col total=$total_col " *
        "scheme=$scheme national_total=$new_total",
    )

    if year in keys(EXPECTED_CONVERTED_DELTAS)
        old_total = colsum(df, "QT_VOTOS_NOMINAIS_VALIDOS") + colsum(df, "QT_VOTOS_LEGENDA_VALIDOS")
        converted_delta =
            colsum(df, "QT_TOTAL_VOTOS_LEG_VALIDOS") -
            colsum(df, "QT_VOTOS_LEGENDA_VALIDOS")
        converted_col = Processing.first_in_df(df, CONVERTED_NOMINAL_TO_LEGEND_COLUMNS)
        converted_col === nothing && error(
            "Year $year missing converted nominal-to-legend vote column",
        )
        converted_sum = colsum(df, converted_col)
        expected_delta = EXPECTED_CONVERTED_DELTAS[year]
        deltas = party_converted_deltas(df, converted_col)
        expected_party_deltas = EXPECTED_PARTY_DELTAS[year]

        converted_delta == converted_sum || error(
            "Year $year converted identity failed: delta=$converted_delta converted_sum=$converted_sum",
        )
        converted_delta == expected_delta || error(
            "Year $year converted delta $converted_delta, expected $expected_delta",
        )
        new_total - old_total == expected_delta || error(
            "Year $year new-old total delta $(new_total - old_total), expected $expected_delta",
        )
        nrow(deltas) == length(expected_party_deltas) || error(
            "Year $year has $(nrow(deltas)) converted-vote party deltas, " *
            "expected $(length(expected_party_deltas))",
        )
        for row in eachrow(deltas)
            expected_party_delta = get(expected_party_deltas, row.SG_PARTIDO, nothing)
            expected_party_delta === nothing && error(
                "Year $year unexpected converted-vote party delta: " *
                "$(row.SG_PARTIDO) $(row.delta)",
            )
            row.delta == expected_party_delta || error(
                "Year $year party $(row.SG_PARTIDO) delta $(row.delta), " *
                "expected $expected_party_delta",
            )
        end

        println(
            "year $year old_direct_legend_total=$old_total " *
            "new_total_delta=$converted_delta converted_col=$converted_col " *
            "converted_nominal_to_legend=$converted_sum",
        )
        println("year $year party converted-vote deltas:")
        show(deltas; allrows = true)
        println()
    end

    return (year = year, nominal = nom_col, legend = leg_col, total = total_col,
            scheme = scheme, national_total = new_total)
end

function main()
    check_first_in_df_priority()
    results = filter(!isnothing, [check_year(year) for year in YEARS])
    isempty(results) && println("No raw party_mun_zone.csv files were available; data checks skipped.")
    return results
end

main()
