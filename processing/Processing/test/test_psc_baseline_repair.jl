using Test
using CSV
using DataFrames
using Dates

const _ROOT_DIR_PSC = abspath(@__DIR__, "..", "..", "..")
const _CANDIDATE_2018_PSC = joinpath(
    _ROOT_DIR_PSC,
    "processing",
    "Processing",
    "data",
    "raw",
    "electionsBR",
    "2018",
    "candidate.csv",
)
const _CABINET_PERIOD_CSV_PSC = joinpath(
    _ROOT_DIR_PSC,
    "scraping",
    "output",
    "partidos_por_periodo.csv",
)

@testset "PSC baseline repair regressions" begin
    needed = [
        :SQ_CANDIDATO,
        :SG_UF,
        :DS_CARGO,
        :NM_CANDIDATO,
        :NM_URNA_CANDIDATO,
        :SG_PARTIDO,
        :DS_SITUACAO_CANDIDATURA,
        :DS_SIT_TOT_TURNO,
    ]
    candidates = CSV.read(
        _CANDIDATE_2018_PSC,
        DataFrame;
        select = needed,
        normalizenames = true,
        types = Dict(:SQ_CANDIDATO => String),
    )
    filter!(
        row -> uppercase(strip(String(row.DS_CARGO))) == "DEPUTADO FEDERAL",
        candidates,
    )
    statuses = uppercase.(strip.(String.(candidates.DS_SIT_TOT_TURNO)))
    counted = in.(statuses, Ref(Processing.WINNER_STATUSES))
    candidates[!, :current_loader_counts] = counted

    @test Set(statuses) == Set([
        "#NULO#",
        "ELEITO POR MÉDIA",
        "ELEITO POR QP",
        "NÃO ELEITO",
        "SUPLENTE",
    ])
    @test count(counted) == 513
    @test length(unique(candidates.SQ_CANDIDATO[counted])) == 513

    elected = candidates[counted, :]
    elected[!, :canonical_party] = [
        Processing.canonical_party(String(raw); year = 2018)
        for raw in elected.SG_PARTIDO
    ]
    raw_seats = combine(groupby(elected, :SG_PARTIDO), nrow => :seats)
    canonical_seats = combine(groupby(elected, :canonical_party), nrow => :seats)
    @test sum(raw_seats.seats) == 513
    @test sum(canonical_seats.seats) == 513
    @test only(raw_seats.seats[raw_seats.SG_PARTIDO .== "PSC"]) == 7
    @test only(
        canonical_seats.seats[canonical_seats.canonical_party .== "PSC"]
    ) == 7

    expected_psc_ids = Set([
        "90000615998",
        "130000611044",
        "170000616969",
        "160000619724",
        "190000607836",
        "250000615219",
        "270000610932",
    ])
    actual_psc_ids = Set(String.(elected.SQ_CANDIDATO[elected.SG_PARTIDO .== "PSC"]))
    @test actual_psc_ids == expected_psc_ids

    valdevan = only(
        eachrow(candidates[candidates.SQ_CANDIDATO .== "260000621977", :])
    )
    marcio = only(
        eachrow(candidates[candidates.SQ_CANDIDATO .== "260000623622", :])
    )
    @test valdevan.NM_URNA_CANDIDATO == "VALDEVAN NOVENTA"
    @test valdevan.SG_PARTIDO == "PSC"
    @test valdevan.DS_SITUACAO_CANDIDATURA == "INAPTO"
    @test valdevan.DS_SIT_TOT_TURNO == "NÃO ELEITO"
    @test !valdevan.current_loader_counts
    @test marcio.NM_URNA_CANDIDATO == "MARCIO MACÊDO"
    @test marcio.SG_PARTIDO == "PT"
    @test marcio.DS_SITUACAO_CANDIDATURA == "APTO"
    @test marcio.DS_SIT_TOT_TURNO == "ELEITO POR MÉDIA"
    @test marcio.current_loader_counts

    cabinet = CSV.read(_CABINET_PERIOD_CSV_PSC, DataFrame; types = Dict(:periodo => String))
    psc_rows = unique(
        select(cabinet[cabinet.partido .== "PSC", :], :periodo, :data_inicio, :data_fim),
    )
    sort!(psc_rows, :data_inicio)
    @test String.(psc_rows.periodo) == [
        "2020.4",
        "2020.5",
        "2021.1",
        "2021.2",
        "2021.3",
        "2022.1",
    ]
    @test first(psc_rows.data_inicio) == Date(2020, 12, 9)
    @test last(psc_rows.data_fim) == Date(2022, 3, 29)
    for (previous, current) in zip(eachrow(psc_rows[1:end-1, :]), eachrow(psc_rows[2:end, :]))
        @test previous.data_fim + Day(1) == current.data_inicio
    end
    @test !any(
        (cabinet.periodo .== "2022.2") .& (cabinet.partido .== "PSC"),
    )
    @test any(
        (cabinet.periodo .== "2022.2") .& (cabinet.partido .== "PL"),
    )
end
