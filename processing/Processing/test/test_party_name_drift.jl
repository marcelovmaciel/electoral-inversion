using Test
using CSV
using DataFrames

const _PROCESSING_MODULE_FILE = joinpath(@__DIR__, "..", "src", "Processing.jl")
const _ROOT_DIR = abspath(@__DIR__, "..", "..", "..")
const _COALITION_CSV = joinpath(_ROOT_DIR, "scraping", "output", "partidos_por_periodo.csv")
const _PMZ_DIR = joinpath(_ROOT_DIR, "data", "raw", "electionsBR")
const _FIXTURES_DIR = joinpath(@__DIR__, "fixtures")

const _PROCESSING_LOADED = let
    try
        include(_PROCESSING_MODULE_FILE)
        @eval using .Processing
        true
    catch err
        @test_skip "Não foi possível carregar módulo Processing: $(sprint(showerror, err))"
        false
    end
end

_parse_period_year(period::AbstractString) = parse(Int, first(split(strip(period), ".")))

function _read_fixture_set(path::AbstractString)::Vector{String}
    isfile(path) || error("Fixture não encontrado: $path")
    vals = String[]
    for line in eachline(path)
        v = strip(line)
        isempty(v) && continue
        push!(vals, v)
    end
    return sort(unique(vals))
end

function _expected_fixture(name::AbstractString)::Vector{String}
    return _read_fixture_set(joinpath(_FIXTURES_DIR, name))
end

function _actual_election_set(year::Int)::Vector{String}
    path = joinpath(_PMZ_DIR, string(year), "party_mun_zone.csv")
    df = CSV.read(path, DataFrame; select=["SG_PARTIDO"])

    vals = Set{String}()
    for x in df.SG_PARTIDO
        raw = strip(string(x))
        isempty(raw) && continue
        canon = Processing.canonical_party(raw; year = year)
        isempty(strip(canon)) || push!(vals, canon)
    end
    return sort(collect(vals))
end

function _actual_mandate_set(election_year::Int)::Vector{String}
    coalition_df = CSV.read(_COALITION_CSV, DataFrame)
    hasproperty(coalition_df, :periodo) || error("CSV de coalizão sem coluna 'periodo'.")
    hasproperty(coalition_df, :partido) || error("CSV de coalizão sem coluna 'partido'.")

    years = election_year == 2014 ? Set(2015:2018) :
            election_year == 2018 ? Set(2019:2022) :
            election_year == 2022 ? Set(2023:2025) :
            error("Ano de eleição sem janela de mandato suportada: $election_year")

    vals = Set{String}()
    for row in eachrow(coalition_df)
        period = strip(string(row.periodo))
        party = strip(string(row.partido))
        isempty(period) && continue
        isempty(party) && continue
        y = _parse_period_year(period)
        y in years || continue
        canon = Processing.canonical_party(party; year = y)
        isempty(strip(canon)) || push!(vals, canon)
    end
    return sort(collect(vals))
end

@testset "Party Name Drift" begin
    if !_PROCESSING_LOADED
        @test_skip "Módulo Processing não carregado; testes de drift ignorados."
    else
        @testset "Cobertura de canonicalização em coalizões" begin
            @test isfile(_COALITION_CSV)
            coalition_df = CSV.read(_COALITION_CSV, DataFrame)
            mapped = String[]
            for row in eachrow(coalition_df)
                period = strip(string(row.periodo))
                party = strip(string(row.partido))
                y = _parse_period_year(period)
                canon = Processing.canonical_party(party; year = y)
                push!(mapped, canon)
            end
            @test all(!isempty(strip(x)) for x in mapped)
        end

        @testset "Aliases históricos exigem year no modo estrito" begin
            @test_throws ErrorException Processing.canonical_party("PMDB")
            @test_throws ErrorException Processing.canonical_party("PR")
            @test_throws ErrorException Processing.canonical_party("PRB")
            @test_throws ErrorException Processing.canonical_party("PPS")
            @test_throws ErrorException Processing.canonical_party("PEN")
            @test_throws ErrorException Processing.canonical_party("PTN")
            @test_throws ErrorException Processing.canonical_party("PT do B")
        end

        @testset "Casos críticos conhecidos" begin
            @test Processing.canonical_party("PMDB"; year = 2014) == "PMDB"
            @test Processing.canonical_party("PMDB"; year = 2022) == "MDB"

            @test Processing.canonical_party("PC do B"; year = 2014) == "PCdoB"
            @test Processing.canonical_party("PCdoB"; year = 2022) == "PCdoB"

            @test Processing.canonical_party("PRB"; year = 2018) == "PRB"
            @test Processing.canonical_party("PRB"; year = 2022) == "REPUBLICANOS"
            @test Processing.canonical_party("REPU"; year = 2022) == "REPUBLICANOS"
            @test Processing.canonical_party("REP"; year = 2022) == "REPUBLICANOS"

            @test Processing.canonical_party("PEN"; year = 2014) == "PEN"
            @test Processing.canonical_party("PEN"; year = 2022) == "PATRIOTA"
            @test Processing.canonical_party("PATRI"; year = 2018) == "PATRIOTA"

            @test Processing.canonical_party("PPS"; year = 2018) == "PPS"
            @test Processing.canonical_party("PPS"; year = 2022) == "CIDADANIA"

            @test Processing.canonical_party("UNIAO"; year = 2022) == "UNIÃO"
            @test Processing.canonical_party("UNIÃO"; year = 2022) == "UNIÃO"
        end

        @testset "Snapshot eleições (fixture)" begin
            for year in (2014, 2018, 2022)
                expected = _expected_fixture("canonical_set_election_$(year).txt")
                actual = _actual_election_set(year)
                @test actual == expected
            end
        end

        @testset "Snapshot mandatos (fixture)" begin
            for election_year in (2014, 2018, 2022)
                expected = _expected_fixture("canonical_set_mandate_$(election_year).txt")
                actual = _actual_mandate_set(election_year)
                @test actual == expected
            end
        end
    end
end
