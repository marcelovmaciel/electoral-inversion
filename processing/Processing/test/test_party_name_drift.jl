using Test
using CSV
using DataFrames

const _PROCESSING_MODULE_FILE_DRIFT = joinpath(@__DIR__, "..", "src", "Processing.jl")
const _ROOT_DIR = abspath(@__DIR__, "..", "..", "..")
const _CABINET_PIN_DRIFT = joinpath(_ROOT_DIR, "processing", "Processing", "data", "cabinet_release_pin.json")
const _PMZ_DIR = joinpath(_ROOT_DIR, "data", "raw", "electionsBR")
const _FIXTURES_DIR = joinpath(@__DIR__, "fixtures")

const _PROCESSING_LOADED_DRIFT = let
    try
        if !isdefined(@__MODULE__, :Processing)
            include(_PROCESSING_MODULE_FILE_DRIFT)
        end
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

function _canonical_election_label_set(values, year::Int)::Vector{String}
    raws = String[]
    seen = Set{String}()
    for value in values
        raw = strip(string(value))
        (isempty(raw) || raw in seen) && continue
        push!(seen, raw)
        push!(raws, raw)
    end
    return Processing.canonicalize_parties(raws; year = year, strict = true)
end

function _actual_election_set(year::Int)::Vector{String}
    path = joinpath(_PMZ_DIR, string(year), "party_mun_zone.csv")
    # Match the authoritative Chamber pipeline: the raw files also contain
    # other offices and their party/coalition labels, outside this analysis.
    df = CSV.read(path, DataFrame; select=["DS_CARGO", "SG_PARTIDO"])
    federal = uppercase.(strip.(String.(df.DS_CARGO))) .== "DEPUTADO FEDERAL"
    return _canonical_election_label_set(df.SG_PARTIDO[federal], year)
end


@testset "Party Name Drift" begin
    if !_PROCESSING_LOADED_DRIFT
        @test_skip "Módulo Processing não carregado; testes de drift ignorados."
    else
        @testset "Explicit cabinet identity coverage" begin
            @test isfile(_CABINET_PIN_DRIFT)
            calendar = Processing.CabinetRelease.calendar_table(_CABINET_PIN_DRIFT)
            parties = Processing.CabinetRelease.identified_parties(_CABINET_PIN_DRIFT)
            for row in eachrow(calendar[calendar.identified, :])
                valid = _expected_fixture("canonical_set_election_$(row.election_year).txt")
                mapped = Processing.CabinetRelease.translate(parties[row.period];
                    election_year = row.election_year, valid_election_parties = valid,
                    period = row.period, historical_period_id = row.period_id)
                @test Set(mapped.party_id) == Set(parties[row.period])
                @test all(in.(mapped.election_party, Ref(Set(valid))))
                @test all(!isempty, mapped.notes)
                @test all(mapped.historical_period_id .== row.period_id)
            end
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

        @testset "Canonicalização em lote preserva o conjunto" begin
            @test _canonical_election_label_set(
                [" PT ", "PT", "PMDB", "PMDB", ""],
                2014,
            ) == ["PMDB", "PT"]
        end

        @testset "Snapshot eleições (fixture)" begin
            for year in (2014, 2018, 2022)
                expected = _expected_fixture("canonical_set_election_$(year).txt")
                actual = _actual_election_set(year)
                @test actual == expected
            end
        end

        @testset "Explicit rename, fusion and rejection semantics" begin
            valid2014 = _expected_fixture("canonical_set_election_2014.txt")
            valid2018 = _expected_fixture("canonical_set_election_2018.txt")
            rename = Processing.CabinetRelease.translate(["PL"]; election_year = 2014,
                valid_election_parties = valid2014)
            @test rename.election_party == ["PR"]
            fusion = Processing.CabinetRelease.translate(["UNIAO", "DEM", "PSL", "UNIAO"];
                election_year = 2018, valid_election_parties = valid2018)
            @test Set(fusion.election_party) == Set(["DEM", "PSL"])
            @test Set(fusion.election_party[fusion.party_id .== "UNIAO"]) == Set(["DEM", "PSL"])
            @test all(fusion.mapping_type[fusion.party_id .== "UNIAO"] .== "crosswalk_fusion_expansion")
            @test length(unique(fusion.election_party)) == 2
            @test_throws ErrorException Processing.CabinetRelease.translate(["UNREVIEWED"];
                election_year = 2018, valid_election_parties = valid2018)
            @test_throws ErrorException Processing.CabinetRelease.translate(["PT"];
                election_year = 2018, valid_election_parties = ["PSDB"])
            @test isempty(Processing.CabinetRelease.translate(String[];
                election_year = 2018, valid_election_parties = valid2018))
            mktempdir() do tempdir
                path = joinpath(tempdir, "crosswalk.csv")
                write(path, "election_year,party_id,election_party,mapping_type,basis\n2018,PT,PT,explicit_identity,fixture\n")
                @test Processing.CabinetRelease.translate(["PT"]; election_year = 2018,
                    valid_election_parties = ["PT"], crosswalk_path = path).election_party == ["PT"]
                write(path, "election_year,party_id,election_party,mapping_type,basis\n2018,PT,PT,inferred,fixture\n")
                @test_throws ErrorException Processing.CabinetRelease.translate(["PT"]; election_year = 2018,
                    valid_election_parties = ["PT"], crosswalk_path = path)
                write(path, "election_year,party_id,election_party,mapping_type,basis\n2018,PT,PT,explicit_identity,fixture\n2018,PT,PT,explicit_identity,duplicate\n")
                @test_throws ErrorException Processing.CabinetRelease.translate(["PT"]; election_year = 2018,
                    valid_election_parties = ["PT"], crosswalk_path = path)
            end
        end
    end
end
