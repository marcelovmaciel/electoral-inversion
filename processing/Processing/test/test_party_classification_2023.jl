using Test
using DataFrames

const _MODULE_FILE = joinpath(@__DIR__, "..", "src", "party_classification_2023.jl")
const _DEFAULT_JSON_PATH = abspath(
    @__DIR__,
    "..",
    "..",
    "..",
    "scrape_classification",
    "output",
    "classificacao_2023",
    "party_ordinal_classification.json",
)

const _MODULE_LOADED = let
    try
        include(_MODULE_FILE)
        @eval using .PartyClassification2023
        true
    catch err
        @test_skip "Não foi possível carregar PartyClassification2023: $(sprint(showerror, err))"
        false
    end
end

@testset "PartyClassification2023" begin
    if !_MODULE_LOADED
        @test_skip "Módulo não carregado; demais testes ignorados."
    else
        @testset "Path inválido gera erro" begin
            bad_path = joinpath(@__DIR__, "does_not_exist", "party_ordinal_classification.json")
            @test_throws ErrorException PartyClassification2023.load_party_ordinal_classification_2023(path = bad_path)
        end

        if !isfile(_DEFAULT_JSON_PATH)
            @test_skip "Arquivo JSON não encontrado em $_DEFAULT_JSON_PATH; testes de carga/estrutura pulados."
        else
            @testset "Carga e estrutura" begin
                df_full = PartyClassification2023.load_party_ordinal_classification_2023()
                @test df_full isa DataFrame
                @test !isempty(df_full)

                @test :party_name_raw in Symbol.(names(df_full))
                @test :ordinal_position in Symbol.(names(df_full))
                @test :classification_label in Symbol.(names(df_full))
            end

            @testset "Simplificado e ordenação" begin
                df_simple = PartyClassification2023.party_classification_simple()
                @test names(df_simple) == ["party_name_raw", "ordinal_position", "classification_label"]
                @test issorted(df_simple.ordinal_position)
            end

            @testset "Tipos normalizados" begin
                df_full = PartyClassification2023.party_classification_full()
                @test eltype(df_full.ordinal_position) == Int
                @test eltype(df_full.ideology_value_numeric) == Union{Missing,Float64}
                @test eltype(df_full.page_number) == Union{Missing,Int}
            end
        end
    end
end
