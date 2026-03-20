using Test
using DataFrames

const _PROCESSING_MODULE_FILE = joinpath(@__DIR__, "..", "src", "Processing.jl")

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

@testset "PartyClassification Generic Loader" begin
    if !_PROCESSING_LOADED
        @test_skip "Módulo Processing não carregado; testes ignorados."
    else
        @test_throws ErrorException Processing.load_party_classification(1999)

        for year in (2023, 2025)
            df = Processing.load_party_classification(year)
            @test df isa DataFrame
            @test nrow(df) > 0
            @test :party_name_raw in Symbol.(names(df))
            @test :ordinal_position in Symbol.(names(df))
            @test :classification_label in Symbol.(names(df))
            @test :source_year in Symbol.(names(df))
            @test all(df.source_year .== year)
        end

        df_2025 = Processing.load_party_classification(2025)
        Processing.canonicalize_party_classification!(df_2025; year = 2025, strict = false)
        @test :party_raw in Symbol.(names(df_2025))
        @test :party_norm in Symbol.(names(df_2025))
        @test :party_canon in Symbol.(names(df_2025))

        minimal = Processing.classification_minimal(df_2025)
        @test names(minimal) == ["party_canon", "ordinal_position", "classification_label", "source_year"]
        @test nrow(minimal) == nrow(df_2025)
        @test issorted(minimal.ordinal_position)

        toy = DataFrame(Partido = ["PARTIDO_INEXISTENTE"], media_ponderada = [5.0], ideologia_categorica = ["centro"])
        Processing.canonicalize_party_classification!(toy; year = 2025, strict = false)
        @test toy.party_canon[1] == Processing.UNKNOWN_PARTY

        toy_strict = DataFrame(Partido = ["PARTIDO_INEXISTENTE"], media_ponderada = [5.0], ideologia_categorica = ["centro"])
        @test_throws ErrorException Processing.canonicalize_party_classification!(toy_strict; year = 2025, strict = true)

        audit = Processing.party_classification_audit(df_2025; year = 2025)
        @test :audit_type in Symbol.(names(audit))
        @test :detail in Symbol.(names(audit))

        tmp_dir = mktempdir()
        audit_path = Processing.write_party_classification_audit(df_2025; year = 2025, out_dir = tmp_dir)
        @test isfile(audit_path)
    end
end
