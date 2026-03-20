using Test
using DataFrames

const _PROCESSING_MODULE_FILE_STRICT = joinpath(@__DIR__, "..", "src", "Processing.jl")

const _PROCESSING_LOADED_STRICT = let
    try
        include(_PROCESSING_MODULE_FILE_STRICT)
        @eval using .Processing
        true
    catch err
        @test_skip "Nao foi possivel carregar modulo Processing: $(sprint(showerror, err))"
        false
    end
end

@testset "coalition strict" begin
    if !_PROCESSING_LOADED_STRICT
        @test_skip "Modulo Processing nao carregado; testes strict ignorados."
    else
        @testset "coalition_metrics strict behavior" begin
            df = DataFrame(
                party = ["A", "B", "C"],
                votes = [60, 30, 10],
                seats = [3, 2, 1],
            )

            metrics = Processing.coalition_metrics(
                df,
                ["A", "B"];
                vote_col = :votes,
                seat_col = :seats,
                party_col = :party,
                coalition_name = "test_success",
                mandate_id = "9999-1002",
                coalition_source = :test,
            )

            @test metrics.coalition_votes == 90.0
            @test metrics.coalition_seats == 5.0
            @test metrics.n_parties_df == 3
            @test metrics.n_parties_coalition == 2
            @test metrics.inversion == ((metrics.seat_share - metrics.vote_share) < 0)

            @test_throws ErrorException Processing.coalition_metrics(
                df,
                ["D"];
                vote_col = :votes,
                seat_col = :seats,
                party_col = :party,
                coalition_name = "test_missing_party",
                mandate_id = "9999-1002",
                coalition_source = :test,
            )

            df_dup = DataFrame(
                party = ["A", "A", "B"],
                votes = [10, 20, 30],
                seats = [1, 1, 2],
            )
            @test_throws ErrorException Processing.coalition_metrics(
                df_dup,
                ["A"];
                vote_col = :votes,
                seat_col = :seats,
                party_col = :party,
                coalition_name = "test_duplicates",
                mandate_id = "9999-1002",
                coalition_source = :test,
            )
        end

        @testset "canonicalize_parties strict behavior" begin
            mapped = Processing.canonicalize_parties(["REPU", "PT"]; year = 2023, strict = true)
            @test mapped == ["PT", "REPUBLICANOS"]

            @test_throws ErrorException Processing.canonicalize_parties(["PR"]; strict = true)
            @test_throws ErrorException Processing.canonicalize_parties(["PARTIDO_INEXISTENTE_ABC"]; year = 2023, strict = true)

            mapped_non_strict = Processing.canonicalize_parties(["PARTIDO_INEXISTENTE_ABC"]; year = 2023, strict = false)
            @test mapped_non_strict == [Processing.UNKNOWN_PARTY]
        end
    end
end
