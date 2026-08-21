using Test
using DataFrames

@testset "effective-vote robustness" begin
    votes = DataFrame(
        SG_UF = ["X", "X", "X", "Y", "Y", "Y"],
        SG_PARTIDO = ["A", "B", "C", "A", "B", "C"],
        votes = [30, 20, 10, 20, 20, 40],
    )
    seats = DataFrame(
        SG_UF = ["X", "X", "X", "Y", "Y", "Y"],
        SG_PARTIDO = ["A", "B", "C", "A", "B", "C"],
        seats = [4, 2, 0, 2, 0, 2],
    )
    lists = DataFrame(
        SG_UF = ["X", "X", "X", "Y", "Y", "Y"],
        SG_PARTIDO = ["A", "B", "C", "A", "B", "C"],
        LISTA = ["AB", "AB", "C", "A", "BC", "BC"],
    )
    result = Processing.coalition_effective_vote_robustness(
        votes,
        seats,
        Dict("p1" => ["A", "B"]);
        election_year = 2022,
        list_map_uf = lists,
    )

    @test Set(result.definition) == Set([
        "all_valid",
        "national_party_seat_winning",
        "state_party_seat_winning",
        "state_list_seat_winning",
    ])
    all_valid = only(eachrow(result[result.definition .== "all_valid", :]))
    state_party = only(eachrow(result[result.definition .== "state_party_seat_winning", :]))
    state_list = only(eachrow(result[result.definition .== "state_list_seat_winning", :]))
    @test all_valid.vote_share == 90 / 140
    @test state_party.denominator_votes == 110
    @test state_party.vote_share == 70 / 110
    @test state_list.denominator_votes == 130
    @test state_list.vote_share == 90 / 130
    @test all(result.coalition_seats .== 8)
    @test all(result.total_seats .== 10)
end
