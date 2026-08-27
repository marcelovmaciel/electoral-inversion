using Test
using CSV
using DataFrames


module RepresentationProfileTestEnvironment
include(joinpath(@__DIR__, "..", "src", "Processing.jl"))
end

const ProfileProcessing = RepresentationProfileTestEnvironment.Processing


function _toy_representation_profile_data()
    years = repeat([2014, 2018, 2022]; inner = 2)
    return DataFrame(
        election_year = years,
        party = repeat(["A", "B"], 3),
        votes = repeat([40, 60], 3),
        vote_share = repeat([0.4, 0.6], 3),
        seats = repeat([0, 513], 3),
        seat_share = repeat([0.0, 1.0], 3),
        quota = repeat([205.2, 307.8], 3),
        seat_diff = repeat([-205.2, 205.2], 3),
    )
end


@testset "party representation profile" begin
    input = _toy_representation_profile_data()
    prepared = ProfileProcessing.prepare_representation_profile(input)

    @test nrow(prepared) == nrow(input) == 6
    @test prepared.vote_share_percent == repeat([40.0, 60.0], 3)
    @test prepared.representation_ratio[1:2] ≈ [0.0, 1 / 0.6]
    @test all(prepared.representation_ratio[prepared.seats .== 0] .== 0)

    invalid_vote_share = copy(input)
    invalid_vote_share.vote_share[1] = 0.0
    @test_throws ArgumentError ProfileProcessing.prepare_representation_profile(
        invalid_vote_share,
    )

    missing_year = input[input.election_year .!= 2022, :]
    @test_throws ArgumentError ProfileProcessing.prepare_representation_profile(missing_year)

    mktempdir() do temporary_directory
        input_path = joinpath(temporary_directory, "input.csv")
        data_output_path = joinpath(temporary_directory, "profile.csv")
        figure_output_path = joinpath(temporary_directory, "profile.pdf")
        CSV.write(input_path, input)

        plotted = ProfileProcessing.make_representation_profile(
            input_path,
            data_output_path,
            figure_output_path,
        )
        written = CSV.read(data_output_path, DataFrame)

        @test nrow(plotted) == nrow(input)
        @test nrow(written) == nrow(input)
        @test written.representation_ratio ≈ plotted.representation_ratio
        @test isfile(figure_output_path)
        @test filesize(figure_output_path) > 0
    end
end
