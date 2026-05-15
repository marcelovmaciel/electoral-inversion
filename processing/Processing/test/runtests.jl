using Test

@testset "Processing.jl" begin
    include("test_party_classification.jl")
    include("test_party_classification_2023.jl")
    include("test_party_name_drift.jl")
    include("test_coalition_strict.jl")
    include("test_coalition_period_linkage.jl")
    include("test_ideological_interval_coalitions.jl")
end
