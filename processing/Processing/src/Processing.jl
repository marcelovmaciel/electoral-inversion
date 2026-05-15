module Processing

using CSV, DataFrames, Statistics, Glob, JSON, JSON3, Dates, Unicode

export normalize_party,
       canonical_party,
       canonicalize_parties,
       load_party_classification,
       canonicalize_party_classification!,
       classification_minimal,
       ideological_interval_coalitions

include("PartyNames.jl")
include("party_classification_2023.jl")
include("party_classification.jl")
include("code.jl")
include("analysis_runner_core.jl")
#include("overengineered_code.jl")

end
