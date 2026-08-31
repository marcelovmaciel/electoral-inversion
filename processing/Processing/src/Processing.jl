module Processing

using CSV, DataFrames, Statistics, Glob, JSON, JSON3, Dates, Unicode

export normalize_party,
       canonical_party,
       canonical_party_at_date,
       canonicalize_parties,
       load_party_lineage_events,
       load_party_classification,
       canonicalize_party_classification!,
       classification_minimal,
       ideological_interval_coalitions,
       ideological_k_gap_coalitions,
       prepare_representation_profile,
       plot_representation_profile,
       representation_profile_summary,
       make_representation_profile

include("PartyNames.jl")
include("party_classification_2023.jl")
include("party_classification.jl")
include("code.jl")
include("analysis_runner_core.jl")
include("representation_profile.jl")
#include("overengineered_code.jl")

end
