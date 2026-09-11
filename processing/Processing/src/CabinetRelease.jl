"""Pinned historical release reader and explicit paper-only electoral adapter."""
module CabinetRelease
using CSV, DataFrames, Dates, JSON3, SHA
export load_release, calendar_table, identified_parties, period_windows, translate,
       default_pin_path, default_crosswalk_path
const ROOT = normpath(joinpath(@__DIR__, "..", "..", ".."))
default_pin_path() = joinpath(ROOT, "processing", "Processing", "data", "cabinet_release_pin.json")
default_crosswalk_path() = joinpath(ROOT, "processing", "Processing", "data", "cabinet_release_election_crosswalk.csv")
sha(path) = bytes2hex(open(SHA.sha256, path))
const CACHE = Dict{String,Any}()
function load_release(pin_path::AbstractString=default_pin_path())
    # No legacy fallback. A pin is mandatory even when a path override is used.
    pin_path = abspath(pin_path)
    haskey(CACHE, pin_path) && return CACHE[pin_path]
    isfile(pin_path) || error("Cabinet release pin missing: $pin_path")
    pin = JSON3.read(read(pin_path, String))
    dir = normpath(joinpath(ROOT, String(pin.release_path)))
    metadata_path = joinpath(dir, "metadata.json")
    sha(metadata_path) == String(pin.metadata_sha256) || error("Cabinet release metadata hash mismatch")
    meta = JSON3.read(read(metadata_path, String))
    string(meta.schema_version) == string(pin.schema_version) == "3" || error("Unsupported or inconsistent cabinet schema")
    String(meta.data_version) == String(pin.data_version) || error("Cabinet release version mismatch")
    String(meta.cutoff_exclusive) == String(pin.cutoff_exclusive) == "2026-03-20" || error("Cabinet cutoff differs from authorized release")
    for (file, digest) in pairs(meta.file_hashes)
        haskey(pin.file_hashes, file) && String(pin.file_hashes[file]) == String(digest) ||
            error("Release pin omits or disagrees with metadata file $file")
    end
    for (file, digest) in pairs(pin.file_hashes)
        path = joinpath(dir, String(file))
        isfile(path) && sha(path) == String(digest) || error("Cabinet file missing/hash mismatch: $file")
    end
    for file in ("daily_coverage.csv", "periods.csv", "membership.csv", "witnesses.csv", "primary_assumptions.csv")
        haskey(pin.file_hashes, Symbol(file)) || error("Release pin omits required file $file")
    end
    meta.primary_complete === true || error("Release has unfilled primary dates")
    Int(meta.primary_covered_days) == 4096 || error("V5 primary day count changed")
    Int(meta.identified_days) == 3996 && Int(meta.provisional_days) == 100 || error("V5 provenance totals changed")
    periods = CSV.read(joinpath(dir, "periods.csv"), DataFrame; stringtype=String)
    membership = CSV.read(joinpath(dir, "membership.csv"), DataFrame; stringtype=String)
    sort!(periods, :start_inclusive)
    sum(periods.days) == 4096 || error("Historical calendar duration mismatch")
    release = (; pin, dir, metadata=meta, periods, membership)
    CACHE[pin_path] = release
    return release
end
isidentified(status) = !occursin("unidentified", lowercase(String(status))) && !occursin("unknown", lowercase(String(status)))
function calendar_table(pin_path::AbstractString=default_pin_path())
    release = load_release(pin_path)
    # The canonical runner prepares these rows from DAILY sets, maps each day,
    # and only then recompresses. No historical-period metrics are calculated.
    adapter = joinpath(ROOT, "generated", "cabinet_v5")
    provenance = JSON3.read(read(joinpath(adapter, "provenance.json"), String))
    String(provenance.source_metadata_sha256) == String(release.pin.metadata_sha256) || error("Stale cabinet daily adapter")
    String(provenance.crosswalk_sha256) == String(release.pin.crosswalk_sha256) || error("Stale cabinet crosswalk adapter")
    analytical = CSV.read(joinpath(adapter, "cabinet_analysis_periods.csv"), DataFrame;
        stringtype=String, types=Dict(:period=>String))
    sum(analytical.days) == 4096 || error("Analytical calendar lost days")
    sum(analytical.established_days) == 3996 && sum(analytical.provisional_days) == 100 || error("Analytical provenance totals changed")
    rows = NamedTuple[]
    for row in eachrow(analytical)
        sources = split(row.source_period_ids, ';')
        push!(rows, (; election_year=Int(row.election_year), period=String(row.period),
            period_id=String(row.analytical_period_id), source_periods=JSON3.write(sources),
            administration_id=String(row.administration), start_inclusive=row.start_inclusive,
            end_exclusive=row.end_exclusive, days=Int(row.days),
            period_start=row.start_inclusive, period_end=row.end_exclusive-Day(1), period_days=Int(row.days),
            composition_status=String(row.historical_status), party_ids=String(row.historical_party_ids),
            election_party_set=String(row.election_party_set),
            unknown_person_ids=string(coalesce(row.provisional_person_ids,"")), decision_ids="",
            bounded_affiliation_ids=string(coalesce(row.sensitivity_ids,"")), bounded_service_ids="",
            established_days=Int(row.established_days), provisional_days=Int(row.provisional_days),
            primary_assumption_ids=string(coalesce(row.primary_assumption_ids,"")), identified=true))
    end
    return DataFrame(rows)
end
function identified_parties(pin_path::AbstractString=default_pin_path())
    calendar=calendar_table(pin_path)
    return Dict(String(row.period) => sort(String.(split(String(row.party_ids), ';'))) for row in eachrow(calendar))
end

function period_windows(pin_path::AbstractString=default_pin_path())
    return Dict{String,Tuple{Union{Date,Nothing},Union{Date,Nothing}}}(String(row.period) => (row.period_start,row.period_end)
        for row in eachrow(calendar_table(pin_path)))
end
function translate(parties; election_year, valid_election_parties,
                   crosswalk_path=default_crosswalk_path(), period="", historical_period_id="")
    # Production uses the pinned crosswalk. An explicit alternate path is a
    # caller-owned test/research fixture, still subject to all mapping checks.
    if abspath(crosswalk_path) == abspath(default_crosswalk_path())
        pin = load_release().pin
        sha(crosswalk_path) == String(pin.crosswalk_sha256) || error("Cabinet electoral crosswalk hash mismatch")
    end
    crosswalk=CSV.read(crosswalk_path,DataFrame; stringtype=String)
    required = [:election_year, :party_id, :election_party, :mapping_type, :basis]
    all(in.(required, Ref(propertynames(crosswalk)))) || error("Invalid cabinet crosswalk schema")
    any(ismissing, Matrix(crosswalk[:, required])) && error("Missing cabinet crosswalk field")
    all(!isempty(strip(String(value))) for name in required[2:end] for value in crosswalk[!, name]) || error("Empty cabinet crosswalk field")
    nrow(unique(crosswalk[:, [:election_year, :party_id, :election_party]])) == nrow(crosswalk) || error("Duplicate cabinet electoral mapping")
    allowed_mapping_types = Set(["explicit_identity", "crosswalk_rename", "crosswalk_fusion_expansion"])
    all(in.(String.(crosswalk.mapping_type), Ref(allowed_mapping_types))) || error("Unsupported cabinet electoral mapping type")
    valid=Set(String.(valid_election_parties)); rows=NamedTuple[]
    for party in sort(unique(String.(parties)))
        matches=crosswalk[(crosswalk.election_year .== election_year) .& (crosswalk.party_id .== party),:]
        nrow(matches)>0 || error("Unmapped cabinet identity $party for $election_year; explicit adjudication required")
        for row in eachrow(matches)
            mapped=String(row.election_party)
            mapped in valid || error("Cabinet crosswalk target $mapped absent from $election_year election input")
            push!(rows,(; election_year=Int(election_year), period=String(period),
                historical_period_id=String(historical_period_id), party_id=party,
                cabinet_party_raw=party, cabinet_party_normalized=party, cabinet_party_canonical=party,
                election_party=mapped, mapping_type=String(row.mapping_type), mapped_party_count=nrow(matches),
                notes=String(row.basis)))
        end
    end
    return isempty(rows) ? DataFrame(election_year=Int[],period=String[],historical_period_id=String[],party_id=String[],
        cabinet_party_raw=String[],cabinet_party_normalized=String[],cabinet_party_canonical=String[],
        election_party=String[],mapping_type=String[],mapped_party_count=Int[],notes=String[]) : DataFrame(rows)
end
end
