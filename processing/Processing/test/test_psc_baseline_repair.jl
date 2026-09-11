using Test
using CSV
using DataFrames
using Dates

const _ROOT_DIR_PSC = abspath(@__DIR__, "..", "..", "..")
const _CANDIDATE_2018_PSC = joinpath(
    _ROOT_DIR_PSC,
    "processing",
    "Processing",
    "data",
    "raw",
    "electionsBR",
    "2018",
    "candidate.csv",
)

@testset "PSC baseline repair regressions" begin
    needed = [
        :SQ_CANDIDATO,
        :SG_UF,
        :DS_CARGO,
        :NM_CANDIDATO,
        :NM_URNA_CANDIDATO,
        :SG_PARTIDO,
        :DS_SITUACAO_CANDIDATURA,
        :DS_SIT_TOT_TURNO,
    ]
    candidates = CSV.read(
        _CANDIDATE_2018_PSC,
        DataFrame;
        select = needed,
        normalizenames = true,
        types = Dict(:SQ_CANDIDATO => String),
    )
    filter!(
        row -> uppercase(strip(String(row.DS_CARGO))) == "DEPUTADO FEDERAL",
        candidates,
    )
    statuses = uppercase.(strip.(String.(candidates.DS_SIT_TOT_TURNO)))
    counted = in.(statuses, Ref(Processing.WINNER_STATUSES))
    candidates[!, :current_loader_counts] = counted

    @test Set(statuses) == Set([
        "#NULO#",
        "ELEITO POR MÉDIA",
        "ELEITO POR QP",
        "NÃO ELEITO",
        "SUPLENTE",
    ])
    @test count(counted) == 513
    @test length(unique(candidates.SQ_CANDIDATO[counted])) == 513

    elected = candidates[counted, :]
    elected[!, :canonical_party] = [
        Processing.canonical_party(String(raw); year = 2018)
        for raw in elected.SG_PARTIDO
    ]
    raw_seats = combine(groupby(elected, :SG_PARTIDO), nrow => :seats)
    canonical_seats = combine(groupby(elected, :canonical_party), nrow => :seats)
    @test sum(raw_seats.seats) == 513
    @test sum(canonical_seats.seats) == 513
    @test only(raw_seats.seats[raw_seats.SG_PARTIDO .== "PSC"]) == 7
    @test only(
        canonical_seats.seats[canonical_seats.canonical_party .== "PSC"]
    ) == 7

    expected_psc_ids = Set([
        "90000615998",
        "130000611044",
        "170000616969",
        "160000619724",
        "190000607836",
        "250000615219",
        "270000610932",
    ])
    actual_psc_ids = Set(String.(elected.SQ_CANDIDATO[elected.SG_PARTIDO .== "PSC"]))
    @test actual_psc_ids == expected_psc_ids

    valdevan = only(
        eachrow(candidates[candidates.SQ_CANDIDATO .== "260000621977", :])
    )
    marcio = only(
        eachrow(candidates[candidates.SQ_CANDIDATO .== "260000623622", :])
    )
    @test valdevan.NM_URNA_CANDIDATO == "VALDEVAN NOVENTA"
    @test valdevan.SG_PARTIDO == "PSC"
    @test valdevan.DS_SITUACAO_CANDIDATURA == "INAPTO"
    @test valdevan.DS_SIT_TOT_TURNO == "NÃO ELEITO"
    @test !valdevan.current_loader_counts
    @test marcio.NM_URNA_CANDIDATO == "MARCIO MACÊDO"
    @test marcio.SG_PARTIDO == "PT"
    @test marcio.DS_SITUACAO_CANDIDATURA == "APTO"
    @test marcio.DS_SIT_TOT_TURNO == "ELEITO POR MÉDIA"
    @test marcio.current_loader_counts

    # Documentary service and affiliation regression, independent of whether the
    # aggregate cabinet is identifiable on the same dates.
    release = Processing.CabinetRelease.load_release()
    affiliations = CSV.read(joinpath(release.dir, "affiliations.csv"), DataFrame; stringtype = String)
    services = CSV.read(joinpath(release.dir, "services.csv"), DataFrame; stringtype = String)
    witnesses = CSV.read(joinpath(release.dir, "witnesses.csv"), DataFrame; stringtype = String)
    gilson = services[(services.person_name .== "Gilson Machado Neto") .& services.included, :]
    @test nrow(gilson) == 1
    service = only(eachrow(gilson))
    @test service.start_inclusive == Date(2020,12,9)
    @test service.end_exclusive == Date(2022,3,31)
    person_affiliations = affiliations[affiliations.person_id .== service.person_id, :]
    psc = only(eachrow(person_affiliations[coalesce.(person_affiliations.party_id .== "PSC", false), :]))
    pl = only(eachrow(person_affiliations[coalesce.(person_affiliations.party_id .== "PL", false), :]))
    @test psc.start_inclusive == Date(2020,12,9)
    @test psc.end_exclusive == pl.start_inclusive == Date(2022,3,30)
    @test pl.end_exclusive >= service.end_exclusive
    @test !isempty(psc.evidence_ids) && !isempty(pl.evidence_ids)
    @test occursin("R24", pl.decision_ids)
    person_witnesses = witnesses[witnesses.person_id .== service.person_id, :]
    @test all(person_witnesses.end_exclusive .<= service.end_exclusive)
    @test all(person_witnesses.start_inclusive .>= service.start_inclusive)
    march30 = person_witnesses[(person_witnesses.start_inclusive .<= Date(2022,3,30)) .&
                              (person_witnesses.end_exclusive .> Date(2022,3,30)), :]
    @test Set(march30.party_id) == Set(["PL"])
    @test all(march30.affiliation_id .== pl.affiliation_id)
    @test all(march30.end_exclusive .== Date(2022,3,31))
end
