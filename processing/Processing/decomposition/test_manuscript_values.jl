using Test, CSV, DataFrames
include(joinpath(@__DIR__, "ManuscriptValues.jl"))
using .ManuscriptValues
const MV = ManuscriptValues
const MV_PAPER = normpath(joinpath(@__DIR__, "..", "output", "paper"))
const MV_MANUSCRIPT = normpath(joinpath(@__DIR__, "..", "..", "..", "writing", "submission_inversions_review", "manuscript", "main_rw_again.tex"))

@testset "Synthetic manuscript label/value drift" begin
    candidates = DataFrame(election = [2014, 2014], ideological_universe = ["seat_winning", "seat_winning"],
        k = [0, 0], minimal_inversion = [true, true], coalition_id = ["A|B", "C|D"],
        left_endpoint = ["A", "C"], right_endpoint = ["B", "D"], parties = ["A, B", "C, D"],
        vote_share = [0.40, 0.45], party_count = [2, 2], votes = [40, 45], V = [100, 100],
        seats = [257, 258], q_C = [205.2, 230.85])
    spec = MV.value_group("IdeologySyntheticStrongest", "Synthetic drift claim", :ideology,
        (; election = 2014, ideological_universe = "seat_winning", k = 0, minimal_inversion = true),
        [("Label", :label, :text), ("VotePct", :vote_share, :pct2),
         ("Seats", :seats, :integer), ("Quota", :q_C, :decimal2)];
        domain = "ideological", universe = "seat_winning", k = 0, selector = :strongest)
    first_registry = build_registry(Dict(:ideology => candidates); specs = [spec])
    @test first_registry.display_value == ["A--B", "40.00", "257", "205.20"]
    @test unique(first_registry.case_id) == ["A|B"]
    candidates.vote_share[2] = 0.39
    candidates.votes[2] = 39
    candidates.q_C[2] = 200.07
    second_registry = build_registry(Dict(:ideology => candidates); specs = [spec])
    @test second_registry.display_value == ["C--D", "39.00", "258", "200.07"]
    @test unique(second_registry.case_id) == ["C|D"]
    @test all(second_registry.source_row_count .== 1)
    @test all(second_registry.case_id .!= first_registry.case_id)
    # Sorting of unrelated input rows cannot move a label independently of its numbers.
    @test isequal(build_registry(Dict(:ideology => reverse(candidates)); specs = [spec]), second_registry)
    candidates.vote_share .= 0.4
    candidates.votes .= 40
    candidates.party_count .= [3, 2]
    @test only(unique(build_registry(Dict(:ideology => candidates); specs = [spec]).case_id)) == "C|D"
    candidates.party_count .= 2
    @test only(unique(build_registry(Dict(:ideology => reverse(candidates)); specs = [spec]).case_id)) == "A|B"

    fixed = merge(spec, (prefix = "IdeologySyntheticFixedAB", selector = :unique, members = ["A", "B"]))
    @test unique(build_registry(Dict(:ideology => candidates); specs = [fixed]).case_id) == ["A|B"]
    @test_throws ErrorException build_registry(Dict(:ideology => candidates[2:2, :]); specs = [fixed])
    @test_throws ErrorException build_registry(Dict(:ideology => vcat(candidates, candidates[1:1, :])); specs = [fixed])
    changed = copy(candidates); changed.parties[1] = "A, X"
    @test_throws ErrorException build_registry(Dict(:ideology => changed); specs = [fixed])
    @test_throws ErrorException build_registry(Dict(:ideology => candidates); specs = [spec, spec])
    @test_throws ErrorException build_registry(Dict(:ideology => candidates); specs = [merge(spec, (universe = "all_parties",))])
    @test_throws ErrorException build_registry(Dict(:ideology => candidates); specs = [merge(spec, (k = 1,))])
    changed_registry = copy(second_registry); changed_registry.case_id[1] = "A|B"
    @test_throws ErrorException validate_registry(changed_registry)
    changed_registry = copy(second_registry); changed_registry.display_value[2] = "99.00"
    @test_throws ErrorException validate_registry(changed_registry)
    changed_registry = copy(second_registry); changed_registry.source_file[1] = "legacy.tex"
    @test_throws ErrorException validate_registry(changed_registry)
end

@testset "Production manuscript contract and table consistency" begin
    sources = load_manuscript_sources(MV_PAPER)
    registry = build_registry(sources)
    source = read(MV_MANUSCRIPT, String)
    @test validate_manuscript(registry, source)
    @test !occursin(raw"\Acct", source)
    @test !occursin("accounting_numeric_macros.tex", source)
    @test_throws ErrorException validate_manuscript(registry, source * raw"\IdeologyStaleCaseOneVotePct{}")
    @test_throws ErrorException validate_manuscript(registry, source * raw"\AcctOldMacro{}")
    @test validate_manuscript(registry, source * "\n% \\IdeologyCommentOnly{}")
    @test allunique(registry.semantic_key)
    @test !any(occursin.(r"Case(One|Two|Three)", registry.macro))
    @test all(row -> occursin("AllParties", row.macro) == (row.ideological_universe == "all_parties"),
              eachrow(registry[registry.domain .== "ideological", :]))
    @test all(registry[registry.domain .== "cabinet", :ideological_universe] .== "not_applicable")
    # Independent table source fields are compared to the prose source object, before rounding.
    for spec in MANUSCRIPT_VALUE_SPECS
        selected = MV.select_group(spec, sources[spec.source]; sources)
        if spec.source == :ideology && spec.selector in (:strongest, :unique) && spec.k == 0
            tables = spec.universe == "seat_winning" ? "tables/table_accounting_minimal_ideological.csv" :
                "all_parties/tables/table_accounting_minimal_ideological.csv"
            table = CSV.read(joinpath(MV_PAPER, tables), DataFrame)
            row = only(eachrow(selected))
            row.minimal_inversion || continue
            match = filter(r -> r.election_year == row.election &&
                MV.membership(r.coalition_parties) == MV.membership(row.parties), table)
            @test nrow(match) == 1
            for metric in (:q_C, :d_C, :A_C, :B_C, :R_C)
                @test isapprox(only(match[!, metric]), row[metric]; atol = 1e-10, rtol = 0)
            end
        end
        # Independent summary/table strongest selection agrees with the registry's explicit rule.
        if spec.selector == :strongest
            row = only(eachrow(selected))
            summary = only(eachrow(filter(r -> r.election == row.election && r.k == row.k &&
                r.ideological_universe == row.ideological_universe, sources[:summary])))
            @test row.coalition_label == summary.strongest_inversion_coalition
            @test row.vote_share == summary.strongest_inversion_vote_share
            @test row.seats == summary.strongest_inversion_seats
        end
    end
    mktempdir() do root
        records = write_manuscript_values(root, registry; manuscript_source = source)
        @test length(records) == 2
        reloaded = CSV.read(joinpath(root, "tables/manuscript_values.csv"), DataFrame;
            types = Dict(:raw_value => String, :display_value => String))
        tex = read(joinpath(root, "latex/manuscript_values.tex"), String)
        @test MV.render_tex(reloaded) == tex == MV.render_tex(registry)
        definitions = Set(m.captures[1] for m in eachmatch(r"\\newcommand\{\\([A-Za-z]+)\}", tex))
        @test MV.manuscript_macro_uses(source) ⊆ definitions
        @test definitions == Set(registry.macro)
    end
end

@testset "Linked strongest span and omission follow the winner" begin
    source = DataFrame(election = fill(2014, 4), ideological_universe = fill("seat_winning", 4),
        k = [0, 0, 1, 1], minimal_inversion = fill(true, 4),
        coalition_id = ["A|B|X", "C|D|Y", "A|B", "C|D"],
        left_endpoint = ["A", "C", "A", "C"], right_endpoint = ["B", "D", "B", "D"],
        party_count = [3, 3, 2, 2], omitted_party = ["", "", "X", "Y"],
        vote_share = [0.52, 0.53, 0.40, 0.45], seats = [270, 280, 257, 258])
    parties = DataFrame(election_year = [2014, 2014], party = ["X", "Y"], s_i = [13, 22])
    span = MV.value_group("IdeologyLinkedSpan", "Span of strongest k=1 inversion", :ideology,
        (; election = 2014, ideological_universe = "seat_winning", k = 0),
        [("Label", :label, :text), ("Seats", :seats, :integer)];
        domain = "ideological", universe = "seat_winning", k = 0, selector = :strongest_span)
    omission = MV.value_group("PartyLinkedOmission", "Omitted party of strongest primary k=1 case", :party,
        (; election_year = 2014), [("Label", :party, :text), ("Seats", :s_i, :integer)];
        domain = "party", selector = :strongest_omission)
    sources = Dict(:ideology => source, :party => parties)
    @test build_registry(sources; specs = [span, omission]).display_value == ["A--B", "270", "X", "13"]
    source.vote_share[4] = 0.39
    @test build_registry(sources; specs = [span, omission]).display_value == ["C--D", "280", "Y", "22"]
end
