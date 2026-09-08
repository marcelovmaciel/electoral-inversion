using Test
using DataFrames
using CSV

const _PROCESSING_MODULE_FILE_INTERVALS = joinpath(@__DIR__, "..", "src", "Processing.jl")

const _PROCESSING_LOADED_INTERVALS = let
    try
        if !isdefined(@__MODULE__, :Processing)
            include(_PROCESSING_MODULE_FILE_INTERVALS)
        end
        @eval using .Processing
        true
    catch err
        @test_skip "Nao foi possivel carregar modulo Processing: $(sprint(showerror, err))"
        false
    end
end

function _summary(parties, votes, seats)
    return DataFrame(
        SG_PARTIDO = String.(parties),
        valid_total = Int.(votes),
        total_seats = Int.(seats),
    )
end

function _ideology(parties; positions = collect(1:length(parties)))
    return DataFrame(
        SG_PARTIDO = String.(parties),
        ordinal_position = Int.(positions),
    )
end

function _interval_row(df::DataFrame, start_party::AbstractString, end_party::AbstractString)
    rows = df[(df.start_party .== start_party) .& (df.end_party .== end_party), :]
    @test nrow(rows) == 1
    return first(eachrow(rows))
end

function _legacy_first_seat_majority_sweep(summary_df::DataFrame, ideology_df::DataFrame)
    ordered = innerjoin(
        ideology_df,
        select(summary_df, :SG_PARTIDO, :valid_total, :total_seats),
        on = :SG_PARTIDO,
    )
    sort!(ordered, :ordinal_position)
    total_votes = sum(ordered.valid_total)
    total_seats = sum(ordered.total_seats)

    rows = NamedTuple[]
    for start_index in 1:nrow(ordered)
        parties = String[]
        coalition_votes = 0
        coalition_seats = 0
        end_index = nothing
        for current_index in start_index:nrow(ordered)
            push!(parties, ordered.SG_PARTIDO[current_index])
            coalition_votes += ordered.valid_total[current_index]
            coalition_seats += ordered.total_seats[current_index]
            if coalition_seats > total_seats / 2
                end_index = current_index
                break
            end
        end
        vote_share = coalition_votes / total_votes
        seat_majority = coalition_seats > total_seats / 2
        push!(rows, (
            start_party = ordered.SG_PARTIDO[start_index],
            end_party = end_index === nothing ? missing : ordered.SG_PARTIDO[end_index],
            parties = join(parties, ", "),
            votes = coalition_votes,
            seats = coalition_seats,
            candidate_inversion = seat_majority && !(vote_share > 0.5),
            reached_seat_majority = seat_majority,
        ))
    end
    return DataFrame(rows)
end

@testset "ideological interval coalitions" begin
    if !_PROCESSING_LOADED_INTERVALS
        @test_skip "Modulo Processing nao carregado; testes de intervalos ignorados."
    else
        @testset "enumerates all intervals" begin
            parties = ["A", "B", "C", "D"]
            df = Processing.ideological_interval_coalitions(
                _summary(parties, [40, 30, 20, 10], [4, 3, 2, 1]),
                _ideology(parties),
            )

            @test nrow(df) == 4 * 5 ÷ 2
            pairs = Set(zip(df.start_index, df.end_index))
            expected = Set((i, j) for i in 1:4 for j in i:4)
            @test pairs == expected
            @test all(combine(groupby(df, [:start_index, :end_index]), nrow => :n).n .== 1)
        end

        @testset "enumerates nested zero-gap and one-gap domains from spans" begin
            parties = ["A", "B", "C", "D"]
            summary = _summary(parties, [40, 30, 20, 10], [4, 3, 2, 1])
            ideology = _ideology(parties)
            d0 = Processing.ideological_k_gap_coalitions(summary, ideology; k = 0)
            d1 = Processing.ideological_k_gap_coalitions(summary, ideology; k = 1)

            @test nrow(d0) == 10
            @test nrow(d1) == 14
            @test issubset(Set(d0.coalition_id), Set(d1.coalition_id))
            @test all(d0.gap_count .== 0)
            @test all(d1.gap_count .<= 1)
            @test length(unique(d1.coalition_id)) == nrow(d1)

            ac = only(eachrow(d1[(d1.left_endpoint .== "A") .& (d1.right_endpoint .== "C") .& (coalesce.(d1.omitted_party, "") .== "B"), :]))
            @test ac.coalition_id == "A|C"
            @test ac.coalition_label == "A--C, omitting B"
            @test ac.parties == "A, C"
            @test ac.party_count == 2
            @test ac.gap_count == 1
            @test ac.votes == 60
            @test ac.seats == 6
        end

        @testset "one-gap minimality uses exact admissible proper subsets" begin
            parties = ["A", "B", "C"]
            summary = _summary(parties, [40, 20, 40], [2, 0, 2])
            ideology = _ideology(parties)
            d0 = Processing.ideological_k_gap_coalitions(summary, ideology; k = 0)
            d1 = Processing.ideological_k_gap_coalitions(summary, ideology; k = 1)

            abc0 = only(eachrow(d0[d0.coalition_id .== "A|B|C", :]))
            abc1 = only(eachrow(d1[d1.coalition_id .== "A|B|C", :]))
            ac1 = only(eachrow(d1[d1.coalition_id .== "A|C", :]))
            @test abc0.minimal_seat_majority == true
            @test abc1.minimal_seat_majority == false
            @test ac1.minimal_seat_majority == true

            winning = d1[d1.seat_majority .== true, :]
            for row in eachrow(d1[d1.minimal_seat_majority .== true, :])
                members = Set(split(row.coalition_id, "|"))
                @test !any(eachrow(winning)) do candidate
                    candidate_members = Set(split(candidate.coalition_id, "|"))
                    length(candidate_members) < length(members) && issubset(candidate_members, members)
                end
            end
        end

        @testset "membership summary counts actual members within each minimal domain" begin
            parties = ["A", "B", "C"]
            # B receives votes and no seats. It belongs to the D0 minimal majority,
            # but is omitted from the smaller D1 minimal majority, which inverts.
            domains = [Processing.ideological_k_gap_coalitions(
                _summary(parties, [20, 60, 20], [2, 0, 2]), _ideology(parties); k = k,
            ) for k in (0, 1)]
            registry = vcat([d[d.minimal_seat_majority, :] for d in domains]...)
            registry.election = fill(2000, nrow(registry))
            summary = Processing.build_k_gap_membership_summary(reverse(registry))
            @test summary.k == [0, 1]
            @test summary.minimal_seat_majority_coalitions == [1, 1]
            @test summary.minimal_inversions == [0, 1]
            @test summary.non_inverted_minimal_majorities == [1, 0]
            @test summary.non_inverted_members_median[1] == 3
            @test summary.inverted_members_median[2] == 2
            @test ismissing(summary.inverted_members_median[1])
            @test ismissing(summary.non_inverted_members_median[2])
            @test registry.party_count == [3, 2]
            @test registry.right_index[2] - registry.left_index[2] + 1 == 3
            @test Processing.validate_k_gap_membership_summary!(summary, registry)
            @test_throws ErrorException Processing.validate_k_gap_membership_regression!(summary)
            @test isequal(Processing.build_k_gap_membership_summary(registry), summary)

            span_count = copy(registry)
            span_count.party_count[2] = 3
            @test_throws ErrorException Processing.build_k_gap_membership_summary(span_count)
            for count in (0, -1, 2.5, 2.0)
                invalid = copy(registry)
                invalid.party_count = Any[3, count]
                @test_throws ErrorException Processing.build_k_gap_membership_summary(invalid)
            end
            for (column, value) in ((:inversion, false), (:parties, "A, B, C"),
                                    (:coalition_id, "A|A"), (:coalition_id, "A|"),
                                    (:omitted_party, "A"), (:gap_count, 0),
                                    (:minimal_seat_majority, false))
                invalid = copy(registry)
                invalid[2, column] = value
                @test_throws ErrorException Processing.build_k_gap_membership_summary(invalid)
            end
            @test_throws ErrorException Processing.build_k_gap_membership_summary(vcat(registry, registry[1:1, :]))
            for column in (:minimal_seat_majority_coalitions, :minimal_inversions,
                           :non_inverted_minimal_majorities, :inverted_members_median,
                           :inverted_members_min, :inverted_members_max)
                invalid = copy(summary)
                invalid[2, column] += 1
                @test_throws ErrorException Processing.validate_k_gap_membership_summary!(invalid, registry)
            end
            @test_throws ErrorException Processing.validate_k_gap_membership_summary!(reverse(summary), registry)

            stats = Processing.coalition_membership_statistics([8, 15])
            @test stats == (median = 11.5, min = 8, max = 15)
            @test Processing.coalition_membership_statistics([12, 14, 16]).median == 14
            @test Processing.coalition_membership_cell(stats...) == "11.5 [8--15]"
            @test Processing.coalition_membership_cell(14.0, 11, 19) == "14 [11--19]"
            @test Processing.coalition_membership_cell(missing, missing, missing) == "None"
            @test_throws ErrorException Processing.coalition_membership_cell(missing, 1, 2)
            latex = Processing.ideology_k_gap_summary_latex(summary)
            @test occursin(raw"\begin{table}[htbp]", latex)
            @test occursin(raw"\end{table}", latex)
            @test occursin(raw"\label{tab:interval-summary}", latex)
            @test occursin("2000 & 0 & 1 & 0 & None & 3 [3--3]", latex)
            @test occursin("2000 & 1 & 1 & 1 & 2 [2--2] & None", latex)
            @test !occursin("strongest", lowercase(latex))
            @test !occursin("r_C", latex)
            @test occursin("parties receiving votes but no seats", latex)
            table_rows = filter(line -> occursin(" & ", line), split(latex, '\n'))
            @test length(table_rows) == nrow(summary) + 2
            @test all(line -> endswith(line, repeat("\\", 2)), table_rows)

            mktemp() do path, io
                close(io)
                CSV.write(path, summary)
                roundtrip = CSV.read(path, DataFrame)
                @test Processing.validate_k_gap_membership_summary!(roundtrip, registry)
                @test Processing.ideology_k_gap_summary_latex(roundtrip) == latex
            end
        end

        @testset "audited membership rows from the authoritative production registry" begin
            registry_path = joinpath(@__DIR__, "..", "output", "paper", "raw",
                                     "ideology_k_gap_minimal_majorities.csv")
            if isfile(registry_path)
                registry = CSV.read(registry_path, DataFrame)
                summary = Processing.build_k_gap_membership_summary(registry)
                @test Processing.validate_k_gap_membership_summary!(summary, registry)
                @test Processing.validate_k_gap_membership_regression!(summary)
                @test nrow(summary) == 6
                @test collect(zip(summary.election, summary.k)) == [
                    (2014, 0), (2014, 1), (2018, 0), (2018, 1), (2022, 0), (2022, 1)]
                empty_inversions = registry[(registry.election .== 2018) .&
                    (registry.k .== 0) .& registry.inversion, :]
                @test isempty(empty_inversions)
                @test occursin("2018 & 0 & 8 & 0 & None & 16.5 [15--20]",
                               Processing.ideology_k_gap_summary_latex(summary))
                @test_throws ErrorException Processing.validate_k_gap_membership_regression!(summary[1:5, :])
                @test_throws ErrorException Processing.validate_k_gap_membership_regression!(reverse(summary))
                altered = copy(summary)
                altered.inverted_members_median[1] += 1
                @test_throws ErrorException Processing.validate_k_gap_membership_regression!(altered)
            else
                @test_skip "Generate paper outputs to check the audited membership registry."
            end
        end

        @testset "k-gap inversion is strict at one-half and exposes accounting metrics" begin
            parties = ["A", "B"]
            tie = Processing.ideological_k_gap_coalitions(
                _summary(parties, [50, 50], [3, 1]),
                _ideology(parties);
                k = 0,
            )
            a_tie = only(eachrow(tie[tie.coalition_id .== "A", :]))
            @test a_tie.vote_share == 0.5
            @test a_tie.seat_majority == true
            @test a_tie.inversion == false
            @test ismissing(a_tie.vote_deficit_pp)

            strict = Processing.ideological_k_gap_coalitions(
                _summary(parties, [49, 51], [3, 1]),
                _ideology(parties);
                k = 0,
            )
            a = only(eachrow(strict[strict.coalition_id .== "A", :]))
            @test a.inversion == true
            @test a.vote_deficit_pp ≈ 1.0
            @test a.q_C ≈ 1.96
            @test a.d_C ≈ 1.04
            @test a.r_C ≈ 1.04
            @test a.R_C ≈ 3 / 1.96
        end

        @testset "arithmetic for votes seats shares quota and seat_diff" begin
            parties = ["A", "B", "C", "D"]
            toy = _summary(parties, [40, 30, 20, 10], [4, 3, 2, 1])
            df = Processing.ideological_interval_coalitions(
                toy,
                _ideology(parties),
            )

            bc = _interval_row(df, "B", "C")
            @test bc.votes == 50
            @test bc.national_vote_total == 100
            @test bc.vote_share == 0.5
            @test bc.seats == 5
            @test bc.seat_share == 0.5
            @test bc.quota == 5.0
            @test bc.seat_diff == 0.0
            @test bc.required_diff == 1.0
            @test bc.representation_ratio == 1.0
            @test bc.representation_ratio ≈ bc.seats / bc.quota
            @test bc.representation_ratio ≈ 1 + bc.seat_diff / bc.quota

            b = _interval_row(df, "B", "B")
            @test b.quota == 3.0
            @test b.seat_diff == 0.0

            party_metrics = Processing.party_summary(
                select(toy, :SG_PARTIDO, :valid_total),
                select(toy, :SG_PARTIDO, :total_seats);
                expected_total_seats = 10,
            )
            @test all(party_metrics.national_vote_total .== 100)
            @test all(
                party_metrics.seat_diff .≈
                party_metrics.quota .* (Float64.(party_metrics.representation_ratio) .- 1),
            )
            member_mask = in.(party_metrics.SG_PARTIDO, Ref(Set(["B", "C"])))
            weighted_ratio = sum(
                party_metrics.quota[member_mask] .*
                Float64.(party_metrics.representation_ratio[member_mask]),
            ) / bc.quota
            @test bc.representation_ratio ≈ weighted_ratio
        end

        @testset "coalition accounting reaches the majority iff differential meets requirement" begin
            metrics = Processing.coalition_accounting_metrics(
                49,
                6;
                national_vote_total = 100,
                total_seats = 10,
                seat_majority_threshold = 6,
            )
            @test metrics.vote_majority == false
            @test metrics.seat_majority == true
            @test metrics.coalition_inversion == true
            @test metrics.quota ≈ 4.9
            @test metrics.seat_diff ≈ 1.1
            @test metrics.required_diff ≈ 1.1
            @test metrics.seat_diff >= metrics.required_diff || metrics.seat_diff ≈ metrics.required_diff
            @test metrics.representation_ratio ≈ metrics.seat_share / metrics.vote_share
            @test metrics.representation_ratio ≈ 6 / metrics.quota
            @test metrics.representation_ratio ≈ 1 + metrics.seat_diff / metrics.quota
        end

        @testset "majority thresholds are strict greater-than" begin
            parties = ["A", "B", "C"]
            df = Processing.ideological_interval_coalitions(
                _summary(parties, [50, 1, 49], [5, 1, 4]),
                _ideology(parties),
            )

            a = _interval_row(df, "A", "A")
            ab = _interval_row(df, "A", "B")
            @test a.votes == 50
            @test a.seats == 5
            @test a.vote_majority == false
            @test a.seat_majority == false
            @test ab.votes == 51
            @test ab.seats == 6
            @test ab.vote_majority == true
            @test ab.seat_majority == true
        end

        @testset "weak and strict inversions differ at vote ties" begin
            parties = ["A", "B"]
            df = Processing.ideological_interval_coalitions(
                _summary(parties, [50, 50], [6, 4]),
                _ideology(parties),
            )

            a = _interval_row(df, "A", "A")
            @test a.weak_inversion == true
            @test a.strict_inversion == false
            @test a.vote_tie_seat_majority == true
        end

        @testset "minimal seat-majority interval" begin
            parties = ["A", "B", "C", "D", "E"]
            df = Processing.ideological_interval_coalitions(
                _summary(parties, [5, 15, 15, 15, 50], [1, 2, 2, 2, 3]),
                _ideology(parties),
            )

            bd = _interval_row(df, "B", "D")
            ad = _interval_row(df, "A", "D")
            @test bd.seat_majority == true
            @test bd.minimal_seat_majority == true
            @test ad.seat_majority == true
            @test ad.minimal_seat_majority == false
        end

        @testset "minimal inversion" begin
            parties = ["A", "B", "C", "D", "E"]
            df = Processing.ideological_interval_coalitions(
                _summary(parties, [5, 15, 15, 15, 50], [1, 2, 2, 2, 3]),
                _ideology(parties),
            )

            bd = _interval_row(df, "B", "D")
            ad = _interval_row(df, "A", "D")
            @test bd.seat_majority == true
            @test bd.vote_majority == false
            @test bd.minimal_inversion == true
            @test ad.weak_inversion == true
            @test ad.minimal_inversion == false
        end

        @testset "old sweep equivalence" begin
            parties = ["A", "B", "C", "D"]
            summary = _summary(parties, [20, 30, 25, 25], [2, 2, 3, 3])
            ideology = _ideology(parties)
            intervals = Processing.ideological_interval_coalitions(summary, ideology)
            legacy = _legacy_first_seat_majority_sweep(summary, ideology)

            for legacy_row in eachrow(legacy[legacy.reached_seat_majority .== true, :])
                matches = intervals[
                    (intervals.start_party .== legacy_row.start_party) .&
                    (intervals.old_sweep_equivalent .== true),
                    :,
                ]
                @test nrow(matches) == 1
                interval_row = first(eachrow(matches))
                @test interval_row.start_party == legacy_row.start_party
                @test interval_row.end_party == legacy_row.end_party
                @test interval_row.parties == legacy_row.parties
                @test interval_row.votes == legacy_row.votes
                @test interval_row.seats == legacy_row.seats
                @test interval_row.weak_inversion == legacy_row.candidate_inversion
            end
        end

        @testset "duplicate ideology ordinal positions error" begin
            parties = ["A", "B", "C"]
            err = try
                Processing.ideological_interval_coalitions(
                    _summary(parties, [40, 30, 30], [4, 3, 3]),
                    _ideology(parties; positions = [1, 1, 2]),
                )
                nothing
            catch caught
                caught
            end
            @test err isa ErrorException
            @test occursin("Duplicate ideology ordinal_position", sprint(showerror, err))
            @test occursin("Resolve tied ideological positions", sprint(showerror, err))
        end

        @testset "zero-seat parties are retained" begin
            parties = ["A", "B", "C"]
            df = Processing.ideological_interval_coalitions(
                _summary(parties, [40, 10, 50], [4, 0, 6]),
                _ideology(parties),
            )

            b = _interval_row(df, "B", "B")
            ac = _interval_row(df, "A", "C")
            a = _interval_row(df, "A", "A")
            @test b.parties == "B"
            @test b.votes == 10
            @test b.seats == 0
            @test b.vote_share == 0.1
            @test ac.votes == 100
            @test a.vote_share == 0.4
        end

        @testset "missing ideology coverage errors" begin
            err = try
                Processing.ideological_interval_coalitions(
                    _summary(["A", "B", "C"], [40, 30, 30], [4, 3, 3]),
                    _ideology(["A", "B"]),
                )
                nothing
            catch caught
                caught
            end
            @test err isa ErrorException
            @test occursin("Missing ideology coverage", sprint(showerror, err))
            @test occursin("C", sprint(showerror, err))
        end
    end
end
