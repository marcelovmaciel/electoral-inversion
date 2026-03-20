using Revise
import Processing as p

using CSV
using DataFrames

# Carrega a trilha principal e os objetos base:
# votes_2022, seats_2022, foo, coalitions_2023, coalitions_2024, coalitions_2025, coalitions_2023_2025.
include(joinpath(@__DIR__, "check_2022.jl"))

function check_party_consistency(votes_df::DataFrame, seats_df::DataFrame; year::Int)
    votes_not_in_seats = setdiff(votes_df.SG_PARTIDO, seats_df.SG_PARTIDO)
    seats_not_in_votes = setdiff(seats_df.SG_PARTIDO, votes_df.SG_PARTIDO)

    if !isempty(votes_not_in_seats) || !isempty(seats_not_in_votes)
        error(
            "check_party_consistency($year) falhou. " *
            "Partidos em votos e ausentes em cadeiras: $(votes_not_in_seats). " *
            "Partidos em cadeiras e ausentes em votos: $(seats_not_in_votes).",
        )
    end

    return (
        votes_not_in_seats = votes_not_in_seats,
        seats_not_in_votes = seats_not_in_votes,
    )
end

function maybe_invalid_votes(year::Int)
    try
        return (status = "pass", value = p.national_invalid_votes(year), message = "")
    catch err
        msg = sprint(showerror, err)
        @warn "national_invalid_votes($year) indisponivel; mantendo execucao. Erro: $msg"
        return (status = "warn", value = missing, message = msg)
    end
end

function assert_share_sum(df::DataFrame, table_name::AbstractString; atol::Float64 = 1e-8)
    vote_sum = sum(df.vote_share)
    seat_sum = sum(df.seat_share)

    if !isapprox(vote_sum, 1.0; atol = atol)
        error(
            "Sanity check falhou em $table_name: sum(vote_share) = $vote_sum (esperado ~= 1).",
        )
    end
    if !isapprox(seat_sum, 1.0; atol = atol)
        error(
            "Sanity check falhou em $table_name: sum(seat_share) = $seat_sum (esperado ~= 1).",
        )
    end

    return (
        table_name = String(table_name),
        coalition_col = "(all_parties)",
        vote_sum = vote_sum,
        seat_sum = seat_sum,
    )
end

function empty_share_rows_df()
    return DataFrame(
        table_name = String[],
        coalition_col = String[],
        vote_sum = Float64[],
        seat_sum = Float64[],
    )
end

function assert_coalition_row_shares(
    df::DataFrame,
    table_name::AbstractString;
    atol::Float64 = 1e-8,
)
    nrow(df) == 0 && return empty_share_rows_df()

    rows = Vector{NamedTuple}(undef, nrow(df))
    for (i, r) in enumerate(eachrow(df))
        vote_sum = r.V_base_share + r.V_out_share
        seat_sum = r.S_base_share + r.S_out_share

        if !isapprox(vote_sum, 1.0; atol = atol)
            error(
                "Sanity check falhou em $table_name / $(r.coalition_col): " *
                "V_base_share + V_out_share = $vote_sum (esperado ~= 1).",
            )
        end
        if !isapprox(seat_sum, 1.0; atol = atol)
            error(
                "Sanity check falhou em $table_name / $(r.coalition_col): " *
                "S_base_share + S_out_share = $seat_sum (esperado ~= 1).",
            )
        end

        rows[i] = (
            table_name = String(table_name),
            coalition_col = String(r.coalition_col),
            vote_sum = vote_sum,
            seat_sum = seat_sum,
        )
    end
    return DataFrame(rows)
end

function empty_coverage_df()
    return DataFrame(
        table_name = String[],
        coalition_type = String[],
        coalition_col = String[],
        n_marked = Int[],
        n_total = Int[],
        coverage = Float64[],
    )
end

function coverage_period_table(
    table_df::DataFrame,
    summary_df::DataFrame,
    periods::Dict{String,Vector{String}},
    table_name::AbstractString,
)
    rows = NamedTuple[]
    n_total = nrow(summary_df)

    if nrow(table_df) == 0
        push!(
            rows,
            (
                table_name = String(table_name),
                coalition_type = "period_json",
                coalition_col = "(none)",
                n_marked = 0,
                n_total = n_total,
                coverage = 0.0,
            ),
        )
        return DataFrame(rows)
    end

    for r in eachrow(table_df)
        key = String(r.coalition_col)
        parties = get(periods, key, String[])
        mask = in.(summary_df.SG_PARTIDO, Ref(Set(parties)))
        n_marked = sum(mask)
        push!(
            rows,
            (
                table_name = String(table_name),
                coalition_type = "period_json",
                coalition_col = key,
                n_marked = n_marked,
                n_total = n_total,
                coverage = n_marked / max(n_total, 1),
            ),
        )
    end
    return DataFrame(rows)
end

function coverage_manual_table(
    table_df::DataFrame,
    base_df::DataFrame,
    table_name::AbstractString;
    default_type::AbstractString = "manual",
)
    nrow(table_df) == 0 && return empty_coverage_df()

    rows = Vector{NamedTuple}(undef, nrow(table_df))
    n_total = nrow(base_df)
    has_type_col = :coalition_type in names(table_df)

    for (i, r) in enumerate(eachrow(table_df))
        col = Symbol(String(r.coalition_col))
        hasproperty(base_df, col) || error(
            "coverage_manual_table: coluna $(r.coalition_col) nao existe em `base_df`.",
        )
        mask = Bool.(coalesce.(base_df[!, col], false))
        n_marked = sum(mask)
        coalition_type = has_type_col ? String(r.coalition_type) : String(default_type)

        rows[i] = (
            table_name = String(table_name),
            coalition_type = coalition_type,
            coalition_col = String(r.coalition_col),
            n_marked = n_marked,
            n_total = n_total,
            coverage = n_marked / max(n_total, 1),
        )
    end

    return DataFrame(rows)
end

function inversion_count(table_df::DataFrame)
    nrow(table_df) == 0 && return 0
    mask = Bool.(coalesce.(table_df.inversion, false))
    return sum(mask)
end

function print_df(df::DataFrame)
    show(stdout, MIME("text/plain"), df)
    println()
end

invalid_votes_info = maybe_invalid_votes(2022)
invalid_votes_2022 = invalid_votes_info.value

party_consistency_2022 = check_party_consistency(votes_2022, seats_2022; year = 2022)

total_seats_2022 = sum(seats_2022.total_seats)
total_seats_2022 == 513 || error(
    "Check de 513 cadeiras falhou: total observado = $total_seats_2022 (esperado = 513).",
)

# Trilha manual para auditoria substantiva (fora do check principal).
const PARTIES_2022 = [
    "PL",
    "PT",
    "UNIÃO",
    "PP",
    "MDB",
    "PSD",
    "REPUBLICANOS",
    "PDT",
    "PSB",
    "PSDB",
    "PSOL",
    "PODE",
    "AVANTE",
    "PSC",
    "PV",
    "PC do B",
    "CIDADANIA",
    "SOLIDARIEDADE",
    "PATRIOTA",
    "PROS",
    "NOVO",
    "REDE",
    "PTB",
    "AGIR",
    "PMN",
    "PCO",
    "PRTB",
    "DC",
    "PSTU",
    "PMB",
    "UP",
    "PCB",
]

const inst_2023 = Set([
    "PT",
    "MDB",
    "PSD",
    "PSB",
    "UNIÃO",
    "PDT",
    "PSOL",
    "PC do B",
    "REDE",
])
const inst_2024 = inst_2023
const inst_2025 = union(inst_2024, Set(["PP", "REPUBLICANOS"]))

const vote_core = Set([
    "PT",
    "PC do B",
    "PV",
    "MDB",
    "PSD",
    "PSB",
    "PSOL",
    "REDE",
])
const vote_2023 = vote_core
const vote_2024 = vote_core
const vote_2025 = vote_core

df_base_lula = DataFrame(
    SG_PARTIDO = PARTIES_2022,
    base_inst_2023 = [p in inst_2023 for p in PARTIES_2022],
    base_inst_2024 = [p in inst_2024 for p in PARTIES_2022],
    base_inst_2025 = [p in inst_2025 for p in PARTIES_2022],
    base_vote_2023 = [p in vote_2023 for p in PARTIES_2022],
    base_vote_2024 = [p in vote_2024 for p in PARTIES_2022],
    base_vote_2025 = [p in vote_2025 for p in PARTIES_2022],
)

df_all = leftjoin(foo, df_base_lula, on = :SG_PARTIDO)

coal_inst_cols = String.([:base_inst_2023, :base_inst_2024, :base_inst_2025])
coal_vote_cols = String.([:base_vote_2023, :base_vote_2024, :base_vote_2025])

coalitions_inst_2023_2025 = p.coalition_table(df_all, coal_inst_cols)
coalitions_vote_2023_2025 = p.coalition_table(df_all, coal_vote_cols)

coalitions_inst_2023_2025_tagged = hcat(
    DataFrame(coalition_type = fill("inst", nrow(coalitions_inst_2023_2025))),
    coalitions_inst_2023_2025,
)
coalitions_vote_2023_2025_tagged = hcat(
    DataFrame(coalition_type = fill("vote", nrow(coalitions_vote_2023_2025))),
    coalitions_vote_2023_2025,
)
coalitions_2023_2025_stacked = vcat(
    coalitions_inst_2023_2025_tagged,
    coalitions_vote_2023_2025_tagged,
)

share_sum_foo = assert_share_sum(foo, "foo")

shares_coalitions_2023 = assert_coalition_row_shares(coalitions_2023, "coalitions_2023")
shares_coalitions_2024 = assert_coalition_row_shares(coalitions_2024, "coalitions_2024")
shares_coalitions_2025 = assert_coalition_row_shares(coalitions_2025, "coalitions_2025")
shares_coalitions_2023_2025 = assert_coalition_row_shares(
    coalitions_2023_2025,
    "coalitions_2023_2025",
)
shares_coalitions_inst_2023_2025 = assert_coalition_row_shares(
    coalitions_inst_2023_2025,
    "coalitions_inst_2023_2025",
)
shares_coalitions_vote_2023_2025 = assert_coalition_row_shares(
    coalitions_vote_2023_2025,
    "coalitions_vote_2023_2025",
)
shares_coalitions_2023_2025_stacked = assert_coalition_row_shares(
    coalitions_2023_2025_stacked,
    "coalitions_2023_2025_stacked",
)

periods_map = p.coalitions_by_period(path = coalition_json_path)

coverage_coalitions_2023 = coverage_period_table(
    coalitions_2023,
    foo,
    periods_map,
    "coalitions_2023",
)
coverage_coalitions_2024 = coverage_period_table(
    coalitions_2024,
    foo,
    periods_map,
    "coalitions_2024",
)
coverage_coalitions_2025 = coverage_period_table(
    coalitions_2025,
    foo,
    periods_map,
    "coalitions_2025",
)
coverage_coalitions_2023_2025 = coverage_period_table(
    coalitions_2023_2025,
    foo,
    periods_map,
    "coalitions_2023_2025",
)
coverage_coalitions_inst_2023_2025 = coverage_manual_table(
    coalitions_inst_2023_2025,
    df_all,
    "coalitions_inst_2023_2025";
    default_type = "inst",
)
coverage_coalitions_vote_2023_2025 = coverage_manual_table(
    coalitions_vote_2023_2025,
    df_all,
    "coalitions_vote_2023_2025";
    default_type = "vote",
)
coverage_coalitions_2023_2025_stacked = coverage_manual_table(
    coalitions_2023_2025_stacked,
    df_all,
    "coalitions_2023_2025_stacked";
    default_type = "stacked",
)

coverage_summary = vcat(
    coverage_coalitions_2023,
    coverage_coalitions_2024,
    coverage_coalitions_2025,
    coverage_coalitions_2023_2025,
    coverage_coalitions_inst_2023_2025,
    coverage_coalitions_vote_2023_2025,
    coverage_coalitions_2023_2025_stacked,
)

share_sums_summary = vcat(
    DataFrame([share_sum_foo]),
    shares_coalitions_2023,
    shares_coalitions_2024,
    shares_coalitions_2025,
    shares_coalitions_2023_2025,
    shares_coalitions_inst_2023_2025,
    shares_coalitions_vote_2023_2025,
    shares_coalitions_2023_2025_stacked,
)

inversion_summary = DataFrame(
    table_name = [
        "coalitions_2023",
        "coalitions_2024",
        "coalitions_2025",
        "coalitions_2023_2025",
        "coalitions_inst_2023_2025",
        "coalitions_vote_2023_2025",
        "coalitions_2023_2025_stacked",
    ],
    inversion_true = [
        inversion_count(coalitions_2023),
        inversion_count(coalitions_2024),
        inversion_count(coalitions_2025),
        inversion_count(coalitions_2023_2025),
        inversion_count(coalitions_inst_2023_2025),
        inversion_count(coalitions_vote_2023_2025),
        inversion_count(coalitions_2023_2025_stacked),
    ],
    n_rows = [
        nrow(coalitions_2023),
        nrow(coalitions_2024),
        nrow(coalitions_2025),
        nrow(coalitions_2023_2025),
        nrow(coalitions_inst_2023_2025),
        nrow(coalitions_vote_2023_2025),
        nrow(coalitions_2023_2025_stacked),
    ],
)

checks_status = DataFrame(
    check = [
        "consistencia_partidos_votes_vs_seats",
        "invalid_votes_opcional",
        "assert_513_cadeiras",
        "share_sum_foo",
        "shares_linhas_tabelas_finais",
    ],
    status = [
        "PASS",
        uppercase(invalid_votes_info.status),
        "PASS",
        "PASS",
        "PASS",
    ],
    detail = [
        "setdiff(votes,seats)=0 e setdiff(seats,votes)=0",
        invalid_votes_info.status == "pass" ? "invalid_votes_2022=$invalid_votes_2022" : invalid_votes_info.message,
        "total_seats_2022=$total_seats_2022",
        "sum(vote_share)=$(share_sum_foo.vote_sum), sum(seat_share)=$(share_sum_foo.seat_sum)",
        "Todas as linhas com V_base+V_out e S_base+S_out ~= 1",
    ],
)

println()
println("AUDITORIA 2022")
println("==============")
println("Trilha principal auditada: coalitions_2023_2025 (JSON padronizado).")
println("Trilha manual auditada: coalitions_inst/vote_2023_2025 (analise substantiva).")
println()
println("Status dos checks (pass/fail):")
print_df(checks_status)
println()
println("Cobertura de coalizoes (n_marked / n_total):")
print_df(coverage_summary)
println()
println("Somas de shares:")
print_df(share_sums_summary)
println()
println("Numero de inversoes por tabela:")
print_df(inversion_summary)
println()
println("Check de 513 cadeiras verificado explicitamente: PASS (total = $total_seats_2022).")
