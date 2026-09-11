# =============================================================================
# Root configuration and paths (simple, explicit)
# =============================================================================

const RAW_ROOT = Ref("../data/raw/electionsBR")
const COALITION_PATH = Ref(CabinetRelease.default_pin_path())

_cabinet_to_election_crosswalk_path() = abspath(
    joinpath(@__DIR__, "..", "data", "cabinet_to_election_party_crosswalk.csv"),
)

"""
    set_root!(path::AbstractString)

Define o diretório raiz onde estão as pastas por ano (1998, 2002, ...).
"""
set_root!(p::AbstractString) = (RAW_ROOT[] = String(p))

"""
    get_root() :: String

Retorna o diretório raiz atualmente configurado.
"""
get_root() = RAW_ROOT[]

"""
    set_coalition_path!(path::AbstractString)

Define o caminho para `partidos_por_periodo.json`.
"""
set_coalition_path!(p::AbstractString) = (COALITION_PATH[] = String(p))

"""
    get_coalition_path() :: String

Retorna o caminho atualmente configurado para `partidos_por_periodo.json`.
"""
get_coalition_path() = COALITION_PATH[]

# Caminhos específicos para cada arquivo TSE que nos interessa.
pmz_path(year::Integer)       = joinpath(get_root(), string(year), "party_mun_zone.csv")
candidate_path(year::Integer) = joinpath(get_root(), string(year), "candidate.csv")
seats_path(year::Integer)     = joinpath(get_root(), string(year), "seats.csv")

# =============================================================================
# Pequenos normalizadores (sem mágica, só o necessário)
# =============================================================================

"""
    upper_strip!(df, col)

Converte a coluna `col` para String, faz strip e uppercase in-place.
"""
function upper_strip!(df, col::Symbol)
    df[!, col] = uppercase.(strip.(String.(df[!, col])))
    return df
end

"""
    stringify!(df, col)

Converte a coluna `col` para String in-place.
"""
function stringify!(df, col::Symbol)
    df[!, col] = String.(df[!, col])
    return df
end

"""
    normalize_party_str(x) :: String

Normaliza uma sigla/label de partido usando regras canônicas determinísticas.
"""
function normalize_party_str(
    x;
    year::Union{Int,Nothing}=nothing,
)
    year_kw = year === nothing ? missing : Int(year)
    return canonical_party(String(x); year = year_kw, strict = true)
end

"""
    normalize_party!(df; col=:SG_PARTIDO)

Aplica canonicalização na coluna de partido.
"""
function normalize_party!(
    df;
    col::Union{Symbol,AbstractString} = :SG_PARTIDO,
    year::Union{Int,Nothing} = nothing,
)
    source_vals = String.(df[!, col])
    unique_vals = unique(source_vals)
    year_kw = year === nothing ? missing : Int(year)
    mapped = canonicalize_parties(unique_vals; year = year_kw, strict = true, with_mapping = true)
    canon_map = Dict(String(row.alias_raw) => String(row.canonical) for row in eachrow(mapped.mapping))
    df[!, col] = [canon_map[raw] for raw in source_vals]
    return df
end

"""
    to_int(x) :: Int

Converte vários tipos em Int, tratando missing/nothing como 0.
Evita explodir se alguma coluna vier como String.
"""
function to_int(x)
    if x === missing || x === nothing
        return 0
    elseif x isa Integer
        return x
    elseif x isa AbstractFloat
        return round(Int, x)
    else
        y = tryparse(Int, String(x))
        return y === nothing ? 0 : y
    end
end

# =============================================================================
# Column detection helpers
# =============================================================================

function first_in_df(df::DataFrame, cands::Vector{String})
    nn = names(df)
    by_name = Dict(uppercase(strip(String(n))) => n for n in nn)
    for c in cands
        key = uppercase(strip(String(c)))
        if haskey(by_name, key)
            return by_name[key]
        end
    end
    return nothing
end

function pick_col(df::DataFrame, col::Union{Symbol,AbstractString,Nothing})
    col === nothing && return nothing
    return first_in_df(df, [String(col)])
end

function detect_vote_cols(df::DataFrame;
    nom_col = nothing,
    leg_col = nothing,
    total_col = nothing,
)
    nom = pick_col(df, nom_col)
    leg = pick_col(df, leg_col)
    total = pick_col(df, total_col)

    if total_col !== nothing && total !== nothing && nom === nothing && leg === nothing
        return (nothing, nothing, total, :total)
    end

    if nom === nothing
        nom = first_in_df(df, ["QT_VOTOS_NOMINAIS_VALIDOS",
                               "QT_VOTOS_NOMINAIS"])
    end

    if leg === nothing
        leg = first_in_df(df, ["QT_TOTAL_VOTOS_LEG_VALIDOS",
                               "QT_VOTOS_LEGENDA_VALIDOS",
                               "QT_VOTOS_LEGENDA"])
    end

    if nom !== nothing && leg !== nothing
        scheme =
            uppercase(strip(String(nom))) == "QT_VOTOS_NOMINAIS_VALIDOS" &&
            uppercase(strip(String(leg))) == "QT_TOTAL_VOTOS_LEG_VALIDOS" ?
            :nominal_valid_plus_total_valid_legend :
            :nominal_plus_legend_legacy
        return (nom, leg, total, scheme)
    end

    error("Could not identify party vote components for party_mun_zone.")
end

# =============================================================================
# Simple validations
# =============================================================================

function expected_total_seats_for_cargo(cargo::AbstractString)
    cargo_up = uppercase(strip(cargo))
    return cargo_up == "DEPUTADO FEDERAL" ? 513 : nothing
end

# =============================================================================
# party_mun_zone: votos por partido
# =============================================================================

"""
    pmz_df(year; cargo="DEPUTADO FEDERAL")

Carrega `party_mun_zone.csv` para o ano dado, normaliza UF/cargo/partido,
filtra pelo cargo e retorna o DataFrame resultante.

NÃO faz suposição sobre quais colunas de votos existem — isso é tratado
em `pmz_party_votes`.
"""
function pmz_df(year::Integer;
    cargo::AbstractString = "DEPUTADO FEDERAL",
)
    path = pmz_path(year)
    isfile(path) || error("pmz_df: arquivo não encontrado para o ano $year em $path")

    # Para não brigar com mudanças de schema, lemos tudo e filtramos colunas
    pmz = CSV.read(path, DataFrame)

    upper_strip!(pmz, :DS_CARGO)
    stringify!(pmz, :SG_UF)
    stringify!(pmz, :SG_PARTIDO)
    normalize_party!(pmz; year = Int(year))

    cargo_up = uppercase(strip(cargo))
    filter!(r -> r.DS_CARGO == cargo_up, pmz)

    return pmz
end



function pmz_party_votes(year::Integer;
    cargo::AbstractString = "DEPUTADO FEDERAL",
    nom_col       = nothing,
    leg_col       = nothing,
    total_col     = nothing,
)
    P = pmz_df(year; cargo=cargo)

    nom_c, leg_c, total_c, scheme = detect_vote_cols(P;
                                                     nom_col = nom_col,
                                                     leg_col = leg_col,
                                                     total_col = total_col)

    if scheme == :total
        total_c === nothing && error("pmz_party_votes: could not detect vote columns.")
        v = to_int.(P[!, total_c])
    else
        v_nom = nom_c === nothing ? zeros(Int, nrow(P)) : to_int.(P[!, nom_c])
        v_leg = leg_c === nothing ? zeros(Int, nrow(P)) : to_int.(P[!, leg_c])
        v = v_nom .+ v_leg
    end


    tmp = DataFrame(
        SG_UF      = P.SG_UF,
        DS_CARGO   = P.DS_CARGO,
        SG_PARTIDO = P.SG_PARTIDO,
        votes      = v,
    )

    g  = groupby(tmp, [:SG_UF, :DS_CARGO, :SG_PARTIDO])
    pv = combine(g, :votes => sum => :votes)

    return pv
end


function national_party_valid_votes(year::Integer;
    cargo::AbstractString = "DEPUTADO FEDERAL",
    nom_col       = nothing,
    leg_col       = nothing,
    total_col     = nothing,
)
    pv = pmz_party_votes(year;
                         cargo   = cargo,
                         nom_col = nom_col,
                         leg_col = leg_col,
                         total_col = total_col)

    g   = groupby(pv, :SG_PARTIDO)
    nat = combine(g, :votes => sum => :valid_total)
    sort!(nat, :valid_total, rev = true)
    return nat
end

function national_invalid_votes(year::Integer;
    cargo::AbstractString = "DEPUTADO FEDERAL",
    nom_invalid_cols = [
        "QT_VOTOS_NOMINAIS_ANUL_SUBJUD",
        "QT_VOTOS_NOMINAIS_ANULADOS",
    ],
    leg_invalid_cols = [
        "QT_VOTOS_LEGENDA_ANUL_SUBJUD",
        "QT_VOTOS_LEGENDA_ANULADOS",
    ],
)
    P = pmz_df(year; cargo=cargo)

    cols = Symbol[]
    for c in nom_invalid_cols
        col = first_in_df(P, [c])
        col === nothing || push!(cols, Symbol(col))
    end
    for c in leg_invalid_cols
        col = first_in_df(P, [c])
        col === nothing || push!(cols, Symbol(col))
    end
    cols = unique(cols)
    isempty(cols) && error("national_invalid_votes: could not detect invalid vote columns.")

    total = 0
    for c in cols
        total += sum(to_int.(P[!, c]))
    end
    return total
end

# =============================================================================
# candidate.csv: candidatos eleitos e cadeiras por partido
# =============================================================================

"""
Conjunto de status que contam como "eleito".

OBS: usamos *igualdade exata* após normalizar com uppercase+strip.
'NAO ELEITO' NÃO aparece aqui, então não há risco do bug de substring
("ELEITO" dentro de "NAO ELEITO").
"""
const WINNER_STATUSES = Set([
    "ELEITO",
    "ELEITO POR QP",
    "ELEITO POR MEDIA",   # sem acento
    "ELEITO POR MÉDIA",   # com acento
])

"""
    candidate_df(year; cargo="DEPUTADO FEDERAL")

Carrega `candidate.csv` para o ano dado, normaliza UF/cargo/partido,
filtra pelo cargo e exige a coluna `DS_SIT_TOT_TURNO`.

Se o arquivo ou a coluna não existirem, lança `error`.
"""
function candidate_df(year::Integer; cargo::AbstractString = "DEPUTADO FEDERAL")
    path = candidate_path(year)
    isfile(path) || error("candidate_df: arquivo não encontrado para o ano $year em $path")

    needed = [
        :ANO_ELEICAO,
        :NR_TURNO,
        :SG_UF,
        :CD_CARGO,
        :DS_CARGO,
        :SG_PARTIDO,
        :DS_SIT_TOT_TURNO,
    ]

    C = CSV.read(path, DataFrame; select=needed, normalizenames=true)



    upper_strip!(C, :DS_CARGO)
    stringify!(C, :SG_UF)
    stringify!(C, :SG_PARTIDO)
    normalize_party!(C; year = Int(year))

    cargo_up = uppercase(strip(cargo))
    filter!(r -> r.DS_CARGO == cargo_up, C)

    return C
end

"""
    cand_winners(year; cargo="DEPUTADO FEDERAL")

Filtra `candidate.csv` para manter apenas candidatos eleitos, com base em
`WINNER_STATUSES`. Sem heurística de substring.
"""
function cand_winners(year::Integer; cargo::AbstractString = "DEPUTADO FEDERAL")
    C = candidate_df(year; cargo=cargo)

    status_raw  = C[!, :DS_SIT_TOT_TURNO]
    status_norm = uppercase.(strip.(String.(status_raw)))

    C[!, :WINNER] = in.(status_norm, Ref(WINNER_STATUSES))
    filter!(r -> r.WINNER, C)

    return C
end

function cand_party_seats(year::Integer; cargo::AbstractString = "DEPUTADO FEDERAL")
    C = candidate_df(year; cargo=cargo)

    # normaliza o status de turno
    status_raw  = C[!, :DS_SIT_TOT_TURNO]
    status_norm = uppercase.(strip.(String.(status_raw)))

    # marca vencedores (true/false)
    C[!, :WINNER] = in.(status_norm, Ref(WINNER_STATUSES))

    # agora contamos quantos vencedores por UF × cargo × partido
    tmp = select(C, :SG_UF, :DS_CARGO, :SG_PARTIDO, :WINNER)
    g   = groupby(tmp, [:SG_UF, :DS_CARGO, :SG_PARTIDO])

    seats = combine(g, :WINNER => (w -> sum(Int.(w))) => :seats)
    return seats
end


function get_agg_party_seats(year::Integer;
                             cargo::AbstractString = "DEPUTADO FEDERAL",
                             expected_total_seats::Union{Int,Nothing} = expected_total_seats_for_cargo(cargo))
    bar = cand_party_seats(year; cargo=cargo)
    sg1 = groupby(bar, :SG_PARTIDO)

    national_party_seats = combine(
        sg1,
        :seats => sum => :total_seats,
    )

    total_seats = sum(national_party_seats.total_seats)
    if expected_total_seats !== nothing
        @assert total_seats == expected_total_seats "get_agg_party_seats: expected $expected_total_seats seats, got $total_seats."
    end

    sort!(national_party_seats, :total_seats, rev = true)
    return national_party_seats
end


function party_summary(votes::DataFrame,
                       seats::DataFrame;
                       vote_col::Symbol = :valid_total,
                       seat_col::Symbol = :total_seats,
                       expected_total_seats::Union{Int,Nothing} = nothing)

    df = outerjoin(votes, seats, on = :SG_PARTIDO)

    # if any party has votes or seats missing, treat as 0
    df[!, vote_col] = coalesce.(df[!, vote_col], 0)
    df[!, seat_col] = coalesce.(df[!, seat_col], 0)

    total_votes = sum(df[!, vote_col])
    total_seats = sum(df[!, seat_col])

    total_votes > 0 || error("party_summary: total votes must be positive.")
    total_seats > 0 || error("party_summary: total seats must be positive.")

    if expected_total_seats !== nothing
        @assert total_seats == expected_total_seats "party_summary: expected $expected_total_seats seats, got $total_seats."
    end

    df[!, :national_vote_total] = fill(total_votes, nrow(df))
    df[!, :vote_share] = df[!, vote_col] ./ total_votes
    df[!, :seat_share] = df[!, seat_col] ./ total_seats

    df[!, :quota]     = df[!, :vote_share] .* total_seats
    df[!, :seat_diff] = df[!, seat_col] .- df[!, :quota]
    df[!, :representation_ratio] = Union{Missing,Float64}[
        quota > 0 ? Float64(seat) / Float64(quota) : missing
        for (seat, quota) in zip(df[!, seat_col], df.quota)
    ]

    return df
end

function _majority_status(vote_majority::Bool, seat_majority::Bool)
    if vote_majority && seat_majority
        return "votes+seats"
    elseif seat_majority
        return "seats_only"
    elseif vote_majority
        return "votes_only"
    else
        return "neither"
    end
end

"""
    coalition_accounting_metrics(coalition_votes, coalition_seats;
                                 national_vote_total,
                                 total_seats=513,
                                 seat_majority_threshold=fld(total_seats, 2) + 1)

Return the common full-precision vote/seat accounting used for both observed
cabinet coalitions and contiguous ideological intervals. `required_diff` is the
absolute seat differential required for the proportional quota to reach the
Chamber-majority threshold. A zero-vote coalition has an undefined
`representation_ratio`, represented by `missing`.
"""
function coalition_accounting_metrics(
    coalition_votes::Real,
    coalition_seats::Real;
    national_vote_total::Real,
    total_seats::Real = 513,
    seat_majority_threshold::Integer = fld(Int(round(total_seats)), 2) + 1,
)
    votes = Float64(coalition_votes)
    seats = Float64(coalition_seats)
    national_votes = Float64(national_vote_total)
    chamber_seats = Float64(total_seats)
    majority_threshold = Int(seat_majority_threshold)

    national_votes > 0 || error("coalition_accounting_metrics: national_vote_total must be positive.")
    chamber_seats > 0 || error("coalition_accounting_metrics: total_seats must be positive.")
    majority_threshold > 0 || error("coalition_accounting_metrics: seat_majority_threshold must be positive.")
    votes >= 0 || error("coalition_accounting_metrics: coalition_votes must be nonnegative.")
    seats >= 0 || error("coalition_accounting_metrics: coalition_seats must be nonnegative.")
    votes <= national_votes || error("coalition_accounting_metrics: coalition votes exceed the national vote total.")
    seats <= chamber_seats || error("coalition_accounting_metrics: coalition seats exceed total seats.")

    vote_share = votes / national_votes
    seat_share = seats / chamber_seats
    quota = chamber_seats * vote_share
    seat_diff = seats - quota
    required_diff = majority_threshold - quota
    representation_ratio = quota > 0 ? seats / quota : missing
    vote_majority = vote_share > 0.5
    seat_majority = seats >= majority_threshold

    return (
        national_vote_total = national_votes,
        vote_share = Float64(vote_share),
        seat_share = Float64(seat_share),
        quota = Float64(quota),
        seat_diff = Float64(seat_diff),
        required_diff = Float64(required_diff),
        representation_ratio = representation_ratio,
        vote_majority = Bool(vote_majority),
        seat_majority = Bool(seat_majority),
        majority_status = _majority_status(vote_majority, seat_majority),
        coalition_inversion = Bool(seat_majority && !vote_majority),
    )
end

function _require_columns(df::DataFrame, cols::Vector{Symbol}, df_name::AbstractString)
    missing_cols = [col for col in cols if !(col in propertynames(df))]
    isempty(missing_cols) || error(
        "$(df_name) is missing required column(s): $(join(String.(missing_cols), ", ")).",
    )
    return nothing
end

function _duplicate_values(df::DataFrame, col::Symbol)
    counts = combine(groupby(DataFrame(value = String.(df[!, col])), :value), nrow => :n)
    return String.(counts.value[counts.n .> 1])
end

"""
    ideological_party_order(summary_df, ideology_df; universe=:seat_winning, tie_policy=:error)

Restrict the existing election order to the explicit ideological universe. The
primary universe contains exactly parties with observed positive Chamber seats;
`:all_parties` retains all election parties. Original ideological ranks are kept
in `original_ordinal_position`; `ordinal_position` and `ideological_index` are
consecutive positions in the selected universe. Ideological positions are never
estimated here. This helper does not define the national vote denominator.
"""
function ideological_party_order(
    summary_df::DataFrame,
    ideology_df::DataFrame;
    universe::Symbol = :seat_winning,
    tie_policy::Symbol = :error,
)
    universe in (:seat_winning, :all_parties) || error(
        "Unsupported ideological universe $(universe). Use :seat_winning or :all_parties.")
    tie_policy == :error || error("Unsupported tie_policy $(tie_policy). Only :error is implemented.")
    _require_columns(summary_df, [:SG_PARTIDO, :valid_total, :total_seats], "summary_df")
    _require_columns(ideology_df, [:SG_PARTIDO, :ordinal_position], "ideology_df")
    for (frame, name) in ((summary_df, "summary_df"), (ideology_df, "ideology_df"))
        duplicates = _duplicate_values(frame, :SG_PARTIDO)
        isempty(duplicates) || error("$(name) has duplicate SG_PARTIDO values: $(join(duplicates, ", ")).")
    end
    ideology_metadata = select(ideology_df, Not(intersect(propertynames(ideology_df),
        [:valid_total, :total_seats, :ideological_index, :ideological_universe])))
    ordered = innerjoin(ideology_metadata,
        select(summary_df, :SG_PARTIDO, :valid_total, :total_seats), on = :SG_PARTIDO,
    )
    summary_parties = sort(String.(summary_df.SG_PARTIDO))
    ordered_parties = sort(String.(ordered.SG_PARTIDO))
    summary_parties == ordered_parties || error(
        "Every party in summary_df must appear exactly once in ideology_df before ideological coalitions are meaningful. " *
        "Missing ideology coverage for: $(join(setdiff(summary_parties, ordered_parties), ", ")).")
    ordinal_counts = combine(groupby(ordered, :ordinal_position), nrow => :n)
    if any(ordinal_counts.n .> 1)
        duplicated_positions = ordinal_counts.ordinal_position[ordinal_counts.n .> 1]
        error("Duplicate ideology ordinal_position value(s) found: $(join(string.(duplicated_positions), ", ")). " *
              "Resolve tied ideological positions before ideological coalitions are meaningful; tied positions cannot be silently sorted by party name.")
    end
    all(ordered.valid_total .>= 0) || error("Party votes must be nonnegative.")
    all(ordered.total_seats .>= 0) || error("Party seats must be nonnegative.")
    all(isinteger, ordered.valid_total) || error("valid_total must be integer-valued for every party.")
    all(isinteger, ordered.total_seats) || error("total_seats must be integer-valued for every party.")
    sort!(ordered, :ordinal_position)
    full_order = String.(ordered.SG_PARTIDO)
    expected_parties = universe == :seat_winning ?
        String.(ordered.SG_PARTIDO[ordered.total_seats .> 0]) : full_order
    if universe == :seat_winning
        ordered = ordered[ordered.total_seats .> 0, :]
    end
    @assert String.(ordered.SG_PARTIDO) == expected_parties
    @assert issorted(indexin(String.(ordered.SG_PARTIDO), full_order))
    if !(:original_ordinal_position in propertynames(ordered))
        ordered[!, :original_ordinal_position] = copy(ordered.ordinal_position)
    end
    ordered[!, :ordinal_position] = collect(1:nrow(ordered))
    ordered[!, :ideological_index] = copy(ordered.ordinal_position)
    ordered[!, :ideological_universe] = fill(String(universe), nrow(ordered))
    return ordered
end

"""
    ideological_k_gap_coalitions(summary_df, ideology_df; k=0, universe=:seat_winning)

Enumerate D_0 or D_1 anew in the selected ideological universe. D_0 contains
complete connected intervals; D_1 additionally omits each single interior party.
Gaps count omitted members of this universe, and winning minimality is recomputed
against every admissible proper subset of the same D_k. Vote shares and quotas
always use all valid votes in `summary_df`, including zero-seat parties.
"""
function ideological_k_gap_coalitions(
    summary_df::DataFrame,
    ideology_df::DataFrame;
    k::Integer = 0,
    universe::Symbol = :seat_winning,
    tie_policy::Symbol = :error,
)
    k in (0, 1) || error("Unsupported ideological gap limit k=$(k). Only k=0 and k=1 are implemented.")
    ordered = ideological_party_order(summary_df, ideology_df; universe, tie_policy)
    parties = String.(ordered.SG_PARTIDO)
    votes = Int.(ordered.valid_total)
    seats = Int.(ordered.total_seats)
    # These totals deliberately come from P, not the filtered ideological order.
    total_votes = sum(Int.(summary_df.valid_total))
    total_seats = sum(Int.(summary_df.total_seats))
    total_votes > 0 || error("Total votes must be positive.")
    total_seats > 0 || error("Total seats must be positive.")
    @assert sum(seats) == total_seats
    seat_majority_threshold = fld(total_seats, 2) + 1
    n = nrow(ordered)
    rows = NamedTuple[]
    member_sets = BitSet[]
    seen_coalition_ids = Set{String}()
    for left_index in 1:n
        for right_index in left_index:n
            omission_indices = Int[0]
            if k == 1 && right_index >= left_index + 2
                append!(omission_indices, (left_index + 1):(right_index - 1))
            end
            for omitted_index in omission_indices
                member_indices = [index for index in left_index:right_index if index != omitted_index]
                member_parties = parties[member_indices]
                coalition_id = join(member_parties, "|")
                coalition_id in seen_coalition_ids && continue
                push!(seen_coalition_ids, coalition_id)
                push!(member_sets, BitSet(member_indices))
                coalition_votes = sum(votes[member_indices])
                coalition_seats = sum(seats[member_indices])
                accounting = coalition_accounting_metrics(coalition_votes, coalition_seats;
                    national_vote_total = total_votes, total_seats, seat_majority_threshold)
                inversion = coalition_seats >= seat_majority_threshold && 2 * coalition_votes < total_votes
                gap_count = right_index - left_index + 1 - length(member_indices)
                omitted_party = omitted_index == 0 ? missing : parties[omitted_index]
                coalition_label = omitted_index == 0 ?
                    "$(parties[left_index])--$(parties[right_index])" :
                    "$(parties[left_index])--$(parties[right_index]), omitting $(parties[omitted_index])"
                @assert universe != :seat_winning || all(seats[member_indices] .> 0)
                @assert universe != :seat_winning || omitted_index == 0 || seats[omitted_index] > 0
                push!(rows, (
                    ideological_universe = String(universe),
                    ideological_party_count = Int(n),
                    full_party_count = Int(nrow(summary_df)),
                    k = Int(k), coalition_id = coalition_id, coalition_label = coalition_label,
                    left_index = Int(left_index), right_index = Int(right_index),
                    left_original_ordinal_position = ordered.original_ordinal_position[left_index],
                    right_original_ordinal_position = ordered.original_ordinal_position[right_index],
                    left_endpoint = parties[left_index], right_endpoint = parties[right_index],
                    omitted_party = omitted_party, party_count = Int(length(member_indices)),
                    parties = join(member_parties, ", "), gap_count = Int(gap_count),
                    votes = Int(coalition_votes), national_vote_total = Int(total_votes),
                    vote_share = accounting.vote_share, seats = Int(coalition_seats),
                    total_seats = Int(total_seats), seat_majority_threshold = Int(seat_majority_threshold),
                    seat_share = accounting.seat_share, q_C = accounting.quota,
                    d_C = accounting.seat_diff, r_C = accounting.required_diff,
                    R_C = accounting.representation_ratio,
                    vote_majority = Bool(accounting.vote_majority),
                    seat_majority = Bool(accounting.seat_majority), inversion = Bool(inversion),
                    vote_deficit_pp = inversion ? 50.0 - 100.0 * accounting.vote_share : missing,
                ))
            end
        end
    end
    result = DataFrame(rows)
    expected_rows = n * (n + 1) ÷ 2 + (k == 1 ? binomial(n, 3) : 0)
    nrow(result) == expected_rows || error(
        "D_$(k) coalition count mismatch: expected $(expected_rows), found $(nrow(result)).")
    length(seen_coalition_ids) == nrow(result) || error("D_$(k) contains duplicate canonical party sets.")
    minimal_seat_majority = falses(nrow(result))
    winning_indices = findall(identity, Bool.(result.seat_majority))
    for coalition_index in winning_indices
        coalition_members = member_sets[coalition_index]
        has_winning_proper_subset = any(winning_indices) do subset_index
            subset_members = member_sets[subset_index]
            length(subset_members) < length(coalition_members) && issubset(subset_members, coalition_members)
        end
        minimal_seat_majority[coalition_index] = !has_winning_proper_subset
    end
    result[!, :minimal_seat_majority] = minimal_seat_majority
    result[!, :minimal_inversion] = result.inversion .& result.minimal_seat_majority
    sort!(result, [:left_index, :right_index, :gap_count, :omitted_party])
    return result
end

"""
    ideological_interval_coalitions(summary_df, ideology_df; universe=:seat_winning)

Exact-connected compatibility view of the shared D_0 enumeration, including
legacy plotting/accounting aliases. `minimal_inversion` is strictly below half
of all valid votes; `weak_inversion` retains a separate vote-tie diagnostic.
"""
function ideological_interval_coalitions(
    summary_df::DataFrame,
    ideology_df::DataFrame;
    universe::Symbol = :seat_winning,
    tie_policy::Symbol = :error,
)
    result = ideological_k_gap_coalitions(summary_df, ideology_df; k = 0, universe, tie_policy)
    for (alias, source) in ((:start_index, :left_index), (:end_index, :right_index),
        (:start_party, :left_endpoint), (:end_party, :right_endpoint),
        (:n_parties, :party_count), (:quota, :q_C), (:seat_diff, :d_C),
        (:required_diff, :r_C), (:representation_ratio, :R_C))
        result[!, alias] = copy(result[!, source])
    end
    result[!, :majority_status] = _majority_status.(result.vote_majority, result.seat_majority)
    result[!, :weak_inversion] = result.seat_majority .& .!result.vote_majority
    result[!, :strict_inversion] = copy(result.inversion)
    result[!, :vote_tie_seat_majority] = result.seat_majority .& (2 .* result.votes .== result.national_vote_total)
    result[!, :complement_votes] = result.national_vote_total .- result.votes
    result[!, :complement_vote_share] = result.complement_votes ./ result.national_vote_total
    result[!, :complement_seats] = result.total_seats .- result.seats
    result[!, :complement_seat_share] = result.complement_seats ./ result.total_seats
    first_majority_end = Dict{Int,Int}()
    for row in eachrow(result)
        row.seat_majority && get!(first_majority_end, row.start_index, row.end_index)
    end
    result[!, :old_sweep_equivalent] = [get(first_majority_end, row.start_index, 0) == row.end_index for row in eachrow(result)]
    return result
end


# =============================================================================
# Coalition periods (ministerial base)
# =============================================================================

function period_sort_key(period::AbstractString)
    parts = split(period, ".")
    if length(parts) == 2
        year = tryparse(Int, parts[1])
        idx = tryparse(Int, parts[2])
        if year !== nothing && idx !== nothing
            return (year, idx, period)
        end
    end
    return (typemax(Int), typemax(Int), period)
end

"""
    coalitions_by_period(; path=get_coalition_path())

Carrega `partidos_por_periodo.json` e retorna um Dict de periodo => partidos,
com siglas normalizadas por `canonical_party`.
"""
function coalitions_by_period(; path::AbstractString = get_coalition_path())
    return CabinetRelease.identified_parties(path)
end

function parse_coalition_date(value, period::AbstractString, field::AbstractString)
    if value === nothing || value === missing
        return nothing
    end
    s = strip(String(value))
    isempty(s) && return nothing
    d = tryparse(Date, s)
    d === nothing && error("parse_coalition_date: data inválida em $period.$field: '$s'")
    return d
end

function coalition_period_windows(; path::AbstractString = get_coalition_path())
    return CabinetRelease.period_windows(path)
end

"""
    coalition_periods_by_label_year(periods, year)

Seleciona períodos cuja chave começa com `YYYY.`. Esta é uma semântica de
rótulo do adaptador, mas não significa que os períodos selecionados sejam
todos os períodos ativos durante o ano civil. Períodos iniciados em anos
anteriores também podem sobrepor a janela consultada.
"""
function coalition_periods_by_label_year(periods::Dict{String,Vector{String}}, year::Integer)
    prefix = string(year) * "."
    period_keys = sort([k for k in keys(periods) if startswith(k, prefix)]; by = period_sort_key)
    return Dict(k => periods[k] for k in period_keys)
end

"""
    overlaps_window(period_start, period_end, window_start, window_end)

Retorna `true` quando dois intervalos fechados de datas se sobrepõem. A
semântica é inclusiva: um período se sobrepõe a uma janela se
`period_start <= window_end && period_end >= window_start`.
"""
function overlaps_window(period_start::Date, period_end::Date, window_start::Date, window_end::Date)
    return period_start <= window_end && period_end >= window_start
end

"""
    coalition_periods_overlapping_window(periods, window_start, window_end; path=get_coalition_path())

Seleciona os períodos de coalizão ativos em qualquer parte da janela fechada
`window_start` a `window_end`, usando as datas da versão histórica fixada.
O adaptador converte uma única vez o fim exclusivo para a interface inclusiva.
O rótulo (`YYYY.k`) indica o ano inicial, não todos os anos ativos. Uma janela
inteiramente não identificada retorna seleção vazia; seu calendário continua
explicitamente representado pela versão histórica.
"""
function coalition_periods_overlapping_window(periods::Dict{String,Vector{String}},
                                              window_start::Date,
                                              window_end::Date;
                                              path::AbstractString = get_coalition_path())
    window_end < window_start && error(
        "coalition_periods_overlapping_window: intervalo inválido ($window_start > $window_end).",
    )

    windows = coalition_period_windows(; path=path)

    selected = Pair{String,Vector{String}}[]
    bounds = Tuple{Date,Date}[]
    missing_dates = String[]

    for (period, parties) in periods
        if !haskey(windows, period)
            push!(missing_dates, period)
            continue
        end

        start_date, end_date = windows[period]
        if start_date === nothing || end_date === nothing
            push!(missing_dates, period)
            continue
        end
        end_date < start_date && error("coalition_periods_overlapping_window: intervalo inválido em $period ($start_date > $end_date).")

        push!(bounds, (start_date, end_date))
        if overlaps_window(start_date, end_date, window_start, window_end)
            push!(selected, period => parties)
        end
    end

    if !isempty(missing_dates)
        sample_vec = sort(unique(missing_dates))
        sample = isempty(sample_vec) ? "nenhum período com datas válidas" :
                 join(sample_vec[1:min(length(sample_vec), 5)], ", ")
        error("coalition_periods_overlapping_window: dados insuficientes para filtrar a janela $window_start a $window_end (faltam datas em: $sample).")
    end

    # An observed window can contain only explicitly unidentified cabinet sets.
    # Its identified-period selection is empty; the full release calendar still
    # records the dates and unknown status. Do not reinterpret this as a parser error.

    sort!(selected, by = p -> begin
        start_date, _ = windows[p.first]
        (start_date::Date, period_sort_key(p.first))
    end)
    return Dict(selected)
end

"""
    coalition_periods_overlapping_year(periods, year; path=get_coalition_path())

Seleciona, por sobreposição inclusiva de datas, todos os períodos ativos em
qualquer parte do ano civil `year`. Use esta função para análises por ano civil;
use `coalition_periods_by_label_year` apenas quando a pergunta for sobre o ano
codificado no rótulo da chave.
"""
function coalition_periods_overlapping_year(periods::Dict{String,Vector{String}}, year::Integer;
                                            path::AbstractString = get_coalition_path())
    return coalition_periods_overlapping_window(
        periods,
        Date(year, 1, 1),
        Date(year, 12, 31);
        path = path,
    )
end




function coalition_summary_mask(df::DataFrame, mask_raw; label)
    @assert length(mask_raw) == nrow(df) "coalition_summary_mask: mask length mismatch."

    mask = Bool.(coalesce.(mask_raw, false))

    # shares (já normalizados em party_summary)
    V_total = sum(df.vote_share)
    S_total = sum(df.seat_share)

    # indexação booleana normal, não via view(mask)
    V_base = sum(df.vote_share[mask])
    S_base = sum(df.seat_share[mask])

    V_out  = V_total - V_base
    S_out  = S_total - S_base

    seatdiff_base = sum(df.seat_diff[mask])


    inversion = (V_base < V_out) && (S_base > S_out)


    return (
        coalition_col = label,
        V_base_share  = V_base,
        V_out_share   = V_out,
        S_base_share  = S_base,
        S_out_share   = S_out,
        seatdiff_base = seatdiff_base,
        inversion     = inversion,
    )
end

function coalition_summary(df::DataFrame, col::Union{Symbol,AbstractString})
    # pega a coluna booleana e trata missings como false
    mask_raw = df[!, col]
    return coalition_summary_mask(df, mask_raw; label=col)
end



"""
    coalition_table(df, cols)

Recebe um DataFrame `df` e um vetor de Symbols `cols` (colunas booleanas que definem coalizões).
Retorna um DataFrame com um resumo por coalizão.
"""
function coalition_table(df::DataFrame, cols)
    rows = CoalitionSummaries = Vector{NamedTuple}(undef, length(cols))
    for (i, c) in enumerate(cols)
        rows[i] = coalition_summary(df, c)
    end
    return DataFrame(rows)
end

"""
    coalition_table_periods(df; year=nothing, years=nothing, path=get_coalition_path())

Retorna um DataFrame com um resumo por periodo (ex.: "2018.1", "2018.2"),
usando os partidos definidos em `partidos_por_periodo.json`.
"""
function coalition_table_periods(df::DataFrame;
                                 year::Union{Int,Nothing} = nothing,
                                 years::Union{AbstractVector{<:Integer},Nothing} = nothing,
                                 path::AbstractString = get_coalition_path())
    if year !== nothing && years !== nothing
        error("coalition_table_periods: use apenas `year` ou `years`.")
    end

    if years !== nothing
        tables = [coalition_table_periods(df; year=y, path=path) for y in years]
        return isempty(tables) ? DataFrame() : vcat(tables...)
    end

    periods = coalitions_by_period(; path=path)
    if year !== nothing
        periods = coalition_periods_overlapping_year(periods, year; path=path)
    end

    period_keys = sort(collect(Base.keys(periods)); by=period_sort_key)
    rows = Vector{NamedTuple}(undef, length(period_keys))
    for (i, key) in enumerate(period_keys)
        parties = periods[key]
        mask = in.(df.SG_PARTIDO, Ref(Set(parties)))
        rows[i] = coalition_summary_mask(df, mask; label=key)
    end
    return DataFrame(rows)
end

function parse_mandate_id(mandate_id::AbstractString)::NamedTuple
    token = strip(String(mandate_id))
    m = match(r"^(\d{4})-(\d{4})$", token)
    m === nothing && error("mandate_id inválido: '$token'. Use YYYY-YYYY.")

    start_year = parse(Int, m.captures[1])
    end_year = parse(Int, m.captures[2])
    end_year == start_year + 3 || error("mandate_id inválido: '$token'. Esperado intervalo de 4 anos.")

    return (
        mandate_id = token,
        start_year = start_year,
        end_year = end_year,
        election_year = start_year - 1,
    )
end

mandate_id_for_election_year(election_year::Integer)::String = begin
    start_year = Int(election_year) + 1
    string(start_year, "-", start_year + 3)
end

election_year_for_mandate_id(mandate_id::AbstractString)::Int = parse_mandate_id(mandate_id).election_year

function coalitions_by_period_raw(; path::AbstractString = get_coalition_path())
    return CabinetRelease.identified_parties(path)
end

"""
    load_cabinet_to_election_crosswalk(path = _cabinet_to_election_crosswalk_path()) -> DataFrame

Carrega a tabela explícita que traduz partidos do objeto ministerial
(verdade em ano de gabinete) para o espaço de identidade do ano eleitoral
usado nos joins de inversão.

Uma linha pode expandir para vários partidos eleitorais. Exemplo:
`UNIÃO` em joins com a eleição de 2018 vira `DEM` + `PSL`.
"""
function load_cabinet_to_election_crosswalk(
    path::AbstractString = _cabinet_to_election_crosswalk_path(),
)::DataFrame
    isfile(path) || error("Crosswalk gabinete->eleição não encontrado: $path")
    df = CSV.read(path, DataFrame)

    for col in (:election_year, :cabinet_party, :election_party)
        hasproperty(df, col) || error("Crosswalk gabinete->eleição sem coluna obrigatória: $col")
    end

    if !hasproperty(df, :notes)
        df[!, :notes] = fill("", nrow(df))
    end
    if !hasproperty(df, :mapping_type)
        df[!, :mapping_type] = fill("", nrow(df))
    end

    df[!, :election_year] = Int.(df.election_year)
    df[!, :cabinet_party] = strip.(String.(coalesce.(df.cabinet_party, "")))
    df[!, :election_party] = strip.(String.(coalesce.(df.election_party, "")))
    df[!, :notes] = String.(coalesce.(df.notes, ""))
    df[!, :mapping_type] = strip.(String.(coalesce.(df.mapping_type, "")))
    df[!, :cabinet_party_norm] = normalize_party.(df.cabinet_party)
    df[!, :election_party] = [
        canonical_party(row.election_party; year = row.election_year, strict = true)
        for row in eachrow(df)
    ]

    filter!(row -> !isempty(row.cabinet_party_norm) && !isempty(row.election_party), df)
    return df
end

"""
    cabinet_parties_in_election_space(cabinet_parties; election_year, valid_election_parties, crosswalk_path)

Traduz partidos do gabinete para o espaço de identidade do ano eleitoral
antes do join com votos/cadeiras.

Regra explícita:
- toda identidade deve ter linha explícita para `cabinet_party × election_year`;
- renomes e ancestralidade aditiva de fusões são documentados no crosswalk;
- os destinos eleitorais são unidos antes da soma, sem duplicação;
- identidade ausente ou destino inexistente falha sem fallback por rótulo.
"""
function cabinet_parties_in_election_space(
    cabinet_parties::AbstractVector{<:AbstractString};
    election_year::Integer,
    valid_election_parties::AbstractVector{<:AbstractString},
    crosswalk_path::AbstractString = CabinetRelease.default_crosswalk_path(),
)::Vector{String}
    report = CabinetRelease.translate(cabinet_parties; election_year,
        valid_election_parties, crosswalk_path = crosswalk_path)
    return sort(unique(String.(report.election_party)))
end

function coalition_metrics(
    df::DataFrame,
    parties;
    vote_col,
    seat_col,
    party_col,
    total_votes=nothing,
    total_seats=nothing,
    coalition_name=nothing,
    mandate_id=nothing,
    coalition_source=nothing,
)
    vote_col_sym = Symbol(vote_col)
    seat_col_sym = Symbol(seat_col)
    party_col_sym = Symbol(party_col)

    for col in (vote_col_sym, seat_col_sym, party_col_sym)
        hasproperty(df, col) || error("coalition_metrics: coluna ausente: $col")
    end

    party_labels = String.(df[!, party_col_sym])
    counts = combine(groupby(DataFrame(party = party_labels), :party), nrow => :count)
    if any(counts.count .> 1)
        dupes = counts.party[counts.count .> 1]
        error("coalition_metrics: partidos duplicados no DataFrame: $(join(String.(dupes), ", ")).")
    end

    coalition_parties = sort(unique(String.(parties)))

    missing_parties = [p for p in coalition_parties if !(p in Set(party_labels))]
    isempty(missing_parties) || error("Partido(s) da coalizão ausente(s) no DataFrame: $(join(missing_parties, ", ")).")

    votes_vec = Float64.(coalesce.(df[!, vote_col_sym], 0))
    seats_vec = Float64.(coalesce.(df[!, seat_col_sym], 0))
    total_votes_val = total_votes === nothing ? sum(votes_vec) : Float64(total_votes)
    total_seats_val = total_seats === nothing ? sum(seats_vec) : Float64(total_seats)
    total_votes_val > 0 || error("total_votes deve ser > 0.")
    total_seats_val > 0 || error("total_seats deve ser > 0.")

    mask = in.(party_labels, Ref(Set(coalition_parties)))
    coalition_votes = sum(votes_vec[mask])
    coalition_seats = sum(seats_vec[mask])
    vote_share = coalition_votes / total_votes_val
    seat_share = coalition_seats / total_seats_val

    return (
        mandate_id = mandate_id,
        coalition_source = coalition_source,
        coalition_name = coalition_name,
        coalition_votes = coalition_votes,
        coalition_seats = coalition_seats,
        vote_share = vote_share,
        seat_share = seat_share,
        seat_minus_vote = seat_share - vote_share,
        inversion = (seat_share - vote_share) < 0,
        n_parties_df = nrow(df),
        n_parties_coalition = length(coalition_parties),
    )
end

function cabinet_coalition_metrics_for_year(
    seat_differentials::DataFrame;
    coalition_year::Integer,
    mandate_id,
    election_year::Union{Nothing,Integer} = nothing,
    path::AbstractString = get_coalition_path(),
    crosswalk_path::AbstractString = _cabinet_to_election_crosswalk_path(),
    vote_col::Symbol = :valid_total,
    seat_col::Symbol = :total_seats,
    party_col::Symbol = :SG_PARTIDO,
)::DataFrame
    periods_raw = coalitions_by_period_raw(; path = path)
    periods_year = coalition_periods_overlapping_year(periods_raw, Int(coalition_year); path = path)
    return cabinet_coalition_metrics_for_periods(
        seat_differentials,
        periods_year;
        mandate_id = mandate_id,
        election_year = election_year,
        path = path,
        crosswalk_path = crosswalk_path,
        vote_col = vote_col,
        seat_col = seat_col,
        party_col = party_col,
    )
end

function cabinet_coalition_metrics_for_periods(
    seat_differentials::DataFrame,
    periods::Dict{String,Vector{String}};
    mandate_id,
    election_year::Union{Nothing,Integer} = nothing,
    path::AbstractString = get_coalition_path(),
    crosswalk_path::AbstractString = _cabinet_to_election_crosswalk_path(),
    vote_col::Symbol = :valid_total,
    seat_col::Symbol = :total_seats,
    party_col::Symbol = :SG_PARTIDO,
)::DataFrame
    mandate_election_year = election_year_for_mandate_id(String(mandate_id))
    if election_year !== nothing && Int(election_year) != mandate_election_year
        error(
            "cabinet_coalition_metrics_for_periods: election_year=$(Int(election_year)) " *
            "incompatível com mandate_id=$(String(mandate_id)) (esperado=$(mandate_election_year)).",
        )
    end
    election_year_resolved = election_year === nothing ? mandate_election_year : Int(election_year)
    keys_sorted = sort(collect(keys(periods)); by = period_sort_key)
    valid_election_parties = String.(seat_differentials[!, party_col])

    rows = NamedTuple[]
    for period in keys_sorted
        period_year = tryparse(Int, first(split(period, ".")))
        period_year === nothing && error(
            "cabinet_coalition_metrics_for_periods: não foi possível inferir ano do período $period.",
        )
        cabinet_canonicals = periods[period] # Stable release identities; never apply calendar aliases twice.
        join_parties = cabinet_parties_in_election_space(
            cabinet_canonicals;
            election_year = election_year_resolved,
            valid_election_parties = valid_election_parties,
            crosswalk_path = crosswalk_path,
        )
        metrics = coalition_metrics(
            seat_differentials,
            join_parties;
            vote_col = vote_col,
            seat_col = seat_col,
            party_col = party_col,
            coalition_name = period,
            mandate_id = mandate_id,
            coalition_source = :cabinet,
        )
        push!(rows, merge(metrics, (coalition_year = period_year, coalition_col = String(period))))
    end

    return DataFrame(rows)
end

function cabinet_inversion_table_for_year(
    seat_differentials::DataFrame;
    coalition_year::Integer,
    mandate_id,
    election_year::Union{Nothing,Integer} = nothing,
    path::AbstractString = get_coalition_path(),
    crosswalk_path::AbstractString = _cabinet_to_election_crosswalk_path(),
    vote_col::Symbol = :valid_total,
    seat_col::Symbol = :total_seats,
    party_col::Symbol = :SG_PARTIDO,
)::DataFrame
    metrics_df = cabinet_coalition_metrics_for_year(
        seat_differentials;
        coalition_year = coalition_year,
        mandate_id = mandate_id,
        election_year = election_year,
        path = path,
        crosswalk_path = crosswalk_path,
        vote_col = vote_col,
        seat_col = seat_col,
        party_col = party_col,
    )

    nrow(metrics_df) == 0 && return DataFrame(
        coalition_col = String[],
        V_base_share = Float64[],
        V_out_share = Float64[],
        S_base_share = Float64[],
        S_out_share = Float64[],
        seatdiff_base = Float64[],
        inversion = Bool[],
        mandate_id = String[],
        coalition_source = String[],
    )

    rows = NamedTuple[]
    for row in eachrow(metrics_df)
        push!(rows, (
            coalition_col = String(row.coalition_col),
            V_base_share = Float64(row.vote_share),
            V_out_share = 1.0 - Float64(row.vote_share),
            S_base_share = Float64(row.seat_share),
            S_out_share = 1.0 - Float64(row.seat_share),
            seatdiff_base = Float64(row.seat_minus_vote),
            inversion = Bool(row.inversion),
            mandate_id = String(row.mandate_id),
            coalition_source = String(row.coalition_source),
        ))
    end
    return DataFrame(rows)
end

function ideology_coalition_metrics(
    seat_differentials::DataFrame;
    mandate_id,
    ideology_path::Union{Nothing,AbstractString} = nothing,
    classification_year::Integer = 2023,
    classification_root_dir::Union{Nothing,AbstractString} = nothing,
    ideology_threshold::Float64 = 5.5,
    coalition_name::AbstractString = "ideology_threshold_leq_5_5",
    vote_col::Symbol = :valid_total,
    seat_col::Symbol = :total_seats,
    party_col::Symbol = :SG_PARTIDO,
)
    source_year = Int(classification_year)

    loaded = if ideology_path === nothing
        classification_root_dir === nothing ?
        load_party_classification(source_year) :
        load_party_classification(source_year; root_dir = String(classification_root_dir))
    else
        path = String(ideology_path)
        isfile(path) || error("ideology_coalition_metrics: arquivo de classificação não encontrado: $path")

        if source_year == 2023
            df = PartyClassification2023.load_party_ordinal_classification_2023(path = path)
            df[!, :source_year] = fill(source_year, nrow(df))
            df
        elseif source_year == 2025
            df = CSV.read(path, DataFrame)
            _postprocess_loaded_classification!(df, source_year)
        else
            error("ideology_coalition_metrics: classificação via ideology_path só suporta anos 2023/2025.")
        end
    end

    hasproperty(loaded, :party_name_raw) || error("ideology_coalition_metrics: coluna party_name_raw ausente.")
    hasproperty(loaded, :ideology_value_numeric) || error("ideology_coalition_metrics: coluna ideology_value_numeric ausente.")

    values = loaded[!, :ideology_value_numeric]
    mask = [x !== missing && Float64(x) <= ideology_threshold for x in values]
    parties_raw = String.(loaded[mask, :party_name_raw])
    isempty(parties_raw) && error("ideology_coalition_metrics: coalizão vazia para threshold=$ideology_threshold.")

    canonicals = canonicalize_parties(parties_raw; year = source_year, strict = true)

    return coalition_metrics(
        seat_differentials,
        canonicals;
        vote_col = vote_col,
        seat_col = seat_col,
        party_col = party_col,
        coalition_name = coalition_name,
        mandate_id = mandate_id,
        coalition_source = :ideology,
    )
end
