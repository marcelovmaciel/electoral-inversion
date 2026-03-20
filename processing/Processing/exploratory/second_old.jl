# -------------------------------------------------------------------
# Configuração básica
# -------------------------------------------------------------------

# Raiz dos dados (ajuste se necessário)
p.set_root!("../data/raw/electionsBR")

# Cargos proporcionais que vamos considerar
const CARGOS_OK = Set([
    "DEPUTADO FEDERAL",
    "DEPUTADO ESTADUAL",
    "DEPUTADO DISTRITAL",
])

to_int(x) = x === missing || x === nothing ? 0 :
            x isa Integer ? x :
            x isa AbstractFloat ? round(Int, x) :
            (tryparse(Int, String(x)) |> y -> y === nothing ? 0 : y)

# Normalização de siglas partidárias (para unificar diferenças entre arquivos)
# Aqui você pode ir adicionando casos especiais conforme detectar alias.
function norm_party_label(s)
    s = strip(String(s))
    # Normalizar variações do Patriota(s)
    if s in ("PATRI", "PATRIOTA", "PATRIOTAS")
        return "PATRI"   # forma canônica única
    end
    return s
end

function norm_party!(df::DataFrame)
    if :SG_PARTIDO in names(df)
        df[!, :SG_PARTIDO] = norm_party_label.(df.SG_PARTIDO)
    end
    return df
end

# -------------------------------------------------------------------
# DETECTAR COLUNA DE STATUS (ELEITO / NÃO ELEITO)
# -------------------------------------------------------------------

"""
    detect_status_col(C::DataFrame) :: Union{Symbol,Nothing}

Tenta encontrar, na ordem de preferência, a coluna de situação do candidato
("eleito", "não eleito", etc.).
"""
function detect_status_col(C::DataFrame)
    nn = names(C)               # Vector{Symbol}
    ss = strip.(String.(nn))    # nomes em String

    wanted = [
        "DS_SIT_TOT_TURNO",
        "DS_SIT_CAND_TOT",
        "DS_SITUACAO_CANDIDATO",
        "DS_SITUACAO",
        "CD_SIT_TOT_TURNO",
    ]

    for w in wanted
        for (i, s) in pairs(ss)
            if s == w
                return nn[i]     # Symbol real
            end
        end
    end
    return nothing
end

# -------------------------------------------------------------------
# VOTOS POR PARTIDO
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# CADEIRAS POR PARTIDO
# -------------------------------------------------------------------

function seats_by_party(year; cargos_ok = CARGOS_OK)
    PS = cand_party_seats(year; cargos_ok=cargos_ok)
    C, status_col = cand_with_status(year; cargos_ok=cargos_ok)
    PS === nothing && return (nothing, status_col)
    return PS, status_col
end
# -------------------------------------------------------------------
# DENOMINADOR DE CADEIRAS (N_SEATS POR UF×CARGO)
# -------------------------------------------------------------------

"""
    seat_denominators(year; cargos_ok=CARGOS_OK) -> (denom::DataFrame, seat_col::Symbol)

Retorna DataFrame com uma linha por (SG_UF, DS_CARGO) e coluna `n_seats`,
derivada de seats.csv, somando as vagas naquela UF×cargo.

Isso serve para calcular share de cadeiras.
"""
function seat_denominators(year; cargos_ok = CARGOS_OK)
    S = p.load_seats(year)
    S === nothing && return (nothing, :none)

    println("\n[seat_denominators] year = $year")
    println("rows raw S: ", nrow(S))

    p.norm_cargo!(S)
    S[!, :DS_CARGO] = uppercase.(strip.(string.(S.DS_CARGO)))
    S[!, :SG_UF]    = string.(S.SG_UF)

    println("unique DS_CARGO (first 10): ",
            unique(S.DS_CARGO)[1:min(10, end)])

    # Filtrar cargos proporcionais (defensivo)
    before = nrow(S)
    filter!(r -> !ismissing(r.DS_CARGO) &&
                r.DS_CARGO in cargos_ok, S)
    println("rows after cargo filter: $before -> ", nrow(S))

    if nrow(S) == 0
        println("WARNING: cargo filter removed all rows in seats.csv.")
        println("Disabling cargo filter; using all rows in S for now.")
        S = p.load_seats(year)
        p.norm_cargo!(S)
        S[!, :DS_CARGO] = uppercase.(strip.(string.(S.DS_CARGO)))
        S[!, :SG_UF]    = string.(S.SG_UF)
        println("rows after reloading without cargo filter: ", nrow(S))
    end

    # Detectar coluna de nº de vagas
    scol = p.seat_col(S)
    @assert scol !== nothing "Could not detect seat column in seats.csv"
    println("using seat_col = ", scol)

    tmp = DataFrame(
        SG_UF    = S.SG_UF,
        DS_CARGO = S.DS_CARGO,
        n_seats  = to_int.(S[!, scol]),
    )

    println("tmp head:")
    first(tmp, 10) |> println

    denom = combine(groupby(tmp, [:SG_UF, :DS_CARGO]),
                    :n_seats => sum => :n_seats)

    println("rows in denom: ", nrow(denom))
    return denom, scol
end

# -------------------------------------------------------------------
# TABELA-MESTRE: PARTIDO × UF × CARGO
# -------------------------------------------------------------------

"""
    party_master_table(year) -> (df::DataFrame, meta::NamedTuple)

Retorna uma tabela com uma linha por UF × cargo × PARTIDO contendo:

    votes, seats, n_seats,
    vote_share, seat_share, quota, seat_diff

Coligações e federações são completamente ignoradas.
Essa é a base que você usa depois para agregar por coalizões exógenas.
"""
function party_master_table(year; cargos_ok = CARGOS_OK)
    party_votes, vote_scheme     = votes_by_party(year; cargos_ok=cargos_ok)
    party_votes === nothing && return (DataFrame(),
                                       (vote_scheme=:none,
                                        status_col=:none,
                                        seat_col=:none))

    party_seats, status_col_used = seats_by_party(year; cargos_ok=cargos_ok)
    denom,       seat_col_used   = seat_denominators(year; cargos_ok=cargos_ok)

    df = party_votes

    # juntar cadeiras por partido (se existirem)
    if party_seats !== nothing
        df = leftjoin(df, party_seats,
                      on = [:SG_UF, :DS_CARGO, :SG_PARTIDO])
        df.seats = coalesce.(df.seats, 0)
    else
        df.seats = zeros(Int, nrow(df))
    end

    # juntar denominador de cadeiras (por UF×cargo)
    if denom !== nothing
        df = leftjoin(df, denom, on=[:SG_UF, :DS_CARGO])
    else
        df.n_seats = 0
    end

    # votos totais por UF×cargo
    df[!, :ANO_ELEICAO] .= year
    totals = combine(groupby(df, [:SG_UF, :DS_CARGO]),
                     :votes => sum => :Vtot)
    df = leftjoin(df, totals, on=[:SG_UF, :DS_CARGO])

    df.vote_share = df.votes ./ max.(df.Vtot, 1)
    df.seat_share = df.seats ./ max.(df.n_seats, 1)
    df.quota      = df.vote_share .* df.n_seats
    df.seat_diff  = df.seats .- df.quota

    meta = (
        vote_scheme = vote_scheme,
        status_col  = status_col_used,
        seat_col    = seat_col_used,
    )
    return df, meta
end

# -------------------------------------------------------------------
# AGREGAÇÃO NACIONAL POR PARTIDO (OPCIONAL)
# -------------------------------------------------------------------

"""
    national_party_table(mt; cargo="DEPUTADO FEDERAL")

Agrega a `party_master_table` nacionalmente para um dado cargo
(DEPUTADO FEDERAL, ESTADUAL, DISTRITAL).

Uma linha por partido (SG_PARTIDO), com votos e cadeiras nacionais.
"""
function national_party_table(mt::DataFrame; cargo::AbstractString = "DEPUTADO FEDERAL")
    d = filter(r -> r.DS_CARGO == cargo, mt)
    nat = combine(groupby(d, :SG_PARTIDO),
                  :votes => sum => :votes,
                  :seats => sum => :seats)
    Vtot = sum(nat.votes)
    N    = sum(nat.seats)
    nat.vote_share = nat.votes ./ max(Vtot, 1)
    nat.seat_share = nat.seats ./ max(N, 1)
        # Miller-style differentials
    nat.share_diff = nat.seat_share .- nat.vote_share         # in proportion points
    nat.seat_diff  = nat.seats .- nat.vote_share .* N         # in seat units

    return nat
end

# -------------------------------------------------------------------
# EXEMPLO DE USO (descoment

# -------------------------------------------------------------------
# EXEMPLO DE USO
# -------------------------------------------------------------------

# Exemplo básico de geração da tabela por partido para um ano:
# y = 2018
# mt, meta = party_master_table(y)
# println(meta)
# first(mt, 10) |> display
#
# # Agregar nacionalmente para deputados federais:
# nat_fed = national_party_table(mt; cargo="DEPUTADO FEDERAL")
# first(nat_fed, 10) |> display
#
# # Se quiser salvar os resultados em CSV para usar em outros scripts:
# using CSV
# CSV.write("party_master_$(y).csv", mt)
# CSV.write("party_national_fed_$(y).csv", nat_fed)

# -------------------------------------------------------------------
# STAGING / DEBUG HELPERS (for interactive REPL inspection)
# -------------------------------------------------------------------

"""
    pmz_raw(year)

Retorna o DataFrame bruto de party_mun_zone (sem nenhuma modificação).
"""
pmz_raw(year) = p.load_pmz(year)

"""
    pmz_norm(year; cargos_ok=CARGOS_OK)

Retorna P já com cargos normalizados, SG_UF/SG_PARTIDO como String e
norm_party! aplicado, ANTES de qualquer filtro.
"""
function pmz_norm(year; cargos_ok=CARGOS_OK)
    P = p.load_pmz(year)
    P === nothing && return nothing
    p.norm_cargo!(P)
    P[!, :DS_CARGO]   = uppercase.(strip.(String.(P.DS_CARGO)))
    P[!, :SG_UF]      = String.(P.SG_UF)
    P[!, :SG_PARTIDO] = String.(P.SG_PARTIDO)
    norm_party!(P)
    return P
end

"""
    pmz_filtered(year; cargos_ok=CARGOS_OK)

Retorna P depois da normalização + filtro de cargos proporcionais +
remoção de votos em trânsito.
"""
function pmz_filtered(year; cargos_ok=CARGOS_OK)
    P = pmz_norm(year; cargos_ok=cargos_ok)
    P === nothing && return nothing

    if cargos_ok !== nothing
        filter!(r -> r.DS_CARGO in cargos_ok, P)
    end

    if :ST_VOTO_EM_TRANSITO in names(P)
        bad = Set(["S", "SIM", "1"])
        filter!(r -> ismissing(r.ST_VOTO_EM_TRANSITO) ||
                    !(String(r.ST_VOTO_EM_TRANSITO) in bad), P)
    end
    return P
end

load_vote_mun_zone(y) = load_table(
    joinpath("../data/raw/electionsBR", string(y), "vote_mun_zone.csv");
    normalizers = (
        df -> norm_uf!(df),
        df -> norm_cargo!(df),
        df -> norm_party!(df),
    )
)
function votes_by_party(year; cargos_ok=CARGOS_OK)
    V = load_vote_mun_zone(year)
    V === nothing && return (nothing, :none)

    println("\n[votes_by_party] year = $year (from vote_mun_zone)")
    println("rows raw V: ", nrow(V))

    # Normalização já aplicada pelo loader: cargo, uf, partido

    # Filtrar cargos proporcionais
    if cargos_ok !== nothing
        before = nrow(V)
        filter!(r -> r.DS_CARGO in cargos_ok, V)
        println("rows after cargo filter: $before -> ", nrow(V))
    end

    # Determinar colunas disponíveis de votos
    namesV = names(V)

    has_nom   = :QT_VOTOS_NOMINAIS     in namesV
    has_leg   = :QT_VOTOS_LEGENDA      in namesV
    has_nom_v = :QT_VOTOS_NOMINAIS_VALIDOS in namesV
    has_leg_v = :QT_VOTOS_LEGENDA_VALIDOS  in namesV
    has_total = :QT_VOTOS in namesV

    if has_nom || has_leg
        v_nom = has_nom ? to_int.(coalesce.(V.QT_VOTOS_NOMINAIS, 0)) : 0
        v_leg = has_leg ? to_int.(coalesce.(V.QT_VOTOS_LEGENDA, 0))  : 0
        votes = v_nom .+ v_leg
        scheme = :nom_plus_leg
    elseif has_nom_v || has_leg_v
        v_nom = has_nom_v ? to_int.(coalesce.(V.QT_VOTOS_NOMINAIS_VALIDOS, 0)) : 0
        v_leg = has_leg_v ? to_int.(coalesce.(V.QT_VOTOS_LEGENDA_VALIDOS, 0))  : 0
        votes = v_nom .+ v_leg
        scheme = :valid_nom_plus_leg
    elseif has_total
        votes = to_int.(V.QT_VOTOS)
        scheme = :QT_VOTOS
    else
        error("Não encontrei colunas de votos em vote_mun_zone.")
    end

    tmp = DataFrame(
        SG_UF      = V.SG_UF,
        DS_CARGO   = V.DS_CARGO,
        SG_PARTIDO = V.SG_PARTIDO,
        votes      = votes,
    )

    normalize_party_column!(tmp, :SG_PARTIDO)

    party_votes = combine(groupby(tmp, [:SG_UF, :DS_CARGO, :SG_PARTIDO]),
                          :votes => sum => :votes)

    println("rows in party_votes: ", nrow(party_votes))

    return party_votes, scheme
end


# -------------------------------------------------------------------
# pmz_party_votes  (PATCHED)
# -------------------------------------------------------------------

"""
    pmz_party_votes(year; cargos_ok=CARGOS_OK)

Pipeline explícito: lê pmz, normaliza, filtra e agrega votos por
(SG_UF, DS_CARGO, SG_PARTIDO).

Equivalente ao que `votes_by_party` faz internamente, mas usando
uma **única coluna de votos totais** em vez de somar nominais + legenda.
"""
function pmz_party_votes(year; cargos_ok=CARGOS_OK)
    P = pmz_filtered(year; cargos_ok=cargos_ok)
    P === nothing && return nothing
    namesP = names(P)

    # ------------------ PATCHED VOTE DETECTION ------------------
    preferred_totals = (
        :QT_VOTOS,
        :TOTAL_VOTOS,
        :QT_VOTOS_VALIDOS,
        :QT_VOTOS_VALIDOS_NOMINAIS,
        :QT_VOTOS_VALIDOS_LEGENDA,
    )

    vc = nothing
    for c in preferred_totals
        if c in namesP
            vc = c
            break
        end
    end

    if vc === nothing
        vc = p.vote_col(P)
    end

    @assert vc !== nothing "Could not detect a vote column in party_mun_zone"
    v = to_int.(P[!, vc])
    # ------------------------------------------------------------

    tmp = DataFrame(
        SG_UF      = P.SG_UF,
        DS_CARGO   = P.DS_CARGO,
        SG_PARTIDO = P.SG_PARTIDO,
        votes      = v,
    )

    party_votes = combine(groupby(tmp, [:SG_UF, :DS_CARGO, :SG_PARTIDO]),
                          :votes => sum => :votes)
    normalize_party_column!(party_votes, :SG_PARTIDO)
    return party_votes
end


# -------------------------------------------------------------------
# Staging para candidatos / cadeiras
# -------------------------------------------------------------------

"""
    cand_raw(year)

Retorna candidate.csv bruto.
"""
cand_raw(year) = p.load_candidate(year)

"""
    cand_norm(year; cargos_ok=CARGOS_OK)

Retorna C normalizado (DS_CARGO, SG_UF, SG_PARTIDO) e com norm_party!,
antes de qualquer filtro de cargo ou status.
"""
function cand_norm(year; cargos_ok=CARGOS_OK)
    C = p.load_candidate(year)
    C === nothing && return nothing

    p.norm_cargo!(C)
    C[!, :DS_CARGO]   = uppercase.(strip.(string.(C.DS_CARGO)))
    C[!, :SG_UF]      = string.(C.SG_UF)
    C[!, :SG_PARTIDO] = string.(C.SG_PARTIDO)
    norm_party!(C)
    return C
end

"""
    cand_filtered(year; cargos_ok=CARGOS_OK)

Retorna C depois de normalizar e filtrar para cargos proporcionais,
sem ainda filtrar por ELEITO.
"""
function cand_filtered(year; cargos_ok=CARGOS_OK)
    C = cand_norm(year; cargos_ok=cargos_ok)
    C === nothing && return nothing
    if cargos_ok !== nothing
        filter!(r -> !ismissing(r.DS_CARGO) && r.DS_CARGO in cargos_ok, C)
    end
    return C
end

"""
    cand_with_status(year; cargos_ok=CARGOS_OK)

Retorna (C, status_col), onde C já está filtrado para cargos
proporcionais e contém a coluna de status que será usada para marcar
ELEITOS. Útil para inspecionar os valores de status antes de filtrar.
"""
function cand_with_status(year; cargos_ok=CARGOS_OK)
    C = cand_filtered(year; cargos_ok=cargos_ok)
    C === nothing && return (nothing, nothing)
    status_col = detect_status_col(C)
    return C, status_col
end

const WINNER_STATUSES = Set([
    "ELEITO",
    "ELEITO POR QP",
    "ELEITO POR MÉDIA",
    # adicionar outras variações que apareçam no TSE
])

function cand_winners(year; cargos_ok=CARGOS_OK)
    C, status_col = cand_with_status(year; cargos_ok=cargos_ok)
    (C === nothing || status_col === nothing) && return (nothing, status_col)

    status_str = strip.(uppercase.(String.(C[!, status_col])))

    C[!, :WINNER] = in.(status_str, Ref(WINNER_STATUSES))
    filter!(r -> r.WINNER, C)
    return C, status_col
end

"""
    cand_party_seats(year; cargos_ok=CARGOS_OK)

Retorna DataFrame com (SG_UF, DS_CARGO, SG_PARTIDO, seats) derivado
apenas de C_winners, sem passar por `seats_by_party`.
"""
function cand_party_seats(year; cargos_ok=CARGOS_OK)
    Cw, _ = cand_winners(year; cargos_ok=cargos_ok)
    Cw === nothing && return nothing
    normalize_party_column!(Cw, :SG_PARTIDO)
    tmp = DataFrame(
        SG_UF      = Cw.SG_UF,
        DS_CARGO   = Cw.DS_CARGO,
        SG_PARTIDO = Cw.SG_PARTIDO,
    )

    party_seats = combine(groupby(tmp, [:SG_UF, :DS_CARGO, :SG_PARTIDO]),
                          nrow => :seats)

    return party_seats
end


const PARTY_MAP = Dict(
    "PATRI"   => "PATRIOTA",
    "PATRIOTA" => "PATRIOTA",
)


function normalize_party_column!(df, col=:SG_PARTIDO; mapping=PARTY_MAP)
    df[!, col] = [get(mapping, String(x), String(x)) for x in df[!, col]]
    return df
end



# -------------------------------------------------------------------
# Staging para denominador de cadeiras
# -------------------------------------------------------------------

"""
    seats_raw(year)

Retorna seats.csv bruto.
"""
seats_raw(year) = p.load_seats(year)

"""
    seats_norm(year; cargos_ok=CARGOS_OK)

Retorna S com DS_CARGO/SG_UF normalizados, antes do filtro de cargos.
"""
function seats_norm(year; cargos_ok=CARGOS_OK)
    S = p.load_seats(year)
    S === nothing && return nothing
    p.norm_cargo!(S)
    S[!, :DS_CARGO] = uppercase.(strip.(string.(S.DS_CARGO)))
    S[!, :SG_UF]    = string.(S.SG_UF)
    return S
end

"""
    seats_filtered(year; cargos_ok=CARGOS_OK)

Retorna S normalizado e filtrado para cargos proporcionais.
"""
function seats_filtered(year; cargos_ok=CARGOS_OK)
    S = seats_norm(year; cargos_ok=cargos_ok)
    S === nothing && return nothing
    if cargos_ok !== nothing
        filter!(r -> !ismissing(r.DS_CARGO) && r.DS_CARGO in cargos_ok, S)
    end
    return S
end
