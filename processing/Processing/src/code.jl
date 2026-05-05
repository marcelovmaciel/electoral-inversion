# =============================================================================
# Root configuration and paths (simple, explicit)
# =============================================================================

const RAW_ROOT = Ref("../data/raw/electionsBR")
const COALITION_PATH = Ref("../scraping/output/partidos_por_periodo.json")

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

    if expected_total_seats !== nothing
        @assert total_seats == expected_total_seats "party_summary: expected $expected_total_seats seats, got $total_seats."
    end

    df[!, :vote_share] = df[!, vote_col] ./ max(total_votes, 1)
    df[!, :seat_share] = df[!, seat_col] ./ max(total_seats, 1)

    df[!, :quota]     = df[!, :vote_share] .* total_seats
    df[!, :seat_diff] = df[!, seat_col] .- df[!, :quota]

    return df
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
    isfile(path) || error("coalitions_by_period: arquivo não encontrado em $path")
    raw = JSON.parsefile(path)

    periods = Dict{String, Vector{String}}()
    for (period, info) in raw
        parties_raw = info["partidos"]
        period_year = tryparse(Int, first(split(period, ".")))
        parties = [
            normalize_party_str(p; year = period_year)
            for p in parties_raw
        ]
        periods[period] = unique(parties)
    end
    return periods
end

function parse_coalition_date(value, period::AbstractString, field::AbstractString)
    if value === nothing || value === missing
        return nothing
    end
    s = strip(String(value))
    isempty(s) && return nothing
    d = tryparse(Date, s)
    d === nothing && error("coalitions_by_year: data inválida em $period.$field: '$s'")
    return d
end

function coalition_period_windows(; path::AbstractString = get_coalition_path())
    isfile(path) || error("coalition_period_windows: arquivo não encontrado em $path")
    raw = JSON.parsefile(path)
    windows = Dict{String,Tuple{Union{Date,Nothing},Union{Date,Nothing}}}()
    for (period, info) in raw
        start_date = parse_coalition_date(get(info, "data_inicio", nothing), period, "data_inicio")
        end_date = parse_coalition_date(get(info, "data_fim", nothing), period, "data_fim")
        windows[period] = (start_date, end_date)
    end
    return windows
end

function coalitions_by_year(periods::Dict{String,Vector{String}}, year::Integer;
                            path::AbstractString = get_coalition_path())
    prefix = string(year) * "."
    direct = Dict(k => v for (k, v) in periods if startswith(k, prefix))
    !isempty(direct) && return direct

    year_start = Date(year, 1, 1)
    year_end = Date(year, 12, 31)
    windows = coalition_period_windows(; path=path)

    overlap = Dict{String,Vector{String}}()
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
        end_date < start_date && error("coalitions_by_year: intervalo inválido em $period ($start_date > $end_date).")

        push!(bounds, (start_date, end_date))
        if start_date <= year_end && end_date >= year_start
            overlap[period] = parties
        end
    end

    if !isempty(overlap)
        @info "coalitions_by_year: ano $year sem chave YYYY.k; retornando período(s) de continuidade que atravessam o ano."
        return overlap
    end

    if isempty(bounds) || !isempty(missing_dates)
        sample_vec = sort(unique(missing_dates))
        sample = isempty(sample_vec) ? "nenhum período com datas válidas" :
                 join(sample_vec[1:min(length(sample_vec), 5)], ", ")
        error("coalitions_by_year: dados insuficientes para filtrar o ano $year (faltam datas em: $sample).")
    end

    min_year = Dates.year(minimum(first.(bounds)))
    max_year = Dates.year(maximum(last.(bounds)))
    if min_year <= year <= max_year
        error("coalitions_by_year: nenhum período cobre $year, mas a série cobre $min_year-$max_year; verifique o parser/dados.")
    end

    return Dict{String,Vector{String}}()
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
        periods = coalitions_by_year(periods, year; path=path)
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
    isfile(path) || error("coalitions_by_period_raw: arquivo não encontrado em $path")
    payload = JSON.parsefile(path)

    out = Dict{String,Vector{String}}()
    for (period, info_any) in payload
        info_any isa AbstractDict || error("Período $period com payload inválido.")
        parties_any = get(info_any, "partidos", nothing)
        parties_any isa AbstractVector || error("Período $period sem vetor 'partidos'.")
        out[String(period)] = String[strip(String(p)) for p in parties_any if !isempty(strip(String(p)))]
    end
    return out
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

    df[!, :election_year] = Int.(df.election_year)
    df[!, :cabinet_party] = strip.(String.(coalesce.(df.cabinet_party, "")))
    df[!, :election_party] = strip.(String.(coalesce.(df.election_party, "")))
    df[!, :notes] = String.(coalesce.(df.notes, ""))
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
- se houver linha no crosswalk para `cabinet_party × election_year`, usa ela;
- sem linha explícita, a identidade só é aceita se o mesmo rótulo já existir
  no DataFrame eleitoral alvo;
- se nenhuma dessas duas condições valer, o join falha fechado e pede edição
  explícita do crosswalk.
"""
function cabinet_parties_in_election_space(
    cabinet_parties::AbstractVector{<:AbstractString};
    election_year::Integer,
    valid_election_parties::AbstractVector{<:AbstractString},
    crosswalk_path::AbstractString = _cabinet_to_election_crosswalk_path(),
)::Vector{String}
    crosswalk = load_cabinet_to_election_crosswalk(crosswalk_path)
    valid_labels = Set(String.(valid_election_parties))

    translated = String[]
    for cabinet_party in String.(cabinet_parties)
        cabinet_norm = normalize_party(cabinet_party)
        mask = [
            crosswalk.election_year[i] == Int(election_year) &&
            crosswalk.cabinet_party_norm[i] == cabinet_norm
            for i in eachindex(crosswalk.election_year)
        ]
        mapped = sort(unique(String.(crosswalk.election_party[mask])))
        if !isempty(mapped)
            append!(translated, mapped)
            continue
        end
        if cabinet_party in valid_labels
            push!(translated, String(cabinet_party))
            continue
        end
        error(
            "Partido do gabinete sem crosswalk compatível com a eleição de $(Int(election_year)): " *
            "'$(String(cabinet_party))'. Edite: $(crosswalk_path)",
        )
    end

    return sort(unique(translated))
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
    mandate_election_year = election_year_for_mandate_id(String(mandate_id))
    if election_year !== nothing && Int(election_year) != mandate_election_year
        error(
            "cabinet_coalition_metrics_for_year: election_year=$(Int(election_year)) " *
            "incompatível com mandate_id=$(String(mandate_id)) (esperado=$(mandate_election_year)).",
        )
    end
    election_year_resolved = election_year === nothing ? mandate_election_year : Int(election_year)
    periods_raw = coalitions_by_period_raw(; path = path)
    periods_year = coalitions_by_year(periods_raw, Int(coalition_year); path = path)
    keys_sorted = sort(collect(keys(periods_year)); by = period_sort_key)
    valid_election_parties = String.(seat_differentials[!, party_col])

    rows = NamedTuple[]
    for period in keys_sorted
        cabinet_canonicals = canonicalize_parties(periods_year[period]; year = Int(coalition_year), strict = true)
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
        push!(rows, merge(metrics, (coalition_year = Int(coalition_year), coalition_col = String(period))))
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
