using Revise
import Processing as p

using DataFrames

# point to your root (optional if default fits)

p.set_root!("../data/raw/electionsBR")

# proportional offices we'll keep
const CARGOS_OK = Set([
    "DEPUTADO FEDERAL",
    "DEPUTADO ESTADUAL",
    "DEPUTADO DISTRITAL",
])

to_int(x) = x === missing || x === nothing ? 0 :
            x isa Integer ? x :
            x isa AbstractFloat ? round(Int, x) :
            (tryparse(Int, String(x)) |> y -> y === nothing ? 0 : y)



"""
    make_list_mapping(year; mode=:coalig_first)

Return a DataFrame with one row per (SG_UF, DS_CARGO, SG_PARTIDO),
and a LISTA column determined by `mode`.

Modes implemented:
- :coalig_first  -> NM_COLIGACAO > NM_FEDERACAO > NM_LEGENDA > SG_PARTIDO
- :feder_first   -> NM_FEDERACAO > NM_COLIGACAO > NM_LEGENDA > SG_PARTIDO
- :party_only    -> SG_PARTIDO only
"""
function make_list_mapping(year; mode::Symbol = :coalig_first)
    L = p.load_legends(year)
    L === nothing && return nothing

    n = nrow(L)
    nm_colig = hasproperty(L, :NM_COLIGACAO) ? String.(L.NM_COLIGACAO) : fill("", n)
    nm_fed   = hasproperty(L, :NM_FEDERACAO) ? String.(L.NM_FEDERACAO) : fill("", n)
    nm_leg   = hasproperty(L, :NM_LEGENDA)   ? String.(L.NM_LEGENDA)   : fill("", n)
    sg_part  = String.(L.SG_PARTIDO)

    LISTA = Vector{String}(undef, n)

    for i in 1:n
        c  = strip(nm_colig[i])
        f  = strip(nm_fed[i])
        lg = strip(nm_leg[i])
        sp = strip(sg_part[i])

        if mode == :party_only
            LISTA[i] = sp
        elseif mode == :feder_first
            if !isempty(f)
                LISTA[i] = f
            elseif !isempty(c)
                LISTA[i] = c
            elseif !isempty(lg)
                LISTA[i] = lg
            else
                LISTA[i] = sp
            end
        else
            # default :coalig_first
            if !isempty(c)
                LISTA[i] = c
            elseif !isempty(f)
                LISTA[i] = f
            elseif !isempty(lg)
                LISTA[i] = lg
            else
                LISTA[i] = sp
            end
        end
    end

    mapping = select(L, [:SG_UF, :DS_CARGO, :SG_PARTIDO])
    mapping.LISTA = LISTA
    unique!(mapping)
    return mapping
end



y = 2018

L = p.load_legends(y)


names(L)

first(L, 10)

lm = make_list_mapping(y; mode=:coalig_first)

first(lm, 10)

lm.LISTA |> unique |> length

function votes_by_party(year; cargos_ok = CARGOS_OK)
    P = p.load_pmz(year)
    P === nothing && return (nothing, :none)

    println("\n[votes_by_party] year = $year")
    println("rows raw P: ", nrow(P))

    # --- normalize cargo & party/UF
    p.norm_cargo!(P)
    P[!, :DS_CARGO] = uppercase.(strip.(String.(P.DS_CARGO)))
    P[!, :SG_UF]    = String.(P.SG_UF)
    P[!, :SG_PARTIDO] = String.(P.SG_PARTIDO)

    println("unique DS_CARGO (first 10): ", unique(P.DS_CARGO)[1:min(10,end)])

    # --- filter by cargos (but don't let it silently kill everything)
    if cargos_ok !== nothing
        before = nrow(P)
        filter!(r -> r.DS_CARGO in cargos_ok, P)
        println("rows after cargo filter: $before -> ", nrow(P))
        if nrow(P) == 0
            println("WARNING: cargo filter removed all rows; disabling cargo filter for now.")
            P = p.load_pmz(year)
            p.norm_cargo!(P)
            P[!, :DS_CARGO] = uppercase.(strip.(String.(P.DS_CARGO)))
            P[!, :SG_UF]    = String.(P.SG_UF)
            P[!, :SG_PARTIDO] = String.(P.SG_PARTIDO)
        end
    end

    # --- transit filter (very defensive)
    if :ST_VOTO_EM_TRANSITO in names(P)
        vals = unique(String.(P.ST_VOTO_EM_TRANSITO))
        println("ST_VOTO_EM_TRANSITO values: ", vals)
        before = nrow(P)
        # keep everything that is NOT an explicit "S" / "SIM" etc.
        bad = Set(["S", "SIM", "1"])
        filter!(r -> ismissing(r.ST_VOTO_EM_TRANSITO) ||
                    !(String(r.ST_VOTO_EM_TRANSITO) in bad), P)
        println("rows after transit filter: $before -> ", nrow(P))
    end

    println("rows after all filters: ", nrow(P))
    if nrow(P) == 0
        println("STILL empty after relaxing filters; returning nothing.")
        return (nothing, :none)
    end

    namesP = names(P)

    # --- choose vote scheme explicitly for your 2018 schema ---
    has_nom_valid = :QT_VOTOS_NOMINAIS_VALIDOS in namesP
    has_tot_leg   = :QT_TOTAL_VOTOS_LEG_VALIDOS in namesP
    has_nom_old   = :QT_VOTOS_NOMINAIS in namesP
    has_leg_old   = :QT_VOTOS_LEGENDA in namesP


    if has_nom_valid && has_tot_leg
        v = to_int.(P.QT_VOTOS_NOMINAIS_VALIDOS) .+
            to_int.(P.QT_TOTAL_VOTOS_LEG_VALIDOS)
        scheme = :valid_nom_plus_leg
    elseif has_nom_old && has_leg_old
        v = to_int.(P.QT_VOTOS_NOMINAIS) .+
            to_int.(P.QT_VOTOS_LEGENDA)
        scheme = :nom_plus_leg
    else
        vc = p.vote_col(P)
        @assert vc !== nothing "Could not detect a vote column in party_mun_zone"
        v = to_int.(P[!, vc])
        scheme = Symbol(vc)
    end

    println("vote scheme used: ", scheme)
    println("votes sample: ", v[1:min(10, length(v))])

    tmp = DataFrame(
        SG_UF      = P.SG_UF,
        DS_CARGO   = P.DS_CARGO,
        SG_PARTIDO = P.SG_PARTIDO,
        votes      = v,
    )

    #println("tmp head:")
    #show(first(tmp, 10); allcols=true, trunc=false)

    party_votes = combine(groupby(tmp, [:SG_UF, :DS_CARGO, :SG_PARTIDO]),
                          :votes => sum => :votes)

    println("rows in party_votes: ", nrow(party_votes))
    return party_votes, scheme
end


P = p.load_pmz(2018)


P |> names |> println

pv, scheme = votes_by_party(y)

scheme
first(pv, 10)




"""
    votes_by_list(year; mode=:coalig_first) -> (list_votes::DataFrame, scheme::Symbol)

Aggregate party votes to LISTA using the mapping from make_list_mapping.
"""
function votes_by_list(year; mode::Symbol = :coalig_first)
    party_votes, scheme = votes_by_party(year)
    party_votes === nothing && return (nothing, scheme)

    mapping = make_list_mapping(year; mode=mode)

    if mapping === nothing
        # no legends: treat each party as its own list
        df = transform(party_votes, :SG_PARTIDO => String => :LISTA)
    else
        df = leftjoin(party_votes, mapping,
                      on = [:SG_UF, :DS_CARGO, :SG_PARTIDO])
        # fallback: if LISTA missing, use party label
        df.LISTA = coalesce.(df.LISTA, String.(df.SG_PARTIDO))
    end

    list_votes = combine(groupby(df, [:SG_UF, :DS_CARGO, :LISTA]),
                         :votes => sum => :votes)
    return list_votes, scheme
end




lv, vote_scheme = votes_by_list(y; mode=:coalig_first)
vote_scheme
first(lv, 10)
function detect_status_col(C::DataFrame)
    nn = names(C)                      # Vector{Symbol}
    ss = strip.(String.(nn))          # stripped string names

    # Ordered preference: DS_* names first, then CD_*
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
                return nn[i]          # actual Symbol in the DataFrame
            end
        end
    end
    return nothing
end
function seats_by_party(year)
    C = p.load_candidate(year)
    C === nothing && return (nothing, :none)

    println("\n[seats_by_party] year = $year")
    println("rows raw C: ", nrow(C))

    # --- normalize basic keys ---
    p.norm_cargo!(C)
    C[!, :DS_CARGO]   = uppercase.(strip.(string.(C.DS_CARGO)))
    C[!, :SG_UF]      = string.(C.SG_UF)
    C[!, :SG_PARTIDO] = string.(C.SG_PARTIDO)

    println("unique DS_CARGO (first 10): ",
            unique(C.DS_CARGO)[1:min(10, end)])

    # --- filter to proportional cargos ---
    before = nrow(C)
    filter!(r -> !ismissing(r.DS_CARGO) &&
                r.DS_CARGO in CARGOS_OK, C)
    println("rows after cargo filter: $before -> ", nrow(C))

    if nrow(C) == 0
        println("WARNING: cargo filter removed all rows; returning nothing.")
        return (nothing, :none)
    end

    # --- detect status column robustly (DS_* preferred) ---
    status_col = detect_status_col(C)
    if status_col === nothing
        println("No status / situation column found in candidate.csv.")
        println("Available columns: ", names(C))
        error("Add the correct status column name to detect_status_col.")
    end
    println("using status_col = ", status_col)

    # --- mark winners: status contains 'ELEIT' ---
    status_str = uppercase.(string.(C[!, status_col]))
    C[!, :WINNER] = occursin.(r"ELEIT", status_str)

    before = nrow(C)
    filter!(r -> r.WINNER == true, C)
    println("rows after WINNER filter: $before -> ", nrow(C))

    if nrow(C) == 0
        println("WARNING: no elected rows found; returning empty result.")
        return (DataFrame(SG_UF=String[], DS_CARGO=String[],
                          SG_PARTIDO=String[], seats=Int[]),
                status_col)
    end

    # --- aggregate to seats per party ---
    tmp = DataFrame(
        SG_UF      = C.SG_UF,
        DS_CARGO   = C.DS_CARGO,
        SG_PARTIDO = C.SG_PARTIDO,
    )

    party_seats = combine(groupby(tmp, [:SG_UF, :DS_CARGO, :SG_PARTIDO]),
                          nrow => :seats)

    println("rows in party_seats: ", nrow(party_seats))
    return party_seats, status_col
end


y = 2018
C = p.load_candidate(y)
names(C) |> println      # see what status columns actually exist

ps, status_col = seats_by_party(y)
status_col                     # e.g. :DS_SIT_TOT_TURNO or :DS_SITUACAO_CANDIDATO
first(ps, 10) |> display


"""
    seats_by_list(year; mode=:coalig_first) -> (list_seats::DataFrame, status_col::Symbol)
"""
function seats_by_list(year; mode::Symbol = :coalig_first)
    party_seats, status_col = seats_by_party(year)
    party_seats === nothing && return (nothing, status_col)

    mapping = make_list_mapping(year; mode=mode)

    if mapping === nothing
        df = transform(party_seats, :SG_PARTIDO => String => :LISTA)
    else
        df = leftjoin(party_seats, mapping,
                      on = [:SG_UF, :DS_CARGO, :SG_PARTIDO])
        df.LISTA = coalesce.(df.LISTA, String.(df.SG_PARTIDO))
    end

    list_seats = combine(groupby(df, [:SG_UF, :DS_CARGO, :LISTA]),
                         :seats => sum => :seats)
    return list_seats, status_col
end




ls, status_col = seats_by_list(y; mode=:coalig_first)

ls

first(ls, 10)


"""
    seat_denominators(year) -> (denom::DataFrame, seat_col::Symbol)

Total number of seats per (UF, cargo) from seats.csv.
"""
function seat_denominators(year; cargos_ok = CARGOS_OK)
    S = p.load_seats(year)
    S === nothing && return (nothing, :none)

    println("\n[seat_denominators] year = $year")
    println("rows raw S: ", nrow(S))

    # normalize cargo + keys
    p.norm_cargo!(S)
    S[!, :DS_CARGO] = uppercase.(strip.(string.(S.DS_CARGO)))
    S[!, :SG_UF]    = string.(S.SG_UF)

    println("unique DS_CARGO (first 10): ",
            unique(S.DS_CARGO)[1:min(10, end)])

    # filter to proportional cargos – but don't silently kill everything
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

    # detect seat column
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


denom, seat_col = seat_denominators(y)

seat_col

first(denom, 10)



"""
    master_table(year; mode=:coalig_first)
        -> (df::DataFrame, meta::NamedTuple)

Returns a table with one row per UF × cargo × LISTA containing:
    votes, seats, n_seats, vote_share, seat_share, quota, seat_diff

meta gives you which columns/schemes were used.
"""
function master_table(year; mode::Symbol = :coalig_first)
    list_votes, vote_scheme   = votes_by_list(year; mode=mode)
    list_seats, status_col    = seats_by_list(year; mode=mode)
    denom, seat_col_used      = seat_denominators(year)

    list_votes === nothing && return (DataFrame(), (vote_scheme=:none, status_col=:none, seat_col=:none))

    df = list_votes

    if list_seats !== nothing
        df = leftjoin(df, list_seats, on=[:SG_UF, :DS_CARGO, :LISTA])
        df.seats = coalesce.(df.seats, 0)
    else
        df.seats = zeros(Int, nrow(df))
    end

    if denom !== nothing
        df = leftjoin(df, denom, on=[:SG_UF, :DS_CARGO])
    else
        df.n_seats = 0
    end

    # total valid votes per UF × cargo
    totals = combine(groupby(df, [:SG_UF, :DS_CARGO]),
                     :votes => sum => :Vtot)
    df = leftjoin(df, totals, on=[:SG_UF, :DS_CARGO])

    df.vote_share = df.votes ./ max.(df.Vtot, 1)
    df.seat_share = df.seats ./ max.(df.n_seats, 1)
    df.quota      = df.vote_share .* df.n_seats
    df.seat_diff  = df.seats .- df.quota

    meta = (
        vote_scheme = vote_scheme,
        status_col  = status_col,
        seat_col    = seat_col_used,
    )
    return df, meta
end




y = 2018
mt, meta = master_table(y; mode=:coalig_first)

meta                # see which columns were used


first(mt, 12)       # inspect a slice

# focus on a single UF × cargo:
filter(row -> row.SG_UF == "SP" && row.DS_CARGO == "DEPUTADO FEDERAL", mt) |> first






using Statistics

function add_disproportionality(mt::DataFrame)
    df = copy(mt)

    # bias
    df.bias = df.seat_share .- df.vote_share

    # seat–vote ratio (handle zero vote_share)
    df.sv_ratio = [df.vote_share[i] > 0 ?
                   df.seat_share[i] / df.vote_share[i] :
                   NaN
                   for i in 1:nrow(df)]

    # district-level LH and G
    g = groupby(df, [:SG_UF, :DS_CARGO])
    stats = combine(g) do sub
        diff = sub.seat_share .- sub.vote_share
        LH = 0.5 * sum(abs.(diff))
        G  = sqrt(0.5 * sum(diff .^ 2))
        (; LH, G)
    end

    # join back
    df = leftjoin(df, stats, on = [:SG_UF, :DS_CARGO])
    return df
end


# usage
mt_stats = add_disproportionality(mt)
first(mt_stats, 12)



"""
    pairwise_inversions_slice(slice) -> DataFrame

Given `slice = filter(r -> r.SG_UF==... && r.DS_CARGO==..., mt)`,
returns all pairs of lists (i,j) that form a Miller-style inversion.
"""
function pairwise_inversions_slice(slice)
    n = nrow(slice)
    invs = DataFrame(
        LISTA_i = String[],
        LISTA_j = String[],
        votes_i = Int[],
        votes_j = Int[],
        seats_i = Int[],
        seats_j = Int[],
    )

    for i in 1:n-1, j in i+1:n
        vi, vj = slice.votes[i], slice.votes[j]
        si, sj = slice.seats[i], slice.seats[j]

        # skip ties
        if vi == vj || si == sj
            continue
        end

        if (vi > vj && si < sj) || (vi < vj && si > sj)
            push!(invs, (String(slice.LISTA[i]),
                         String(slice.LISTA[j]),
                         Int(vi), Int(vj), Int(si), Int(sj)))
        end
    end
    invs
end
"""
    pairwise_inversions_all(mt) -> DataFrame

One row per inversion, with UF, cargo and the two lists.
"""
function pairwise_inversions_all(mt)
    res = DataFrame(
        SG_UF    = String[],
        DS_CARGO = String[],
        LISTA_i  = String[],
        LISTA_j  = String[],
        votes_i  = Int[],
        votes_j  = Int[],
        seats_i  = Int[],
        seats_j  = Int[],
    )

    for (key, slice) in pairs(groupby(mt, [:SG_UF, :DS_CARGO]))
        invs = pairwise_inversions_slice(slice)
        for row in eachrow(invs)
            push!(res, (String(key.SG_UF), String(key.DS_CARGO),
                        row.LISTA_i, row.LISTA_j,
                        row.votes_i, row.votes_j,
                        row.seats_i, row.seats_j))
        end
    end

    res
end


pair_inv = pairwise_inversions_all(mt)
first(pair_inv, 20)

"""
    coalition_inversions_slice(slice; max_lists=18) -> DataFrame

Given one UF×cargo slice of `mt`, search all subsets of LISTA (up to `max_lists`)
such that:

  sum(seats) > n_seats/2   and   sum(votes) < Vtot/2.

Returns one row per coalition witness.
"""
function coalition_inversions_slice(slice::DataFrame; max_lists::Int = 18)
    L = nrow(slice)
    if L == 0 || L > max_lists
        return DataFrame()
    end

    N    = first(slice.n_seats)
    Vtot = sum(slice.votes)

    res = DataFrame(
        coalition = Vector{String}[],
        votes_sum = Int[],
        seats_sum = Int[],
        n_lists   = Int[],
        n_seats   = Int[],
        Vtot      = Int[],
    )

    limit = (1 << L) - 1
    for mask in 1:limit
        seats_sum = 0
        votes_sum = 0
        idxs = Int[]
        for i in 1:L
            if (mask >> (i-1)) & 0x1 == 1
                seats_sum += Int(slice.seats[i])
                votes_sum += Int(slice.votes[i])
                push!(idxs, i)
            end
        end
        if 2*seats_sum > N && 2*votes_sum < Vtot   # strict majority seats, strict minority votes
            coal_names = String.(slice.LISTA[idxs])
            push!(res, (coal_names, votes_sum, seats_sum,
                        length(idxs), N, Vtot))
        end
    end

    res
end


"""
    coalition_inversions_all(mt; max_lists=18) -> DataFrame

Run `coalition_inversions_slice` for each UF×cargo slice of `mt`.
"""
function coalition_inversions_all(mt::DataFrame; max_lists::Int = 18)
    res = DataFrame(
        SG_UF     = String[],
        DS_CARGO  = String[],
        coalition = Vector{String}[],
        votes_sum = Int[],
        seats_sum = Int[],
        n_lists   = Int[],
        n_seats   = Int[],
        Vtot      = Int[],
    )

    for (key, slice) in pairs(groupby(mt, [:SG_UF, :DS_CARGO]))
        invs = coalition_inversions_slice(slice; max_lists=max_lists)
        for row in eachrow(invs)
            push!(res, (String(key.SG_UF), String(key.DS_CARGO),
                        row.coalition, row.votes_sum, row.seats_sum,
                        row.n_lists, row.n_seats, row.Vtot))
        end
    end

    res
end

# usage TODO broken
coal_inv = coalition_inversions_all(mt; max_lists=18)
first(coal_inv, 10)



function national_table(mt; cargo="DEPUTADO FEDERAL")
    d = filter(r -> r.DS_CARGO == cargo, mt)
    nat = combine(groupby(d, :LISTA),
                  :votes => sum => :votes,
                  :seats => sum => :seats)
    Vtot = sum(nat.votes)
    N    = sum(nat.seats)
    nat.vote_share = nat.votes ./ Vtot
    nat.seat_share = nat.seats ./ N
    nat
end

nat_df = national_table(mt; cargo="DEPUTADO FEDERAL")
nat_df
