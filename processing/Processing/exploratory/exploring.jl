using Revise
import Processing as p

# point to your root (optional if default fits)

p.set_root!("../data/raw/electionsBR")

p.years()






y = p.maximum(p.years())          # pick a year you want to inspect

paths = p.paths(y)

# open any table (no assumptions)
L = p.load_legends(y)

P = p.load_pmz(y)

C = p.load_candidate(y)

S = p.load_seats(y)

# quick peeks
p.head(L);

p.head(P);

p.head(C);

p.head(S)

# see columns / types
cols(L); types(L)
cols(P); findcols(P, r"(?i)vot|part|colig|feder")  # search by regex

# “hint” columns you might want to use later (no commitment)
p.vote_col(P)        # e.g., :QT_VOTOS_* or similar
seat_col(S)        # e.g., :QT_VAGA(S)
list_label_col(L)  # e.g., :NM_COLIGACAO / :NM_FEDERACAO / :NM_LEGENDA

# enumerate values you care about (you decide the grouping later)
p.uniquevals(P, :SG_UF)         # states present



p.uniquevals(P, :DS_CARGO)      # offices present (should include proportional ones)

p.uniquevals(P, :SG_PARTIDO)    # parties in year 'y'

p.uniquevals(L, :NM_COLIGACAO)  # coalition names if available

p.uniquevals(L, :NM_FEDERACAO)  # federation names if available

# you can always filter/select ad hoc
first(select(P, [:SG_UF, :DS_CARGO, :SG_PARTIDO, vote_col(P)]), 20)

# inspect seats table columns & values
seatcol = seat_col(S)
first(select(S, [:SG_UF, :DS_CARGO, seatcol]), 20)
