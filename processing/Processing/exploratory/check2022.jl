using Revise
import Processing as p

p.set_root!("../data/raw/electionsBR")



using CSV
using DataFrames



votes_2022 = p.national_party_valid_votes(2022)

seats_2022 = p.get_agg_party_seats(2022)



foo = p.party_summary(votes_2022, seats_2022)


using DataFrames

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

# ------------------------
# Base ministerial Lula 3
# ------------------------

# 2023–24: ministérios distribuídos entre
# PT, MDB, PSD, PSB, UNIÃO, PDT, PSOL, PCdoB, REDE
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

# 2025: PP e Republicanos ganham um ministério cada
const inst_2025 = union(inst_2024, Set(["PP", "REPUBLICANOS"]))

# ------------------------
# Base legislativa “estrita”
# (apenas partidos marcados como Governo na Câmara)
# ------------------------

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

# ------------------------
# DataFrame de bases Lula 3
# ------------------------

df_base_lula = DataFrame(
    SG_PARTIDO      = PARTIES_2022,
    base_inst_2023  = [p in inst_2023  for p in PARTIES_2022],
    base_inst_2024  = [p in inst_2024  for p in PARTIES_2022],
    base_inst_2025  = [p in inst_2025  for p in PARTIES_2022],
    base_vote_2023  = [p in vote_2023  for p in PARTIES_2022],
    base_vote_2024  = [p in vote_2024  for p in PARTIES_2022],
    base_vote_2025  = [p in vote_2025  for p in PARTIES_2022],
)

df_base_lula |> names







df_all = leftjoin(foo, df_base_lula, on = :SG_PARTIDO)


coal_inst_cols = String.([:base_inst_2023, :base_inst_2024, :base_inst_2025])
coal_vote_cols = String.([:base_vote_2023, :base_vote_2024, :base_vote_2025])



inst_results  = p.coalition_table(df_all, coal_inst_cols)
vote_results  = p.coalition_table(df_all, coal_vote_cols)
