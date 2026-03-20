using Revise
import Processing

# Exemplo REPL: troque entre year=2023 e year=2025.
classification_year = 2025

df = Processing.load_party_classification(classification_year)
df = Processing.canonicalize_party_classification!(df; year = classification_year, strict = false)
df_min = Processing.classification_minimal(df)

first(df_min, 20)

audit_path = Processing.write_party_classification_audit(
    df;
    year = classification_year,
    out_dir = joinpath(@__DIR__, "out", "party_classification_2025_audit"),
)
println("Audit CSV: ", audit_path)
