#!/usr/bin/env Rscript

# ============================================================
# electionsBR extraction — RAW ONLY (no preprocessing)
# Goal: dump exactly what electionsBR returns per YEAR (UF = "all")
#       into a clean year-based folder tree. Any checks/joins go to a
#       separate script writing to data/derived/ later.
# ============================================================

# ---- Setup --------------------------------------------------
required_pkgs <- c("electionsBR", "readr")
missing <- required_pkgs[!vapply(required_pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) install.packages(missing, repos = "https://cloud.r-project.org")

invisible(lapply(required_pkgs, library, character.only = TRUE))
options(timeout = max(600, getOption("timeout")))

# ---- Parameters ---------------------------------------------
YEARS <- c(1998, 2002, 2006, 2010, 2014, 2018, 2022)
TYPES <- c(
  "legends",
  "party_mun_zone",
  "candidate",
  "seats",
  "vote_section",        # ← ESSENCIAL
  "vote_mun_zone"        # ← opcional, também funciona
)

RAW_DIR <- file.path("data", "raw", "electionsBR")
dir.create(RAW_DIR, recursive = TRUE, showWarnings = FALSE)

# NOTE: Derived (preprocessed) tables will live elsewhere (not used here)
DERIVED_DIR <- file.path("data", "derived", "inversion_prep")

# ---- Minimal helpers (I/O only) -----------------------------
write_out <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  readr::write_csv(df, path)
}

fetch_one <- function(type, year) {
  electionsBR::elections_tse(year = year, type = type, uf = "all", readme_pdf = TRUE)
}

# ---- Extraction (raw dumps) ---------------------------------
process_year <- function(year) {
  message("=========== YEAR ", year, " ===========")
  year_dir <- file.path(RAW_DIR, as.character(year))
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)

  for (tp in TYPES) {
    message("Downloading ", tp, " …")
    df <- try(fetch_one(tp, year), silent = TRUE)
    if (inherits(df, "try-error")) {
      message("[WARN] Failed to fetch type=", tp, " year=", year, ": ", attr(df, "condition")$message)
      next
    }
    if (is.null(df) || (is.data.frame(df) && nrow(df) == 0)) {
      message("[INFO] Empty table returned (", tp, ") — writing nothing.")
      next
    }
    out_path <- file.path(year_dir, paste0(tp, ".csv"))
    write_out(df, out_path)
    message("Saved → ", out_path)
  }
}

run_extraction <- function(years = YEARS) {
  for (yr in years) process_year(yr)
  message("
RAW dump complete at ", normalizePath(RAW_DIR, winslash = "/", mustWork = FALSE))
  message("(Any preprocessing belongs in a separate script writing to ", DERIVED_DIR, ")")
}

# ---- Run ---------------------------------------------------------------
run_extraction(c(2018))
