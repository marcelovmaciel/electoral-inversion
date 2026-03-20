# Purpose: Read RAW CSVs from data/raw/electionsBR/<year>/ and produce
#          derived, inversion-ready tables in data/derived/inversion_prep/<year>/
#          (coalition/federation mapping, party & list aggregates for
#          votes and seats). No internet calls. Robust to schema drift.
# ======================================================================

# --------------------------
# Setup
# --------------------------
pre_required_pkgs <- c("dplyr", "readr", "stringr", "purrr", "tidyr")
missing <- pre_required_pkgs[!vapply(pre_required_pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) install.packages(missing, repos = "https://cloud.r-project.org")
invisible(lapply(pre_required_pkgs, library, character.only = TRUE))

RAW_DIR     <- file.path("data", "raw", "electionsBR")
DERIVED_DIR <- file.path("data", "derived", "inversion_prep")
dir.create(DERIVED_DIR, recursive = TRUE, showWarnings = FALSE)

CARGOS_OK <- toupper(c("DEPUTADO FEDERAL", "DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL"))

# --------------------------
# Helpers (schema-robust)
# --------------------------
col_or <- function(df, choices) {
  hit <- choices[choices %in% names(df)]
  if (length(hit)) hit[[1]] else NA_character_
}

read_if_exists <- function(path) {
  if (file.exists(path)) readr::read_csv(path, show_col_types = FALSE) else NULL
}

norm_uf <- function(df) {
  uf <- col_or(df, c("SG_UF", "UF"))
  if (!is.na(uf)) df[[uf]] <- as.character(df[[uf]])
  names(df)[names(df) == uf] <- "SG_UF"
  df
}

norm_cargo <- function(df) {
  cg <- col_or(df, c("DS_CARGO", "DESCRICAO_CARGO"))
  if (!is.na(cg)) df[[cg]] <- toupper(as.character(df[[cg]]))
  names(df)[names(df) == cg] <- "DS_CARGO"
  df
}

norm_party <- function(df) {
  p <- col_or(df, c("SG_PARTIDO", "SG_PART", "SG_LEGENDA"))
  if (!is.na(p)) df[[p]] <- as.character(df[[p]])
  names(df)[names(df) == p] <- "SG_PARTIDO"
  df
}

vote_col <- function(df) {
  hit <- c("QTDE_VOTOS", "QT_VOTOS", "VOTOS", "TOTAL_VOTOS")
  x <- hit[hit %in% names(df)]
  if (length(x)) return(x[[1]])
  rx <- grep("vot", names(df), ignore.case = TRUE, value = TRUE)
  if (length(rx)) rx[[1]] else NA_character_
}

winner_flag <- function(df) {
  w <- col_or(df, c("DS_SIT_TOT_TURNO", "DS_SIT_CAND_TOT", "DS_SITUACAO_CANDIDATO", "DS_SITUACAO"))
  if (is.na(w)) return(rep(NA, nrow(df)))
  stringr::str_detect(toupper(as.character(df[[w]])), "ELEIT")
}

seat_col <- function(df) {
  hit <- c("QT_VAGAS", "QTDE_VAGAS", "NR_VAGAS", "QT_VAGAS_DISPUTADAS", "QT_VAGAS_EM_DISPUTA", "QT_VAGAS_TOTAL")
  x <- hit[hit %in% names(df)]
  if (length(x)) return(x[[1]])
  rx <- grep("(vaga|vag)", names(df), ignore.case = TRUE, value = TRUE)
  if (length(rx)) rx[[1]] else NA_character_
}

list_label_col <- function(df) {
  # Prefer explicit coalition/federation/list name if present
  prefs <- c("NM_COLIGACAO", "NOME_COLIGACAO", "NM_FEDERACAO", "NOME_FEDERACAO", "NM_LEGENDA", "NOME_LEGENDA")
  x <- prefs[prefs %in% names(df)]
  if (length(x)) return(x[[1]])
  # Fallback: often 'NM_PARTIDO' can stand in for solo lists
  fb <- c("NM_PARTIDO")
  y <- fb[fb %in% names(df)]
  if (length(y)) return(y[[1]])
  NA_character_
}

# --------------------------
# Year processing
# --------------------------
process_year_pre <- function(year) {
  message("
===== PREPROCESS YEAR ", year, " =====")
  in_dir  <- file.path(RAW_DIR, as.character(year))
  out_dir <- file.path(DERIVED_DIR, as.character(year))
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  legends <- read_if_exists(file.path(in_dir, "legends.csv"))
  pmz     <- read_if_exists(file.path(in_dir, "party_mun_zone.csv"))
  cand    <- read_if_exists(file.path(in_dir, "candidate.csv"))
  seats   <- read_if_exists(file.path(in_dir, "seats.csv"))

  # ---- 1) Party votes aggregated to UF × party × cargo ----
  if (!is.null(pmz)) {
    pmz <- pmz |> norm_uf() |> norm_cargo() |> norm_party()
    vcol <- vote_col(pmz)
    if (!is.na(vcol)) {
      party_votes <- pmz |>
        dplyr::filter(DS_CARGO %in% CARGOS_OK) |>
        dplyr::group_by(SG_UF, SG_PARTIDO, DS_CARGO) |>
        dplyr::summarise(votes = sum(.data[[vcol]], na.rm = TRUE), .groups = "drop")
      readr::write_csv(party_votes, file.path(out_dir, "party_votes_uf.csv"))
    } else {
      message("[WARN] No vote column detected in party_mun_zone for year ", year)
    }
  }

  # ---- 2) Elected seats per UF × party × cargo -------------
  if (!is.null(cand)) {
    cand <- cand |> norm_uf() |> norm_cargo() |> norm_party()
    wf <- winner_flag(cand)
    cand$WINNER <- wf
    elected_by_party <- cand |>
      dplyr::filter(DS_CARGO %in% CARGOS_OK, WINNER %in% TRUE) |>
      dplyr::group_by(SG_UF, SG_PARTIDO, DS_CARGO) |>
      dplyr::summarise(seats = dplyr::n(), .groups = "drop")
    readr::write_csv(elected_by_party, file.path(out_dir, "elected_by_party_uf.csv"))
  }

  # ---- 3) Seat denominators UF × cargo ----------------------
  if (!is.null(seats)) {
    seats <- seats |> norm_uf() |> norm_cargo()
    scol <- seat_col(seats)
    if (!is.na(scol)) {
      seat_den <- seats |>
        dplyr::filter(DS_CARGO %in% CARGOS_OK) |>
        dplyr::group_by(SG_UF, DS_CARGO) |>
        dplyr::summarise(n_seats = sum(.data[[scol]], na.rm = TRUE), .groups = "drop")
      readr::write_csv(seat_den, file.path(out_dir, "seat_denominators_uf.csv"))
    } else {
      message("[WARN] No seat column detected in seats for year ", year)
    }
  }

  # ---- 4) List (coalition/federation/party) mapping ---------
  if (!is.null(legends)) {
    legends <- legends |> norm_uf() |> norm_party()
    lcol <- list_label_col(legends)
    if (!is.na(lcol)) {
      list_map <- legends |>
        dplyr::select(dplyr::any_of(c("SG_UF", "SG_PARTIDO", lcol))) |>
        dplyr::rename(LISTA = dplyr::all_of(lcol)) |>
        dplyr::mutate(LISTA = dplyr::if_else(is.na(LISTA) | LISTA == "", SG_PARTIDO, LISTA)) |>
        dplyr::distinct()
      readr::write_csv(list_map, file.path(out_dir, "list_map_uf.csv"))
    } else {
      message("[WARN] Could not detect a list label column in legends for year ", year, "; skipping list_map.")
    }
  }

  # ---- 5) Aggregate to list (votes & seats) -----------------
  # Requires party_votes and elected_by_party plus list_map
  pv_path  <- file.path(out_dir, "party_votes_uf.csv")
  ep_path  <- file.path(out_dir, "elected_by_party_uf.csv")
  lm_path  <- file.path(out_dir, "list_map_uf.csv")

  if (file.exists(pv_path) && file.exists(lm_path)) {
    party_votes <- readr::read_csv(pv_path, show_col_types = FALSE)
    list_map    <- readr::read_csv(lm_path, show_col_types = FALSE)
    list_votes <- party_votes |>
      dplyr::left_join(list_map, by = c("SG_UF", "SG_PARTIDO")) |>
      dplyr::mutate(LISTA = dplyr::coalesce(LISTA, SG_PARTIDO)) |>
      dplyr::group_by(SG_UF, DS_CARGO, LISTA) |>
      dplyr::summarise(votes = sum(votes, na.rm = TRUE), .groups = "drop")
    readr::write_csv(list_votes, file.path(out_dir, "list_votes_uf.csv"))
  }

  if (file.exists(ep_path) && file.exists(lm_path)) {
    elected_by_party <- readr::read_csv(ep_path, show_col_types = FALSE)
    list_map         <- readr::read_csv(lm_path, show_col_types = FALSE)
    list_seats <- elected_by_party |>
      dplyr::left_join(list_map, by = c("SG_UF", "SG_PARTIDO")) |>
      dplyr::mutate(LISTA = dplyr::coalesce(LISTA, SG_PARTIDO)) |>
      dplyr::group_by(SG_UF, DS_CARGO, LISTA) |>
      dplyr::summarise(seats = sum(seats, na.rm = TRUE), .groups = "drop")
    readr::write_csv(list_seats, file.path(out_dir, "list_seats_uf.csv"))
  }
}

run_preprocessing <- function(years = NULL) {
  years <- if (is.null(years)) {
    ys <- list.dirs(RAW_DIR, recursive = FALSE, full.names = FALSE)
    suppressWarnings(as.integer(ys[!is.na(as.integer(ys))]))
  } else years
  for (yr in years) process_year_pre(yr)
  message("
Derived tables written under ", normalizePath(DERIVED_DIR, winslash = "/", mustWork = FALSE))
}

run_preprocessing()

# To execute: source this script and call run_preprocessing()
