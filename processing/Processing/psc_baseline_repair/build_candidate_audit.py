#!/usr/bin/env python3
"""Reproduce the 2018 federal-deputy candidate-level PSC seat audit."""

import csv
import re
import unicodedata
from collections import Counter
from pathlib import Path

REPAIR_DIR = Path(__file__).resolve().parent
PROCESSING_ROOT = REPAIR_DIR.parent
REPO_ROOT = PROCESSING_ROOT.parent.parent
CANDIDATE_PATH = (
    PROCESSING_ROOT / "data" / "raw" / "electionsBR" / "2018" / "candidate.csv"
)
ALIASES_PATH = PROCESSING_ROOT / "data" / "party_aliases.csv"
OUTPUT_PATH = REPAIR_DIR / "psc_2018_elected_candidates.csv"
DISCREPANCY_PATH = REPAIR_DIR / "psc_2018_discrepancy_rows.csv"

WINNER_STATUSES = {
    "ELEITO",
    "ELEITO POR QP",
    "ELEITO POR MEDIA",
    "ELEITO POR MÉDIA",
}
AUDIT_COLUMNS = [
    "candidate_name",
    "ballot_name",
    "candidate_id",
    "uf",
    "raw_party",
    "canonical_party",
    "DS_CARGO",
    "DS_SITUACAO_CANDIDATURA",
    "CD_SITUACAO_CANDIDATURA",
    "DS_SIT_TOT_TURNO",
    "CD_SIT_TOT_TURNO",
    "current_loader_counts",
    "raw_party_seat_count",
    "canonical_party_seat_count",
    "psc_current_winner",
]


def normalize_party(value):
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9]+", " ", value.upper()).strip()


def load_aliases():
    aliases = {}
    with ALIASES_PATH.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            start = int(row["valid_from_year"]) if row["valid_from_year"] else None
            end = int(row["valid_to_year"]) if row["valid_to_year"] else None
            aliases.setdefault(row["alias_norm"], []).append(
                (row["canonical"], start, end)
            )
    return aliases


def canonical_party(raw_party, year, aliases):
    key = normalize_party(raw_party)
    candidates = {
        canonical
        for canonical, start, end in aliases.get(key, [])
        if (start is None or start <= year) and (end is None or year <= end)
    }
    if len(candidates) != 1:
        raise ValueError(
            f"Expected one canonical party for raw={raw_party!r}, year={year}; "
            f"found {sorted(candidates)}"
        )
    return next(iter(candidates))


def audit_row(raw, aliases):
    status = raw["DS_SIT_TOT_TURNO"].strip().upper()
    counted = raw["DS_CARGO"].strip().upper() == "DEPUTADO FEDERAL" and (
        status in WINNER_STATUSES
    )
    return {
        "candidate_name": raw["NM_CANDIDATO"].strip(),
        "ballot_name": raw["NM_URNA_CANDIDATO"].strip(),
        "candidate_id": raw["SQ_CANDIDATO"].strip(),
        "uf": raw["SG_UF"].strip(),
        "raw_party": raw["SG_PARTIDO"].strip(),
        "canonical_party": canonical_party(raw["SG_PARTIDO"], 2018, aliases),
        "DS_CARGO": raw["DS_CARGO"].strip(),
        "DS_SITUACAO_CANDIDATURA": raw["DS_SITUACAO_CANDIDATURA"].strip(),
        "CD_SITUACAO_CANDIDATURA": raw["CD_SITUACAO_CANDIDATURA"].strip(),
        "DS_SIT_TOT_TURNO": raw["DS_SIT_TOT_TURNO"].strip(),
        "CD_SIT_TOT_TURNO": raw["CD_SIT_TOT_TURNO"].strip(),
        "current_loader_counts": str(counted).lower(),
        "raw_party_seat_count": "",
        "canonical_party_seat_count": "",
        "psc_current_winner": "",
    }


def main():
    aliases = load_aliases()
    winners = []
    discrepancy = []
    discrepancy_ids = {"260000621977", "260000623622"}
    with CANDIDATE_PATH.open(encoding="utf-8", newline="") as handle:
        for raw in csv.DictReader(handle):
            if raw["DS_CARGO"].strip().upper() != "DEPUTADO FEDERAL":
                continue
            row = audit_row(raw, aliases)
            if row["current_loader_counts"] == "true":
                winners.append(row)
            if row["candidate_id"] in discrepancy_ids:
                discrepancy.append(row)

    if len(winners) != 513:
        raise ValueError(f"Expected 513 elected rows, found {len(winners)}")
    if len({row["candidate_id"] for row in winners}) != 513:
        raise ValueError("A counted candidate identifier appears more than once")

    raw_counts = Counter(row["raw_party"] for row in winners)
    canonical_counts = Counter(row["canonical_party"] for row in winners)
    if sum(raw_counts.values()) != 513 or sum(canonical_counts.values()) != 513:
        raise ValueError("Raw/canonical seat aggregations do not both sum to 513")
    if raw_counts["PSC"] != 7 or canonical_counts["PSC"] != 7:
        raise ValueError("Adjudicated PSC seat count regression failed")

    for row in winners:
        row["raw_party_seat_count"] = raw_counts[row["raw_party"]]
        row["canonical_party_seat_count"] = canonical_counts[
            row["canonical_party"]
        ]
        row["psc_current_winner"] = str(row["canonical_party"] == "PSC").lower()

    winners.sort(key=lambda row: (row["canonical_party"], row["uf"], row["candidate_id"]))
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_COLUMNS)
        writer.writeheader()
        writer.writerows(winners)

    discrepancy.sort(key=lambda row: row["candidate_id"])
    with DISCREPANCY_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_COLUMNS)
        writer.writeheader()
        writer.writerows(discrepancy)

    print(f"Wrote {len(winners)} elected rows to {OUTPUT_PATH}")
    print(f"Raw seats: 513; canonical seats: 513; PSC seats: {canonical_counts['PSC']}")


if __name__ == "__main__":
    main()
