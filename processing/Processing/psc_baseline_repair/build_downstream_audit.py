#!/usr/bin/env python3
"""Build reproducible pre/post PSC cabinet and artifact comparisons."""

import csv
import hashlib
from datetime import date
from pathlib import Path

REPAIR_DIR = Path(__file__).resolve().parent
PROCESSING_ROOT = REPAIR_DIR.parent
OLD_PAPER = REPAIR_DIR / "pre_repair" / "paper"
NEW_PAPER = PROCESSING_ROOT / "output" / "paper"
OLD_METRICS = OLD_PAPER / "raw" / "cabinet_coalition_metrics.csv"
NEW_METRICS = NEW_PAPER / "raw" / "cabinet_coalition_metrics.csv"
DIFF_OUTPUT = REPAIR_DIR / "observed_cabinet_old_vs_new.csv"
HASH_OUTPUT = REPAIR_DIR / "paper_artifact_old_vs_new_hashes.csv"


def read_metrics(path):
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        row["_start"] = date.fromisoformat(row["period_start"])
        row["_end"] = date.fromisoformat(row["period_end"])
        row["_parties"] = {
            item.strip() for item in row["parties"].split(",") if item.strip()
        }
    return rows


def overlaps(left, right):
    return max(left["_start"], right["_start"]) <= min(left["_end"], right["_end"])


def changed_pair(old, new):
    keys = [
        "period",
        "period_start",
        "period_end",
        "parties",
        "votes",
        "vote_share",
        "seats",
        "quota",
        "seat_diff",
        "required_diff",
        "representation_ratio",
        "vote_majority",
        "seat_majority",
        "majority_status",
        "coalition_inversion",
        "period_days",
    ]
    return any(old[key] != new[key] for key in keys)


def pair_row(old, new):
    added = sorted(new["_parties"] - old["_parties"])
    removed = sorted(old["_parties"] - new["_parties"])
    metrics_changed = any(
        old[key] != new[key]
        for key in [
            "votes",
            "vote_share",
            "seats",
            "quota",
            "seat_diff",
            "required_diff",
            "representation_ratio",
            "vote_majority",
            "seat_majority",
            "majority_status",
            "coalition_inversion",
        ]
    )
    if added or removed:
        change_class = "membership_and_metrics" if metrics_changed else "membership_only"
    elif old["period_start"] != new["period_start"] or old["period_end"] != new["period_end"]:
        change_class = "boundary_only"
    else:
        change_class = "period_relabel_only"

    result = {
        "election_year": old["election_year"],
        "overlap_start": max(old["_start"], new["_start"]).isoformat(),
        "overlap_end": min(old["_end"], new["_end"]).isoformat(),
        "old_period": old["period"],
        "new_period": new["period"],
        "old_start": old["period_start"],
        "old_end": old["period_end"],
        "new_start": new["period_start"],
        "new_end": new["period_end"],
        "old_party_set": old["parties"],
        "new_party_set": new["parties"],
        "parties_added": ", ".join(added),
        "parties_removed": ", ".join(removed),
        "change_class": change_class,
    }
    mappings = [
        ("coalition_votes", "votes"),
        ("vote_share", "vote_share"),
        ("seats", "seats"),
        ("q_C", "quota"),
        ("d_C", "seat_diff"),
        ("r_C", "required_diff"),
        ("R_C", "representation_ratio"),
        ("vote_majority", "vote_majority"),
        ("seat_majority", "seat_majority"),
        ("majority_status", "majority_status"),
        ("inversion_status", "coalition_inversion"),
        ("duration_days", "period_days"),
        ("mandate_overlap_days", "days_overlapping_mandate"),
    ]
    for output_name, source_name in mappings:
        result[f"old_{output_name}"] = old[source_name]
        result[f"new_{output_name}"] = new[source_name]
    return result


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    old_rows = read_metrics(OLD_METRICS)
    new_rows = read_metrics(NEW_METRICS)
    pairs = []
    for old in old_rows:
        for new in new_rows:
            if old["election_year"] != new["election_year"] or not overlaps(old, new):
                continue
            if changed_pair(old, new):
                pairs.append(pair_row(old, new))
    pairs.sort(
        key=lambda row: (
            int(row["election_year"]),
            row["overlap_start"],
            row["old_period"],
            row["new_period"],
        )
    )
    with DIFF_OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(pairs[0]))
        writer.writeheader()
        writer.writerows(pairs)

    old_files = {
        path.relative_to(OLD_PAPER): path
        for path in OLD_PAPER.rglob("*")
        if path.is_file()
    }
    new_files = {
        path.relative_to(NEW_PAPER): path
        for path in NEW_PAPER.rglob("*")
        if path.is_file()
    }
    hash_rows = []
    for relative in sorted(set(old_files) | set(new_files)):
        old_path = old_files.get(relative)
        new_path = new_files.get(relative)
        old_hash = sha256(old_path) if old_path else ""
        new_hash = sha256(new_path) if new_path else ""
        status = (
            "unchanged"
            if old_hash and old_hash == new_hash
            else "changed"
            if old_hash and new_hash
            else "added"
            if new_hash
            else "removed"
        )
        hash_rows.append(
            {
                "artifact": str(relative),
                "old_sha256": old_hash,
                "new_sha256": new_hash,
                "status": status,
            }
        )
    with HASH_OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(hash_rows[0]))
        writer.writeheader()
        writer.writerows(hash_rows)

    print(f"Wrote {len(pairs)} affected old/new overlap rows to {DIFF_OUTPUT}")
    print(
        "Artifact comparison:",
        {status: sum(row["status"] == status for row in hash_rows) for status in sorted({row["status"] for row in hash_rows})},
    )


if __name__ == "__main__":
    main()
