#!/usr/bin/env python3
"""Regenerate cabinet witness/change diagnostics from the paper's pinned release.

The historical location is retained for the existing runner. This module never
imports the former mixed reconstruction, reads a dashboard, or accesses network.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

PROCESSING_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROCESSING_ROOT.parents[1]
DEFAULT_PIN = PROCESSING_ROOT / "data/cabinet_release_pin.json"
DIAGNOSTICS_DIR = PROCESSING_ROOT / "output/paper/diagnostics"


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_diagnostics(pin_path=DEFAULT_PIN, output_dir=DIAGNOSTICS_DIR):
    pin_path, output_dir = Path(pin_path), Path(output_dir)
    pin = json.loads(pin_path.read_text(encoding="utf-8"))
    release = REPO_ROOT / pin["release_path"]
    if sha256(release / "metadata.json") != pin["metadata_sha256"]:
        raise ValueError("Cabinet release metadata hash mismatch")
    meta = json.loads((release / "metadata.json").read_text(encoding="utf-8"))
    if str(meta["schema_version"]) != "3" or meta["data_version"] != pin["data_version"]:
        raise ValueError("Cabinet release schema/version mismatch")
    if meta["cutoff_exclusive"] != "2026-03-20":
        raise ValueError("Cabinet release cutoff changed")
    for filename, digest in pin["file_hashes"].items():
        if sha256(release / filename) != digest:
            raise ValueError(f"Cabinet pinned hash mismatch: {filename}")
    required = {"periods.csv", "membership.csv", "witnesses.csv", "services.csv", "affiliations.csv"}
    if not required.issubset(pin["file_hashes"]):
        raise ValueError("Cabinet release pin omits diagnostic provenance inputs")
    periods = sorted(read_csv(release / "periods.csv"), key=lambda r: r["start_inclusive"])
    witnesses = read_csv(release / "witnesses.csv")
    services = {r["service_id"]: r for r in read_csv(release / "services.csv")}
    affiliations = {r["affiliation_id"]: r for r in read_csv(release / "affiliations.csv")}
    compositions = {r["period_id"]: r for r in periods}
    provenance = dict(data_version=pin["data_version"], release_metadata_sha256=pin["metadata_sha256"])
    spells = []
    for witness in witnesses:
        period = compositions[witness["period_id"]]
        service = services[witness["service_id"]]
        affiliation = affiliations[witness["affiliation_id"]]
        start = max(witness["start_inclusive"], period["start_inclusive"])
        end = min(witness["end_exclusive"], period["end_exclusive"])
        if start >= end:
            raise ValueError(f"Witness outside period: {witness['witness_id']}")
        spells.append(dict(period=witness["period_id"], start_date=start,
            end_date=(date.fromisoformat(end) - timedelta(days=1)).isoformat(),
            start_inclusive=start, end_exclusive=end, minister=service["person_name"],
            office=service["office_name"], person_id=witness["person_id"],
            canonical_party_at_date=witness["party_id"], affiliation_state=affiliation["state"],
            composition_status=period["composition_status"], service_id=witness["service_id"],
            affiliation_id=witness["affiliation_id"], witness_id=witness["witness_id"],
            evidence_ids=witness["evidence_ids"], decision_ids=witness["decision_ids"],
            notes="Dated contemporaneous affiliation witness; end_date is compatibility inclusive display.",
            **provenance))
    spells.sort(key=lambda r: (r["start_inclusive"], r["period"], r["canonical_party_at_date"], r["witness_id"]))
    changes = []
    for previous, current in zip(periods, periods[1:]):
        full = all("unidentified" not in r["composition_status"].lower() for r in (previous, current))
        old = set(filter(None, previous["party_ids"].split(";")))
        new = set(filter(None, current["party_ids"].split(";")))
        boundary = current["start_inclusive"]
        events = sorted({f"{kind}:{r['service_id']}:{r['person_name']}:{r['office_name']}"
                         for r in services.values() for kind, field in
                         (("service_start", "start_inclusive"), ("service_end", "end_exclusive"))
                         if r[field] == boundary})
        events += sorted({f"{kind}:{r['affiliation_id']}:{r['state']}:{r['party_id']}"
                          for r in affiliations.values() for kind, field in
                          (("affiliation_start", "start_inclusive"), ("affiliation_end", "end_exclusive"))
                          if r[field] == boundary})
        changes.append(dict(old_period=previous["period_id"], new_period=current["period_id"],
            change_date=boundary, entered_contemporaneous_parties=";".join(sorted(new-old)) if full else "",
            left_contemporaneous_parties=";".join(sorted(old-new)) if full else "",
            change_status="identified_set_difference" if full else "unavailable_at_unidentified_boundary",
            old_composition_status=previous["composition_status"], new_composition_status=current["composition_status"],
            raw_minister_events=" | ".join(events), decision_ids=current["decision_ids"], **provenance))
    spell_fields = ["period", "start_date", "end_date", "start_inclusive", "end_exclusive", "minister", "office",
                    "person_id", "canonical_party_at_date", "affiliation_state", "composition_status", "service_id",
                    "affiliation_id", "witness_id", "evidence_ids", "decision_ids", "notes", *provenance]
    change_fields = ["old_period", "new_period", "change_date", "entered_contemporaneous_parties",
                     "left_contemporaneous_parties", "change_status", "old_composition_status", "new_composition_status",
                     "raw_minister_events", "decision_ids", *provenance]
    write_csv(output_dir / "cabinet_period_source_spells.csv", spells, spell_fields)
    write_csv(output_dir / "cabinet_period_party_set_changes.csv", changes, change_fields)
    return {"source_spells": len(spells), "party_set_changes": len(changes), **provenance}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pin", type=Path, default=DEFAULT_PIN)
    parser.add_argument("--output-dir", type=Path, default=DIAGNOSTICS_DIR)
    args = parser.parse_args()
    print(json.dumps(build_diagnostics(args.pin, args.output_dir), indent=2))


if __name__ == "__main__":
    main()
