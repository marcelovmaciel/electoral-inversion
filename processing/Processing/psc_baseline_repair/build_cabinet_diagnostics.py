#!/usr/bin/env python3
"""Regenerate cabinet appointment-spell and party-set-change diagnostics."""

import csv
import json
from datetime import date, timedelta
from pathlib import Path

REPAIR_DIR = Path(__file__).resolve().parent
PROCESSING_ROOT = REPAIR_DIR.parent
REPO_ROOT = PROCESSING_ROOT.parent.parent
DASHBOARD_PATH = REPO_ROOT / "scraping" / "output" / "cabinet_timeline_dashboard.json"
PERIOD_PATH = REPO_ROOT / "scraping" / "output" / "partidos_por_periodo.json"
DIAGNOSTICS_DIR = PROCESSING_ROOT / "output" / "paper" / "diagnostics"
SPELL_OUTPUT = DIAGNOSTICS_DIR / "cabinet_period_source_spells.csv"
CHANGE_OUTPUT = DIAGNOSTICS_DIR / "cabinet_period_party_set_changes.csv"
FUSION_DATE = date(2022, 2, 8)


def parse_date(value):
    return date.fromisoformat(value) if value else None


def overlap(start_a, end_a, start_b, end_b):
    return max(start_a, start_b) <= min(end_a, end_b)


def party_at_date(party, current_date):
    if current_date >= FUSION_DATE and party in {"DEM", "PSL"}:
        return "UNIÃO"
    if party in {"Patriota", "Republicanos"}:
        return party.upper()
    return party


def event_label(kind, appointment, party):
    return ":".join(
        [
            kind,
            appointment["person"] or "",
            appointment["ministry"] or "",
            party or appointment["party"] or "",
        ]
    )


def main():
    dashboard = json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))
    raw_periods = json.loads(PERIOD_PATH.read_text(encoding="utf-8"))
    periods = [
        {
            "period": period,
            "start": parse_date(item["data_inicio"]),
            "end": parse_date(item["data_fim"]),
            "parties": set(item["partidos"]),
        }
        for period, item in raw_periods.items()
    ]
    periods.sort(key=lambda item: item["start"])

    appointments = []
    for item in dashboard["appointments"]:
        if not item["party_codes"] or not item["start"]:
            continue
        appointments.append(
            {
                **item,
                "_start": parse_date(item["start"]),
                "_end": parse_date(item["end"]) or parse_date(dashboard["source_as_of"]),
            }
        )

    spell_rows = []
    for period in periods:
        for appointment in appointments:
            if not overlap(
                period["start"],
                period["end"],
                appointment["_start"],
                appointment["_end"],
            ):
                continue
            effective_party = party_at_date(
                appointment["party_codes"][0],
                period["start"],
            )
            if effective_party not in period["parties"]:
                continue
            resolution = appointment["party_resolution"]
            notes = (
                f"resolution={resolution['method']}; "
                f"source_record_id={appointment['source_record_id']}"
            )
            if appointment.get("spell_evidence_url"):
                notes += f"; dated_evidence={appointment['spell_evidence_url']}"
            spell_rows.append(
                {
                    "period": period["period"],
                    "start_date": max(period["start"], appointment["_start"]).isoformat(),
                    "end_date": min(period["end"], appointment["_end"]).isoformat(),
                    "minister": appointment["person"],
                    "office": appointment["ministry"],
                    "raw_party": appointment["party"],
                    "canonical_party_at_date": effective_party,
                    "source_url_or_page": appointment["source_url"],
                    "notes": notes,
                }
            )
    spell_rows.sort(
        key=lambda row: (
            parse_date(row["start_date"]),
            row["period"],
            row["canonical_party_at_date"],
            row["office"],
            row["minister"],
        )
    )

    change_rows = []
    for previous, current in zip(periods, periods[1:]):
        change_date = current["start"]
        triggered = []
        for appointment in appointments:
            party = party_at_date(appointment["party_codes"][0], change_date)
            if appointment["_start"] == change_date:
                triggered.append(event_label("start", appointment, party))
            if appointment["_end"] + timedelta(days=1) == change_date:
                prior_party = party_at_date(
                    appointment["party_codes"][0],
                    change_date - timedelta(days=1),
                )
                triggered.append(event_label("end", appointment, prior_party))
        lineage = ""
        if change_date == FUSION_DATE:
            lineage = "DEM + PSL -> UNIÃO (effective 2022-02-08)"
        change_rows.append(
            {
                "old_period": previous["period"],
                "new_period": current["period"],
                "change_date": change_date.isoformat(),
                "entered_contemporaneous_parties": ", ".join(
                    sorted(current["parties"] - previous["parties"])
                ),
                "left_contemporaneous_parties": ", ".join(
                    sorted(previous["parties"] - current["parties"])
                ),
                "raw_minister_events": " | ".join(sorted(set(triggered))),
                "party_lineage_event_triggered": lineage,
            }
        )

    DIAGNOSTICS_DIR.mkdir(parents=True, exist_ok=True)
    with SPELL_OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(spell_rows[0]))
        writer.writeheader()
        writer.writerows(spell_rows)
    with CHANGE_OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(change_rows[0]))
        writer.writeheader()
        writer.writerows(change_rows)

    print(f"Wrote {len(spell_rows)} source-spell rows to {SPELL_OUTPUT}")
    print(f"Wrote {len(change_rows)} party-change rows to {CHANGE_OUTPUT}")


if __name__ == "__main__":
    main()
