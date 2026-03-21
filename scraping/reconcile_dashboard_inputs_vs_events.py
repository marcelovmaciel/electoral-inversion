#!/usr/bin/env python3
"""
Focused reconciliation between the old dashboard-input JSON files and the
event-level cabinet timeline used by the dashboard.

This script is detection-only. It does not modify the legacy inputs.
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "scraping" / "output"

DASHBOARD_JSON_PATH = OUTPUT_DIR / "cabinet_timeline_dashboard.json"
RAW_EVENTS_PATH = OUTPUT_DIR / "ministerios_eventos.json"
ORGAOS_PATH = OUTPUT_DIR / "orgaos_ministeriais.json"
PARTY_PERIODS_PATH = OUTPUT_DIR / "partidos_por_periodo.json"

ISSUES_JSON_PATH = OUTPUT_DIR / "dashboard_input_vs_event_issues.json"
ISSUES_MD_PATH = OUTPUT_DIR / "dashboard_input_vs_event_issues_readable.md"

# These are intentionally explicit so the reconciliation does not depend on a
# larger framework or hidden date logic elsewhere in the repo.
GOVERNMENT_WINDOWS = [
    {"government_id": "dilma_1", "start": date(2011, 1, 1), "end": date(2014, 12, 31)},
    {"government_id": "dilma_2", "start": date(2015, 1, 1), "end": date(2016, 5, 11)},
    {"government_id": "temer", "start": date(2016, 5, 12), "end": date(2018, 12, 31)},
    {"government_id": "bolsonaro", "start": date(2019, 1, 1), "end": date(2022, 12, 31)},
    {"government_id": "lula_3", "start": date(2023, 1, 1), "end": None},
]

PAGE_WINDOWS = {
    "dilma_rousseff": (date(2011, 1, 1), date(2016, 5, 11)),
    "michel_temer": (date(2016, 5, 12), date(2018, 12, 31)),
    "jair_bolsonaro": (date(2019, 1, 1), date(2022, 12, 31)),
    "lula_2023_presente": (date(2023, 1, 1), None),
}

NEAR_BOUNDARY_DAYS = 14
SUBSTANTIVE_INTERIM_DAYS = 7
STRUCTURAL_UNKNOWN_TERMS = ("criada", "criado", "extinta", "extinto", "extin", "estrutural")
CONTESTED_UNKNOWN_TERMS = (
    "suspensa",
    "suspenso",
    "não assumiu",
    "nao assumiu",
    "judicial",
    "contestada",
    "contestado",
)

ISSUE_ORDER = [
    "interim_case_missed",
    "party_ambiguity_hidden",
    "coalition_period_too_coarse",
    "ministry_continuity_problem",
    "date_boundary_problem",
    "uncertainty_lost",
]

SECTION_TITLES = {
    "interim_case_missed": "Interim / Assumption Cases Missed",
    "party_ambiguity_hidden": "Party Ambiguity Hidden By Old Data",
    "coalition_period_too_coarse": "Coarse Coalition Periods Hiding Turnover",
    "ministry_continuity_problem": "Ministry Naming / Continuity Issues",
    "date_boundary_problem": "Date Boundary Issues",
    "uncertainty_lost": "Loss Of Uncertainty In Old Data",
}


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def iso_or_none(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def strip_parenthetical_notes(label: str | None) -> str:
    return normalize_space(re.sub(r"\s*\([^)]*\)", "", label or ""))


def normalize_for_match(label: str | None) -> str:
    stripped = strip_parenthetical_notes(label)
    stripped = unicodedata.normalize("NFKD", stripped).encode("ascii", "ignore").decode("ascii")
    stripped = stripped.lower()
    stripped = re.sub(r"[^a-z0-9]+", " ", stripped)
    return normalize_space(stripped)


def government_for_date(value: date | None) -> str | None:
    if value is None:
        return None
    for window in GOVERNMENT_WINDOWS:
        if value < window["start"]:
            continue
        end = window["end"]
        if end is None or value <= end:
            return window["government_id"]
    return None


def duration_days(start: date | None, end: date | None) -> int | None:
    if start is None or end is None:
        return None
    return (end - start).days + 1


def build_old_periods(raw_periods: dict) -> list[dict]:
    periods = []
    for period_id, info in sorted(raw_periods.items()):
        start = parse_iso_date(info["data_inicio"])
        end = parse_iso_date(info["data_fim"])
        periods.append(
            {
                "period_id": period_id,
                "start": start,
                "end": end,
                "parties": list(info.get("partidos", [])),
                "government_id": government_for_date(start),
            }
        )
    return periods


def appointment_page_label(appointment: dict) -> str:
    return appointment["appointment_id"].split(":", 1)[0]


def appointment_start(appointment: dict) -> date | None:
    return parse_iso_date(appointment.get("actual_start") or appointment.get("start"))


def appointment_end(appointment: dict) -> date | None:
    return parse_iso_date(appointment.get("actual_end") or appointment.get("end"))


def date_overlap(start_a: date | None, end_a: date | None, start_b: date, end_b: date) -> bool:
    if start_a is None and end_a is None:
        return False
    effective_start = start_a or end_a
    effective_end = end_a or start_a
    return effective_start <= end_b and start_b <= effective_end


def overlapping_periods(periods: list[dict], government_id: str, start: date | None, end: date | None) -> list[dict]:
    return [
        period
        for period in periods
        if period["government_id"] == government_id and date_overlap(start, end, period["start"], period["end"])
    ]


def covering_period(periods: list[dict], government_id: str, value: date | None) -> dict | None:
    if value is None:
        return None
    for period in periods:
        if period["government_id"] != government_id:
            continue
        if period["start"] <= value <= period["end"]:
            return period
    return None


def nearest_boundary(periods: list[dict], government_id: str, value: date | None) -> tuple[date | None, int | None]:
    if value is None:
        return None, None
    boundaries = []
    for period in periods:
        if period["government_id"] != government_id:
            continue
        boundaries.append(period["start"])
        boundaries.append(period["end"])
    if not boundaries:
        return None, None
    boundary = min(boundaries, key=lambda item: abs((item - value).days))
    return boundary, abs((boundary - value).days)


def matched_old_ministry(old_labels: list[str], event_label: str) -> str | None:
    target = normalize_for_match(event_label)
    for label in old_labels:
        if normalize_for_match(label) == target:
            return label
    return None


def format_party_list(parties: list[str]) -> str:
    if not parties:
        return "[]"
    return "[" + ", ".join(parties) + "]"


def describe_periods(periods: list[dict]) -> str:
    if not periods:
        return "no overlapping `partidos_por_periodo.json` coverage"
    rendered = []
    for period in periods:
        rendered.append(
            f"{period['period_id']} ({period['start'].isoformat()} to {period['end'].isoformat()}; parties={format_party_list(period['parties'])})"
        )
    return "; ".join(rendered)


def source_page_title_to_label(dashboard_payload: dict) -> dict[str, str]:
    return {item["title"]: item["label"] for item in dashboard_payload.get("source_pages", [])}


def build_event_lookup(events: list[dict]) -> dict[tuple, list[dict]]:
    lookup = defaultdict(list)
    for event in events:
        person = event.get("person_name_canonical") or event.get("person_name_raw")
        key = (
            event.get("government_id"),
            event.get("ministerio_canonical"),
            event.get("event_date_start"),
            person,
        )
        lookup[key].append(event)
    return lookup


def find_supporting_event(appointment: dict, event_lookup: dict[tuple, list[dict]]) -> dict | None:
    key = (
        appointment.get("government_id"),
        appointment.get("ministry"),
        appointment.get("start"),
        appointment.get("person"),
    )
    candidates = event_lookup.get(key, [])
    if not candidates:
        return None
    preferred = {"assumption": 0, "entry": 1, "return": 2}
    return sorted(candidates, key=lambda item: preferred.get(item.get("event_type"), 9))[0]


def is_structural_event(event: dict) -> bool:
    if event.get("event_type") in {"merge", "rename", "split"}:
        return True
    if event.get("event_type") != "unknown":
        return False
    notes = (event.get("notes") or "").lower()
    return any(term in notes for term in STRUCTURAL_UNKNOWN_TERMS)


def is_uncertain_event(event: dict) -> bool:
    notes = (event.get("notes") or "").lower()
    if event.get("event_date_start") is None:
        return True
    if "múltiplas datas" in notes or "multiplas datas" in notes:
        return True
    if event.get("event_type") == "unknown":
        return any(term in notes for term in CONTESTED_UNKNOWN_TERMS) or bool(notes)
    return any(term in notes for term in CONTESTED_UNKNOWN_TERMS)


def short_event_reference(event: dict | None, appointment: dict | None = None) -> str:
    if event is not None:
        date_value = event.get("event_date_start") or event.get("event_date_display") or "unknown date"
        event_type = event.get("event_type") or "unknown"
        person = event.get("person_name_canonical") or event.get("person_name_raw") or "no person"
        snippet = normalize_space(event.get("source_snippet"))
        return f"{date_value} | {event_type} | {person} | {snippet}"
    if appointment is not None:
        return (
            f"{appointment.get('start') or 'unknown'} to {appointment.get('end') or 'unknown'} | "
            f"{appointment.get('appointment_type')} | {appointment.get('person') or 'no person'} | "
            f"{normalize_space(appointment.get('source_snippet'))}"
        )
    return "no event evidence available"


def severity_counts(issues: list[dict]) -> dict[str, int]:
    return dict(Counter(issue["severity"] for issue in issues))


def type_counts(issues: list[dict]) -> dict[str, int]:
    return dict(Counter(issue["issue_type"] for issue in issues))


def build_issue_key(issue: dict) -> tuple:
    snippet = issue.get("evidence", {}).get("source_snippet")
    return (
        issue["issue_type"],
        issue["severity"],
        issue.get("government_id"),
        issue.get("ministry_canonical_event"),
        snippet or issue.get("event_interval_start"),
        None if snippet else issue.get("event_interval_end"),
        issue.get("party_event"),
    )


def add_issue(target: list[dict], seen: set[tuple], issue: dict) -> None:
    key = build_issue_key(issue)
    if key in seen:
        return
    seen.add(key)
    target.append(issue)


def make_issue(
    *,
    issue_type: str,
    severity: str,
    government_id: str | None,
    ministry_canonical_event: str,
    ministry_raw_event: str | None,
    ministry_old: str | None,
    party_old: str | None,
    party_event: str | None,
    old_period: dict | None,
    event_start: date | None,
    event_end: date | None,
    reason: str,
    old_coarse_representation: str,
    event_source_field: str,
    old_source_field: str,
    source_snippet: str | None,
    source_url: str | None = None,
    notes: str | None = None,
    page_label: str | None = None,
    needs_review: bool = True,
    extra_evidence: dict | None = None,
) -> dict:
    evidence = {
        "old_source_field": old_source_field,
        "event_source_field": event_source_field,
        "source_snippet": source_snippet,
    }
    if source_url:
        evidence["source_url"] = source_url
    if notes:
        evidence["notes"] = notes
    if page_label:
        evidence["old_page_label"] = page_label
    if extra_evidence:
        evidence.update(extra_evidence)
    return {
        "issue_type": issue_type,
        "severity": severity,
        "government_id": government_id,
        "ministry_raw_event": ministry_raw_event,
        "ministry_canonical_event": ministry_canonical_event,
        "ministry_old": ministry_old,
        "party_old": party_old,
        "party_event": party_event,
        "old_period_id": old_period["period_id"] if old_period else None,
        "old_period_start": iso_or_none(old_period["start"]) if old_period else None,
        "old_period_end": iso_or_none(old_period["end"]) if old_period else None,
        "event_interval_start": iso_or_none(event_start),
        "event_interval_end": iso_or_none(event_end),
        "reason": reason,
        "old_coarse_representation": old_coarse_representation,
        "evidence": evidence,
        "needs_review": needs_review,
    }


def sorted_grouped_appointments(appointments: list[dict]) -> dict[tuple[str, str], list[dict]]:
    grouped = defaultdict(list)
    for appointment in appointments:
        grouped[(appointment["government_id"], appointment["ministry"])].append(appointment)
    for group in grouped.values():
        group.sort(
            key=lambda item: (
                item.get("_start") or date.max,
                item.get("_end") or date.max,
                item.get("appointment_id"),
            )
        )
    return grouped


def compare_ministry_sets(
    issues: list[dict],
    seen: set[tuple],
    old_orgaos: dict,
    dashboard_payload: dict,
) -> None:
    label_to_event_ministries = defaultdict(set)
    title_to_label = source_page_title_to_label(dashboard_payload)
    for event in dashboard_payload.get("events", []):
        page_label = title_to_label.get(event.get("source_page"))
        if page_label:
            label_to_event_ministries[page_label].add(event.get("ministerio_canonical"))

    for page_label, payload in old_orgaos.items():
        old_labels = payload.get("orgaos", [])
        old_lookup = {normalize_for_match(label): label for label in old_labels}
        event_lookup = {normalize_for_match(label): label for label in label_to_event_ministries.get(page_label, set())}
        for key in sorted(old_lookup.keys() - event_lookup.keys()):
            add_issue(
                issues,
                seen,
                make_issue(
                    issue_type="ministry_continuity_problem",
                    severity="likely_ambiguity",
                    government_id=government_for_date(PAGE_WINDOWS[page_label][0]),
                    ministry_canonical_event=old_lookup[key],
                    ministry_raw_event=old_lookup[key],
                    ministry_old=old_lookup[key],
                    party_old=None,
                    party_event=None,
                    old_period=None,
                    event_start=PAGE_WINDOWS[page_label][0],
                    event_end=PAGE_WINDOWS[page_label][1],
                    reason="Old ministry list has a label with no normalized event-level match. This may be annotation drift or a continuity/canonicalization problem.",
                    old_coarse_representation=f"`orgaos_ministeriais.json[{page_label}]` includes `{old_lookup[key]}` as a static ministry label.",
                    event_source_field="cabinet_timeline_dashboard.json:events[].ministerio_canonical",
                    old_source_field=f"orgaos_ministeriais.json[{page_label}].orgaos[]",
                    source_snippet=None,
                    page_label=page_label,
                ),
            )
        for key in sorted(event_lookup.keys() - old_lookup.keys()):
            add_issue(
                issues,
                seen,
                make_issue(
                    issue_type="ministry_continuity_problem",
                    severity="likely_ambiguity",
                    government_id=government_for_date(PAGE_WINDOWS[page_label][0]),
                    ministry_canonical_event=event_lookup[key],
                    ministry_raw_event=event_lookup[key],
                    ministry_old=None,
                    party_old=None,
                    party_event=None,
                    old_period=None,
                    event_start=PAGE_WINDOWS[page_label][0],
                    event_end=PAGE_WINDOWS[page_label][1],
                    reason="Event timeline has a ministry label with no normalized match in the old ministry list. This suggests old canonicalization may be too coarse.",
                    old_coarse_representation=f"`orgaos_ministeriais.json[{page_label}]` does not contain a normalized match for `{event_lookup[key]}`.",
                    event_source_field="cabinet_timeline_dashboard.json:events[].ministerio_canonical",
                    old_source_field=f"orgaos_ministeriais.json[{page_label}].orgaos[]",
                    source_snippet=None,
                    page_label=page_label,
                ),
            )


def compare_interim_cases(
    issues: list[dict],
    seen: set[tuple],
    grouped_appointments: dict[tuple[str, str], list[dict]],
    old_periods: list[dict],
    old_orgaos: dict,
    event_lookup: dict[tuple, list[dict]],
) -> None:
    for (government_id, ministry), appointments in grouped_appointments.items():
        for index, appointment in enumerate(appointments):
            if appointment.get("appointment_type") != "interim":
                continue
            start = appointment["_start"]
            end = appointment["_end"]
            days = duration_days(start, end)
            if days is not None and days < SUBSTANTIVE_INTERIM_DAYS and appointment.get("party_codes"):
                continue

            page_label = appointment["_page_label"]
            old_labels = old_orgaos.get(page_label, {}).get("orgaos", [])
            old_ministry = matched_old_ministry(old_labels, ministry)
            overlapping = overlapping_periods(old_periods, government_id, start, end)
            supporting_event = find_supporting_event(appointment, event_lookup)
            previous_party = next(
                (
                    item.get("party")
                    for item in reversed(appointments[:index])
                    if item.get("appointment_type") == "permanent"
                ),
                None,
            )
            next_party = next(
                (
                    item.get("party")
                    for item in appointments[index + 1 :]
                    if item.get("appointment_type") == "permanent"
                ),
                None,
            )
            boundary, distance = nearest_boundary(old_periods, government_id, start)
            missing_party = not appointment.get("party_codes")
            near_boundary = (
                distance is not None
                and 0 < distance <= NEAR_BOUNDARY_DAYS
                and (days is None or days <= 60)
            )

            if (
                missing_party
                and days is not None
                and days < SUBSTANTIVE_INTERIM_DAYS
                and previous_party
                and next_party
                and previous_party == next_party
            ):
                continue

            if near_boundary:
                issue_type = "date_boundary_problem"
            elif missing_party:
                issue_type = "party_ambiguity_hidden"
            else:
                issue_type = "interim_case_missed"

            severity = "likely_ambiguity" if missing_party else "likely_over_coarsening"
            if not overlapping and government_id != "dilma_1":
                severity = "probable_mistake"
                issue_type = "date_boundary_problem"

            old_repr = (
                f"`orgaos_ministeriais.json[{page_label}]` keeps `{old_ministry or ministry}` as a static ministry label; "
                f"`partidos_por_periodo.json` overlap: {describe_periods(overlapping)}."
            )

            reason = (
                f"Event timeline records {appointment.get('person') or 'an interim officeholder'} as temporary control of `{ministry}`"
                f" from {appointment.get('start') or 'unknown'} to {appointment.get('end') or 'unknown'}"
            )
            if days is not None:
                reason += f" ({days} days)"
            reason += ". "
            if missing_party:
                reason += "The temporary holder has no usable party code, so the old coalition-period view preserves more certainty than the event layer supports."
            else:
                reason += "The old inputs have no place to represent the temporary holder as a distinct interval."
            if previous_party or next_party:
                reason += f" Adjacent permanent parties: previous={previous_party or 'unknown'}, next={next_party or 'unknown'}."
            if near_boundary and boundary is not None:
                reason += f" The change falls {distance} day(s) from old coalition boundary {boundary.isoformat()}."
            if not overlapping and government_id != "dilma_1":
                reason += " The old coalition-period file has no coverage for this interval."

            add_issue(
                issues,
                seen,
                make_issue(
                    issue_type=issue_type,
                    severity=severity,
                    government_id=government_id,
                    ministry_canonical_event=ministry,
                    ministry_raw_event=appointment.get("ministry_raw"),
                    ministry_old=old_ministry,
                    party_old=describe_periods(overlapping),
                    party_event=appointment.get("party"),
                    old_period=overlapping[0] if overlapping else None,
                    event_start=start,
                    event_end=end,
                    reason=reason,
                    old_coarse_representation=old_repr,
                    event_source_field="cabinet_timeline_dashboard.json:appointments[]",
                    old_source_field=f"orgaos_ministeriais.json[{page_label}].orgaos[] + partidos_por_periodo.json",
                    source_snippet=(supporting_event or {}).get("source_snippet") or appointment.get("source_snippet"),
                    source_url=(supporting_event or {}).get("source_url") or appointment.get("source_url"),
                    notes=(supporting_event or {}).get("notes") or appointment.get("notes"),
                    page_label=page_label,
                    extra_evidence={
                        "supporting_event_type": (supporting_event or {}).get("event_type"),
                        "adjacent_previous_party": previous_party,
                        "adjacent_next_party": next_party,
                        "nearest_old_boundary": boundary.isoformat() if boundary else None,
                        "nearest_old_boundary_distance_days": distance,
                    },
                ),
            )


def compare_party_turnover(
    issues: list[dict],
    seen: set[tuple],
    grouped_appointments: dict[tuple[str, str], list[dict]],
    old_periods: list[dict],
    old_orgaos: dict,
) -> None:
    for (government_id, ministry), appointments in grouped_appointments.items():
        for previous, current in zip(appointments, appointments[1:]):
            if previous.get("appointment_type") == "interim" or current.get("appointment_type") == "interim":
                continue
            change_date = current["_start"]
            if change_date is None:
                continue

            previous_period = covering_period(old_periods, government_id, previous["_end"])
            current_period = covering_period(old_periods, government_id, change_date)
            if current_period is None or previous_period is None:
                continue
            if previous_period["period_id"] != current_period["period_id"]:
                continue

            previous_codes = set(previous.get("party_codes") or [])
            current_codes = set(current.get("party_codes") or [])
            if previous_codes == current_codes:
                continue

            boundary, distance = nearest_boundary(old_periods, government_id, change_date)
            if distance == 0:
                continue

            page_label = previous["_page_label"]
            old_labels = old_orgaos.get(page_label, {}).get("orgaos", [])
            old_ministry = matched_old_ministry(old_labels, ministry)
            near_boundary = distance is not None and distance <= NEAR_BOUNDARY_DAYS

            if previous_codes and current_codes:
                issue_type = "date_boundary_problem" if near_boundary else "coalition_period_too_coarse"
                severity = "likely_over_coarsening"
                reason = (
                    f"`partidos_por_periodo.json[{current_period['period_id']}]` treats the coalition as stable from "
                    f"{current_period['start'].isoformat()} to {current_period['end'].isoformat()}, but the event timeline shows "
                    f"`{ministry}` moving from {previous.get('party') or 'unknown party'} to {current.get('party') or 'unknown party'} on "
                    f"{change_date.isoformat()}."
                )
                if near_boundary and boundary is not None:
                    reason += f" The handoff is {distance} day(s) away from old boundary {boundary.isoformat()}, which suggests the period cut may be too coarse."
            else:
                missing_side = previous if not previous_codes else current
                missing_duration = duration_days(missing_side["_start"], missing_side["_end"]) or 0
                if not near_boundary and missing_side.get("appointment_type") != "interim" and missing_duration < 14:
                    continue
                issue_type = "date_boundary_problem" if near_boundary else "party_ambiguity_hidden"
                severity = "likely_ambiguity"
                reason = (
                    f"The event timeline changes control of `{ministry}` on {change_date.isoformat()} inside "
                    f"`partidos_por_periodo.json[{current_period['period_id']}]`, but one side of the handoff has no usable party code "
                    f"({previous.get('party') or 'unknown'} -> {current.get('party') or 'unknown'})."
                )
                if near_boundary and boundary is not None:
                    reason += f" The handoff is {distance} day(s) away from old boundary {boundary.isoformat()}."

            add_issue(
                issues,
                seen,
                make_issue(
                    issue_type=issue_type,
                    severity=severity,
                    government_id=government_id,
                    ministry_canonical_event=ministry,
                    ministry_raw_event=current.get("ministry_raw"),
                    ministry_old=old_ministry,
                    party_old=f"{current_period['period_id']} parties={format_party_list(current_period['parties'])}",
                    party_event=f"{previous.get('party') or 'unknown'} -> {current.get('party') or 'unknown'}",
                    old_period=current_period,
                    event_start=change_date,
                    event_end=current["_end"],
                    reason=reason,
                    old_coarse_representation=(
                        f"`partidos_por_periodo.json[{current_period['period_id']}]` is one stable interval; "
                        f"`orgaos_ministeriais.json[{page_label}]` keeps `{old_ministry or ministry}` as one static ministry entry."
                    ),
                    event_source_field="cabinet_timeline_dashboard.json:appointments[]",
                    old_source_field=f"partidos_por_periodo.json[{current_period['period_id']}]",
                    source_snippet=current.get("source_snippet"),
                    source_url=current.get("source_url"),
                    notes=current.get("notes"),
                    page_label=page_label,
                    extra_evidence={
                        "previous_appointment": short_event_reference(appointment=previous, event=None),
                        "current_appointment": short_event_reference(appointment=current, event=None),
                        "nearest_old_boundary": boundary.isoformat() if boundary else None,
                        "nearest_old_boundary_distance_days": distance,
                    },
                ),
            )


def compare_structural_events(
    issues: list[dict],
    seen: set[tuple],
    events: list[dict],
    old_orgaos: dict,
    page_lookup: dict[str, str],
) -> None:
    for event in events:
        if not is_structural_event(event):
            continue
        page_label = page_lookup.get(event.get("source_page"))
        old_labels = old_orgaos.get(page_label, {}).get("orgaos", []) if page_label else []
        old_ministry = matched_old_ministry(old_labels, event.get("ministerio_canonical"))

        if event.get("event_type") in {"merge", "rename", "split"}:
            severity = "likely_over_coarsening"
        else:
            severity = "likely_ambiguity"

        reason = (
            f"The event timeline marks `{event.get('ministerio_canonical')}` as `{event.get('event_type')}` on "
            f"{event.get('event_date_start') or event.get('event_date_display') or 'unknown date'}, but the old ministry list is a static set without structural timing."
        )
        if event.get("notes"):
            reason += f" Notes: {normalize_space(event['notes'])}"

        add_issue(
            issues,
            seen,
            make_issue(
                issue_type="ministry_continuity_problem",
                severity=severity,
                government_id=event.get("government_id"),
                ministry_canonical_event=event.get("ministerio_canonical"),
                ministry_raw_event=event.get("ministerio_raw"),
                ministry_old=old_ministry,
                party_old=None,
                party_event=event.get("party"),
                old_period=None,
                event_start=parse_iso_date(event.get("event_date_start")),
                event_end=parse_iso_date(event.get("event_date_end")),
                reason=reason,
                old_coarse_representation=(
                    f"`orgaos_ministeriais.json[{page_label}]` keeps `{old_ministry or event.get('ministerio_canonical')}`"
                    f" as a membership entry, without a dated rename/merge/split/extinction record."
                ),
                event_source_field="ministerios_eventos.json:events[]",
                old_source_field=f"orgaos_ministeriais.json[{page_label}].orgaos[]" if page_label else "orgaos_ministeriais.json",
                source_snippet=event.get("source_snippet"),
                source_url=event.get("source_url"),
                notes=event.get("notes"),
                page_label=page_label,
            ),
        )


def compare_uncertainty_events(
    issues: list[dict],
    seen: set[tuple],
    events: list[dict],
    old_orgaos: dict,
    page_lookup: dict[str, str],
    old_periods: list[dict],
) -> None:
    for event in events:
        if not is_uncertain_event(event) or is_structural_event(event):
            continue
        event_date = parse_iso_date(event.get("event_date_start"))
        government_id = event.get("government_id") or government_for_date(event_date)
        page_label = page_lookup.get(event.get("source_page"))
        old_labels = old_orgaos.get(page_label, {}).get("orgaos", []) if page_label else []
        old_ministry = matched_old_ministry(old_labels, event.get("ministerio_canonical"))
        old_period = covering_period(old_periods, government_id, event_date) if government_id else None

        reason = (
            f"Event timeline keeps `{event.get('ministerio_canonical')}` as an uncertain/contested case"
            f" ({event.get('event_type')}, confidence={event.get('confidence')})"
        )
        if event.get("event_date_start"):
            reason += f" on {event['event_date_start']}"
        else:
            reason += " with no parsed date"
        reason += "."
        if event.get("notes"):
            reason += f" Notes: {normalize_space(event['notes'])}"

        add_issue(
            issues,
            seen,
            make_issue(
                issue_type="uncertainty_lost",
                severity="likely_ambiguity",
                government_id=government_id,
                ministry_canonical_event=event.get("ministerio_canonical"),
                ministry_raw_event=event.get("ministerio_raw"),
                ministry_old=old_ministry,
                party_old=describe_periods([old_period] if old_period else []),
                party_event=event.get("party"),
                old_period=old_period,
                event_start=event_date,
                event_end=parse_iso_date(event.get("event_date_end")),
                reason=reason,
                old_coarse_representation=(
                    f"Old inputs keep the ministry/coalition context as stable (`orgaos_ministeriais.json` + "
                    f"`partidos_por_periodo.json`), but the event layer explicitly keeps this row unresolved."
                ),
                event_source_field="ministerios_eventos.json:events[]",
                old_source_field="orgaos_ministeriais.json + partidos_por_periodo.json",
                source_snippet=event.get("source_snippet"),
                source_url=event.get("source_url"),
                notes=event.get("notes"),
                page_label=page_label,
                extra_evidence={"confidence": event.get("confidence")},
            ),
        )


def compare_party_period_coverage(
    issues: list[dict],
    seen: set[tuple],
    events: list[dict],
    old_periods: list[dict],
) -> None:
    events_by_government = defaultdict(list)
    for event in events:
        event_date = parse_iso_date(event.get("event_date_start"))
        if event_date is None or event.get("government_id") is None:
            continue
        events_by_government[event["government_id"]].append((event_date, event))

    for government_id, dated_events in events_by_government.items():
        periods = [period for period in old_periods if period["government_id"] == government_id]
        if not periods:
            continue
        latest_period_end = max(period["end"] for period in periods)
        latest_event_date, latest_event = max(dated_events, key=lambda item: item[0])
        if latest_event_date <= latest_period_end:
            continue
        add_issue(
            issues,
            seen,
            make_issue(
                issue_type="date_boundary_problem",
                severity="probable_mistake",
                government_id=government_id,
                ministry_canonical_event=latest_event.get("ministerio_canonical"),
                ministry_raw_event=latest_event.get("ministerio_raw"),
                ministry_old=None,
                party_old=f"party-period coverage ends on {latest_period_end.isoformat()}",
                party_event=latest_event.get("party"),
                old_period=max(periods, key=lambda item: item["end"]),
                event_start=latest_event_date,
                event_end=parse_iso_date(latest_event.get("event_date_end")),
                reason=(
                    f"`partidos_por_periodo.json` stops at {latest_period_end.isoformat()} for `{government_id}`, "
                    f"but the event timeline continues through {latest_event_date.isoformat()}. The old coalition input is stale relative to the dashboard event data."
                ),
                old_coarse_representation=f"The last old coalition period for `{government_id}` ends on {latest_period_end.isoformat()}.",
                event_source_field="ministerios_eventos.json:events[]",
                old_source_field="partidos_por_periodo.json",
                source_snippet=latest_event.get("source_snippet"),
                source_url=latest_event.get("source_url"),
                notes=latest_event.get("notes"),
            ),
        )


def render_issue(issue: dict) -> list[str]:
    header = (
        f"- {issue.get('government_id') or 'unknown government'} | "
        f"{issue.get('ministry_canonical_event') or 'unknown ministry'} | "
        f"{issue['issue_type']} | {issue['severity']}"
    )
    old_line = f"  old/coarse: {issue.get('old_coarse_representation') or 'n/a'}"
    event_line = (
        "  event evidence: "
        f"{issue.get('evidence', {}).get('source_snippet') or issue.get('reason')}"
    )
    why_line = f"  why this matters: {issue.get('reason') or 'n/a'}"
    return [header, old_line, event_line, why_line]


def write_readable_report(issues: list[dict]) -> None:
    lines = [
        "# Dashboard Input vs Event Issues",
        "",
        f"- generated_at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        f"- issues_flagged: {len(issues)}",
        f"- severity_counts: {json.dumps(severity_counts(issues), ensure_ascii=False, sort_keys=True)}",
        f"- type_counts: {json.dumps(type_counts(issues), ensure_ascii=False, sort_keys=True)}",
        "- comparison_note: coalition-period comparisons only apply where `partidos_por_periodo.json` has coverage; that file starts on 2015-01-01.",
        "",
    ]

    probable = [issue for issue in issues if issue["severity"] == "probable_mistake"]
    remainder = [issue for issue in issues if issue["severity"] != "probable_mistake"]

    lines.append("## Probable Old-Data Mistakes")
    lines.append("")
    if probable:
        for issue in probable:
            lines.extend(render_issue(issue))
            lines.append("")
    else:
        lines.append("- none flagged")
        lines.append("")

    by_type = defaultdict(list)
    for issue in remainder:
        by_type[issue["issue_type"]].append(issue)

    for issue_type in ISSUE_ORDER:
        lines.append(f"## {SECTION_TITLES[issue_type]}")
        lines.append("")
        bucket = by_type.get(issue_type, [])
        if not bucket:
            lines.append("- none flagged")
            lines.append("")
            continue
        for issue in bucket:
            lines.extend(render_issue(issue))
            lines.append("")

    ISSUES_MD_PATH.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> None:
    dashboard_payload = load_json(DASHBOARD_JSON_PATH)
    raw_events_payload = load_json(RAW_EVENTS_PATH)
    old_orgaos = load_json(ORGAOS_PATH)
    old_party_periods_raw = load_json(PARTY_PERIODS_PATH)

    events = raw_events_payload.get("events", [])
    old_periods = build_old_periods(old_party_periods_raw)
    event_lookup = build_event_lookup(events)
    page_lookup = source_page_title_to_label(dashboard_payload)

    appointments = []
    for appointment in dashboard_payload.get("appointments", []):
        item = dict(appointment)
        item["_page_label"] = appointment_page_label(item)
        item["_start"] = appointment_start(item)
        item["_end"] = appointment_end(item)
        appointments.append(item)
    grouped_appointments = sorted_grouped_appointments(appointments)

    issues: list[dict] = []
    seen: set[tuple] = set()

    compare_ministry_sets(issues, seen, old_orgaos, dashboard_payload)
    compare_interim_cases(issues, seen, grouped_appointments, old_periods, old_orgaos, event_lookup)
    compare_party_turnover(issues, seen, grouped_appointments, old_periods, old_orgaos)
    compare_structural_events(issues, seen, events, old_orgaos, page_lookup)
    compare_uncertainty_events(issues, seen, events, old_orgaos, page_lookup, old_periods)
    compare_party_period_coverage(issues, seen, events, old_periods)

    issues.sort(
        key=lambda item: (
            ["probable_mistake", "likely_over_coarsening", "likely_ambiguity"].index(item["severity"]),
            item.get("government_id") or "",
            item.get("event_interval_start") or "9999-12-31",
            item.get("ministry_canonical_event") or "",
            item["issue_type"],
        )
    )

    payload = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "inputs": {
            "event_timeline": str(DASHBOARD_JSON_PATH.relative_to(ROOT)),
            "raw_event_log": str(RAW_EVENTS_PATH.relative_to(ROOT)),
            "orgaos_ministeriais": str(ORGAOS_PATH.relative_to(ROOT)),
            "partidos_por_periodo": str(PARTY_PERIODS_PATH.relative_to(ROOT)),
        },
        "summary": {
            "issue_count": len(issues),
            "severity_counts": severity_counts(issues),
            "type_counts": type_counts(issues),
            "notes": [
                "Coalition-period checks start in 2015 because partidos_por_periodo.json has no earlier coverage.",
                "The script uses dashboard appointments as the event-level interval layer and raw events for provenance/evidence.",
            ],
        },
        "issues": issues,
    }

    ISSUES_JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_readable_report(issues)

    print(f"Issues flagged: {len(issues)}")
    print(f"Severity counts: {severity_counts(issues)}")
    print(f"Type counts: {type_counts(issues)}")
    print(f"Wrote {ISSUES_JSON_PATH.relative_to(ROOT)}")
    print(f"Wrote {ISSUES_MD_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
