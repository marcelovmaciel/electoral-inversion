#!/usr/bin/env python3
import csv
import json
import re
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from scrape_wikipedia_ministerios import (
    PAGES,
    USER_AGENT,
    WIKI_API,
    clean_text,
    detect_header,
    find_column,
    normalize_header,
    parse_date_pt,
    strip_accents,
)


ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "scraping" / "output"
ORGAOS_PATH = OUTPUT_DIR / "orgaos_ministeriais.json"
PARTY_PERIODS_PATH = OUTPUT_DIR / "partidos_por_periodo.json"
RAW_JSON_PATH = OUTPUT_DIR / "ministerios_eventos.json"
DASHBOARD_JSON_PATH = OUTPUT_DIR / "cabinet_timeline_dashboard.json"
EVENTS_CSV_PATH = OUTPUT_DIR / "ministerios_eventos.csv"
INTERVALS_CSV_PATH = OUTPUT_DIR / "ministerios_nomeacoes_intervalos.csv"
REPORT_PATH = OUTPUT_DIR / "ministerios_eventos_review_report.md"

GOVERNMENT_WINDOWS = [
    {
        "government_id": "dilma_1",
        "label": "Primeiro gabinete de Dilma Rousseff",
        "president": "Dilma Rousseff",
        "start": date(2011, 1, 1),
        "end": date(2014, 12, 31),
    },
    {
        "government_id": "dilma_2",
        "label": "Segundo gabinete de Dilma Rousseff",
        "president": "Dilma Rousseff",
        "start": date(2015, 1, 1),
        "end": date(2016, 5, 11),
    },
    {
        "government_id": "temer",
        "label": "Gabinete de Michel Temer",
        "president": "Michel Temer",
        "start": date(2016, 5, 12),
        "end": date(2018, 12, 31),
    },
    {
        "government_id": "bolsonaro",
        "label": "Gabinete de Jair Bolsonaro",
        "president": "Jair Bolsonaro",
        "start": date(2019, 1, 1),
        "end": date(2022, 12, 31),
    },
    {
        "government_id": "lula_3",
        "label": "Terceiro gabinete de Lula",
        "president": "Luiz Inacio Lula da Silva",
        "start": date(2023, 1, 1),
        "end": None,
    },
]

PAGE_WINDOWS = {
    "dilma_rousseff": (date(2011, 1, 1), date(2016, 5, 11)),
    "michel_temer": (date(2016, 5, 12), date(2018, 12, 31)),
    "jair_bolsonaro": (date(2019, 1, 1), date(2022, 12, 31)),
    "lula_2023_presente": (date(2023, 1, 1), None),
}

UF_CODES = {
    "ac",
    "al",
    "ap",
    "am",
    "ba",
    "ce",
    "df",
    "es",
    "go",
    "ma",
    "mg",
    "ms",
    "mt",
    "pa",
    "pb",
    "pe",
    "pi",
    "pr",
    "rj",
    "rn",
    "ro",
    "rr",
    "rs",
    "sc",
    "se",
    "sp",
    "to",
}

OPEN_ENDED_TERMS = {"em exercicio", "em exercício", "presente", "atual", "ate o presente"}
NON_ENTRY_TERMS = {
    "nao assumiu",
    "não assumiu",
    "sub judice",
    "nomeacao suspensa",
    "nomeação suspensa",
    "decisao judicial",
    "decisão judicial",
}


def normalize_space(value):
    return re.sub(r"\s+", " ", value or "").strip()


def iso_or_none(value):
    if value is None:
        return None
    return value.isoformat()


def page_url(page_info, title=None, fragment=None):
    if "page" in page_info:
        target = page_info["page"]
    elif title:
        target = title
    else:
        return f"https://pt.wikipedia.org/?curid={page_info['pageid']}"
    url = f"https://pt.wikipedia.org/wiki/{quote(target.replace(' ', '_'), safe=':_()/%-')}"
    if fragment:
        url = f"{url}#{quote(fragment, safe='')}"
    return url


def load_allowed_roles():
    data = json.loads(ORGAOS_PATH.read_text(encoding="utf-8"))
    allowed = {}
    for page_label, info in data.items():
        allowed[page_label] = {
            normalize_header(role): {
                "raw": role,
                "status_type": classify_status_type(role),
            }
            for role in info.get("orgaos", [])
        }
    return allowed


def load_party_periods():
    data = json.loads(PARTY_PERIODS_PATH.read_text(encoding="utf-8"))
    periods = []
    for period_id, info in sorted(data.items()):
        start = parse_iso_date(info["data_inicio"])
        end = parse_iso_date(info["data_fim"])
        periods.append(
            {
                "period_id": period_id,
                "start": start,
                "end": end,
                "parties": info.get("partidos", []),
                "government_id": government_for_date(start),
            }
        )
    return periods


def parse_iso_date(value):
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def classify_status_type(label):
    norm = normalize_header(label)
    orgao_prefixes = (
        "advocacia geral",
        "banco central",
        "casa civil",
        "controladoria geral",
        "gabinete",
        "presidente do banco central",
        "secretaria",
    )
    if norm.startswith(orgao_prefixes):
        return "orgao_status_ministerial"
    return "ministerio"


def fetch_page_payload(page_info):
    params = {"action": "parse", "prop": "text|revid", "format": "json"}
    if "page" in page_info:
        params["page"] = page_info["page"]
    else:
        params["pageid"] = page_info["pageid"]
    response = requests.get(
        WIKI_API,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()["parse"]
    return {
        "title": payload["title"],
        "html": payload["text"]["*"],
        "revid": payload.get("revid"),
    }


def clean_cell_text(value):
    value = clean_text(value)
    value = re.sub(r"\b(?:Ficheiro|File):[^ ]+\b", "", value, flags=re.I)
    value = value.replace("†", " ")
    return normalize_space(value)


def extract_links(cell):
    links = []
    for anchor in cell.find_all("a", href=True):
        text = clean_cell_text(anchor.get_text(" ", strip=True))
        if not text or text.lower().startswith(("ficheiro:", "file:")):
            continue
        href = anchor["href"]
        if href.startswith("/wiki/"):
            url = f"https://pt.wikipedia.org{href}"
        elif href.startswith("//"):
            url = f"https:{href}"
        elif href.startswith("http"):
            url = href
        else:
            continue
        links.append({"text": text, "url": url})
    return links


def cell_to_dict(cell):
    return {
        "text": clean_cell_text(cell.get_text(" ", strip=True)),
        "html": str(cell),
        "links": extract_links(cell),
        "tag": cell.name,
    }


def expand_table(table):
    rows = []
    span_map = {}
    for tr in table.find_all("tr"):
        row = []
        col_idx = 0
        cells = tr.find_all(["th", "td"], recursive=False)
        while col_idx in span_map:
            carried = dict(span_map[col_idx]["cell"])
            row.append(carried)
            span_map[col_idx]["rows_left"] -= 1
            if span_map[col_idx]["rows_left"] <= 0:
                del span_map[col_idx]
            col_idx += 1
        for cell in cells:
            while col_idx in span_map:
                carried = dict(span_map[col_idx]["cell"])
                row.append(carried)
                span_map[col_idx]["rows_left"] -= 1
                if span_map[col_idx]["rows_left"] <= 0:
                    del span_map[col_idx]
                col_idx += 1
            cell_dict = cell_to_dict(cell)
            colspan = int(cell.get("colspan", 1))
            rowspan = int(cell.get("rowspan", 1))
            for _ in range(colspan):
                row.append(dict(cell_dict))
                if rowspan > 1:
                    span_map[col_idx] = {
                        "cell": dict(cell_dict),
                        "rows_left": rowspan - 1,
                    }
                col_idx += 1
        rows.append(row)
    max_cols = max((len(row) for row in rows), default=0)
    for row in rows:
        if len(row) < max_cols:
            row.extend([{"text": "", "html": "", "links": [], "tag": "td"}] * (max_cols - len(row)))
    return rows


def text_grid(cell_rows):
    return [[cell["text"] for cell in row] for row in cell_rows]


def find_heading_context(table, page_title):
    heading = table.find_previous(["h2", "h3", "h4"])
    if heading is None:
        return page_title, None
    headline = heading.find(class_="mw-headline")
    if headline is not None:
        section_text = clean_cell_text(headline.get_text(" ", strip=True))
        fragment = headline.get("id")
    else:
        section_text = clean_cell_text(heading.get_text(" ", strip=True))
        fragment = heading.get("id")
    return section_text or page_title, fragment


def extract_person_name(cell):
    if cell is None:
        return ""
    candidates = []
    for link in cell.get("links", []):
        text = link["text"]
        if text and text.lower() not in {"ref", "nota"}:
            candidates.append(text)
    raw = cell["text"]
    if candidates:
        longest = max(candidates, key=len)
        if raw.lower().startswith(("ficheiro:", "file:")) or longest not in raw:
            return longest
    return raw


def canonicalize_person_name(raw_name):
    value = clean_cell_text(raw_name)
    value = re.sub(r"\s*\([^)]*\)\s*$", "", value)
    return normalize_space(value)


def canonicalize_ministry(raw_label):
    value = clean_cell_text(raw_label)
    value = re.sub(r"\s*\([^)]*\)\s*$", "", value)
    return normalize_space(value)


def normalize_key(value):
    value = strip_accents(value).lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return normalize_space(value)


def extract_date_candidates(value):
    normalized = normalize_space(strip_accents(value).lower())
    normalized = re.sub(r"\bno dia\b", "", normalized)
    normalized = normalized.replace(",", " ")
    patterns = [
        r"\d{1,2}/\d{1,2}/\d{4}",
        r"\d{1,2}\s*\.?\s*o?\s+de\s+[a-z]+(?:\s+de)?\s+\d{4}",
    ]
    matches = []
    for pattern in patterns:
        for match in re.finditer(pattern, normalized):
            matches.append((match.start(), match.group(0)))
    seen = set()
    ordered = []
    for _, text in sorted(matches):
        if text not in seen:
            seen.add(text)
            ordered.append(text)
    return ordered


def parse_date_field(value):
    raw = clean_cell_text(value)
    if not raw:
        return {
            "date": None,
            "raw": raw,
            "is_open_ended": False,
            "multiple_dates": False,
            "notes": [],
        }
    normalized = normalize_space(strip_accents(raw).lower())
    if normalized in {"-", "—"}:
        return {
            "date": None,
            "raw": raw,
            "is_open_ended": False,
            "multiple_dates": False,
            "notes": [],
        }
    if any(term in normalized for term in OPEN_ENDED_TERMS):
        return {
            "date": None,
            "raw": raw,
            "is_open_ended": True,
            "multiple_dates": False,
            "notes": [],
        }
    candidates = extract_date_candidates(raw)
    parsed_dates = [parse_date_pt(text) for text in candidates]
    parsed_dates = [item for item in parsed_dates if item is not None]
    fallback = parse_date_pt(raw)
    parsed_date = parsed_dates[0] if parsed_dates else fallback
    return {
        "date": parsed_date,
        "raw": raw,
        "is_open_ended": False,
        "multiple_dates": len(parsed_dates) > 1,
        "notes": [],
    }


def parse_party_field(raw_party):
    raw_party = clean_cell_text(raw_party)
    normalized = normalize_space(strip_accents(raw_party).lower())
    if not raw_party or normalized in {"-", "—", "--", "---"}:
        return None, []
    chunks = [chunk.strip() for chunk in raw_party.replace(" / ", "/").split("/") if chunk.strip()]
    parties = []
    for chunk in chunks:
        if re.fullmatch(r"[-—–]+", chunk):
            continue
        norm = normalize_space(strip_accents(chunk).lower())
        if norm in UF_CODES:
            continue
        if norm.startswith("sem partido"):
            continue
        parties.append(chunk)
    return raw_party, parties


def infer_source_scope(page_label, table_index, rows):
    if page_label == "dilma_rousseff":
        starts = []
        for row in rows:
            for cell in row:
                parsed = parse_date_pt(cell["text"])
                if parsed is not None:
                    starts.append(parsed)
                    break
        if starts and max(item.year for item in starts) <= 2014:
            return GOVERNMENT_WINDOWS[0]
        return GOVERNMENT_WINDOWS[1]
    for government in GOVERNMENT_WINDOWS[2:]:
        if government["government_id"] == "temer" and page_label == "michel_temer":
            return government
        if government["government_id"] == "bolsonaro" and page_label == "jair_bolsonaro":
            return government
        if government["government_id"] == "lula_3" and page_label == "lula_2023_presente":
            return government
    return None


def government_for_date(value):
    if value is None:
        return None
    for government in GOVERNMENT_WINDOWS:
        end = government["end"] or date.max
        if government["start"] <= value <= end:
            return government["government_id"]
    return None


def interval_overlap(start_a, end_a, start_b, end_b):
    if start_a is None or start_b is None:
        return False
    left = max(start_a, start_b)
    right = min(end_a or date.max, end_b or date.max)
    return left <= right


def classify_role(person_raw, row_snippet):
    low = normalize_space(strip_accents(f"{person_raw} {row_snippet}").lower())
    if "secretario executivo" in low or "secretario-executivo" in low:
        return "secretario_executivo_assumiu"
    if "interin" in low:
        return "interino"
    if "substitut" in low:
        return "substituto"
    return "minister"


def classify_appointment_type(role_classification):
    if role_classification == "interino":
        return "interim"
    if role_classification in {"substituto", "secretario_executivo_assumiu"}:
        return "assumed"
    if role_classification == "minister":
        return "permanent"
    return "unknown"


def note_flags(text):
    low = normalize_space(strip_accents(text).lower())
    flags = []
    if any(term in low for term in NON_ENTRY_TERMS):
        flags.append("non_entry")
    if "renunciou" in low:
        flags.append("resignation")
    if "demitido" in low:
        flags.append("dismissal")
    if "impeachment" in low:
        flags.append("impeachment")
    if "suspensa" in low or "suspenso" in low:
        flags.append("suspension")
    if "extinta" in low:
        flags.append("extinction")
    if "transformada em" in low:
        flags.append("rename")
    if "incorporada" in low:
        flags.append("merge")
    if "criada" in low:
        flags.append("creation")
    return flags


def extract_structural_target(text, marker):
    match = re.search(marker + r"\s+(.+)", clean_cell_text(text), flags=re.I)
    if not match:
        return None
    target = match.group(1)
    target = re.sub(r"\).*", "", target)
    return normalize_space(target)


def build_structural_events(record):
    events = []
    row_text = record["source_snippet"]
    low = normalize_space(strip_accents(row_text).lower())

    if "transformada em" in low:
        successor = extract_structural_target(row_text, r"transformada em")
        events.append(
            structural_event(
                record,
                "rename",
                record["end_date"],
                "saida no dia",
                successor_raw=successor,
                confidence="medium",
                notes="Evento estrutural inferido de 'transformada em' no texto da linha.",
            )
        )

    if "incorporada" in low:
        successor = extract_structural_target(row_text, r"incorporada(?:\s+ao|\s+a|\s+a[oà])")
        events.append(
            structural_event(
                record,
                "merge",
                record["end_date"],
                "saida no dia",
                successor_raw=successor,
                confidence="medium",
                notes="Evento estrutural inferido de 'incorporada' no texto da linha.",
            )
        )

    if "criada" in low:
        predecessor = None
        match = re.search(r"pertencia ao ([^)]*)", row_text, flags=re.I)
        if match:
            predecessor = normalize_space(match.group(1))
        event_type = "split" if predecessor else "unknown"
        events.append(
            structural_event(
                record,
                event_type,
                record["start_date"],
                "posse no dia",
                predecessor_raw=predecessor,
                confidence="medium" if predecessor else "low",
                notes="Evento estrutural inferido de 'criada' no texto da linha.",
            )
        )

    if "extinta" in low and not any(item["event_type"] == "merge" for item in events):
        events.append(
            structural_event(
                record,
                "unknown",
                record["end_date"],
                "saida no dia",
                confidence="low",
                notes="Wikipedia indica extinção do órgão, sem sucessor institucional explícito.",
            )
        )

    return events


def structural_event(
    record,
    event_type,
    event_date,
    column_name,
    predecessor_raw=None,
    successor_raw=None,
    confidence="medium",
    notes="",
):
    return {
        "government_id": government_for_date(event_date) or record["source_scope"]["government_id"],
        "source_page": record["source_page"],
        "source_url": record["source_url"],
        "source_section": record["source_section"],
        "source_locator": {
            "table_index": record["table_index"],
            "row_index": record["row_index"],
            "column_name": column_name,
            "html_fragment_id": record["html_fragment_id"],
            "wikipedia_revision_id": record["revid"],
        },
        "ministerio_canonical": record["ministerio_canonical"],
        "ministerio_raw": record["ministerio_raw"],
        "ministerio_status_type": record["ministerio_status_type"],
        "event_type": event_type,
        "person_name_raw": None,
        "person_name_canonical": None,
        "party": None,
        "role_title_raw": record["role_title_raw"],
        "role_classification": "unknown",
        "event_date_start": iso_or_none(event_date),
        "event_date_end": None,
        "event_date_display": record["end_raw"] if column_name == "saida no dia" else record["start_raw"],
        "predecessor_raw": predecessor_raw,
        "successor_raw": successor_raw,
        "confidence": confidence,
        "needs_review": confidence == "low",
        "notes": notes,
        "source_snippet": record["source_snippet"],
    }


def extract_records():
    allowed_roles = load_allowed_roles()
    page_outputs = []
    records = []
    for page_info in PAGES:
        payload = fetch_page_payload(page_info)
        page_outputs.append(
            {
                "label": page_info["label"],
                "title": payload["title"],
                "url": page_url(page_info, title=payload["title"]),
                "revid": payload["revid"],
            }
        )
        soup = BeautifulSoup(payload["html"], "html.parser")
        for table_index, table in enumerate(soup.find_all("table")):
            cell_rows = expand_table(table)
            if not cell_rows:
                continue
            rows = text_grid(cell_rows)
            header_idx, header = detect_header(rows)
            if header is None:
                continue
            role_idx = find_column(header, ["cargo", "ministerio"])
            party_idx = find_column(header, ["partido"])
            start_idx = find_column(header, ["posse", "inicio", "tomou posse", "data de posse"])
            end_idx = find_column(header, ["saida", "fim", "ate o dia"])
            name_indices = [idx for idx, value in enumerate(header) if value in {"incumbente", "titular", "nome"}]
            if role_idx is None or not name_indices or start_idx is None or end_idx is None:
                continue
            name_idx = name_indices[-1]
            section_text, fragment_id = find_heading_context(table, payload["title"])
            source_scope = infer_source_scope(page_info["label"], table_index, cell_rows[header_idx + 1 :])
            for row_index, row in enumerate(cell_rows[header_idx + 1 :], start=header_idx + 1):
                if role_idx >= len(row):
                    continue
                role_raw = row[role_idx]["text"]
                role_key = normalize_header(role_raw)
                if role_key not in allowed_roles.get(page_info["label"], {}):
                    continue
                person_raw = extract_person_name(row[name_idx]) if name_idx < len(row) else ""
                party_raw = row[party_idx]["text"] if party_idx is not None and party_idx < len(row) else ""
                start_info = parse_date_field(row[start_idx]["text"])
                end_info = parse_date_field(row[end_idx]["text"])
                party_display, party_codes = parse_party_field(party_raw)
                snippet = " | ".join(cell["text"] for cell in row if cell["text"])
                role_classification = classify_role(person_raw, snippet)
                canonical_person = canonicalize_person_name(person_raw)
                notes = []
                flags = note_flags(snippet)
                needs_review = False
                confidence = "high"

                if any(flag in {"non_entry", "suspension"} for flag in flags):
                    confidence = "low"
                    needs_review = True
                    notes.append("Linha indica nomeação contestada, suspensa ou não consumada.")
                if start_info["multiple_dates"] or end_info["multiple_dates"]:
                    confidence = "low"
                    needs_review = True
                    notes.append("Campo de data contém múltiplas datas; intervalo não foi forçado.")
                if start_info["date"] is None and not start_info["is_open_ended"]:
                    needs_review = True
                    confidence = "low"
                    notes.append("Data inicial ausente ou não parseada.")
                if end_info["date"] is None and not end_info["is_open_ended"] and row[end_idx]["text"] not in {"", "-", "—"}:
                    needs_review = True
                    confidence = "low"
                    notes.append("Data final ausente ou não parseada.")
                if start_info["date"] and end_info["date"] and end_info["date"] < start_info["date"]:
                    needs_review = True
                    confidence = "low"
                    notes.append("Data final anterior à data inicial.")
                if role_classification != "minister" and confidence == "high":
                    confidence = "medium"
                if page_info["label"] != "dilma_rousseff":
                    scope_start = PAGE_WINDOWS[page_info["label"]][0]
                    if start_info["date"] and start_info["date"] < scope_start:
                        notes.append(
                            "Data real de início antecede a janela do gabinete da página; o dashboard usa o recorte da janela do gabinete."
                        )
                if not canonical_person:
                    needs_review = True
                    confidence = "low"
                    notes.append("Nome da pessoa não foi extraído com segurança.")

                record = {
                    "record_id": f"{page_info['label']}:{table_index}:{row_index}",
                    "source_page_id": page_info["label"],
                    "source_page": payload["title"],
                    "source_url": page_url(page_info, title=payload["title"], fragment=fragment_id),
                    "source_section": section_text,
                    "table_index": table_index,
                    "row_index": row_index,
                    "html_fragment_id": fragment_id,
                    "revid": payload["revid"],
                    "source_scope": source_scope or {
                        "government_id": page_info["label"],
                        "label": payload["title"],
                        "start": PAGE_WINDOWS.get(page_info["label"], (None, None))[0],
                        "end": PAGE_WINDOWS.get(page_info["label"], (None, None))[1],
                    },
                    "ministerio_raw": role_raw,
                    "ministerio_canonical": canonicalize_ministry(role_raw),
                    "ministerio_status_type": allowed_roles[page_info["label"]][role_key]["status_type"],
                    "person_name_raw": person_raw,
                    "person_name_canonical": canonical_person,
                    "party": party_display,
                    "party_codes": party_codes,
                    "role_title_raw": person_raw,
                    "role_classification": role_classification,
                    "start_date": start_info["date"],
                    "end_date": end_info["date"],
                    "start_raw": start_info["raw"],
                    "end_raw": end_info["raw"],
                    "start_open_ended": start_info["is_open_ended"],
                    "end_open_ended": end_info["is_open_ended"],
                    "start_multiple_dates": start_info["multiple_dates"],
                    "end_multiple_dates": end_info["multiple_dates"],
                    "confidence": confidence,
                    "needs_review": needs_review,
                    "notes": notes,
                    "flags": flags,
                    "source_snippet": snippet,
                }
                records.append(record)
    return page_outputs, records


def build_events(records):
    events = []
    by_ministry = defaultdict(list)
    for record in records:
        by_ministry[record["ministerio_canonical"]].append(record)

    for ministry_records in by_ministry.values():
        ministry_records.sort(
            key=lambda item: (
                item["start_date"] or date.max,
                item["end_date"] or date.max,
                item["record_id"],
            )
        )
        seen_people = set()
        for index, record in enumerate(ministry_records):
            previous = ministry_records[index - 1] if index > 0 else None
            following = ministry_records[index + 1] if index + 1 < len(ministry_records) else None

            if record["person_name_canonical"]:
                if "non_entry" in record["flags"]:
                    start_event_type = "unknown"
                elif record["role_classification"] in {
                    "interino",
                    "substituto",
                    "secretario_executivo_assumiu",
                }:
                    start_event_type = "assumption"
                elif previous and previous["person_name_canonical"] == record["person_name_canonical"]:
                    start_event_type = "entry"
                elif record["person_name_canonical"] in seen_people:
                    start_event_type = "return"
                else:
                    start_event_type = "entry"
                seen_people.add(record["person_name_canonical"])
                events.append(
                    appointment_event(
                        record,
                        start_event_type,
                        record["start_date"],
                        "posse no dia",
                        predecessor_raw=previous["person_name_raw"] if previous else None,
                        successor_raw=following["person_name_raw"] if following else None,
                        event_date_display=record["start_raw"],
                    )
                )

            if record["end_date"] is not None:
                events.append(
                    appointment_event(
                        record,
                        "exit",
                        record["end_date"],
                        "saida no dia",
                        predecessor_raw=previous["person_name_raw"] if previous else None,
                        successor_raw=following["person_name_raw"] if following else None,
                        event_date_display=record["end_raw"],
                    )
                )

            events.extend(build_structural_events(record))

            if previous and previous["end_date"] and record["start_date"]:
                gap = (record["start_date"] - previous["end_date"]).days
                if gap > 1:
                    vacancy_start = previous["end_date"] + timedelta(days=1)
                    events.append(
                        vacancy_event(
                            record,
                            "vacancy_start",
                            vacancy_start,
                            previous["person_name_raw"],
                            record["person_name_raw"],
                            "Derivado de lacuna entre linhas consecutivas do mesmo órgão.",
                        )
                    )
                    events.append(
                        vacancy_event(
                            record,
                            "vacancy_end",
                            record["start_date"],
                            previous["person_name_raw"],
                            record["person_name_raw"],
                            "Derivado de lacuna entre linhas consecutivas do mesmo órgão.",
                        )
                    )

                if gap < 0 and record["role_classification"] == "minister":
                    # Overlaps between two non-interim rows are kept, but flagged.
                    for event in events[-2:]:
                        event["needs_review"] = True
                        event["confidence"] = "low"
                        event["notes"] = join_notes(
                            event.get("notes"),
                            "Há sobreposição temporal com outra linha não classificada como interina.",
                        )
            if record["needs_review"]:
                for event in events[-3:]:
                    if event["source_locator"]["table_index"] == record["table_index"] and event["source_locator"]["row_index"] == record["row_index"]:
                        event["needs_review"] = True
                        event["confidence"] = "low" if record["confidence"] == "low" else event["confidence"]
                        event["notes"] = join_notes(event.get("notes"), "; ".join(record["notes"]))

    events.sort(key=lambda item: (item["event_date_start"] or "9999-12-31", item["ministerio_canonical"], item["event_type"]))
    return events


def appointment_event(
    record,
    event_type,
    event_date,
    column_name,
    predecessor_raw=None,
    successor_raw=None,
    event_date_display=None,
):
    confidence = record["confidence"]
    notes = "; ".join(record["notes"]) if record["notes"] else ""
    if event_type == "return" and confidence == "high":
        confidence = "medium"
    return {
        "government_id": government_for_date(event_date) or record["source_scope"]["government_id"],
        "source_page": record["source_page"],
        "source_url": record["source_url"],
        "source_section": record["source_section"],
        "source_locator": {
            "table_index": record["table_index"],
            "row_index": record["row_index"],
            "column_name": column_name,
            "html_fragment_id": record["html_fragment_id"],
            "wikipedia_revision_id": record["revid"],
        },
        "ministerio_canonical": record["ministerio_canonical"],
        "ministerio_raw": record["ministerio_raw"],
        "ministerio_status_type": record["ministerio_status_type"],
        "event_type": event_type,
        "person_name_raw": record["person_name_raw"],
        "person_name_canonical": record["person_name_canonical"],
        "party": record["party"],
        "role_title_raw": record["role_title_raw"],
        "role_classification": record["role_classification"],
        "event_date_start": iso_or_none(event_date),
        "event_date_end": None,
        "event_date_display": event_date_display,
        "predecessor_raw": predecessor_raw,
        "successor_raw": successor_raw,
        "confidence": confidence,
        "needs_review": record["needs_review"],
        "notes": notes,
        "source_snippet": record["source_snippet"],
    }


def vacancy_event(record, event_type, event_date, predecessor_raw, successor_raw, notes):
    return {
        "government_id": government_for_date(event_date) or record["source_scope"]["government_id"],
        "source_page": record["source_page"],
        "source_url": record["source_url"],
        "source_section": record["source_section"],
        "source_locator": {
            "table_index": record["table_index"],
            "row_index": record["row_index"],
            "column_name": "derived",
            "html_fragment_id": record["html_fragment_id"],
            "wikipedia_revision_id": record["revid"],
        },
        "ministerio_canonical": record["ministerio_canonical"],
        "ministerio_raw": record["ministerio_raw"],
        "ministerio_status_type": record["ministerio_status_type"],
        "event_type": event_type,
        "person_name_raw": None,
        "person_name_canonical": None,
        "party": None,
        "role_title_raw": None,
        "role_classification": "unknown",
        "event_date_start": iso_or_none(event_date),
        "event_date_end": None,
        "event_date_display": iso_or_none(event_date),
        "predecessor_raw": predecessor_raw,
        "successor_raw": successor_raw,
        "confidence": "medium",
        "needs_review": False,
        "notes": notes,
        "source_snippet": record["source_snippet"],
    }


def join_notes(existing, extra):
    if not extra:
        return existing or ""
    if not existing:
        return extra
    if extra in existing:
        return existing
    return f"{existing}; {extra}"


def build_dashboard_appointments(records, party_periods):
    appointments = []
    for record in records:
        actual_start = record["start_date"]
        actual_end = record["end_date"]
        if actual_start is None and "non_entry" not in record["flags"]:
            continue
        if record["start_multiple_dates"] or record["end_multiple_dates"]:
            continue
        scope_start = record["source_scope"]["start"]
        scope_end = record["source_scope"]["end"]
        clipped_start = max(item for item in [actual_start, scope_start] if item is not None)
        clipped_end_candidates = [item for item in [actual_end, scope_end] if item is not None]
        clipped_end = min(clipped_end_candidates) if clipped_end_candidates else None
        if clipped_end is not None and clipped_start is not None and clipped_end < clipped_start:
            continue

        intervals = split_by_government(clipped_start, clipped_end)
        if not intervals and "non_entry" not in record["flags"]:
            intervals = [
                {
                    "government_id": government_for_date(clipped_start) or record["source_scope"]["government_id"],
                    "start": clipped_start,
                    "end": clipped_end,
                }
            ]
        if "non_entry" in record["flags"]:
            intervals = []

        for interval in intervals:
            matches = coalition_matches(
                record["party_codes"],
                interval["start"],
                interval["end"],
                party_periods,
            )
            appointments.append(
                {
                    "appointment_id": record["record_id"],
                    "ministry": record["ministerio_canonical"],
                    "ministry_raw": record["ministerio_raw"],
                    "ministry_status_type": record["ministerio_status_type"],
                    "person": record["person_name_canonical"],
                    "person_raw": record["person_name_raw"],
                    "party": record["party"],
                    "party_codes": record["party_codes"],
                    "start": iso_or_none(interval["start"]),
                    "end": iso_or_none(interval["end"]),
                    "actual_start": iso_or_none(actual_start),
                    "actual_end": iso_or_none(actual_end),
                    "appointment_type": classify_appointment_type(record["role_classification"]),
                    "role_classification": record["role_classification"],
                    "government_id": interval["government_id"],
                    "source_url": record["source_url"],
                    "source_section": record["source_section"],
                    "source_snippet": record["source_snippet"],
                    "notes": "; ".join(record["notes"]) if record["notes"] else "",
                    "confidence": record["confidence"],
                    "needs_review": record["needs_review"],
                    "coalition_matches": matches,
                }
            )
    appointments.sort(key=lambda item: (item["start"] or "9999-12-31", item["ministry"], item["person"]))
    return appointments


def split_by_government(start_value, end_value):
    if start_value is None:
        return []
    slices = []
    for government in GOVERNMENT_WINDOWS:
        government_end = government["end"] or date.max
        if interval_overlap(start_value, end_value, government["start"], government_end):
            slice_start = max(start_value, government["start"])
            slice_end_candidates = [item for item in [end_value, government["end"]] if item is not None]
            slice_end = min(slice_end_candidates) if slice_end_candidates else None
            slices.append(
                {
                    "government_id": government["government_id"],
                    "start": slice_start,
                    "end": slice_end,
                }
            )
    return slices


def coalition_matches(party_codes, start_value, end_value, party_periods):
    if not party_codes or start_value is None:
        return []
    matches = []
    for period in party_periods:
        if interval_overlap(start_value, end_value, period["start"], period["end"]):
            overlap_parties = sorted(set(party_codes).intersection(period["parties"]))
            if overlap_parties:
                matches.append(
                    {
                        "period_id": period["period_id"],
                        "government_id": period["government_id"],
                        "matching_parties": overlap_parties,
                    }
                )
    return matches


def build_ministry_index(records):
    ministries = {}
    for record in records:
        key = record["ministerio_canonical"]
        entry = ministries.setdefault(
            key,
            {
                "ministry": key,
                "status_type": record["ministerio_status_type"],
                "raw_variants": set(),
                "source_pages": set(),
            },
        )
        entry["raw_variants"].add(record["ministerio_raw"])
        entry["source_pages"].add(record["source_page_id"])
    result = []
    for key in sorted(ministries):
        item = ministries[key]
        result.append(
            {
                "ministry": item["ministry"],
                "status_type": item["status_type"],
                "raw_variants": sorted(item["raw_variants"]),
                "source_pages": sorted(item["source_pages"]),
            }
        )
    return result


def write_raw_json(page_outputs, events):
    payload = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source_pages": page_outputs,
        "heuristics": [
            "Linhas com 'interino' foram classificadas como assunções temporárias.",
            "Linhas com datas múltiplas ou faltantes foram preservadas com needs_review=true, sem forçar precisão inexistente.",
            "Eventos estruturais só foram inferidos quando a própria linha mencionava criação, transformação, incorporação ou extinção.",
            "O dashboard recorta nomeações à janela do gabinete para evitar duplicidades visuais por carryover entre páginas.",
        ],
        "events": events,
    }
    RAW_JSON_PATH.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def write_dashboard_json(page_outputs, records, events, party_periods):
    governments = [
        {
            "government_id": item["government_id"],
            "label": item["label"],
            "president": item["president"],
            "start": iso_or_none(item["start"]),
            "end": iso_or_none(item["end"]),
        }
        for item in GOVERNMENT_WINDOWS
    ]
    appointments = build_dashboard_appointments(records, party_periods)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source_pages": page_outputs,
        "governments": governments,
        "ministries": build_ministry_index(records),
        "party_periods": [
            {
                "period_id": item["period_id"],
                "government_id": item["government_id"],
                "start": iso_or_none(item["start"]),
                "end": iso_or_none(item["end"]),
                "parties": item["parties"],
            }
            for item in party_periods
        ],
        "appointments": appointments,
        "events": events,
    }
    DASHBOARD_JSON_PATH.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return appointments


def write_events_csv(events):
    fieldnames = [
        "government_id",
        "ministerio_canonical",
        "ministerio_raw",
        "ministerio_status_type",
        "event_type",
        "person_name_canonical",
        "person_name_raw",
        "party",
        "role_classification",
        "event_date_start",
        "event_date_end",
        "event_date_display",
        "predecessor_raw",
        "successor_raw",
        "confidence",
        "needs_review",
        "source_page",
        "source_url",
        "source_section",
        "table_index",
        "row_index",
        "column_name",
        "wikipedia_revision_id",
        "notes",
        "source_snippet",
    ]
    with EVENTS_CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for event in events:
            writer.writerow(
                {
                    "government_id": event["government_id"],
                    "ministerio_canonical": event["ministerio_canonical"],
                    "ministerio_raw": event["ministerio_raw"],
                    "ministerio_status_type": event["ministerio_status_type"],
                    "event_type": event["event_type"],
                    "person_name_canonical": event["person_name_canonical"],
                    "person_name_raw": event["person_name_raw"],
                    "party": event["party"],
                    "role_classification": event["role_classification"],
                    "event_date_start": event["event_date_start"],
                    "event_date_end": event["event_date_end"],
                    "event_date_display": event["event_date_display"],
                    "predecessor_raw": event["predecessor_raw"],
                    "successor_raw": event["successor_raw"],
                    "confidence": event["confidence"],
                    "needs_review": event["needs_review"],
                    "source_page": event["source_page"],
                    "source_url": event["source_url"],
                    "source_section": event["source_section"],
                    "table_index": event["source_locator"]["table_index"],
                    "row_index": event["source_locator"]["row_index"],
                    "column_name": event["source_locator"]["column_name"],
                    "wikipedia_revision_id": event["source_locator"]["wikipedia_revision_id"],
                    "notes": event["notes"],
                    "source_snippet": event["source_snippet"],
                }
            )


def write_intervals_csv(appointments):
    fieldnames = [
        "appointment_id",
        "government_id",
        "ministry",
        "ministry_raw",
        "ministry_status_type",
        "person",
        "person_raw",
        "party",
        "appointment_type",
        "role_classification",
        "start",
        "end",
        "actual_start",
        "actual_end",
        "confidence",
        "needs_review",
        "coalition_period_ids",
        "source_url",
        "source_section",
        "notes",
        "source_snippet",
    ]
    with INTERVALS_CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for appointment in appointments:
            writer.writerow(
                {
                    "appointment_id": appointment["appointment_id"],
                    "government_id": appointment["government_id"],
                    "ministry": appointment["ministry"],
                    "ministry_raw": appointment["ministry_raw"],
                    "ministry_status_type": appointment["ministry_status_type"],
                    "person": appointment["person"],
                    "person_raw": appointment["person_raw"],
                    "party": appointment["party"],
                    "appointment_type": appointment["appointment_type"],
                    "role_classification": appointment["role_classification"],
                    "start": appointment["start"],
                    "end": appointment["end"],
                    "actual_start": appointment["actual_start"],
                    "actual_end": appointment["actual_end"],
                    "confidence": appointment["confidence"],
                    "needs_review": appointment["needs_review"],
                    "coalition_period_ids": ",".join(item["period_id"] for item in appointment["coalition_matches"]),
                    "source_url": appointment["source_url"],
                    "source_section": appointment["source_section"],
                    "notes": appointment["notes"],
                    "source_snippet": appointment["source_snippet"],
                }
            )


def select_examples(appointments):
    wanted = [
        "Casa Civil",
        "Advocacia-Geral da União",
        "Gabinete de Segurança Institucional",
        "Secretaria de Comunicação Social",
    ]
    examples = defaultdict(list)
    for appointment in appointments:
        if appointment["ministry"] in wanted:
            examples[appointment["ministry"]].append(appointment)
    return examples


def write_review_report(records, events, appointments):
    review_records = [record for record in records if record["needs_review"]]
    review_events = [event for event in events if event["needs_review"]]
    interim_appointments = [
        appointment
        for appointment in appointments
        if appointment["appointment_type"] in {"interim", "assumed"}
    ]
    examples = select_examples(appointments)

    lines = [
        "# Relatorio de revisao dos eventos ministeriais",
        "",
        "## Resumo",
        "",
        f"- Linhas extraidas: {len(records)}",
        f"- Eventos emitidos: {len(events)}",
        f"- Intervalos para dashboard: {len(appointments)}",
        f"- Casos needs_review nas linhas: {len(review_records)}",
        f"- Casos needs_review nos eventos: {len(review_events)}",
        f"- Intervalos interinos/assumidos preservados: {len(interim_appointments)}",
        "",
        "## Heuristicas aplicadas",
        "",
        "- `interino`, `interina` e variantes foram tratados como assuncoes temporarias.",
        "- Nao forcei intervalos quando a propria linha tinha multiplas datas, datas faltantes ou indicava que a nomeacao nao se consumou.",
        "- Eventos estruturais so foram emitidos quando a linha mencionava explicitamente criacao, transformacao, incorporacao ou extincao.",
        "- O dashboard recorta intervalos pela janela do gabinete da pagina para evitar duplicidade visual em casos de carryover entre gabinetes.",
        "",
        "## Linhas com needs_review",
        "",
    ]

    for record in sorted(review_records, key=lambda item: item["record_id"])[:40]:
        lines.extend(
            [
                f"- `{record['record_id']}`: {record['ministerio_raw']} — {record['person_name_raw'] or 'sem nome'}",
                f"  Fonte: {record['source_url']}",
                f"  Linha: {record['source_snippet']}",
                f"  Notas: {'; '.join(record['notes']) if record['notes'] else 'sem notas adicionais'}",
            ]
        )

    lines.extend(
        [
            "",
            "## Eventos com needs_review",
            "",
        ]
    )

    for event in review_events[:60]:
        lines.extend(
            [
                f"- {event['event_date_start'] or event['event_date_display'] or 'sem data'} | {event['ministerio_canonical']} | {event['event_type']} | {event['person_name_canonical'] or event['person_name_raw'] or 'sem pessoa'}",
                f"  Fonte: {event['source_url']}",
                f"  Linha: {event['source_snippet']}",
                f"  Notas: {event['notes'] or 'sem notas adicionais'}",
            ]
        )

    lines.extend(
        [
            "",
            "## Checagens manuais sugeridas",
            "",
        ]
    )

    for ministry, items in sorted(examples.items()):
        lines.append(f"- {ministry}:")
        for appointment in items[:5]:
            lines.append(
                "  "
                + f"{appointment['government_id']} | {appointment['person']} | {appointment['start']} -> {appointment['end']} | {appointment['appointment_type']}"
            )

    lines.extend(
        [
            "",
            "## Casos interinos preservados",
            "",
        ]
    )
    for appointment in interim_appointments[:25]:
        lines.append(
            f"- {appointment['ministry']} — {appointment['person']} ({appointment['appointment_type']}) [{appointment['start']} -> {appointment['end']}]"
        )

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    page_outputs, records = extract_records()
    party_periods = load_party_periods()
    events = build_events(records)
    write_raw_json(page_outputs, events)
    appointments = write_dashboard_json(page_outputs, records, events, party_periods)
    write_events_csv(events)
    write_intervals_csv(appointments)
    write_review_report(records, events, appointments)
    print(
        "Wrote",
        RAW_JSON_PATH,
        DASHBOARD_JSON_PATH,
        EVENTS_CSV_PATH,
        INTERVALS_CSV_PATH,
        REPORT_PATH,
    )


if __name__ == "__main__":
    main()
