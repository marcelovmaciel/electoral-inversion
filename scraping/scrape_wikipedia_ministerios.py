#!/usr/bin/env python3
import csv
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

WIKI_API = "https://pt.wikipedia.org/w/api.php"
USER_AGENT = "electoral-inversions-bot/0.1 (contact: local)"

PAGES = [
    {
        "page": "Lista de membros do gabinete de Dilma Rousseff",
        "label": "dilma_rousseff",
        "gov_name": "Dilma Rousseff",
        "term_end": date(2016, 5, 11),
    },
    {
        "page": "Lista de membros do gabinete de Michel Temer",
        "label": "michel_temer",
        "gov_name": "Michel Temer",
        "term_end": date(2018, 12, 31),
    },
    {
        "page": "Lista de membros do gabinete de Jair Bolsonaro",
        "label": "jair_bolsonaro",
        "gov_name": "Jair Bolsonaro",
        "term_end": date(2022, 12, 31),
    },
    {
        "pageid": 7020219,
        "label": "lula_2023_presente",
        "gov_name": "Luiz Inacio Lula da Silva (2023-presente)",
    },
]

TARGET_START_YEAR = 2015
TARGET_END_YEAR = date.today().year

START_TERMS = ("posse", "inicio", "tomou posse", "entrada")
END_TERMS = ("saida", "fim", "ate o dia", "ate")


def strip_accents(value):
    if value is None:
        return ""
    return (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
    )


def normalize_space(value):
    value = re.sub(r"\s+", " ", value or "").strip()
    return value


def normalize_header(value):
    value = strip_accents(value).lower()
    value = re.sub(r"[^a-z0-9 ]", " ", value)
    return normalize_space(value)


def clean_text(value):
    value = normalize_space(value)
    value = re.sub(r"\[.*?\]", "", value)
    return normalize_space(value)


def fetch_page_html(page_info):
    params = {
        "action": "parse",
        "prop": "text",
        "format": "json",
    }
    if "pageid" in page_info:
        params["pageid"] = page_info["pageid"]
    else:
        params["page"] = page_info["page"]
    resp = requests.get(
        WIKI_API, params=params, headers={"User-Agent": USER_AGENT}, timeout=30
    )
    resp.raise_for_status()
    data = resp.json()
    if "parse" not in data:
        raise RuntimeError(f"missing parse content for {page_info}")
    return data["parse"]["text"]["*"]


def table_to_grid(table):
    rows = []
    span_map = {}
    for tr in table.find_all("tr"):
        row = []
        col_idx = 0
        cells = tr.find_all(["th", "td"])
        while col_idx in span_map:
            row.append(span_map[col_idx]["value"])
            span_map[col_idx]["rows_left"] -= 1
            if span_map[col_idx]["rows_left"] <= 0:
                del span_map[col_idx]
            col_idx += 1
        for cell in cells:
            while col_idx in span_map:
                row.append(span_map[col_idx]["value"])
                span_map[col_idx]["rows_left"] -= 1
                if span_map[col_idx]["rows_left"] <= 0:
                    del span_map[col_idx]
                col_idx += 1
            text = clean_text(cell.get_text(" ", strip=True))
            colspan = int(cell.get("colspan", 1))
            rowspan = int(cell.get("rowspan", 1))
            for _ in range(colspan):
                row.append(text)
                if rowspan > 1:
                    span_map[col_idx] = {
                        "value": text,
                        "rows_left": rowspan - 1,
                    }
                col_idx += 1
        rows.append(row)
    max_cols = max((len(r) for r in rows), default=0)
    for row in rows:
        if len(row) < max_cols:
            row.extend([""] * (max_cols - len(row)))
    return rows


def detect_header(rows):
    for idx, row in enumerate(rows):
        normalized = [normalize_header(cell) for cell in row]
        if not any(normalized):
            continue
        has_party = any("partido" in cell for cell in normalized)
        has_start = any(any(term in cell for term in START_TERMS) for cell in normalized)
        if has_party and has_start:
            return idx, normalized
    return None, None


def find_column(header, options):
    for idx, cell in enumerate(header):
        for option in options:
            if option in cell:
                return idx
    return None


def parse_date_pt(value, fallback_year=None):
    if not value:
        return None
    value = clean_text(value)
    value = strip_accents(value).lower()
    if not value or value in {"-", "—"}:
        return None
    if any(term in value for term in ["presente", "atual", "hoje", "em exercicio"]):
        return None

    value = re.sub(r"\(.*?\)", "", value).strip()
    value = re.sub(r"(\d+)\s*\.?\s*o\.?\b", r"\1", value)
    value = re.sub(r"\bno dia\b", "", value)
    value = value.replace(",", " ")
    value = normalize_space(value)

    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", value)
    if m:
        day, month, year = map(int, m.groups())
        return date(year, month, day)

    m = re.search(r"(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})", value)
    if not m:
        m = re.search(r"(\d{1,2})\s+de\s+([a-z]+)\s+(\d{4})", value)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).rstrip(".")
        year = int(m.group(3))
        months = {
            "janeiro": 1,
            "fevereiro": 2,
            "marco": 3,
            "abril": 4,
            "maio": 5,
            "junho": 6,
            "julho": 7,
            "agosto": 8,
            "setembro": 9,
            "outubro": 10,
            "novembro": 11,
            "dezembro": 12,
            "jan": 1,
            "fev": 2,
            "mar": 3,
            "abr": 4,
            "mai": 5,
            "jun": 6,
            "jul": 7,
            "ago": 8,
            "set": 9,
            "out": 10,
            "nov": 11,
            "dez": 12,
        }
        month = months.get(month_name)
        if month:
            return date(year, month, day)
    if fallback_year:
        m = re.search(r"(\d{1,2})\s+de\s+([a-z]+)", value)
        if m:
            day = int(m.group(1))
            month_name = m.group(2).rstrip(".")
            months = {
                "janeiro": 1,
                "fevereiro": 2,
                "marco": 3,
                "abril": 4,
                "maio": 5,
                "junho": 6,
                "julho": 7,
                "agosto": 8,
                "setembro": 9,
                "outubro": 10,
                "novembro": 11,
                "dezembro": 12,
                "jan": 1,
                "fev": 2,
                "mar": 3,
                "abr": 4,
                "mai": 5,
                "jun": 6,
                "jul": 7,
                "ago": 8,
                "set": 9,
                "out": 10,
                "nov": 11,
                "dez": 12,
            }
            month = months.get(month_name)
            if month:
                return date(fallback_year, month, day)
    return None


def split_parties(value):
    value = clean_text(value)
    if not value:
        return []
    normalized = strip_accents(value).lower()
    if normalized in {"sem partido", "sem-partido", "independente"}:
        return []
    uf_codes = {
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
    if "/" in value:
        parts = [p.strip() for p in value.split("/") if p.strip()]
    elif " e " in value:
        parts = [p.strip() for p in value.split(" e ") if p.strip()]
    else:
        parts = [value]
    cleaned = []
    for part in parts:
        part_clean = part.strip()
        if not part_clean or re.fullmatch(r"[-—]+", part_clean):
            continue
        norm = strip_accents(part_clean).lower()
        if norm in {"sem partido", "sem-partido", "independente"}:
            continue
        if norm in uf_codes:
            continue
        cleaned.append(part_clean)
    return cleaned


def load_orgaos_ministeriais(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing orgaos list at {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    orgaos = {}
    for label, info in data.items():
        orgaos[label] = {normalize_header(item) for item in info.get("orgaos", [])}
    return orgaos


def build_periods(entries, start_year, end_year):
    events = defaultdict(list)
    for entry in entries:
        start_date = entry["start_date"]
        end_date = entry["end_date"]
        if end_date < start_date:
            print(
                f"WARNING: end before start {start_date} > {end_date}",
                file=sys.stderr,
            )
            continue
        for party in entry["parties"]:
            events[start_date].append((party, 1))
            events[end_date + timedelta(days=1)].append((party, -1))

    start_range = date(start_year, 1, 1)
    end_range = date(end_year + 1, 1, 1)
    boundary_dates = set(events.keys())
    boundary_dates.add(start_range)
    boundary_dates.add(end_range)
    sorted_dates = sorted(boundary_dates)
    counts = Counter()
    periods = []
    for idx, current_date in enumerate(sorted_dates):
        for party, delta in events.get(current_date, []):
            counts[party] += delta
            if counts[party] <= 0:
                counts.pop(party, None)
        if current_date >= end_range:
            break
        if idx == len(sorted_dates) - 1:
            break
        next_date = sorted_dates[idx + 1]
        if next_date <= start_range:
            continue
        interval_start = max(current_date, start_range)
        interval_end = min(next_date, end_range)
        if interval_start >= interval_end:
            continue
        parties = sorted(counts.keys())
        periods.append(
            {
                "start": interval_start,
                "end": interval_end,
                "parties": parties,
            }
        )
    return periods


def iter_years(start, end):
    year = start.year
    while year <= end.year:
        yield year
        year += 1


def collect_minister_parties(html, label, allowed_orgaos, term_end=None):
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    for table in soup.find_all("table"):
        rows = table_to_grid(table)
        header_idx, header = detect_header(rows)
        if header is None:
            continue

        role_idx = find_column(header, ["cargo", "ministerio"])
        party_idx = find_column(header, ["partido"])
        start_idx = find_column(header, list(START_TERMS))
        end_idx = find_column(header, list(END_TERMS))
        if role_idx is None or party_idx is None or start_idx is None:
            print(
                f"WARNING: missing role/party/start columns in {label}",
                file=sys.stderr,
            )
            continue

        for row in rows[header_idx + 1 :]:
            if (
                role_idx >= len(row)
                or party_idx >= len(row)
                or start_idx >= len(row)
            ):
                continue
            role_cell = row[role_idx]
            if normalize_header(role_cell) not in allowed_orgaos:
                continue
            party_cell = row[party_idx]
            if normalize_header(party_cell) == "partido":
                continue
            start_cell = row[start_idx]
            end_cell = row[end_idx] if end_idx is not None and end_idx < len(row) else ""

            parties = split_parties(party_cell)
            if not parties:
                continue

            start_date = parse_date_pt(start_cell)
            if start_date is None:
                if not re.search(r"\d", start_cell or ""):
                    continue
                print(
                    f"WARNING: could not parse start date '{start_cell}' in {label}",
                    file=sys.stderr,
                )
                continue
            if term_end is not None and start_date > term_end:
                continue
            end_date = parse_date_pt(end_cell, fallback_year=start_date.year)
            if end_date is None and re.search(r"\d", end_cell or ""):
                print(
                    f"WARNING: could not parse end date '{end_cell}' in {label}",
                    file=sys.stderr,
                )
            if end_date is None:
                end_date = term_end or date.today()
            if term_end is not None and end_date > term_end:
                end_date = term_end
            entries.append(
                {
                    "parties": parties,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
    return entries


def main():
    output_dir = Path(__file__).resolve().parent / "output"
    orgaos_path = output_dir / "orgaos_ministeriais.json"
    orgaos_map = load_orgaos_ministeriais(orgaos_path)

    entries = []
    for page_info in PAGES:
        label = page_info["label"]
        html = fetch_page_html(page_info)
        allowed_orgaos = orgaos_map.get(label)
        if not allowed_orgaos:
            print(f"WARNING: no orgaos list for {label}", file=sys.stderr)
            continue
        term_end = page_info.get("term_end")
        page_entries = collect_minister_parties(
            html, label, allowed_orgaos, term_end=term_end
        )
        if not page_entries:
            print(f"WARNING: no entries parsed for {label}", file=sys.stderr)
        entries.extend(page_entries)

    periods = build_periods(entries, TARGET_START_YEAR, TARGET_END_YEAR)
    periods_by_year = defaultdict(list)
    for period in periods:
        if not period["parties"]:
            continue
        periods_by_year[period["start"].year].append(period)

    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "partidos_por_periodo.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["periodo", "data_inicio", "data_fim", "partido"])
        for year in sorted(periods_by_year):
            for idx, period in enumerate(periods_by_year[year], start=1):
                key = f"{year}.{idx}"
                start_date = period["start"].isoformat()
                end_date = (period["end"] - timedelta(days=1)).isoformat()
                for party in period["parties"]:
                    writer.writerow([key, start_date, end_date, party])

    json_path = output_dir / "partidos_por_periodo.json"
    with json_path.open("w", encoding="utf-8") as f:
        payload = {}
        for year in sorted(periods_by_year):
            for idx, period in enumerate(periods_by_year[year], start=1):
                key = f"{year}.{idx}"
                payload[key] = {
                    "data_inicio": period["start"].isoformat(),
                    "data_fim": (period["end"] - timedelta(days=1)).isoformat(),
                    "partidos": period["parties"],
                }
        json.dump(payload, f, ensure_ascii=True, indent=2)

    print(f"Wrote {csv_path} and {json_path}")


if __name__ == "__main__":
    main()
