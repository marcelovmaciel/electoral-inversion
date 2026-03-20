#!/usr/bin/env python3
import csv
import json
import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from scrape_wikipedia_ministerios import (
    PAGES,
    clean_text,
    detect_header,
    fetch_page_html,
    find_column,
    normalize_header,
    table_to_grid,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
WIKI_API = "https://pt.wikipedia.org/w/api.php"
USER_AGENT = "electoral-inversions-bot/0.1 (contact: local)"

ROLE_SECTIONS = {
    "lula_2023_presente": 1,
}

IGNORE_LABELS = {
    "vice presidente",
    "vice-presidente",
    "ministerios",
    "ministros",
    "secretarias",
    "orgaos",
    "gabinete pessoal",
    "governo",
    "mulheres igualdade racial e direitos humanos",
    "trabalho e previdencia",
    "industria com ext e servicos",
}

STOPWORDS = {
    "de",
    "da",
    "do",
    "das",
    "dos",
    "e",
    "para",
    "a",
    "o",
    "as",
    "os",
    "ao",
    "aos",
    "em",
}


def clean_label(label):
    label = clean_text(label)
    label = re.sub(r"\s*\\[.*?\\]\\s*", "", label)
    return clean_text(label)


def is_ignored_label(label):
    norm = normalize_header(label)
    if norm in IGNORE_LABELS:
        return True
    if norm.startswith("secretarias "):
        return True
    if norm.startswith("orgaos "):
        return True
    return False


def extract_navbox_labels(html):
    soup = BeautifulSoup(html, "html.parser")
    labels = []
    for table in soup.find_all("table"):
        classes = table.get("class") or []
        if not any("navbox" in c for c in classes):
            continue
        title = table.find("th", class_="navbox-title")
        title_text = clean_text(title.get_text(" ", strip=True)) if title else ""
        if "gabinete" not in normalize_header(title_text):
            continue
        for row in table.find_all("tr"):
            th = row.find("th")
            if not th:
                continue
            if "navbox-title" in (th.get("class") or []):
                continue
            label = clean_label(th.get_text(" ", strip=True))
            if not label or label.startswith("v d e"):
                continue
            if is_ignored_label(label):
                continue
            labels.append(label)
    return sorted(set(labels))


def extract_role_names(html):
    soup = BeautifulSoup(html, "html.parser")
    roles = set()
    for table in soup.find_all("table"):
        rows = table_to_grid(table)
        header_idx, header = detect_header(rows)
        if header is None:
            continue
        role_idx = find_column(header, ["cargo", "ministerio"])
        if role_idx is None:
            continue
        for row in rows[header_idx + 1 :]:
            if role_idx >= len(row):
                continue
            role = clean_text(row[role_idx])
            if role and normalize_header(role) not in {"cargo", "ministerio"}:
                roles.add(role)
    return sorted(roles)


def fetch_section_html(page_info, section):
    params = {
        "action": "parse",
        "prop": "text",
        "format": "json",
        "section": section,
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
        raise RuntimeError(f"missing parse content for {page_info} section {section}")
    return data["parse"]["text"]["*"]


def token_match(label_norm, role_norm):
    label_tokens = [t for t in label_norm.split() if t not in STOPWORDS]
    role_tokens = [t for t in role_norm.split() if t not in STOPWORDS]
    if not label_tokens or not role_tokens:
        return False
    matched = 0
    for token in label_tokens:
        if any(rt.startswith(token) for rt in role_tokens):
            matched += 1
    return matched / len(label_tokens) >= 0.6


def tokenize(value):
    return [t for t in value.split() if t not in STOPWORDS]


def match_roles(nav_labels, roles):
    role_norms = {role: normalize_header(role) for role in roles}
    role_tokens = {role: tokenize(role_norm) for role, role_norm in role_norms.items()}
    matched = set()
    for label in nav_labels:
        label_norm = normalize_header(label)
        label_tokens = tokenize(label_norm)
        best_role = None
        best_score = None
        best_token_len = None
        best_len = None
        for role, role_norm in role_norms.items():
            score = None
            if label_norm == role_norm:
                score = 3
            elif label_tokens and set(label_tokens).issubset(role_tokens[role]):
                score = 2
            elif role_tokens[role] and set(role_tokens[role]).issubset(label_tokens):
                score = 1.5
            elif token_match(label_norm, role_norm):
                score = 1
            if score is None:
                continue
            if (
                best_score is None
                or score > best_score
                or (score == best_score and len(role_tokens[role]) < best_token_len)
                or (
                    score == best_score
                    and len(role_tokens[role]) == best_token_len
                    and len(role_norm) < best_len
                )
            ):
                best_role = role
                best_score = score
                best_token_len = len(role_tokens[role])
                best_len = len(role_norm)
        if best_role is None:
            print(
                f"WARNING: no role match for nav label '{label}'",
                file=sys.stderr,
            )
            continue
        matched.add(best_role)
    return sorted(matched)


def page_url(page_info):
    if "pageid" in page_info:
        return f"https://pt.wikipedia.org/?curid={page_info['pageid']}"
    page = page_info["page"].replace(" ", "_")
    return f"https://pt.wikipedia.org/wiki/{page}"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    orgaos_by_gov = {}
    for page_info in PAGES:
        label = page_info["label"]
        full_html = fetch_page_html(page_info)
        nav_labels = extract_navbox_labels(full_html)
        section = ROLE_SECTIONS.get(label)
        roles_html = (
            fetch_section_html(page_info, section) if section is not None else full_html
        )
        roles = extract_role_names(roles_html)
        orgaos = match_roles(nav_labels, roles)
        if not orgaos:
            print(f"WARNING: no orgaos found for {label}", file=sys.stderr)
        orgaos_by_gov[label] = {
            "governo": page_info.get("gov_name", label),
            "fonte": page_url(page_info),
            "orgaos": orgaos,
        }

    json_path = OUTPUT_DIR / "orgaos_ministeriais.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(orgaos_by_gov, f, ensure_ascii=True, indent=2)

    csv_path = OUTPUT_DIR / "orgaos_ministeriais.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["governo", "orgao"])
        for label in sorted(orgaos_by_gov):
            for orgao in orgaos_by_gov[label]["orgaos"]:
                writer.writerow([label, orgao])

    md_path = OUTPUT_DIR / "orgaos_ministeriais.md"
    with md_path.open("w", encoding="utf-8") as f:
        f.write("# Orgaos com status ministerial por governo\n\n")
        for label in sorted(orgaos_by_gov):
            info = orgaos_by_gov[label]
            f.write(f"## {info['governo']}\n\n")
            f.write(f"Fonte: {info['fonte']}\n\n")
            for orgao in info["orgaos"]:
                f.write(f"- {orgao}\n")
            f.write("\n")

    print(f"Wrote {json_path}, {csv_path}, {md_path}")


if __name__ == "__main__":
    main()
