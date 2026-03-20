"""Agente 6: geração de artefatos finais (JSON/CSV/Markdown)."""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from .constants import (
    EVIDENCE_METHOD,
    EVIDENCE_TABLE,
    EVIDENCE_TEXT_FRAGMENT,
    EVIDENCE_EXTRACTION_ERROR,
    EXTRACTION_FAILED,
    EXTRACTION_NEEDS_OCR,
    EXTRACTION_READY,
    MISMATCH_AMBIGUOUS,
    MISMATCH_CLASSIFIED,
    MISMATCH_NOT_FOUND,
    MISMATCH_ONLY_MENTION,
    MISMATCH_TABLE_NO_CLASS,
    PDF_TYPE_TEXT,
)
from .models import ClassificationRecord, EvidenceItem, MatchResult, PDFInventoryEntry
from .utils import ensure_parent_dir, json_dumps_pretty, markdown_anchor, now_iso, shrink_text


def write_json(data: Any, path: Path) -> None:
    """Escreve JSON formatado em UTF-8."""
    ensure_parent_dir(path)
    path.write_text(json_dumps_pretty(data), encoding="utf-8")


def _write_rows(rows: list[dict[str, Any]], path: Path, fieldnames: list[str] | None = None) -> None:
    ensure_parent_dir(path)
    if not rows and fieldnames is None:
        fieldnames = []
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []

    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_csv_from_records(records: list[ClassificationRecord], path: Path) -> None:
    """Serializa registros estruturados para CSV."""
    rows = [record.to_dict() for record in records]
    fieldnames = list(rows[0].keys()) if rows else [
        "pdf_filename",
        "citation_key_candidate",
        "publication_year",
        "reference_time_raw",
        "reference_year_start",
        "reference_year_end",
        "year_type",
        "party_name_raw",
        "party_name_normalized_minimal",
        "mentioned_in_article",
        "appears_in_relevant_table",
        "explicitly_classified",
        "ideology_value_raw",
        "ideology_value_type",
        "scale_type",
        "scale_description",
        "methodology_note",
        "page_number",
        "quoted_snippet",
        "evidence_type",
        "extraction_confidence",
        "ambiguity_flag",
        "parser_notes",
    ]
    _write_rows(rows, path, fieldnames)


def write_match_results(results: list[MatchResult], path: Path) -> None:
    """Escreve resultado de matching em CSV."""
    rows = [result.to_dict() for result in results]
    fieldnames = list(rows[0].keys()) if rows else [
        "party_from_csv",
        "period_from_csv",
        "exact_match_found",
        "conservative_match_found",
        "probable_match_found",
        "matched_party_name",
        "matched_pdf_count",
        "match_method",
        "mismatch_type",
        "notes",
    ]
    _write_rows(rows, path, fieldnames)


def write_per_pdf_summary(
    inventory: list[PDFInventoryEntry],
    records: list[ClassificationRecord],
    evidence_by_pdf: dict[str, list[EvidenceItem]],
    path: Path,
) -> None:
    """Gera tabela resumida por PDF para auditoria rápida."""
    rows: list[dict[str, Any]] = []
    grouped_records: dict[str, list[ClassificationRecord]] = defaultdict(list)
    for record in records:
        grouped_records[record.pdf_filename].append(record)

    for entry in inventory:
        pdf_records = grouped_records.get(entry.pdf_filename, [])
        evidence = evidence_by_pdf.get(entry.pdf_filename, [])
        rows.append(
            {
                "pdf_filename": entry.pdf_filename,
                "pdf_type": entry.pdf_type,
                "extraction_status": entry.extraction_status,
                "page_count": entry.page_count,
                "evidence_count": len(evidence),
                "text_fragment_count": sum(1 for e in evidence if e.evidence_type == EVIDENCE_TEXT_FRAGMENT),
                "record_count": len(pdf_records),
                "mentioned_count": sum(1 for r in pdf_records if r.mentioned_in_article),
                "table_count": sum(1 for r in pdf_records if r.appears_in_relevant_table),
                "explicit_count": sum(1 for r in pdf_records if r.explicitly_classified),
                "ambiguity_count": sum(1 for r in pdf_records if r.ambiguity_flag),
            }
        )

    _write_rows(rows, path)


def _to_float_safe(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip().replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_category_label(text: str) -> str:
    lowered = text.lower().strip()
    mapping = {
        "extrema-esquerda": "extrema-esquerda",
        "esquerda": "esquerda",
        "centro-esquerda": "centro-esquerda",
        "centro": "centro",
        "centro-direita": "centro-direita",
        "direita": "direita",
        "extrema-direita": "extrema-direita",
    }
    return mapping.get(lowered, text.strip())


def _extract_scale_rules_from_evidence(evidence: list[EvidenceItem]) -> list[dict[str, Any]]:
    pattern = re.compile(
        r"(?:entre|de)\s*(\d+(?:[.,]\d+)?)\s*(?:a|e)\s*(\d+(?:[.,]\d+)?)"
        r"(?:[^\n]{0,120}?)\b("
        r"extrema-esquerda|centro-esquerda|centro-direita|extrema-direita|esquerda|direita|centro"
        r")\b",
        flags=re.IGNORECASE,
    )

    rules: list[dict[str, Any]] = []
    seen: set[tuple[float, float, str]] = set()

    for item in evidence:
        snippet = item.quoted_snippet or ""
        for match in pattern.finditer(snippet):
            start = _to_float_safe(match.group(1))
            end = _to_float_safe(match.group(2))
            category = _normalize_category_label(match.group(3))
            if start is None or end is None:
                continue
            low = min(start, end)
            high = max(start, end)
            key = (low, high, category)
            if key in seen:
                continue
            seen.add(key)
            rules.append(
                {
                    "range_start": low,
                    "range_end": high,
                    "category": category,
                    "page_number": item.page_number,
                    "snippet": shrink_text(snippet, 220),
                }
            )

    return sorted(rules, key=lambda x: (x["range_start"], x["range_end"], x["category"]))


def _extract_scale_rules_codato_2023(pdf_path: Path) -> list[dict[str, Any]]:
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return []

    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) < 16:
            return []
        page_text = (pdf.pages[15].extract_text() or "").replace("-\n-", "-").replace("\n", " ")

    pattern = re.compile(
        r"(?:entre|de)\s*(\d+(?:[.,]\d+)?)\s*(?:a|e)\s*(\d+(?:[.,]\d+)?)"
        r"(?:.{0,180}?)\b("
        r"extrema-esquerda|centro-esquerda|centro-direita|extrema-direita|esquerda|direita|centro"
        r")\b",
        flags=re.IGNORECASE,
    )

    rules: list[dict[str, Any]] = []
    seen: set[tuple[float, float, str]] = set()
    for match in pattern.finditer(page_text):
        start = _to_float_safe(match.group(1))
        end = _to_float_safe(match.group(2))
        category = _normalize_category_label(match.group(3))
        if start is None or end is None:
            continue
        low = min(start, end)
        high = max(start, end)
        key = (low, high, category)
        if key in seen:
            continue
        seen.add(key)
        snippet = shrink_text(page_text[max(0, match.start() - 30) : match.end() + 30], 220)
        rules.append(
            {
                "range_start": low,
                "range_end": high,
                "category": category,
                "page_number": 16,
                "snippet": snippet,
            }
        )

    return sorted(rules, key=lambda x: (x["range_start"], x["range_end"], x["category"]))


def write_party_ordinal_classification_codato_2023(
    *,
    pdf_path: Path,
    csv_path: Path,
    json_path: Path,
) -> list[dict[str, Any]]:
    """Extrai Tabela 1 do artigo de 2023 e produz posição ordinal + classificação."""
    try:
        import pdfplumber  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"pdfplumber indisponível para extração da tabela de 2023: {exc}") from exc

    table_pattern = re.compile(
        r"^(?P<party>[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ]{1,24})\s+"
        r"(?P<mean>\d+(?:[.,]\d+)?)\s+"
        r"(?P<median>\d+(?:[.,]\d+)?)\s+"
        r"(?P<mode>\d+(?:[.,]\d+)?)\s+"
        r"(?P<stddev>\d+(?:[.,]\d+)?)\s+"
        r"(?P<n>\d{2,4})\s+"
        r"(?P<cv>\d+(?:[.,]\d+)?)$"
    )

    rows_raw: list[dict[str, Any]] = []
    with pdfplumber.open(pdf_path) as pdf:
        target_pages = [6, 7]  # Tabela 1 no artigo de 2023.
        for page_number in target_pages:
            if page_number > len(pdf.pages):
                continue
            page = pdf.pages[page_number - 1]
            text = page.extract_text() or ""
            for line in text.splitlines():
                compact = line.strip()
                match = table_pattern.match(compact)
                if not match:
                    continue
                group = match.groupdict()
                party = group["party"]
                mean_value = _to_float_safe(group["mean"])
                if mean_value is None:
                    continue
                rows_raw.append(
                    {
                        "party_name_raw": party,
                        "ideology_value_raw": group["mean"],
                        "ideology_value_numeric": mean_value,
                        "median_raw": group["median"],
                        "mode_raw": group["mode"],
                        "stddev_raw": group["stddev"],
                        "n_raw": group["n"],
                        "coef_var_raw": group["cv"],
                        "page_number": page_number,
                        "quoted_snippet": compact,
                    }
                )

    dedup: dict[str, dict[str, Any]] = {}
    for row in rows_raw:
        party = row["party_name_raw"]
        if party not in dedup:
            dedup[party] = row

    rows = sorted(dedup.values(), key=lambda x: (x["ideology_value_numeric"], x["party_name_raw"]))
    for idx, row in enumerate(rows, start=1):
        row["ordinal_position"] = idx
        row["pdf_filename"] = pdf_path.name

    scale_rules = _extract_scale_rules_codato_2023(pdf_path)
    for row in rows:
        value = row["ideology_value_numeric"]
        matching = [
            rule for rule in scale_rules if rule["range_start"] <= value <= rule["range_end"]
        ]
        if len(matching) == 1:
            row["classification_label"] = matching[0]["category"]
            row["classification_source"] = "derived_from_explicit_scale_rule_page16"
            row["scale_rule_reference"] = (
                f"p.16 {matching[0]['range_start']}–{matching[0]['range_end']} => {matching[0]['category']}"
            )
        elif len(matching) > 1:
            row["classification_label"] = "ambiguous"
            row["classification_source"] = "ambiguous_scale_rule_overlap"
            row["scale_rule_reference"] = "; ".join(
                f"{rule['range_start']}–{rule['range_end']}:{rule['category']}" for rule in matching
            )
        else:
            row["classification_label"] = None
            row["classification_source"] = "not_available"
            row["scale_rule_reference"] = None

    ensure_parent_dir(csv_path)
    with csv_path.open("w", encoding="utf-8", newline="") as file_obj:
        fieldnames = [
            "pdf_filename",
            "party_name_raw",
            "ideology_value_raw",
            "ideology_value_numeric",
            "ordinal_position",
            "classification_label",
            "classification_source",
            "scale_rule_reference",
            "median_raw",
            "mode_raw",
            "stddev_raw",
            "n_raw",
            "coef_var_raw",
            "page_number",
            "quoted_snippet",
        ]
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    write_json(rows, json_path)
    return rows


def write_party_ordinal_classification(
    *,
    records: list[ClassificationRecord],
    evidence: list[EvidenceItem],
    pdf_filename: str,
    csv_path: Path,
    json_path: Path,
) -> list[dict[str, Any]]:
    """Gera tabela por partido com posição ordinal e classificação textual/categórica."""
    from collections import Counter

    pdf_records = [record for record in records if record.pdf_filename == pdf_filename]
    numeric_records = [
        record
        for record in pdf_records
        if record.explicitly_classified and record.ideology_value_type == "NUMERIC"
    ]

    # Captura classificação categórica explícita quando houver.
    categorical_by_party: dict[str, str] = {}
    for record in pdf_records:
        if not record.explicitly_classified:
            continue
        if record.ideology_value_type != "CATEGORICAL":
            continue
        if not record.party_name_raw or not record.ideology_value_raw:
            continue
        categorical_by_party[record.party_name_raw] = _normalize_category_label(record.ideology_value_raw)

    # Regras de escala explícitas no texto (ex.: "entre 0 e 1,5 ... extrema-esquerda").
    scale_rules = _extract_scale_rules_from_evidence(evidence)

    by_party: dict[str, list[ClassificationRecord]] = defaultdict(list)
    for record in numeric_records:
        if record.party_name_raw:
            by_party[record.party_name_raw].append(record)

    rows: list[dict[str, Any]] = []
    for party, party_records in by_party.items():
        values: list[tuple[float, str, ClassificationRecord]] = []
        for record in party_records:
            parsed = _to_float_safe(record.ideology_value_raw)
            if parsed is None:
                continue
            values.append((parsed, record.ideology_value_raw or "", record))
        if not values:
            continue

        raw_counter = Counter(raw for _, raw, _ in values)
        best_raw, _ = sorted(raw_counter.items(), key=lambda x: (-x[1], _to_float_safe(x[0]) or 0))[0]
        best_value = _to_float_safe(best_raw)
        if best_value is None:
            continue
        representative = next(record for _, raw, record in values if raw == best_raw)

        category = categorical_by_party.get(party)
        source = "direct_text"
        rule_ref = None
        if not category:
            matching_rules = [
                rule
                for rule in scale_rules
                if rule["range_start"] <= best_value <= rule["range_end"]
            ]
            if len(matching_rules) == 1:
                category = matching_rules[0]["category"]
                source = "derived_from_explicit_scale_rule"
                rule_ref = (
                    f"p.{matching_rules[0]['page_number']}: "
                    f"{matching_rules[0]['range_start']}–{matching_rules[0]['range_end']} => {category}"
                )
            elif len(matching_rules) > 1:
                category = "ambiguous"
                source = "ambiguous_scale_rule_overlap"
                rule_ref = "; ".join(
                    f"{rule['range_start']}–{rule['range_end']}:{rule['category']}" for rule in matching_rules
                )
            else:
                category = None
                source = "not_available"

        rows.append(
            {
                "pdf_filename": pdf_filename,
                "party_name_raw": party,
                "ideology_value_raw": best_raw,
                "ideology_value_numeric": best_value,
                "classification_label": category,
                "classification_source": source,
                "scale_rule_reference": rule_ref,
                "page_number": representative.page_number,
                "quoted_snippet": shrink_text(representative.quoted_snippet, 240),
            }
        )

    rows = sorted(rows, key=lambda x: (x["ideology_value_numeric"], x["party_name_raw"]))
    for idx, row in enumerate(rows, start=1):
        row["ordinal_position"] = idx

    ensure_parent_dir(csv_path)
    with csv_path.open("w", encoding="utf-8", newline="") as file_obj:
        fieldnames = [
            "pdf_filename",
            "party_name_raw",
            "ideology_value_raw",
            "ideology_value_numeric",
            "ordinal_position",
            "classification_label",
            "classification_source",
            "scale_rule_reference",
            "page_number",
            "quoted_snippet",
        ]
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    write_json(rows, json_path)
    return rows


def _format_party_listing(records: list[ClassificationRecord], include_classification: bool) -> list[str]:
    lines: list[str] = []
    if not records:
        return lines

    grouped: dict[str, list[ClassificationRecord]] = defaultdict(list)
    for record in records:
        party = record.party_name_raw or "<sem_partido>"
        grouped[party].append(record)

    for party in sorted(grouped):
        items = grouped[party]
        pages = sorted({item.page_number for item in items})
        page_text = ", ".join(str(page) for page in pages)

        if include_classification:
            best = next((item for item in items if item.explicitly_classified), items[0])
            lines.append(
                f"- **{party}** | valor: `{best.ideology_value_raw}` | tipo: `{best.ideology_value_type}` | "
                f"escala: `{best.scale_type}` | período: `{best.reference_time_raw or 'n/d'}` | página: {best.page_number}"
            )
            lines.append(f"  snippet: `{shrink_text(best.quoted_snippet, 220)}`")
        else:
            best = items[0]
            lines.append(f"- **{party}** | páginas: {page_text}")
            lines.append(f"  snippet: `{shrink_text(best.quoted_snippet, 200)}`")

    return lines


def _group_results_by_party(results: list[MatchResult]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for result in results:
        key = result.party_from_csv
        if key not in grouped:
            grouped[key] = {
                "party": key,
                "statuses": set(),
                "periods": set(),
                "methods": set(),
                "matched_names": set(),
                "max_pdf_count": 0,
                "notes": [],
            }
        row = grouped[key]
        row["statuses"].add(result.mismatch_type)
        if result.period_from_csv:
            row["periods"].add(result.period_from_csv)
        if result.match_method:
            row["methods"].add(result.match_method)
        if result.matched_party_name:
            row["matched_names"].add(result.matched_party_name)
        row["max_pdf_count"] = max(row["max_pdf_count"], result.matched_pdf_count)
        if result.notes:
            row["notes"].append(result.notes)
    return grouped


def _best_status(statuses: set[str]) -> str:
    if MISMATCH_CLASSIFIED in statuses:
        return MISMATCH_CLASSIFIED
    if MISMATCH_TABLE_NO_CLASS in statuses:
        return MISMATCH_TABLE_NO_CLASS
    if MISMATCH_ONLY_MENTION in statuses:
        return MISMATCH_ONLY_MENTION
    if MISMATCH_AMBIGUOUS in statuses:
        return MISMATCH_AMBIGUOUS
    if MISMATCH_NOT_FOUND in statuses:
        return MISMATCH_NOT_FOUND
    return sorted(statuses)[0] if statuses else MISMATCH_NOT_FOUND


def write_audit_report(
    path: Path,
    *,
    inventory: list[PDFInventoryEntry],
    evidence_by_pdf: dict[str, list[EvidenceItem]],
    year_mentions_by_pdf: dict[str, list[dict]],
    records: list[ClassificationRecord],
    match_results: list[MatchResult],
    audit_data: dict,
    extraction_failures: list[dict],
    dependency_notes: list[str],
    pdf_input_dir: Path,
    csv_input_path: Path,
) -> None:
    """Escreve relatório final de auditoria legível em Markdown."""
    ensure_parent_dir(path)

    records_by_pdf: dict[str, list[ClassificationRecord]] = defaultdict(list)
    for record in records:
        records_by_pdf[record.pdf_filename].append(record)

    evidence_total = sum(len(items) for items in evidence_by_pdf.values())
    total_pdfs = len(inventory)
    total_text = sum(1 for item in inventory if item.pdf_type == PDF_TYPE_TEXT)
    total_ocr = sum(1 for item in inventory if item.extraction_status == EXTRACTION_NEEDS_OCR)
    total_failed = sum(1 for item in inventory if item.extraction_status == EXTRACTION_FAILED)

    mentioned_total = sum(1 for record in records if record.mentioned_in_article)
    table_total = sum(1 for record in records if record.appears_in_relevant_table)
    explicit_total = sum(1 for record in records if record.explicitly_classified)

    match_confident = sum(
        1
        for row in match_results
        if (row.exact_match_found or row.conservative_match_found)
        and row.mismatch_type != MISMATCH_AMBIGUOUS
    )
    mismatch_total = sum(1 for row in match_results if row.mismatch_type != MISMATCH_CLASSIFIED)

    high_risk = audit_data.get("high_risk_pdfs", [])
    risk_preview = ", ".join(high_risk[:5]) if high_risk else "nenhum"

    report_lines: list[str] = []

    # 1. Título e resumo executivo
    report_lines.append("# Relatório de Auditoria da Pipeline de Classificação Ideológica")
    report_lines.append("")
    report_lines.append(f"- Data/hora de execução: {now_iso()}")
    report_lines.append(f"- Diretório de PDFs analisado: `{pdf_input_dir}`")
    report_lines.append(f"- CSV de comparação: `{csv_input_path}`")
    report_lines.append(
        "- Objetivo: extrair evidências textuais e registros partidários apenas quando sustentados por trechos literais dos PDFs, sem inferência externa."
    )
    if dependency_notes:
        report_lines.append("- Dependências indisponíveis/observações:")
        for note in dependency_notes:
            report_lines.append(f"  - {note}")

    # 2. Sumário geral
    report_lines.append("")
    report_lines.append("# Sumário Geral")
    report_lines.append("")
    report_lines.append(f"- total de PDFs encontrados: {total_pdfs}")
    report_lines.append(f"- total de PDFs texto nativo: {total_text}")
    report_lines.append(f"- total de PDFs que precisam OCR: {total_ocr}")
    report_lines.append(f"- total de PDFs com falha: {total_failed}")
    report_lines.append(f"- total de evidências capturadas: {evidence_total}")
    report_lines.append(f"- total de registros estruturados: {len(records)}")
    report_lines.append(f"- total de partidos mencionados: {mentioned_total}")
    report_lines.append(f"- total de partidos em tabela relevante: {table_total}")
    report_lines.append(f"- total de partidos explicitamente classificados: {explicit_total}")
    report_lines.append(f"- total de partidos do CSV com match confiável: {match_confident}")
    report_lines.append(f"- total de mismatches: {mismatch_total}")
    report_lines.append(f"- PDFs com maior risco de erro: {risk_preview}")

    # 3. Índice navegável
    report_lines.append("")
    report_lines.append("# Índice dos PDFs")
    report_lines.append("")
    for entry in inventory:
        anchor = markdown_anchor(entry.pdf_filename)
        report_lines.append(f"- [{entry.pdf_filename}](#{anchor})")

    # 4. Seção fixa por PDF
    for entry in inventory:
        pdf_name = entry.pdf_filename
        pdf_records = records_by_pdf.get(pdf_name, [])
        pdf_evidence = evidence_by_pdf.get(pdf_name, [])
        year_mentions = year_mentions_by_pdf.get(pdf_name, [])

        mention_only = [
            record
            for record in pdf_records
            if record.mentioned_in_article and not record.appears_in_relevant_table and not record.explicitly_classified
        ]
        table_only = [
            record
            for record in pdf_records
            if record.appears_in_relevant_table and not record.explicitly_classified
        ]
        explicit = [record for record in pdf_records if record.explicitly_classified]

        report_lines.append("")
        report_lines.append(f"## {pdf_name}")
        report_lines.append("")

        report_lines.append("### 1. Identificação do documento")
        report_lines.append(f"- nome do arquivo: `{pdf_name}`")
        report_lines.append(f"- caminho relativo: `{entry.pdf_path}`")
        report_lines.append(f"- hash do arquivo: `{entry.file_hash}`")
        report_lines.append(f"- número de páginas: {entry.page_count if entry.page_count is not None else 'n/d'}")
        report_lines.append(f"- tipo do PDF: `{entry.pdf_type}`")
        report_lines.append(f"- status de extração: `{entry.extraction_status}`")

        report_lines.append("")
        report_lines.append("### 2. Status de extração e qualidade")
        fragment_count = sum(1 for ev in pdf_evidence if ev.evidence_type == EVIDENCE_TEXT_FRAGMENT)
        table_evidence_count = sum(1 for ev in pdf_evidence if ev.evidence_type == EVIDENCE_TABLE)
        extraction_errors = [ev for ev in pdf_evidence if ev.evidence_type == EVIDENCE_EXTRACTION_ERROR]
        risk_label = "alto" if pdf_name in high_risk else "médio" if entry.extraction_status == EXTRACTION_NEEDS_OCR else "baixo"
        report_lines.append(f"- resumo: `{entry.diagnostic_notes}`")
        report_lines.append(f"- texto fragmentário detectado: {'sim' if fragment_count > 0 else 'não'} ({fragment_count})")
        report_lines.append(f"- tabelas/listas com parsing potencialmente instável: {table_evidence_count}")
        report_lines.append(f"- risco alto de erro: {'sim' if risk_label == 'alto' else 'não'}")
        report_lines.append(f"- observações técnicas: risco geral `{risk_label}`")
        if extraction_errors:
            for ev in extraction_errors:
                report_lines.append(f"- falha de extração: `{ev.parser_notes}`")

        report_lines.append("")
        report_lines.append("### 3. Anos e períodos detectados")
        if year_mentions:
            aggregated: dict[tuple[str, str], dict[str, Any]] = {}
            for year in year_mentions:
                key = (year["raw"], year["year_type"])
                if key not in aggregated:
                    aggregated[key] = {
                        "raw": year["raw"],
                        "year_type": year["year_type"],
                        "pages": set(),
                        "snippet": year["snippet"],
                    }
                aggregated[key]["pages"].add(year["page_number"])

            for key in sorted(aggregated, key=lambda k: (k[1], k[0])):
                row = aggregated[key]
                pages = ", ".join(str(page) for page in sorted(row["pages"]))
                report_lines.append(
                    f"- valor bruto: `{row['raw']}` | tipo: `{row['year_type']}` | página(s): {pages}"
                )
                report_lines.append(f"  snippet: `{row['snippet']}`")

            year_types = {row["year_type"] for row in aggregated.values()}
            if "publication_year" in year_types and len(year_types) > 1:
                report_lines.append(
                    "- ambiguidade explícita: há menções simultâneas de ano de publicação e possíveis anos de classificação."
                )
        else:
            report_lines.append("- nenhum ano/período identificável com confiança textual.")

        report_lines.append("")
        report_lines.append("### 4. Partidos mencionados no artigo")
        mention_lines = _format_party_listing(mention_only, include_classification=False)
        if mention_lines:
            report_lines.extend(mention_lines)
        else:
            report_lines.append("- não houve partidos claramente identificáveis apenas como menção.")

        report_lines.append("")
        report_lines.append("### 5. Partidos em tabelas ou listas relevantes")
        if table_only:
            grouped_table: dict[str, list[ClassificationRecord]] = defaultdict(list)
            for rec in table_only:
                grouped_table[rec.party_name_raw or "<sem_partido>"].append(rec)
            for party in sorted(grouped_table):
                items = grouped_table[party]
                pages = sorted({item.page_number for item in items})
                fragmentary = any(item.ambiguity_flag or item.extraction_confidence == "LOW" for item in items)
                best = items[0]
                report_lines.append(
                    f"- **{party}** | páginas: {', '.join(str(p) for p in pages)} | tabela: {'fragmentária' if fragmentary else 'íntegra'}"
                )
                report_lines.append(f"  snippet: `{shrink_text(best.quoted_snippet, 200)}`")
        else:
            report_lines.append("- não houve partidos em tabela/lista relevante sem valor classificatório legível.")

        report_lines.append("")
        report_lines.append("### 6. Partidos explicitamente classificados")
        explicit_lines = _format_party_listing(explicit, include_classification=True)
        if explicit_lines:
            report_lines.extend(explicit_lines)
        else:
            report_lines.append("- não há partidos explicitamente classificados com evidência textual suficiente.")

        report_lines.append("")
        report_lines.append("### 7. Método ou descrição da escala")
        method_notes: set[str] = set()
        for rec in pdf_records:
            if rec.methodology_note:
                method_notes.add(rec.methodology_note)
        if not method_notes:
            for ev in pdf_evidence:
                if ev.evidence_type == EVIDENCE_METHOD:
                    method_notes.add(shrink_text(ev.quoted_snippet, 220))

        if method_notes:
            for note in sorted(method_notes):
                report_lines.append(f"- método/escala (trecho): `{note}`")
        else:
            report_lines.append("- método de escala não identificado de forma textual clara.")

        report_lines.append("")
        report_lines.append("### 8. Divergências, ambiguidades e problemas")
        problems: list[str] = []
        if mention_only and not explicit:
            problems.append("há partidos mencionados sem classificação explícita")
        if table_only:
            problems.append("há partidos em tabela relevante sem valor classificatório legível")
        if any(rec.ambiguity_flag for rec in pdf_records):
            problems.append("há ambiguidades de ano/período e/ou associação de linha partidária")
        if fragment_count > 0:
            problems.append("texto ilegível ou fragmentário em parte do documento")
        if extraction_errors:
            problems.append("falhas de parsing/extrator em pelo menos uma tentativa")
        if entry.extraction_status == EXTRACTION_NEEDS_OCR:
            problems.append("documento parcial ou totalmente dependente de OCR")
        if not problems:
            problems.append("nenhuma divergência estrutural relevante detectada")
        for item in problems:
            report_lines.append(f"- {item}")

        report_lines.append("")
        report_lines.append("### 9. Resumo interpretativo mínimo")
        if explicit:
            classifies_text = "sim"
        elif table_only:
            classifies_text = "incerto"
        else:
            classifies_text = "não"

        reliability = "alto"
        if entry.extraction_status != EXTRACTION_READY:
            reliability = "médio"
        if not explicit or entry.extraction_status == EXTRACTION_FAILED or pdf_name in high_risk:
            reliability = "baixo"

        top_parties = sorted({rec.party_name_raw for rec in explicit if rec.party_name_raw})
        top_parties_text = ", ".join(top_parties[:10]) if top_parties else "nenhum"
        limitations = "; ".join(problems[:3])

        report_lines.append(f"- este PDF classifica partidos concretos? **{classifies_text}**")
        report_lines.append(f"- este PDF é confiável para extração automatizada? **{reliability}**")
        report_lines.append(f"- principais partidos aproveitáveis deste PDF: {top_parties_text}")
        report_lines.append(f"- limitações principais: {limitations}")

    # 5. Mismatch com CSV
    report_lines.append("")
    report_lines.append("# Mismatch com `partidos_por_periodo.csv`")
    report_lines.append("")

    grouped_results = _group_results_by_party(match_results)
    records_by_party: dict[str, set[str]] = defaultdict(set)
    for rec in records:
        if rec.party_name_raw:
            records_by_party[rec.party_name_raw].add(rec.pdf_filename)
        if rec.party_name_normalized_minimal:
            records_by_party[rec.party_name_normalized_minimal].add(rec.pdf_filename)

    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for party, data in grouped_results.items():
        status = _best_status(data["statuses"])
        data["status"] = status
        buckets[status].append(data)

    def _write_bucket(title: str, items: list[dict[str, Any]]) -> None:
        report_lines.append(f"## {title}")
        if not items:
            report_lines.append("- nenhum caso nesta categoria.")
            report_lines.append("")
            return
        for item in sorted(items, key=lambda x: x["party"]):
            matched_names = sorted(item["matched_names"])
            matched_name = matched_names[0] if matched_names else None
            pdfs = sorted(records_by_party.get(matched_name or "", set())) if matched_name else []
            pdf_text = ", ".join(pdfs) if pdfs else "n/d"
            periods_count = len(item["periods"])
            report_lines.append(
                f"- {item['party']} | períodos no CSV: {periods_count} | match_method: {', '.join(sorted(item['methods'])) or 'n/d'} | PDFs: {pdf_text}"
            )
        report_lines.append("")

    _write_bucket("Partidos do CSV não encontrados em nenhum PDF", buckets.get(MISMATCH_NOT_FOUND, []))
    _write_bucket("Partidos encontrados apenas como menção", buckets.get(MISMATCH_ONLY_MENTION, []))
    _write_bucket(
        "Partidos encontrados em tabelas, mas sem classificação legível",
        buckets.get(MISMATCH_TABLE_NO_CLASS, []),
    )
    _write_bucket("Partidos com classificação explícita", buckets.get(MISMATCH_CLASSIFIED, []))
    _write_bucket("Matches ambíguos ou múltiplos candidatos", buckets.get(MISMATCH_AMBIGUOUS, []))

    # Apêndice breve de falhas
    report_lines.append("## Falhas de extração registradas")
    if extraction_failures:
        for row in extraction_failures:
            report_lines.append(
                f"- {row.get('pdf_filename')} | stage={row.get('stage')} | details={row.get('details')}"
            )
    else:
        report_lines.append("- nenhuma falha crítica de extração registrada.")

    path.write_text("\n".join(report_lines).rstrip() + "\n", encoding="utf-8")
