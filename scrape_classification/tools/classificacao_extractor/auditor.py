"""Agente 5: validação estrutural e semântica dos registros extraídos."""

from __future__ import annotations

import re
from collections import defaultdict

from .constants import CLASSIFICATION_HINT_REGEX, IDEOLOGY_REGEX
from .models import ClassificationRecord


def _base_issue(record: ClassificationRecord, issue: str, severity: str) -> dict:
    return {
        "pdf_filename": record.pdf_filename,
        "party_name_raw": record.party_name_raw,
        "page_number": record.page_number,
        "issue": issue,
        "severity": severity,
    }


def validate_record_structure(records: list[ClassificationRecord]) -> list[dict]:
    """Valida invariantes estruturais mínimos de cada registro."""
    issues: list[dict] = []

    for record in records:
        if record.page_number is None or record.page_number <= 0:
            issues.append(_base_issue(record, "page_number ausente ou inválido", "ERROR"))
        if not record.quoted_snippet or not record.quoted_snippet.strip():
            issues.append(_base_issue(record, "quoted_snippet ausente", "ERROR"))
        if not record.pdf_filename:
            issues.append(_base_issue(record, "pdf_filename ausente", "ERROR"))
        if not record.party_name_raw:
            issues.append(_base_issue(record, "party_name_raw ausente", "ERROR"))

    return issues


def validate_record_semantics(records: list[ClassificationRecord]) -> list[dict]:
    """Valida coerência semântica conservadora de classificação partidária."""
    issues: list[dict] = []

    for record in records:
        snippet = record.quoted_snippet or ""
        has_classification_hint = bool(
            IDEOLOGY_REGEX.search(snippet)
            or CLASSIFICATION_HINT_REGEX.search(snippet)
            or re.search(r"(?<!\d)-?\d{1,2}(?:[\.,]\d{1,3})?(?!\d)", snippet)
        )

        if record.explicitly_classified and not record.ideology_value_raw:
            issues.append(
                _base_issue(
                    record,
                    "explicitly_classified=True requer ideology_value_raw preenchido",
                    "ERROR",
                )
            )

        if record.explicitly_classified and not has_classification_hint:
            issues.append(
                _base_issue(
                    record,
                    "explicitly_classified=True sem indício textual plausível no snippet",
                    "ERROR",
                )
            )

        if record.reference_year_start and record.reference_year_end:
            if record.reference_year_start > record.reference_year_end:
                issues.append(
                    _base_issue(record, "intervalo de ano invertido (start > end)", "ERROR")
                )

        if record.year_type == "ambiguous" and not record.ambiguity_flag:
            issues.append(
                _base_issue(record, "year_type ambíguo sem ambiguity_flag", "WARNING")
            )

        if record.explicitly_classified and record.ideology_value_type == "UNKNOWN":
            issues.append(
                _base_issue(record, "classificação explícita com tipo de valor desconhecido", "WARNING")
            )

    return issues


def sample_records_for_recheck(records: list[ClassificationRecord], max_per_pdf: int = 5) -> list[ClassificationRecord]:
    """Seleciona amostra pequena por PDF para revisão humana prioritária."""
    grouped: dict[str, list[ClassificationRecord]] = defaultdict(list)
    for record in records:
        grouped[record.pdf_filename].append(record)

    sampled: list[ClassificationRecord] = []
    for pdf_name, pdf_records in grouped.items():
        sorted_records = sorted(
            pdf_records,
            key=lambda r: (
                int(r.ambiguity_flag),
                int(r.explicitly_classified),
                int(r.appears_in_relevant_table),
                1 if r.extraction_confidence == "LOW" else 0,
                -r.page_number,
            ),
            reverse=True,
        )
        sampled.extend(sorted_records[:max_per_pdf])

    return sampled


def audit_records(records: list[ClassificationRecord]) -> dict:
    """Executa auditoria completa e retorna estrutura resumida."""
    structure_issues = validate_record_structure(records)
    semantic_issues = validate_record_semantics(records)

    per_pdf: dict[str, dict] = defaultdict(
        lambda: {
            "record_count": 0,
            "mentioned_count": 0,
            "table_count": 0,
            "explicit_count": 0,
            "ambiguous_count": 0,
            "low_confidence_count": 0,
        }
    )

    for record in records:
        row = per_pdf[record.pdf_filename]
        row["record_count"] += 1
        row["mentioned_count"] += int(record.mentioned_in_article)
        row["table_count"] += int(record.appears_in_relevant_table)
        row["explicit_count"] += int(record.explicitly_classified)
        row["ambiguous_count"] += int(record.ambiguity_flag)
        row["low_confidence_count"] += int(record.extraction_confidence == "LOW")

    high_risk_pdfs: list[str] = []
    for pdf_name, stats in per_pdf.items():
        record_count = stats["record_count"]
        ambiguous_ratio = (stats["ambiguous_count"] / record_count) if record_count else 0.0
        no_explicit = stats["explicit_count"] == 0
        low_conf_high = stats["low_confidence_count"] >= 3
        if ambiguous_ratio >= 0.30 or no_explicit or low_conf_high:
            high_risk_pdfs.append(pdf_name)

    sample = sample_records_for_recheck(records, max_per_pdf=5)

    return {
        "structure_issues": structure_issues,
        "semantic_issues": semantic_issues,
        "total_structure_issues": len(structure_issues),
        "total_semantic_issues": len(semantic_issues),
        "total_issues": len(structure_issues) + len(semantic_issues),
        "per_pdf": dict(per_pdf),
        "high_risk_pdfs": sorted(high_risk_pdfs),
        "sample_for_recheck": [record.to_dict() for record in sample],
    }
