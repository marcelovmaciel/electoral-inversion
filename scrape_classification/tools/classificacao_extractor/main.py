"""Coordenação da pipeline de classificação ideológica com checkpoints auditáveis."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

from .auditor import audit_records, validate_record_structure
from .constants import (
    EVIDENCE_EXTRACTION_ERROR,
    EXTRACTION_FAILED,
    EXTRACTION_NEEDS_OCR,
    EXTRACTION_READY,
    MISMATCH_AMBIGUOUS,
    MISMATCH_CLASSIFIED,
    PDF_TYPE_TEXT,
)
from .evidence_extractor import extract_evidence_for_pdf, save_evidence_jsonl
from .matcher import build_mismatch_report, load_partidos_csv
from .parser import build_classification_records, infer_year_mentions_from_evidence
from .pdf_inventory import build_pdf_inventory, save_pdf_inventory
from .reporting import (
    write_audit_report,
    write_csv_from_records,
    write_json,
    write_match_results,
    write_party_ordinal_classification_codato_2023,
    write_party_ordinal_classification,
    write_per_pdf_summary,
)
from .utils import command_exists, ensure_dir


def _resolve_pdf_dir(project_root: Path) -> Path:
    candidates = [project_root / "pdf" / "classificacao", project_root / "pdfs" / "classificacao"]
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        "Diretório de PDFs não encontrado. Esperado em 'pdf/classificacao' ou 'pdfs/classificacao'."
    )


def _collect_dependency_notes() -> list[str]:
    notes: list[str] = []
    try:
        import fitz  # noqa: F401  # pragma: no cover
    except Exception:
        notes.append("PyMuPDF (fitz) indisponível; fallback para outros extratores.")

    try:
        import pdfplumber  # noqa: F401  # pragma: no cover
    except Exception:
        notes.append("pdfplumber indisponível; fallback para pdftotext quando disponível.")

    try:
        import pandas  # noqa: F401  # pragma: no cover
    except Exception:
        notes.append("pandas indisponível; leitura/escrita CSV usando biblioteca padrão.")

    if not command_exists("pdftotext"):
        notes.append("pdftotext indisponível; extração de texto pode falhar sem fitz/pdfplumber.")
    if not command_exists("pdfinfo"):
        notes.append("pdfinfo indisponível; contagem de páginas pode ficar incompleta.")

    return notes


def _to_rows(table_like: Any) -> list[dict[str, Any]]:
    try:
        import pandas as pd  # type: ignore

        if isinstance(table_like, pd.DataFrame):
            return table_like.to_dict(orient="records")
    except Exception:
        pass

    if isinstance(table_like, list):
        return [dict(row) for row in table_like]

    raise TypeError("Objeto de tabela não suportado para serialização.")


def _write_candidates_csv(table_like: Any, path: Path) -> None:
    rows = _to_rows(table_like)
    ensure_dir(path.parent)
    fieldnames = list(rows[0].keys()) if rows else [
        "party_from_csv",
        "period_from_csv",
        "candidate_party",
        "exact_match",
        "conservative_match",
        "probable_score",
        "candidate_pdf_count",
        "candidate_explicit_count",
        "candidate_table_count",
        "candidate_mention_count",
    ]
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _checkpoint_or_fail(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(f"CHECKPOINT FAILED: {message}")


def run_pipeline(
    *,
    target_pdf_filename: str | None = None,
    output_subdir: str = "classificacao",
) -> int:
    """Executa pipeline completa dos 6 agentes com checkpoints rígidos."""
    project_root = Path.cwd()
    output_dir = project_root / "output" / output_subdir
    raw_evidence_dir = output_dir / "raw_evidence"
    ensure_dir(output_dir)
    ensure_dir(raw_evidence_dir)

    dependency_notes = _collect_dependency_notes()
    extraction_failures: list[dict[str, Any]] = []

    try:
        pdf_dir = _resolve_pdf_dir(project_root)
        csv_path = project_root / "scraping" / "output" / "partidos_por_periodo.csv"

        # Agente 1: inventário
        inventory = build_pdf_inventory(pdf_dir)
        if target_pdf_filename:
            inventory = [entry for entry in inventory if entry.pdf_filename == target_pdf_filename]
            _checkpoint_or_fail(
                len(inventory) == 1,
                f"PDF alvo não encontrado no diretório: {target_pdf_filename}",
            )
        inventory_path = output_dir / "pdf_inventory.json"
        save_pdf_inventory(inventory, inventory_path)

        _checkpoint_or_fail(inventory_path.exists(), "pdf_inventory.json não foi criado")
        _checkpoint_or_fail(len(inventory) > 0, "nenhum PDF encontrado para inventário")
        _checkpoint_or_fail(
            all(item.pdf_type for item in inventory),
            "há PDFs sem classificação de tipo",
        )

        # Agente 2: extração de evidências
        evidence_by_pdf: dict[str, list[Any]] = {}
        year_mentions_by_pdf: dict[str, list[dict]] = {}

        for entry in inventory:
            evidence = extract_evidence_for_pdf(entry)
            evidence_by_pdf[entry.pdf_filename] = evidence
            year_mentions_by_pdf[entry.pdf_filename] = infer_year_mentions_from_evidence(evidence)

            raw_file = raw_evidence_dir / f"{Path(entry.pdf_filename).stem}.jsonl"
            save_evidence_jsonl(evidence, raw_file)

            if any(item.evidence_type == EVIDENCE_EXTRACTION_ERROR for item in evidence):
                extraction_failures.append(
                    {
                        "pdf_filename": entry.pdf_filename,
                        "stage": "evidence_extractor",
                        "details": "; ".join(item.parser_notes for item in evidence if item.parser_notes),
                    }
                )

        extractable_entries = [entry for entry in inventory if entry.extraction_status != EXTRACTION_FAILED]
        _checkpoint_or_fail(
            all((raw_evidence_dir / f"{Path(entry.pdf_filename).stem}.jsonl").exists() for entry in extractable_entries),
            "nem todo PDF extraível possui JSONL de evidência",
        )

        # Agente 3: parser conservador
        records: list = []
        for entry in inventory:
            pdf_records = build_classification_records(entry.pdf_filename, evidence_by_pdf.get(entry.pdf_filename, []))
            records.extend(pdf_records)

        master_json_path = output_dir / "classificacao_master.json"
        master_csv_path = output_dir / "classificacao_master.csv"
        write_json([record.to_dict() for record in records], master_json_path)
        write_csv_from_records(records, master_csv_path)

        structure_issues = validate_record_structure(records)
        bad_snippet_or_page = [
            issue
            for issue in structure_issues
            if "page_number" in issue["issue"] or "quoted_snippet" in issue["issue"]
        ]
        _checkpoint_or_fail(master_json_path.exists() and master_csv_path.exists(), "classificacao_master não foi criado")
        _checkpoint_or_fail(
            not bad_snippet_or_page,
            "há registros finais sem snippet ou sem página",
        )

        # Agente 4: matching
        csv_df = load_partidos_csv(csv_path)
        match_results, candidate_table = build_mismatch_report(csv_df, records)

        mismatch_path = output_dir / "mismatch_report.csv"
        candidates_path = output_dir / "party_match_candidates.csv"
        write_match_results(match_results, mismatch_path)
        _write_candidates_csv(candidate_table, candidates_path)

        _checkpoint_or_fail(mismatch_path.exists(), "mismatch_report.csv não foi criado")

        # Agente 5: auditoria
        audit_data = audit_records(records)

        # Agente 6: reporting
        per_pdf_summary_path = output_dir / "per_pdf_summary.csv"
        failures_path = output_dir / "extraction_failures.csv"
        audit_report_path = output_dir / "audit_report.md"

        write_per_pdf_summary(inventory, records, evidence_by_pdf, per_pdf_summary_path)

        if extraction_failures:
            with failures_path.open("w", encoding="utf-8", newline="") as file_obj:
                writer = csv.DictWriter(file_obj, fieldnames=["pdf_filename", "stage", "details"])
                writer.writeheader()
                for row in extraction_failures:
                    writer.writerow(row)
        else:
            with failures_path.open("w", encoding="utf-8", newline="") as file_obj:
                writer = csv.DictWriter(file_obj, fieldnames=["pdf_filename", "stage", "details"])
                writer.writeheader()

        write_audit_report(
            audit_report_path,
            inventory=inventory,
            evidence_by_pdf=evidence_by_pdf,
            year_mentions_by_pdf=year_mentions_by_pdf,
            records=records,
            match_results=match_results,
            audit_data=audit_data,
            extraction_failures=extraction_failures,
            dependency_notes=dependency_notes,
            pdf_input_dir=pdf_dir,
            csv_input_path=csv_path,
        )

        if target_pdf_filename:
            ordinal_csv = output_dir / "party_ordinal_classificacao.csv"
            ordinal_json = output_dir / "party_ordinal_classificacao.json"
            target_entry = next((item for item in inventory if item.pdf_filename == target_pdf_filename), None)
            if target_entry and target_pdf_filename == "codato_2023.pdf":
                write_party_ordinal_classification_codato_2023(
                    pdf_path=Path(target_entry.pdf_path),
                    csv_path=ordinal_csv,
                    json_path=ordinal_json,
                )
            else:
                write_party_ordinal_classification(
                    records=records,
                    evidence=evidence_by_pdf.get(target_pdf_filename, []),
                    pdf_filename=target_pdf_filename,
                    csv_path=ordinal_csv,
                    json_path=ordinal_json,
                )

        _checkpoint_or_fail(audit_report_path.exists(), "audit_report.md não foi criado")

        # Sumário final no terminal
        total_text = sum(1 for item in inventory if item.pdf_type == PDF_TYPE_TEXT)
        total_ocr = sum(1 for item in inventory if item.extraction_status == EXTRACTION_NEEDS_OCR)
        total_failed = sum(1 for item in inventory if item.extraction_status == EXTRACTION_FAILED)
        total_evidence = sum(len(items) for items in evidence_by_pdf.values())
        total_mentioned = sum(1 for rec in records if rec.mentioned_in_article)
        total_table = sum(1 for rec in records if rec.appears_in_relevant_table)
        total_explicit = sum(1 for rec in records if rec.explicitly_classified)
        total_match_confident = sum(
            1
            for row in match_results
            if (row.exact_match_found or row.conservative_match_found)
            and row.mismatch_type != MISMATCH_AMBIGUOUS
        )
        total_mismatch = sum(1 for row in match_results if row.mismatch_type != MISMATCH_CLASSIFIED)
        high_risk = audit_data.get("high_risk_pdfs", [])

        print(f"total de PDFs encontrados: {len(inventory)}")
        print(f"total de PDFs texto nativo: {total_text}")
        print(f"total de PDFs que precisam OCR: {total_ocr}")
        print(f"total de PDFs com falha: {total_failed}")
        print(f"total de evidências capturadas: {total_evidence}")
        print(f"total de registros estruturados: {len(records)}")
        print(f"total de partidos mencionados: {total_mentioned}")
        print(f"total de partidos em tabela: {total_table}")
        print(f"total de partidos explicitamente classificados: {total_explicit}")
        print(f"total de partidos do CSV com match confiável: {total_match_confident}")
        print(f"total de mismatches: {total_mismatch}")
        print(
            "lista dos PDFs com maior risco de erro: "
            + (", ".join(high_risk) if high_risk else "nenhum")
        )

        return 0

    except Exception as exc:
        print(f"ERRO na pipeline: {exc}", file=sys.stderr)
        return 1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline de classificação ideológica em PDFs.")
    parser.add_argument(
        "--pdf-filename",
        dest="pdf_filename",
        default=None,
        help="Filtra execução para um único PDF (ex.: codato_2023.pdf).",
    )
    parser.add_argument(
        "--output-subdir",
        dest="output_subdir",
        default="classificacao",
        help="Subdiretório em output/ para os artefatos gerados.",
    )
    return parser.parse_args()


if __name__ == "__main__":  # pragma: no cover
    args = _parse_args()
    raise SystemExit(
        run_pipeline(
            target_pdf_filename=args.pdf_filename,
            output_subdir=args.output_subdir,
        )
    )
