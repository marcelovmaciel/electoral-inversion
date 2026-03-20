"""Agente 1: inventário e diagnóstico de extração dos PDFs."""

from __future__ import annotations

import hashlib
from pathlib import Path

from .constants import (
    EXTRACTION_FAILED,
    EXTRACTION_NEEDS_OCR,
    EXTRACTION_READY,
    MIN_MEANINGFUL_TEXT_LEN,
    MIXED_PDF_RATIO_THRESHOLD,
    PDF_TYPE_CORRUPTED,
    PDF_TYPE_MIXED,
    PDF_TYPE_SCANNED,
    PDF_TYPE_TEXT,
    PDF_TYPE_UNKNOWN,
    TEXT_PDF_RATIO_THRESHOLD,
)
from .models import PDFInventoryEntry
from .reporting import write_json
from .utils import command_exists, run_command


def list_pdfs(pdf_dir: Path) -> list[Path]:
    """Lista PDFs de um diretório em ordem alfabética."""
    if not pdf_dir.exists():
        return []
    return sorted(p for p in pdf_dir.glob("*.pdf") if p.is_file())


def compute_file_hash(path: Path) -> str:
    """Calcula hash SHA-256 do arquivo."""
    sha = hashlib.sha256()
    with path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(1024 * 1024)
            if not chunk:
                break
            sha.update(chunk)
    return sha.hexdigest()


def _page_count_from_pdfinfo(path: Path) -> tuple[int | None, str]:
    if not command_exists("pdfinfo"):
        return None, "pdfinfo indisponível"
    code, out, err = run_command(["pdfinfo", str(path)])
    if code != 0:
        return None, f"pdfinfo falhou: {err.strip() or out.strip()}"
    for line in out.splitlines():
        if line.startswith("Pages:"):
            raw = line.split(":", 1)[1].strip()
            try:
                return int(raw), "page_count via pdfinfo"
            except ValueError:
                return None, f"page_count inválido em pdfinfo: {raw}"
    return None, "campo Pages não encontrado em pdfinfo"


def _extract_pages_for_diagnosis(path: Path) -> tuple[list[tuple[int, str]] | None, str]:
    try:
        from .evidence_extractor import extract_text_by_page

        pages = extract_text_by_page(path)
        return pages, "extração por backend disponível"
    except Exception as exc:  # pragma: no cover - rota de erro explícita
        return None, f"falha na extração de texto: {exc}"


def classify_pdf_text_profile(page_texts: list[str]) -> tuple[str, str, str]:
    """Classifica tipo/status do PDF por densidade de texto extraível."""
    total = len(page_texts)
    if total == 0:
        return PDF_TYPE_UNKNOWN, EXTRACTION_FAILED, "sem páginas detectadas"

    nonempty = sum(1 for text in page_texts if len(text.strip()) >= MIN_MEANINGFUL_TEXT_LEN)
    ratio = nonempty / total

    if nonempty == 0:
        return PDF_TYPE_SCANNED, EXTRACTION_NEEDS_OCR, "nenhuma página com texto significativo"
    if ratio >= TEXT_PDF_RATIO_THRESHOLD:
        return PDF_TYPE_TEXT, EXTRACTION_READY, f"{nonempty}/{total} páginas com texto"
    if ratio >= MIXED_PDF_RATIO_THRESHOLD:
        return PDF_TYPE_MIXED, EXTRACTION_NEEDS_OCR, f"texto parcial ({nonempty}/{total} páginas)"
    return PDF_TYPE_SCANNED, EXTRACTION_NEEDS_OCR, f"texto muito escasso ({nonempty}/{total} páginas)"


def diagnose_pdf(path: Path) -> PDFInventoryEntry:
    """Diagnostica tipo de PDF e estado de extração sem OCR."""
    file_hash = compute_file_hash(path)
    file_size = path.stat().st_size
    page_count, page_count_note = _page_count_from_pdfinfo(path)

    pages, extraction_note = _extract_pages_for_diagnosis(path)
    if pages is None:
        return PDFInventoryEntry(
            pdf_filename=path.name,
            pdf_path=str(path),
            file_hash=file_hash,
            file_size=file_size,
            page_count=page_count,
            pdf_type=PDF_TYPE_CORRUPTED,
            extraction_status=EXTRACTION_FAILED,
            diagnostic_notes=f"{page_count_note}; {extraction_note}",
        )

    page_texts = [text for _, text in pages]
    detected_count = len(page_texts)
    if page_count is None:
        page_count = detected_count

    pdf_type, extraction_status, profile_note = classify_pdf_text_profile(page_texts)
    note = "; ".join(x for x in [page_count_note, extraction_note, profile_note] if x)

    if pdf_type == PDF_TYPE_UNKNOWN and extraction_status != EXTRACTION_FAILED:
        extraction_status = EXTRACTION_FAILED

    return PDFInventoryEntry(
        pdf_filename=path.name,
        pdf_path=str(path),
        file_hash=file_hash,
        file_size=file_size,
        page_count=page_count,
        pdf_type=pdf_type,
        extraction_status=extraction_status,
        diagnostic_notes=note,
    )


def build_pdf_inventory(pdf_dir: Path) -> list[PDFInventoryEntry]:
    """Monta inventário completo para todos os PDFs do diretório."""
    entries: list[PDFInventoryEntry] = []
    for pdf_path in list_pdfs(pdf_dir):
        entries.append(diagnose_pdf(pdf_path))
    return entries


def save_pdf_inventory(entries: list[PDFInventoryEntry], outpath: Path) -> None:
    """Salva inventário em JSON para trilha de auditoria."""
    payload = [entry.to_dict() for entry in entries]
    write_json(payload, outpath)
