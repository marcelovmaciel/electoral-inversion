"""Agente 2: extração de evidências textuais por página."""

from __future__ import annotations

import json
import re
from pathlib import Path

from .constants import (
    CLASSIFICATION_HINT_REGEX,
    EVIDENCE_CLASSIFICATION,
    EVIDENCE_EXTRACTION_ERROR,
    EVIDENCE_IDEOLOGY,
    EVIDENCE_METHOD,
    EVIDENCE_PARTY,
    EVIDENCE_PERIOD,
    EVIDENCE_TABLE,
    EVIDENCE_TEXT_FRAGMENT,
    EVIDENCE_YEAR,
    IDEOLOGY_REGEX,
    METHODOLOGY_REGEX,
    MIN_MEANINGFUL_TEXT_LEN,
    PARTY_FULLNAME_PATTERN,
    PARTY_SIGLA_PATTERN,
    PARTY_STOPWORDS,
    PERIOD_PATTERN,
    SNIPPET_MAX_CHARS,
    SNIPPET_MIN_CHARS,
    TABLE_HINT_REGEX,
    YEAR_PATTERN,
    YEAR_RANGE_PATTERN,
)
from .models import EvidenceItem, PDFInventoryEntry
from .utils import command_exists, ensure_parent_dir, run_command, split_pdftotext_pages, shrink_text

try:  # pragma: no cover - depende do ambiente
    import fitz  # type: ignore
except Exception:  # pragma: no cover - depende do ambiente
    fitz = None

try:  # pragma: no cover - depende do ambiente
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover - depende do ambiente
    pdfplumber = None


def _extract_text_by_page_fitz(pdf_path: Path) -> list[tuple[int, str]]:
    if fitz is None:
        raise RuntimeError("PyMuPDF (fitz) indisponível")

    pages: list[tuple[int, str]] = []
    doc = fitz.open(pdf_path)  # type: ignore[operator]
    try:
        for idx in range(doc.page_count):
            page = doc.load_page(idx)
            text = page.get_text("text")
            pages.append((idx + 1, text or ""))
    finally:
        doc.close()
    return pages


def _extract_text_by_page_pdfplumber(pdf_path: Path) -> list[tuple[int, str]]:
    if pdfplumber is None:
        raise RuntimeError("pdfplumber indisponível")

    pages: list[tuple[int, str]] = []
    with pdfplumber.open(pdf_path) as pdf:  # type: ignore[operator]
        for idx, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            pages.append((idx, text))
    return pages


def _extract_text_by_page_pdftotext(pdf_path: Path) -> list[tuple[int, str]]:
    if not command_exists("pdftotext"):
        raise RuntimeError("pdftotext indisponível")

    code, out, err = run_command(["pdftotext", "-layout", str(pdf_path), "-"])
    if code != 0:
        raise RuntimeError(err.strip() or out.strip() or "pdftotext retornou erro")

    pages_raw = split_pdftotext_pages(out)
    return [(idx + 1, page_text) for idx, page_text in enumerate(pages_raw)]


def extract_text_by_page(pdf_path: Path) -> list[tuple[int, str]]:
    """Extrai texto por página com fallback entre backends disponíveis."""
    errors: list[str] = []
    for backend in (_extract_text_by_page_fitz, _extract_text_by_page_pdfplumber, _extract_text_by_page_pdftotext):
        try:
            pages = backend(pdf_path)
            if pages:
                return pages
            errors.append(f"{backend.__name__}: sem páginas")
        except Exception as exc:
            errors.append(f"{backend.__name__}: {exc}")

    raise RuntimeError(" ; ".join(errors) if errors else "sem backend de extração disponível")


def _context_snippet(lines: list[str], idx: int, radius: int = 1) -> str:
    start = max(0, idx - radius)
    end = min(len(lines), idx + radius + 1)
    window = [line.rstrip() for line in lines[start:end] if line.strip()]
    if not window:
        window = [lines[idx].strip()] if idx < len(lines) else []
    snippet = "\n".join(window).strip()
    if len(snippet) < SNIPPET_MIN_CHARS:
        snippet = lines[idx].strip()
    return shrink_text(snippet, SNIPPET_MAX_CHARS)


def _line_looks_table_row(line: str) -> bool:
    compact = line.strip()
    if not compact:
        return False
    has_digit = bool(re.search(r"\d", compact))
    has_columns = bool(re.search(r"\s{2,}", compact)) or "\t" in compact
    has_letters = bool(re.search(r"[A-Za-zÀ-ÿ]", compact))
    return has_digit and has_columns and has_letters


def _line_has_party_like_token(line: str) -> bool:
    if PARTY_FULLNAME_PATTERN.search(line):
        return True

    tokens = PARTY_SIGLA_PATTERN.findall(line)
    for token in tokens:
        if token in PARTY_STOPWORDS:
            continue
        if len(token) >= 2:
            return True
    return False


def _line_has_numeric_scale_value(line: str) -> bool:
    values = re.findall(r"(?<!\d)(?:-?\d{1,2}(?:[\.,]\d{1,3})?)(?!\d)", line)
    return any(v for v in values)


def _add_evidence(
    collector: list[EvidenceItem],
    seen: set[tuple[int, str, str]],
    *,
    pdf_filename: str,
    page_number: int,
    evidence_type: str,
    snippet: str,
    notes: str,
    confidence: str,
) -> None:
    key = (page_number, evidence_type, snippet)
    if key in seen:
        return
    seen.add(key)
    collector.append(
        EvidenceItem(
            pdf_filename=pdf_filename,
            page_number=page_number,
            evidence_type=evidence_type,
            quoted_snippet=snippet,
            parser_notes=notes,
            extraction_confidence=confidence,
        )
    )


def classify_evidence_snippets(pdf_filename: str, pages: list[tuple[int, str]]) -> list[EvidenceItem]:
    """Classifica snippets candidatos sem inferir classificação final."""
    evidence: list[EvidenceItem] = []
    seen: set[tuple[int, str, str]] = set()

    for page_number, page_text in pages:
        if len(page_text.strip()) < MIN_MEANINGFUL_TEXT_LEN:
            _add_evidence(
                evidence,
                seen,
                pdf_filename=pdf_filename,
                page_number=page_number,
                evidence_type=EVIDENCE_TEXT_FRAGMENT,
                snippet=shrink_text(page_text, SNIPPET_MAX_CHARS),
                notes="página com pouco ou nenhum texto extraível",
                confidence="LOW",
            )
            continue

        lines = page_text.splitlines()
        for idx, line in enumerate(lines):
            line_clean = line.strip()
            if not line_clean:
                continue

            snippet = _context_snippet(lines, idx)

            has_year = bool(YEAR_PATTERN.search(line_clean))
            has_period = bool(YEAR_RANGE_PATTERN.search(line_clean) or PERIOD_PATTERN.search(line_clean))
            has_ideology = bool(IDEOLOGY_REGEX.search(line_clean))
            has_method = bool(METHODOLOGY_REGEX.search(line_clean))
            table_hint = bool(TABLE_HINT_REGEX.search(line_clean) or _line_looks_table_row(line_clean))
            has_party = _line_has_party_like_token(line_clean)
            has_numeric = _line_has_numeric_scale_value(line_clean)
            classification_hint = bool(CLASSIFICATION_HINT_REGEX.search(line_clean))
            likely_classification = has_party and (has_ideology or classification_hint or (table_hint and has_numeric))

            if has_year:
                _add_evidence(
                    evidence,
                    seen,
                    pdf_filename=pdf_filename,
                    page_number=page_number,
                    evidence_type=EVIDENCE_YEAR,
                    snippet=snippet,
                    notes="regex de ano detectada",
                    confidence="LOW",
                )

            if has_period:
                _add_evidence(
                    evidence,
                    seen,
                    pdf_filename=pdf_filename,
                    page_number=page_number,
                    evidence_type=EVIDENCE_PERIOD,
                    snippet=snippet,
                    notes="intervalo/período detectado",
                    confidence="MEDIUM",
                )

            if has_ideology:
                _add_evidence(
                    evidence,
                    seen,
                    pdf_filename=pdf_filename,
                    page_number=page_number,
                    evidence_type=EVIDENCE_IDEOLOGY,
                    snippet=snippet,
                    notes="termo ideológico detectado",
                    confidence="MEDIUM",
                )

            if has_method:
                _add_evidence(
                    evidence,
                    seen,
                    pdf_filename=pdf_filename,
                    page_number=page_number,
                    evidence_type=EVIDENCE_METHOD,
                    snippet=snippet,
                    notes="trecho metodológico potencial",
                    confidence="MEDIUM",
                )

            if has_party:
                _add_evidence(
                    evidence,
                    seen,
                    pdf_filename=pdf_filename,
                    page_number=page_number,
                    evidence_type=EVIDENCE_PARTY,
                    snippet=snippet,
                    notes="token de partido potencial detectado",
                    confidence="MEDIUM",
                )

            if table_hint:
                _add_evidence(
                    evidence,
                    seen,
                    pdf_filename=pdf_filename,
                    page_number=page_number,
                    evidence_type=EVIDENCE_TABLE,
                    snippet=snippet,
                    notes="linha com padrão de tabela/lista",
                    confidence="MEDIUM",
                )

            if likely_classification:
                _add_evidence(
                    evidence,
                    seen,
                    pdf_filename=pdf_filename,
                    page_number=page_number,
                    evidence_type=EVIDENCE_CLASSIFICATION,
                    snippet=snippet,
                    notes="linha candidata a classificação partido-posição",
                    confidence="HIGH" if (has_numeric or has_ideology) else "MEDIUM",
                )

    return evidence


def extract_evidence_for_pdf(entry: PDFInventoryEntry) -> list[EvidenceItem]:
    """Extrai evidências para um PDF inventariado, sem OCR."""
    pdf_path = Path(entry.pdf_path)
    if not pdf_path.exists():
        return [
            EvidenceItem(
                pdf_filename=entry.pdf_filename,
                page_number=0,
                evidence_type=EVIDENCE_EXTRACTION_ERROR,
                quoted_snippet="",
                parser_notes="arquivo PDF não encontrado no caminho informado",
                extraction_confidence="LOW",
            )
        ]

    try:
        pages = extract_text_by_page(pdf_path)
    except Exception as exc:
        return [
            EvidenceItem(
                pdf_filename=entry.pdf_filename,
                page_number=0,
                evidence_type=EVIDENCE_EXTRACTION_ERROR,
                quoted_snippet="",
                parser_notes=f"falha de extração: {exc}",
                extraction_confidence="LOW",
            )
        ]

    evidence = classify_evidence_snippets(entry.pdf_filename, pages)
    if evidence:
        return evidence

    return [
        EvidenceItem(
            pdf_filename=entry.pdf_filename,
            page_number=1 if pages else 0,
            evidence_type=EVIDENCE_TEXT_FRAGMENT,
            quoted_snippet="",
            parser_notes="nenhuma evidência candidata detectada",
            extraction_confidence="LOW",
        )
    ]


def save_evidence_jsonl(evidence: list[EvidenceItem], outpath: Path) -> None:
    """Salva evidências em JSONL (uma evidência por linha)."""
    ensure_parent_dir(outpath)
    with outpath.open("w", encoding="utf-8") as out_file:
        for item in evidence:
            out_file.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")
