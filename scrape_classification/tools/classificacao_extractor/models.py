"""Modelos de dados da pipeline de classificação ideológica."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class PDFInventoryEntry:
    """Registro de inventário e diagnóstico de extração de um PDF."""

    pdf_filename: str
    pdf_path: str
    file_hash: str
    file_size: int
    page_count: int | None
    pdf_type: str
    extraction_status: str
    diagnostic_notes: str

    def to_dict(self) -> dict:
        """Serializa o objeto para dicionário simples."""
        return asdict(self)


@dataclass
class EvidenceItem:
    """Evidência textual extraída de uma página específica."""

    pdf_filename: str
    page_number: int
    evidence_type: str
    quoted_snippet: str
    parser_notes: str
    extraction_confidence: str

    def to_dict(self) -> dict:
        """Serializa o objeto para dicionário simples."""
        return asdict(self)


@dataclass
class ClassificationRecord:
    """Registro estruturado de partido derivado estritamente de evidência textual."""

    pdf_filename: str
    citation_key_candidate: str | None
    publication_year: int | None
    reference_time_raw: str | None
    reference_year_start: int | None
    reference_year_end: int | None
    year_type: str
    party_name_raw: str | None
    party_name_normalized_minimal: str | None
    mentioned_in_article: bool
    appears_in_relevant_table: bool
    explicitly_classified: bool
    ideology_value_raw: str | None
    ideology_value_type: str
    scale_type: str
    scale_description: str | None
    methodology_note: str | None
    page_number: int
    quoted_snippet: str
    evidence_type: str
    extraction_confidence: str
    ambiguity_flag: bool
    parser_notes: str

    def to_dict(self) -> dict:
        """Serializa o objeto para dicionário simples."""
        return asdict(self)


@dataclass
class MatchResult:
    """Resultado de matching entre partido do CSV e registros extraídos dos PDFs."""

    party_from_csv: str
    period_from_csv: str | None
    exact_match_found: bool
    conservative_match_found: bool
    probable_match_found: bool
    matched_party_name: str | None
    matched_pdf_count: int
    match_method: str | None
    mismatch_type: str
    notes: str

    def to_dict(self) -> dict:
        """Serializa o objeto para dicionário simples."""
        return asdict(self)
