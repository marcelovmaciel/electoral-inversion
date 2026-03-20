"""Constantes e regex da pipeline de extração conservadora."""

from __future__ import annotations

import re

YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
YEAR_RANGE_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\s*(?:-|–|—|a|até)\s*(19\d{2}|20\d{2})\b", re.IGNORECASE)
PERIOD_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\s*[/\-]\s*(19\d{2}|20\d{2})\b")

IDEOLOGY_REGEX = re.compile(
    r"\b(esquerda|direita|centro|ideolog(?:ia|ias|ico|ica|icamente)?|"
    r"posicionamento|classifica(?:ção|cao|ções|coes)|escala|expert\s+survey|survey)\b",
    re.IGNORECASE,
)

METHODOLOGY_REGEX = re.compile(
    r"\b(metodolog(?:ia|ico|ica)|question[aá]rio|amostra|respondentes|"
    r"coleta|expert\s+survey|survey|escala|pondera(?:ção|cao)|fator\s+de\s+ajuste)\b",
    re.IGNORECASE,
)

TABLE_HINT_REGEX = re.compile(
    r"\b(tabela|quadro|figura|anexo|descritivas|partido|legenda|ideologia|"
    r"classifica(?:ção|cao))\b",
    re.IGNORECASE,
)

CLASSIFICATION_HINT_REGEX = re.compile(
    r"\b(classificad[oa]s?|classifica(?:ção|cao)|posi(?:ção|cao)|"
    r"m[ée]dia\s+ideol[óo]gica|extrema\s+esquerda|extrema\s+direita|"
    r"centro(?:-esquerda|-direita)?)\b",
    re.IGNORECASE,
)

PARTY_FULLNAME_PATTERN = re.compile(
    r"\bPartido\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ-]+(?:\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ-]+){1,5}\b"
)
PARTY_SIGLA_PATTERN = re.compile(r"\b[A-Z]{2,8}\b")

PARTY_STOPWORDS = {
    "A",
    "ABCP",
    "ANEXO",
    "ARTIGO",
    "AS",
    "BRASILEIRO",
    "BRASILEIROS",
    "CESOP",
    "CENTRO",
    "CLASSIFICACAO",
    "CLASSIFICAÇÃO",
    "CV",
    "DA",
    "DADOS",
    "DAS",
    "DE",
    "DIREITA",
    "DEMOCRACIAS",
    "DOI",
    "DOS",
    "DO",
    "E",
    "E",
    "ESQUERDA",
    "EXTREMA",
    "FIGURA",
    "FONTE",
    "IDEOLOGIA",
    "ISSN",
    "LEGENDAS",
    "NO",
    "NOVA",
    "N",
    "NA",
    "O",
    "OS",
    "NOVA",
    "OCDE",
    "OECD",
    "PARTIDO",
    "PARTIDOS",
    "P",
    "PDF",
    "POLITICO",
    "POLITICOS",
    "POLÍTICO",
    "POLÍTICOS",
    "PR",
    "RJ",
    "SC",
    "SISTEMA",
    "SP",
    "TABELA",
    "UFPR",
    "VOL",
}

SNIPPET_MIN_CHARS = 40
SNIPPET_MAX_CHARS = 900

MIN_MEANINGFUL_TEXT_LEN = 25
TEXT_PDF_RATIO_THRESHOLD = 0.80
MIXED_PDF_RATIO_THRESHOLD = 0.05

PDF_TYPE_TEXT = "TEXT"
PDF_TYPE_SCANNED = "SCANNED"
PDF_TYPE_MIXED = "MIXED"
PDF_TYPE_CORRUPTED = "CORRUPTED"
PDF_TYPE_UNKNOWN = "UNKNOWN"

EXTRACTION_READY = "READY"
EXTRACTION_NEEDS_OCR = "NEEDS_OCR"
EXTRACTION_FAILED = "FAILED"

EVIDENCE_YEAR = "YEAR_MENTION"
EVIDENCE_PERIOD = "PERIOD_MENTION"
EVIDENCE_IDEOLOGY = "IDEOLOGY_KEYWORD"
EVIDENCE_PARTY = "PARTY_MENTION"
EVIDENCE_TABLE = "TABLE_CANDIDATE"
EVIDENCE_METHOD = "METHODOLOGY"
EVIDENCE_CLASSIFICATION = "CLASSIFICATION_CANDIDATE"
EVIDENCE_TEXT_FRAGMENT = "TEXT_FRAGMENT"
EVIDENCE_EXTRACTION_ERROR = "EXTRACTION_ERROR"

MATCH_METHOD_EXACT = "exact_casefold"
MATCH_METHOD_CONSERVATIVE = "conservative_normalization"
MATCH_METHOD_PROBABLE = "probable_similarity"

MISMATCH_NOT_FOUND = "CSV_PARTY_NOT_FOUND"
MISMATCH_ONLY_MENTION = "FOUND_ONLY_MENTION"
MISMATCH_TABLE_NO_CLASS = "FOUND_IN_RELEVANT_TABLE_NO_CLASSIFICATION"
MISMATCH_CLASSIFIED = "FOUND_WITH_EXPLICIT_CLASSIFICATION"
MISMATCH_AMBIGUOUS = "MATCH_AMBIGUOUS"
MISMATCH_NO_CONFIDENT = "NO_CONFIDENT_MATCH"
