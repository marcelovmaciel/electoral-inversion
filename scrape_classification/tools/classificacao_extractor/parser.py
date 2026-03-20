"""Agente 3: parser conservador para registros estruturados de classificação."""

from __future__ import annotations

import re
from collections import defaultdict

from .constants import (
    CLASSIFICATION_HINT_REGEX,
    EVIDENCE_CLASSIFICATION,
    EVIDENCE_IDEOLOGY,
    EVIDENCE_METHOD,
    EVIDENCE_PARTY,
    EVIDENCE_PERIOD,
    EVIDENCE_TABLE,
    EVIDENCE_YEAR,
    IDEOLOGY_REGEX,
    PARTY_FULLNAME_PATTERN,
    PARTY_SIGLA_PATTERN,
    PARTY_STOPWORDS,
    PERIOD_PATTERN,
    YEAR_PATTERN,
    YEAR_RANGE_PATTERN,
)
from .models import ClassificationRecord, EvidenceItem
from .utils import collapse_spaces, shrink_text


def normalize_party_name_minimal(name: str) -> str:
    """Normaliza nome de partido de forma mínima, sem expansão semântica."""
    normalized = collapse_spaces(name.strip())
    return normalized.upper()


def _guess_year_type(text: str) -> str:
    lowered = text.lower()
    if "elei" in lowered:
        return "electoral_year"
    if "survey" in lowered or "question" in lowered:
        return "survey_year"
    if "public" in lowered or "vol." in lowered or "dados," in lowered:
        return "publication_year"
    if "mandato" in lowered or "legisl" in lowered:
        return "legislative_period"
    return "unspecified_year"


def _extract_year_mentions_from_text(text: str) -> list[dict]:
    mentions: list[dict] = []
    seen: set[tuple[str, int | None, int | None]] = set()

    for m in YEAR_RANGE_PATTERN.finditer(text):
        y1 = int(m.group(1))
        y2 = int(m.group(2))
        raw = m.group(0)
        key = (raw, y1, y2)
        if key in seen:
            continue
        seen.add(key)
        mentions.append(
            {
                "raw": raw,
                "reference_year_start": min(y1, y2),
                "reference_year_end": max(y1, y2),
                "year_type": "period_range",
            }
        )

    for m in PERIOD_PATTERN.finditer(text):
        y1 = int(m.group(1))
        y2 = int(m.group(2))
        raw = m.group(0)
        key = (raw, y1, y2)
        if key in seen:
            continue
        seen.add(key)
        mentions.append(
            {
                "raw": raw,
                "reference_year_start": min(y1, y2),
                "reference_year_end": max(y1, y2),
                "year_type": "period_range",
            }
        )

    for m in YEAR_PATTERN.finditer(text):
        y = int(m.group(1))
        raw = m.group(0)
        key = (raw, y, y)
        if key in seen:
            continue
        seen.add(key)
        mentions.append(
            {
                "raw": raw,
                "reference_year_start": y,
                "reference_year_end": y,
                "year_type": _guess_year_type(text),
            }
        )

    return mentions


def infer_year_mentions_from_evidence(evidence: list[EvidenceItem]) -> list[dict]:
    """Extrai candidatos de anos/períodos a partir das evidências textuais."""
    mentions: list[dict] = []
    seen: dict[tuple[str, int], dict] = {}

    def _relevant_year_context(item: EvidenceItem) -> bool:
        if item.evidence_type in {
            EVIDENCE_CLASSIFICATION,
            EVIDENCE_TABLE,
            EVIDENCE_METHOD,
            EVIDENCE_PERIOD,
            EVIDENCE_IDEOLOGY,
        }:
            return True
        if item.evidence_type == EVIDENCE_YEAR:
            lowered = item.quoted_snippet.lower()
            markers = [
                "elei",
                "survey",
                "escala",
                "classifica",
                "ideolog",
                "partido",
                "tabela",
                "quadro",
                "reta",
            ]
            return any(marker in lowered for marker in markers)
        return False

    def _year_type_priority(value: str) -> int:
        ranking = {
            "period_range": 5,
            "survey_year": 4,
            "electoral_year": 3,
            "publication_year": 2,
            "legislative_period": 2,
            "unspecified_year": 1,
        }
        return ranking.get(value, 0)

    for item in evidence:
        if not _relevant_year_context(item):
            continue
        extracted = _extract_year_mentions_from_text(item.quoted_snippet)
        for row in extracted:
            key = (row["raw"], item.page_number)
            candidate = {
                "raw": row["raw"],
                "reference_year_start": row["reference_year_start"],
                "reference_year_end": row["reference_year_end"],
                "year_type": row["year_type"],
                "page_number": item.page_number,
                "snippet": shrink_text(item.quoted_snippet, 300),
            }

            if key not in seen:
                seen[key] = candidate
                continue

            existing = seen[key]
            if _year_type_priority(candidate["year_type"]) > _year_type_priority(existing["year_type"]):
                seen[key] = candidate

    mentions = list(seen.values())
    return sorted(mentions, key=lambda x: (x["page_number"], x["reference_year_start"] or 0, x["raw"]))


def _line_looks_table_row(line: str) -> bool:
    compact = line.strip()
    if not compact:
        return False
    return bool(re.search(r"\d", compact) and re.search(r"\s{2,}", compact) and re.search(r"[A-Za-zÀ-ÿ]", compact))


GENERIC_NON_PARTY_TOKENS = {
    "a",
    "all",
    "as",
    "catch",
    "centro",
    "classificacao",
    "classificação",
    "da",
    "das",
    "de",
    "direita",
    "do",
    "dos",
    "e",
    "esquerda",
    "extrema",
    "figura",
    "fonte",
    "ideologia",
    "na",
    "no",
    "o",
    "os",
    "partido",
    "partidos",
    "sistema",
    "survey",
    "tabela",
    "que",
}


def _is_valid_party_token(token: str, *, table_context: bool = False) -> bool:
    clean = token.strip(".,;:()[]{}")
    if not clean:
        return False
    if not re.fullmatch(r"[A-Za-zÀ-ÿ-]{2,20}", clean):
        return False

    lower = clean.lower()
    upper = clean.upper()

    if lower in GENERIC_NON_PARTY_TOKENS:
        return False
    if upper in PARTY_STOPWORDS:
        return False

    if clean.islower():
        return False
    if not (clean.isupper() or clean[0].isupper()):
        return False

    if len(clean) <= 2:
        # Siglas de 2 letras só entram em contexto forte (tabela, parênteses ou "Partido XX").
        return table_context and clean.isupper()

    if clean.isupper() and len(clean) >= 7 and not table_context:
        return False

    return True


def _extract_party_candidates(text: str) -> list[str]:
    parties: set[str] = set()

    for line in text.splitlines():
        compact = line.strip()
        if not compact:
            continue

        # Nome partidário explícito em linguagem natural.
        for match in PARTY_FULLNAME_PATTERN.finditer(compact):
            value = collapse_spaces(match.group(0).strip())
            if "polític" in value.lower() or "politic" in value.lower():
                continue
            parties.add(value)

        # Siglas entre parênteses, padrão comum em textos acadêmicos.
        for sigla in re.findall(r"\(([A-Z]{2,8})\)", compact):
            if _is_valid_party_token(sigla, table_context=True):
                parties.add(sigla)

        # Captura sigla após "Partido XX".
        for sigla in re.findall(r"(?i)\bpartido\s+([A-Z]{2,8})\b", compact):
            if _is_valid_party_token(sigla, table_context=True):
                parties.add(sigla)

        # Em linha de tabela, use apenas o primeiro campo como candidato partidário.
        if _line_looks_table_row(compact):
            first = re.split(r"\s+", compact, maxsplit=1)[0].strip(".,;:()[]")
            if _is_valid_party_token(first, table_context=True):
                parties.add(first)

        # Último fallback conservador: siglas em caixa alta em linha que já cite "partido(s)".
        if re.search(r"(?i)\bpartido(s)?\b", compact):
            for token in PARTY_SIGLA_PATTERN.findall(compact):
                if _is_valid_party_token(token, table_context=True):
                    parties.add(token)

    filtered = [p for p in parties if not re.fullmatch(r"\d+", p)]
    return sorted(filtered)


def extract_candidate_party_lines(evidence: list[EvidenceItem]) -> list[EvidenceItem]:
    """Retorna evidências com potencial menção de partido específico."""
    candidates: list[EvidenceItem] = []
    for item in evidence:
        if item.evidence_type not in {EVIDENCE_PARTY, EVIDENCE_TABLE, EVIDENCE_CLASSIFICATION}:
            continue
        if _extract_party_candidates(item.quoted_snippet):
            candidates.append(item)
    return candidates


def _snippet_is_non_ideology_table(snippet: str) -> bool:
    lowered = snippet.lower()
    negative_markers = [
        "taxa de não respostas",
        "taxa de nao respostas",
        "não respostas por partido",
        "coeficiente de variação",
        "coeficiente de variacao",
    ]
    return any(marker in lowered for marker in negative_markers)


def _is_plausible_ideology_numeric(value_raw: str | None) -> bool:
    if not value_raw:
        return False
    try:
        value = float(value_raw.replace(",", "."))
    except ValueError:
        return False
    return -1.0 <= value <= 11.0


def _snippet_is_relevant_to_classification(snippet: str) -> bool:
    if _snippet_is_non_ideology_table(snippet):
        return False
    if IDEOLOGY_REGEX.search(snippet) or CLASSIFICATION_HINT_REGEX.search(snippet):
        return True

    # fallback conservador: linha de tabela com número plausível da escala 0-10
    for line in snippet.splitlines():
        if re.search(r"\s{2,}", line) and re.search(r"\d", line):
            numbers = re.findall(r"(?<!\d)-?\d{1,2}(?:[\.,]\d{1,3})?(?!\d)", line)
            if numbers and _is_plausible_ideology_numeric(numbers[0]):
                return True
    return False


def _extract_party_line(snippet: str, party: str) -> str:
    party_escaped = re.escape(party)
    for line in snippet.splitlines():
        if re.search(party_escaped, line, flags=re.IGNORECASE):
            return line.strip()
    return snippet


def _extract_ideology_value_for_party(snippet: str, party: str) -> tuple[str | None, str]:
    party_line = _extract_party_line(snippet, party)

    category_match = re.search(
        r"\b(extrema\s+esquerda|esquerda|centro-esquerda|centro-direita|centro|direita|extrema\s+direita)\b",
        party_line,
        flags=re.IGNORECASE,
    )
    if category_match:
        raw = collapse_spaces(category_match.group(1))
        return raw, "CATEGORICAL"

    number_candidates = re.findall(r"(?<!\d)-?\d{1,2}(?:[\.,]\d{1,3})?(?!\d)", party_line)
    if number_candidates:
        # Em tabelas, o primeiro valor numérico costuma ser a métrica principal do partido.
        numeric = number_candidates[0]
        if numeric not in {"0", "1"} or _is_plausible_ideology_numeric(numeric):
            return numeric, "NUMERIC"

    return None, "UNKNOWN"


def _infer_scale_type(snippet: str, ideology_value_type: str) -> tuple[str, str | None]:
    lowered = snippet.lower()
    if ideology_value_type == "NUMERIC":
        if re.search(r"zero\s+a\s+dez|0\s*a\s*10|onze\s+pontos|escala", lowered):
            return "NUMERIC_LEFT_RIGHT", "escala espacial numérica de esquerda-direita"
        return "NUMERIC", "valor numérico em trecho potencialmente classificatório"

    if ideology_value_type == "CATEGORICAL":
        return "CATEGORICAL_LEFT_RIGHT", "categoria textual de esquerda-direita"

    if "escala" in lowered:
        return "UNSPECIFIED_SCALE", "menção de escala sem valor partidário explícito"

    return "UNKNOWN", None


def _reference_time_for_record(
    snippet: str,
    page_number: int,
    years_by_page: dict[int, list[dict]],
) -> tuple[str | None, int | None, int | None, str, bool]:
    local = _extract_year_mentions_from_text(snippet)
    if local:
        if len(local) == 1:
            row = local[0]
            return (
                row["raw"],
                row["reference_year_start"],
                row["reference_year_end"],
                row["year_type"],
                False,
            )

        raws = sorted({item["raw"] for item in local})
        return (
            "; ".join(raws),
            None,
            None,
            "ambiguous",
            True,
        )

    page_years = years_by_page.get(page_number, [])
    unique_raw = sorted({y["raw"] for y in page_years})
    if len(unique_raw) == 1:
        row = page_years[0]
        return (
            row["raw"],
            row["reference_year_start"],
            row["reference_year_end"],
            row["year_type"],
            False,
        )
    if len(unique_raw) > 1:
        return (
            "; ".join(unique_raw),
            None,
            None,
            "ambiguous",
            True,
        )
    return None, None, None, "unknown", False


def build_classification_records(pdf_filename: str, evidence: list[EvidenceItem]) -> list[ClassificationRecord]:
    """Constrói registros conservadores sem extrapolar além dos snippets."""
    year_mentions = infer_year_mentions_from_evidence(evidence)
    years_by_page: dict[int, list[dict]] = defaultdict(list)
    for year_row in year_mentions:
        years_by_page[year_row["page_number"]].append(year_row)

    pub_candidates = {
        row["reference_year_start"]
        for row in year_mentions
        if row["year_type"] == "publication_year" and row["reference_year_start"] == row["reference_year_end"]
    }
    publication_year = next(iter(pub_candidates)) if len(pub_candidates) == 1 else None

    method_by_page: dict[int, str] = {}
    for item in evidence:
        if item.evidence_type == EVIDENCE_METHOD and item.page_number not in method_by_page:
            method_by_page[item.page_number] = shrink_text(item.quoted_snippet, 240)

    candidate_lines = extract_candidate_party_lines(evidence)

    records: list[ClassificationRecord] = []
    seen: set[tuple[str, int, str, str, bool, bool]] = set()

    for item in candidate_lines:
        snippet = item.quoted_snippet.strip()
        if not snippet:
            continue

        parties = _extract_party_candidates(snippet)
        if not parties:
            continue

        for party in parties:
            party_line = _extract_party_line(snippet, party)
            party_norm = normalize_party_name_minimal(party)
            relevance = _snippet_is_relevant_to_classification(snippet)
            appears_in_relevant_table = item.evidence_type in {EVIDENCE_TABLE, EVIDENCE_CLASSIFICATION} and relevance

            ideology_value_raw, ideology_value_type = _extract_ideology_value_for_party(snippet, party)
            if ideology_value_type == "NUMERIC" and not _is_plausible_ideology_numeric(ideology_value_raw):
                ideology_value_raw = None
                ideology_value_type = "UNKNOWN"

            explicit_context = bool(
                IDEOLOGY_REGEX.search(snippet)
                or CLASSIFICATION_HINT_REGEX.search(snippet)
                or item.evidence_type == EVIDENCE_CLASSIFICATION
                or appears_in_relevant_table
            )
            explicitly_classified = bool(ideology_value_raw and explicit_context and relevance)

            if not explicitly_classified:
                ideology_value_raw = None
                if ideology_value_type == "NUMERIC":
                    ideology_value_type = "UNKNOWN"

            scale_type, scale_description = _infer_scale_type(snippet, ideology_value_type)
            ref_raw, year_start, year_end, year_type, ambiguous_year = _reference_time_for_record(
                snippet=snippet,
                page_number=item.page_number,
                years_by_page=years_by_page,
            )

            ambiguity_flag = ambiguous_year
            if len(parties) > 1 and not explicitly_classified:
                ambiguity_flag = True

            parser_notes = [f"source_evidence={item.evidence_type}"]
            if not relevance:
                parser_notes.append("snippet sem marcador ideológico robusto")
            if ambiguity_flag:
                parser_notes.append("ambiguidade detectada")

            key = (
                party_norm,
                item.page_number,
                ideology_value_raw or "",
                item.evidence_type,
                appears_in_relevant_table,
                explicitly_classified,
            )
            if key in seen:
                continue
            seen.add(key)

            records.append(
                ClassificationRecord(
                    pdf_filename=pdf_filename,
                    citation_key_candidate=pdf_filename.rsplit(".", 1)[0],
                    publication_year=publication_year,
                    reference_time_raw=ref_raw,
                    reference_year_start=year_start,
                    reference_year_end=year_end,
                    year_type=year_type,
                    party_name_raw=party,
                    party_name_normalized_minimal=party_norm,
                    mentioned_in_article=True,
                    appears_in_relevant_table=appears_in_relevant_table,
                    explicitly_classified=explicitly_classified,
                    ideology_value_raw=ideology_value_raw,
                    ideology_value_type=ideology_value_type,
                    scale_type=scale_type,
                    scale_description=scale_description,
                    methodology_note=method_by_page.get(item.page_number),
                    page_number=item.page_number,
                    quoted_snippet=snippet,
                    evidence_type=item.evidence_type,
                    extraction_confidence=item.extraction_confidence,
                    ambiguity_flag=ambiguity_flag,
                    parser_notes="; ".join(parser_notes),
                )
            )

    return sorted(records, key=lambda r: (r.page_number, r.party_name_normalized_minimal or "", r.evidence_type))
