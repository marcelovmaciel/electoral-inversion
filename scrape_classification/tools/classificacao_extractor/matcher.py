"""Agente 4: matching conservador entre CSV de partidos e registros extraídos."""

from __future__ import annotations

import csv
import difflib
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any

from .constants import (
    MATCH_METHOD_CONSERVATIVE,
    MATCH_METHOD_EXACT,
    MATCH_METHOD_PROBABLE,
    MISMATCH_AMBIGUOUS,
    MISMATCH_CLASSIFIED,
    MISMATCH_NOT_FOUND,
    MISMATCH_NO_CONFIDENT,
    MISMATCH_ONLY_MENTION,
    MISMATCH_TABLE_NO_CLASS,
)
from .models import ClassificationRecord, MatchResult

try:  # pragma: no cover - depende do ambiente
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - depende do ambiente
    pd = None


DataFrameLike = Any


def _iter_rows(df: DataFrameLike) -> list[dict[str, Any]]:
    if pd is not None and hasattr(pd, "DataFrame") and isinstance(df, pd.DataFrame):
        return df.to_dict(orient="records")
    if isinstance(df, list):
        return [dict(row) for row in df]
    raise TypeError("formato de tabela não suportado")


def load_partidos_csv(path: Path) -> DataFrameLike:
    """Carrega CSV de partidos com fallback sem pandas."""
    if not path.exists():
        raise FileNotFoundError(f"CSV de partidos não encontrado: {path}")

    if pd is not None:
        return pd.read_csv(path)

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            rows.append(dict(row))
    return rows


def extract_parties_of_interest(df: DataFrameLike) -> list[dict]:
    """Extrai pares (partido, período) do CSV de referência."""
    rows = _iter_rows(df)
    parties: list[dict] = []
    seen: set[tuple[str, str | None]] = set()

    for row in rows:
        party = row.get("partido")
        period = row.get("periodo")
        if party is None:
            continue
        party_text = str(party).strip()
        if not party_text:
            continue
        period_text = str(period).strip() if period is not None else None
        key = (party_text, period_text)
        if key in seen:
            continue
        seen.add(key)
        parties.append({"party": party_text, "period": period_text})

    return parties


def conservative_normalize_for_match(s: str) -> str:
    """Normalização mínima para matching conservador de nomes."""
    base = unicodedata.normalize("NFKD", s)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower().strip()
    base = re.sub(r"[\W_]+", "", base)
    return base


def _record_names(record: ClassificationRecord) -> list[str]:
    names: list[str] = []
    if record.party_name_raw:
        names.append(record.party_name_raw)
    if record.party_name_normalized_minimal:
        names.append(record.party_name_normalized_minimal)
    return names


def _pick_canonical_name(records: list[ClassificationRecord]) -> str | None:
    counts: dict[str, int] = defaultdict(int)
    for rec in records:
        if rec.party_name_raw:
            counts[rec.party_name_raw] += 1
    if not counts:
        return None
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]


def _mismatch_type_from_records(records: list[ClassificationRecord]) -> str:
    if not records:
        return MISMATCH_NOT_FOUND
    if any(rec.explicitly_classified for rec in records):
        return MISMATCH_CLASSIFIED
    if any(rec.appears_in_relevant_table for rec in records):
        return MISMATCH_TABLE_NO_CLASS
    if any(rec.mentioned_in_article for rec in records):
        return MISMATCH_ONLY_MENTION
    return MISMATCH_NO_CONFIDENT


def _make_match_result(
    *,
    party: str,
    period: str | None,
    matched_records: list[ClassificationRecord],
    exact: bool,
    conservative: bool,
    probable: bool,
    method: str | None,
    mismatch_type: str,
    notes: str,
) -> MatchResult:
    matched_party_name = _pick_canonical_name(matched_records)
    pdf_count = len({rec.pdf_filename for rec in matched_records}) if matched_records else 0
    return MatchResult(
        party_from_csv=party,
        period_from_csv=period,
        exact_match_found=exact,
        conservative_match_found=conservative,
        probable_match_found=probable,
        matched_party_name=matched_party_name,
        matched_pdf_count=pdf_count,
        match_method=method,
        mismatch_type=mismatch_type,
        notes=notes,
    )


def match_party_to_records(party: str, records: list[ClassificationRecord]) -> MatchResult:
    """Faz matching partido->registros em ordem exato, conservador, provável."""
    target = party.strip()
    target_norm = conservative_normalize_for_match(target)

    if not target:
        return _make_match_result(
            party=party,
            period=None,
            matched_records=[],
            exact=False,
            conservative=False,
            probable=False,
            method=None,
            mismatch_type=MISMATCH_NOT_FOUND,
            notes="partido vazio no CSV",
        )

    exact_records: list[ClassificationRecord] = []
    for rec in records:
        if any(name.casefold() == target.casefold() for name in _record_names(rec)):
            exact_records.append(rec)

    if exact_records:
        mismatch_type = _mismatch_type_from_records(exact_records)
        return _make_match_result(
            party=target,
            period=None,
            matched_records=exact_records,
            exact=True,
            conservative=True,
            probable=False,
            method=MATCH_METHOD_EXACT,
            mismatch_type=mismatch_type,
            notes=f"match exato encontrado em {len(exact_records)} registro(s)",
        )

    conservative_records: list[ClassificationRecord] = []
    for rec in records:
        rec_names = _record_names(rec)
        if any(conservative_normalize_for_match(name) == target_norm for name in rec_names):
            conservative_records.append(rec)

    if conservative_records:
        candidates = sorted({rec.party_name_raw or "" for rec in conservative_records if rec.party_name_raw})
        if len(candidates) > 1:
            return _make_match_result(
                party=target,
                period=None,
                matched_records=conservative_records,
                exact=False,
                conservative=True,
                probable=False,
                method=MATCH_METHOD_CONSERVATIVE,
                mismatch_type=MISMATCH_AMBIGUOUS,
                notes=f"múltiplos candidatos no match conservador: {', '.join(candidates)}",
            )

        mismatch_type = _mismatch_type_from_records(conservative_records)
        return _make_match_result(
            party=target,
            period=None,
            matched_records=conservative_records,
            exact=False,
            conservative=True,
            probable=False,
            method=MATCH_METHOD_CONSERVATIVE,
            mismatch_type=mismatch_type,
            notes=f"match conservador encontrado em {len(conservative_records)} registro(s)",
        )

    probable_candidates: dict[str, tuple[float, list[ClassificationRecord]]] = {}
    if len(target_norm) >= 4:
        by_name: dict[str, list[ClassificationRecord]] = defaultdict(list)
        for rec in records:
            if rec.party_name_raw:
                by_name[rec.party_name_raw].append(rec)

        for candidate_name, recs in by_name.items():
            cand_norm = conservative_normalize_for_match(candidate_name)
            if len(cand_norm) < 4:
                continue
            ratio = difflib.SequenceMatcher(a=target_norm, b=cand_norm).ratio()
            probable_candidates[candidate_name] = (ratio, recs)

    if probable_candidates:
        ranked = sorted(probable_candidates.items(), key=lambda kv: kv[1][0], reverse=True)
        top_name, (top_ratio, top_records) = ranked[0]
        second_ratio = ranked[1][1][0] if len(ranked) > 1 else 0.0

        if top_ratio >= 0.93 and (top_ratio - second_ratio >= 0.05):
            mismatch_type = _mismatch_type_from_records(top_records)
            return _make_match_result(
                party=target,
                period=None,
                matched_records=top_records,
                exact=False,
                conservative=False,
                probable=True,
                method=MATCH_METHOD_PROBABLE,
                mismatch_type=mismatch_type,
                notes=f"match provável: {top_name} (score={top_ratio:.3f})",
            )

        if top_ratio >= 0.90 and (top_ratio - second_ratio < 0.05):
            ambiguous_records: list[ClassificationRecord] = []
            names = []
            for name, (score, recs) in ranked[:3]:
                if score >= top_ratio - 0.03:
                    names.append(f"{name}({score:.3f})")
                    ambiguous_records.extend(recs)
            return _make_match_result(
                party=target,
                period=None,
                matched_records=ambiguous_records,
                exact=False,
                conservative=False,
                probable=True,
                method=MATCH_METHOD_PROBABLE,
                mismatch_type=MISMATCH_AMBIGUOUS,
                notes=f"match provável ambíguo: {', '.join(names)}",
            )

    return _make_match_result(
        party=target,
        period=None,
        matched_records=[],
        exact=False,
        conservative=False,
        probable=False,
        method=None,
        mismatch_type=MISMATCH_NOT_FOUND,
        notes="nenhum match confiável",
    )


def build_mismatch_report(
    csv_df: DataFrameLike,
    records: list[ClassificationRecord],
) -> tuple[list[MatchResult], DataFrameLike]:
    """Gera relatório de mismatch e candidatos de correspondência."""
    parties = extract_parties_of_interest(csv_df)
    results: list[MatchResult] = []

    for row in parties:
        base_result = match_party_to_records(row["party"], records)
        base_result.period_from_csv = row.get("period")
        results.append(base_result)

    # Tabela auxiliar de candidatos para auditoria de matching.
    candidate_rows: list[dict[str, Any]] = []
    extracted_unique: dict[str, dict[str, Any]] = {}
    for rec in records:
        if not rec.party_name_raw:
            continue
        name = rec.party_name_raw
        if name not in extracted_unique:
            extracted_unique[name] = {
                "party_extracted": name,
                "party_extracted_norm": conservative_normalize_for_match(name),
                "pdf_count": 0,
                "explicit_count": 0,
                "table_count": 0,
                "mention_count": 0,
            }
        extracted_unique[name]["pdf_count"] = len(
            {
                r.pdf_filename
                for r in records
                if r.party_name_raw == name
            }
        )
        extracted_unique[name]["explicit_count"] += int(rec.explicitly_classified)
        extracted_unique[name]["table_count"] += int(rec.appears_in_relevant_table)
        extracted_unique[name]["mention_count"] += int(rec.mentioned_in_article)

    for row in parties:
        csv_party = row["party"]
        csv_norm = conservative_normalize_for_match(csv_party)
        for extracted_name, stats in extracted_unique.items():
            ext_norm = stats["party_extracted_norm"]
            exact = extracted_name.casefold() == csv_party.casefold()
            conservative = ext_norm == csv_norm
            score = difflib.SequenceMatcher(a=csv_norm, b=ext_norm).ratio() if csv_norm and ext_norm else 0.0
            if exact or conservative or score >= 0.85:
                candidate_rows.append(
                    {
                        "party_from_csv": csv_party,
                        "period_from_csv": row.get("period"),
                        "candidate_party": extracted_name,
                        "exact_match": exact,
                        "conservative_match": conservative,
                        "probable_score": round(score, 4),
                        "candidate_pdf_count": stats["pdf_count"],
                        "candidate_explicit_count": stats["explicit_count"],
                        "candidate_table_count": stats["table_count"],
                        "candidate_mention_count": stats["mention_count"],
                    }
                )

    if pd is not None:
        return results, pd.DataFrame(candidate_rows)
    return results, candidate_rows
