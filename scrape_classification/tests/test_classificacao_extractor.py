from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from tools.classificacao_extractor.auditor import validate_record_semantics
from tools.classificacao_extractor.constants import EXTRACTION_NEEDS_OCR, PDF_TYPE_SCANNED
from tools.classificacao_extractor.matcher import MATCH_METHOD_CONSERVATIVE, MATCH_METHOD_EXACT, match_party_to_records
from tools.classificacao_extractor.models import ClassificationRecord
from tools.classificacao_extractor.parser import normalize_party_name_minimal
from tools.classificacao_extractor.pdf_inventory import classify_pdf_text_profile
from tools.classificacao_extractor.reporting import write_csv_from_records, write_json


class ClassificacaoExtractorTests(unittest.TestCase):
    def _sample_record(self, **overrides) -> ClassificationRecord:
        data = {
            "pdf_filename": "sample.pdf",
            "citation_key_candidate": "sample",
            "publication_year": 2023,
            "reference_time_raw": "2018",
            "reference_year_start": 2018,
            "reference_year_end": 2018,
            "year_type": "survey_year",
            "party_name_raw": "PT",
            "party_name_normalized_minimal": "PT",
            "mentioned_in_article": True,
            "appears_in_relevant_table": True,
            "explicitly_classified": True,
            "ideology_value_raw": "2,97",
            "ideology_value_type": "NUMERIC",
            "scale_type": "NUMERIC_LEFT_RIGHT",
            "scale_description": "escala 0-10",
            "methodology_note": "survey com experts",
            "page_number": 6,
            "quoted_snippet": "PT 2,97",
            "evidence_type": "CLASSIFICATION_CANDIDATE",
            "extraction_confidence": "HIGH",
            "ambiguity_flag": False,
            "parser_notes": "teste",
        }
        data.update(overrides)
        return ClassificationRecord(**data)

    def test_normalize_party_name_minimal(self) -> None:
        self.assertEqual(normalize_party_name_minimal("  PC do B  "), "PC DO B")
        self.assertEqual(normalize_party_name_minimal("pt"), "PT")

    def test_empty_text_pdf_detection(self) -> None:
        pdf_type, status, _ = classify_pdf_text_profile(["   ", "\n", ""])
        self.assertEqual(pdf_type, PDF_TYPE_SCANNED)
        self.assertEqual(status, EXTRACTION_NEEDS_OCR)

    def test_explicit_requires_ideology_value(self) -> None:
        broken = self._sample_record(ideology_value_raw=None)
        issues = validate_record_semantics([broken])
        self.assertTrue(any("ideology_value_raw" in issue["issue"] for issue in issues))

    def test_exact_vs_conservative_match(self) -> None:
        exact_rec = self._sample_record(party_name_raw="PT", party_name_normalized_minimal="PT")
        conservative_rec = self._sample_record(
            party_name_raw="PC do B",
            party_name_normalized_minimal="PC DO B",
            ideology_value_raw=None,
            explicitly_classified=False,
            appears_in_relevant_table=False,
        )

        exact = match_party_to_records("PT", [exact_rec, conservative_rec])
        self.assertTrue(exact.exact_match_found)
        self.assertEqual(exact.match_method, MATCH_METHOD_EXACT)

        conservative = match_party_to_records("PCdoB", [exact_rec, conservative_rec])
        self.assertFalse(conservative.exact_match_found)
        self.assertTrue(conservative.conservative_match_found)
        self.assertEqual(conservative.match_method, MATCH_METHOD_CONSERVATIVE)

    def test_dataclass_serialization_to_json_csv(self) -> None:
        record = self._sample_record()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            json_path = root / "out.json"
            csv_path = root / "out.csv"

            write_json([record.to_dict()], json_path)
            write_csv_from_records([record], csv_path)

            with json_path.open("r", encoding="utf-8") as fjson:
                payload = json.load(fjson)
            self.assertEqual(payload[0]["party_name_raw"], "PT")

            with csv_path.open("r", encoding="utf-8", newline="") as fcsv:
                rows = list(csv.DictReader(fcsv))
            self.assertEqual(rows[0]["party_name_raw"], "PT")


if __name__ == "__main__":
    unittest.main()
