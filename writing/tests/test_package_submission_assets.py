from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from writing import package_submission_assets as package


class DeterministicSubmissionPackageTests(unittest.TestCase):
    def test_archive_is_stable_and_has_fixed_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first = root / "b.txt"
            second = root / "a.txt"
            first.write_text("second alphabetically\n", encoding="utf-8")
            second.write_text("first alphabetically\n", encoding="utf-8")
            destination = root / "assets.zip"

            first_digest = package.write_deterministic_zip(
                destination, [first, second]
            )
            first_bytes = destination.read_bytes()
            second_digest = package.write_deterministic_zip(
                destination, [second, first]
            )

            self.assertEqual(first_digest, second_digest)
            self.assertEqual(first_bytes, destination.read_bytes())
            with ZipFile(destination) as archive:
                self.assertEqual(archive.namelist(), ["a.txt", "b.txt"])
                self.assertTrue(
                    all(info.date_time == package.FIXED_ZIP_TIME for info in archive.infolist())
                )

    def test_missing_input_fails_loudly(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with self.assertRaises(FileNotFoundError):
                package.write_deterministic_zip(
                    root / "assets.zip", [root / "missing.txt"]
                )


    def test_packages_authoritative_source_and_referenced_inputs(self) -> None:
        self.assertEqual(package.MAIN_TEX.name, "main_rw_again.tex")
        tables, figures = package.referenced_assets()
        self.assertIn("table_appendix_ideological_universe_comparison.tex", {p.name for p in tables})
        self.assertIn("table_accounting_minimal_ideological_all_parties.tex", {p.name for p in tables})
        self.assertIn("manuscript_values.tex", {p.name for p in tables})
        self.assertNotIn("accounting_numeric_macros.tex", {p.name for p in tables})
        self.assertNotIn("AccountingIntegration.jl", {p.name for p in package.manuscript_tex_sources()})
        self.assertEqual(set(package.manuscript_tex_sources()), {package.MAIN_TEX, *tables})
        self.assertNotIn("main.tex", {p.name for p in package.manuscript_tex_sources()})
        package.require_files(tables + figures)


if __name__ == "__main__":
    unittest.main()
