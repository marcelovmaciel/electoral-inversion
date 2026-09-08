import os
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


os.environ.setdefault("MPLBACKEND", "Agg")

REPO_ROOT = Path(__file__).resolve().parents[2]
WRITING_DIR = REPO_ROOT / "writing"
sys.path.insert(0, str(WRITING_DIR))

import make_coalition_figures as figures  # noqa: E402


ARTIFACT_ROOT = REPO_ROOT / "processing" / "Processing" / "output" / "paper"
EXPECTED_PDFS = {
    "party_vote_share_vs_seat_share.pdf",
    "observed_coalition_timeline.pdf",
    "ideological_interval_heatmap_2014.pdf",
    "ideological_interval_heatmap_2018.pdf",
    "ideological_interval_heatmap_2022.pdf",
    "inversion_decomposition_components.pdf",
    "accounting_state_weighting_anatomy.pdf",
    "district_electoral_weight_by_magnitude.pdf",
    "cross_domain_components.pdf",
}


def state_weighting_fixture() -> pd.DataFrame:
    rows = []
    for focal_order, (case_id, case_display) in enumerate(
        figures.EXPECTED_STATE_WEIGHTING_CASES, start=1
    ):
        positive_eight = 3.0 + 0.25 * focal_order
        positive_other = 2.0
        negative_sp = 4.0
        negative_other = 1.5
        rows.append(
            {
                "case_id": case_id,
                "case_display": case_display,
                "case_order": 100 - focal_order,
                "focal_order": focal_order,
                "b_positive_eight_seat": positive_eight,
                "b_positive_other": positive_other,
                "b_negative_sp": negative_sp,
                "b_negative_other": negative_other,
                "B_C": positive_eight + positive_other - negative_sp - negative_other,
                "largest_positive_state": "RR",
                "largest_positive_b_Cd": 1.25,
            }
        )
    return pd.DataFrame(rows[::-1])


def write_state_weighting_fixture(artifact_root: Path, data: pd.DataFrame) -> Path:
    figure_data_dir = artifact_root / "figure_data"
    figure_data_dir.mkdir(parents=True, exist_ok=True)
    path = figure_data_dir / "accounting_state_weighting_anatomy.csv"
    data.to_csv(path, index=False)
    return path


class StateWeightingAnatomyRegressions(unittest.TestCase):
    def test_loader_accepts_exact_seven_case_registry_and_sorts_focal_order(self) -> None:
        with tempfile.TemporaryDirectory(prefix="state-weighting-loader-") as temp_dir:
            artifact_root = Path(temp_dir)
            write_state_weighting_fixture(artifact_root, state_weighting_fixture())
            loaded = figures.load_accounting_state_weighting_anatomy(artifact_root)

        self.assertEqual(tuple(loaded["focal_order"]), tuple(range(1, 8)))
        self.assertEqual(
            tuple(loaded["case_id"]),
            tuple(case_id for case_id, _ in figures.EXPECTED_STATE_WEIGHTING_CASES),
        )
        self.assertEqual(len(loaded), 7)

    def test_loader_rejects_missing_schema_column(self) -> None:
        with tempfile.TemporaryDirectory(prefix="state-weighting-schema-") as temp_dir:
            artifact_root = Path(temp_dir)
            data = state_weighting_fixture().drop(columns="b_negative_sp")
            write_state_weighting_fixture(artifact_root, data)
            with self.assertRaisesRegex(ValueError, "b_negative_sp"):
                figures.load_accounting_state_weighting_anatomy(artifact_root)

    def test_loader_rejects_changed_focal_registry(self) -> None:
        with tempfile.TemporaryDirectory(prefix="state-weighting-registry-") as temp_dir:
            artifact_root = Path(temp_dir)
            data = state_weighting_fixture()
            data.loc[data.index[0], "case_id"] = "ideological/2022/unrestricted"
            write_state_weighting_fixture(artifact_root, data)
            with self.assertRaisesRegex(ValueError, "focal-case registry changed"):
                figures.load_accounting_state_weighting_anatomy(artifact_root)

    def test_renderer_writes_pdf_from_validated_csv(self) -> None:
        with tempfile.TemporaryDirectory(prefix="state-weighting-render-") as temp_dir:
            root = Path(temp_dir)
            artifact_root = root / "artifacts"
            output_dir = root / "figures"
            output_dir.mkdir()
            write_state_weighting_fixture(artifact_root, state_weighting_fixture())
            output = figures.save_accounting_state_weighting_anatomy(
                artifact_root, output_dir
            )
            self.assertEqual(output.name, "accounting_state_weighting_anatomy.pdf")
            self.assertGreater(output.stat().st_size, 1_000)
            self.assertEqual(output.read_bytes()[:5], b"%PDF-")


class CoalitionFigureOutputRegressions(unittest.TestCase):
    def test_actual_artifacts_have_frozen_empirical_counts(self) -> None:
        observed = figures.load_observed_coalition_timeline(ARTIFACT_ROOT)
        self.assertEqual(len(observed), 23)
        self.assertEqual(int(observed["coalition_inversion"].sum()), 4)

        ideological = figures.load_ideological_interval_heatmap(ARTIFACT_ROOT)
        actual_counts = {
            int(year): (
                int(rows["coalition_inversion"].sum()),
                int(rows["minimal_ideological_interval_inversion"].sum()),
            )
            for year, rows in ideological.groupby("election_year")
        }
        self.assertEqual(actual_counts, figures.EXPECTED_IDEOLOGICAL_COUNTS)

        decomposition = figures.load_inversion_decomposition_components(ARTIFACT_ROOT)
        self.assertEqual(len(decomposition), 4)

        anatomy = figures.load_accounting_state_weighting_anatomy(ARTIFACT_ROOT)
        self.assertEqual(len(anatomy), 7)
        self.assertEqual(
            tuple(anatomy["case_id"]),
            tuple(case_id for case_id, _ in figures.EXPECTED_STATE_WEIGHTING_CASES),
        )

        district_weights = figures.load_district_electoral_weight(ARTIFACT_ROOT)
        self.assertEqual(len(district_weights), 81)
        self.assertEqual(
            tuple(sorted(district_weights["election_year"].unique())),
            (2014, 2018, 2022),
        )

    def test_actual_artifacts_generate_every_expected_pdf(self) -> None:
        with tempfile.TemporaryDirectory(prefix="coalition-figure-test-") as temp_dir:
            output_dir = Path(temp_dir)
            outputs = figures.generate_figures(ARTIFACT_ROOT, output_dir)
            self.assertEqual({path.name for path in outputs}, EXPECTED_PDFS)
            self.assertEqual({path.name for path in output_dir.glob("*.pdf")}, EXPECTED_PDFS)
            for path in outputs:
                self.assertTrue(path.is_file(), path)
                self.assertGreater(path.stat().st_size, 1_000, path)
                self.assertEqual(path.read_bytes()[:5], b"%PDF-", path)


if __name__ == "__main__":
    unittest.main()
