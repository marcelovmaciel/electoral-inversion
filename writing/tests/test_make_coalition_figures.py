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
    "ideological_interval_heatmap_legend.pdf",
    "minimal_connected_winning_inversions_3x1_diamond.pdf",
    "inversion_decomposition_components.pdf",
    "accounting_state_weighting_anatomy.pdf",
    "district_electoral_weight_by_magnitude.pdf",
    "cross_domain_components.pdf",
}

EXPECTED_PNGS = {"minimal_connected_winning_inversions_3x1_diamond.png"}


# A deliberately small fixture registry checks dynamic focal-case handling.
FIXTURE_STATE_WEIGHTING_CASES = (
    ("cabinet/test/first", "Cabinet first"),
    ("ideological/seat_winning/test/second", "Parliamentary second"),
    ("ideological/seat_winning/test/third", "Parliamentary third"),
)


def state_weighting_fixture() -> pd.DataFrame:
    rows = []
    for focal_order, (case_id, case_display) in enumerate(
        FIXTURE_STATE_WEIGHTING_CASES, start=1
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
    tables_dir = artifact_root / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(FIXTURE_STATE_WEIGHTING_CASES, columns=["case_id", "case_display"]).to_csv(
        tables_dir / "table_accounting_focal_cases.csv", index=False
    )
    return path


class StateWeightingAnatomyRegressions(unittest.TestCase):
    def test_loader_accepts_generated_case_registry_and_sorts_focal_order(self) -> None:
        with tempfile.TemporaryDirectory(prefix="state-weighting-loader-") as temp_dir:
            artifact_root = Path(temp_dir)
            write_state_weighting_fixture(artifact_root, state_weighting_fixture())
            loaded = figures.load_accounting_state_weighting_anatomy(artifact_root)

        self.assertEqual(tuple(loaded["focal_order"]), tuple(range(1, len(FIXTURE_STATE_WEIGHTING_CASES) + 1)))
        self.assertEqual(
            tuple(loaded["case_id"]),
            tuple(case_id for case_id, _ in FIXTURE_STATE_WEIGHTING_CASES),
        )
        self.assertEqual(len(loaded), len(FIXTURE_STATE_WEIGHTING_CASES))

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
            with self.assertRaisesRegex(ValueError, "differs from the generated focal registry"):
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
    def test_actual_artifacts_match_primary_registry_and_cabinet_regressions(self) -> None:
        observed = figures.load_observed_coalition_timeline(ARTIFACT_ROOT)
        self.assertEqual(len(observed), 23)
        self.assertEqual(int(observed["coalition_inversion"].sum()), 4)

        ideological = figures.load_ideological_interval_heatmap(ARTIFACT_ROOT)
        self.assertEqual(set(ideological["ideological_universe"]), {"seat_winning"})
        registry = pd.read_csv(ARTIFACT_ROOT / "raw" / "ideology_k_gap_minimal_majorities.csv")
        registry = registry[(registry["ideological_universe"] == "seat_winning") & (registry["k"] == 0)]
        for year, rows in ideological.groupby("election_year"):
            minimal_inversions = rows[rows["minimal_ideological_interval_inversion"]]
            source = registry[(registry["election"] == year) & registry["inversion"]]
            self.assertEqual(len(minimal_inversions), len(source))
            self.assertEqual(
                set(zip(minimal_inversions["start_party"], minimal_inversions["end_party"])),
                set(zip(source["left_endpoint"], source["right_endpoint"])),
            )
        # The parliamentary order admits the 2018 exact-connected inversion.
        inversion_2018 = ideological[(ideological["election_year"] == 2018)
            & ideological["minimal_ideological_interval_inversion"]]
        self.assertEqual(len(inversion_2018), 1)
        self.assertEqual(tuple(inversion_2018[["start_party", "end_party"]].iloc[0]), ("PT", "PSDB"))
        self.assertTrue((inversion_2018["vote_share"] < 0.5).all())
        self.assertTrue((inversion_2018["seats"] >= 257).all())

        decomposition = figures.load_inversion_decomposition_components(ARTIFACT_ROOT)
        self.assertEqual(len(decomposition), 4)

        anatomy = figures.load_accounting_state_weighting_anatomy(ARTIFACT_ROOT)
        focal_registry = pd.read_csv(ARTIFACT_ROOT / "tables" / "table_accounting_focal_cases.csv")
        self.assertEqual(set(anatomy["case_id"]), set(focal_registry["case_id"]))
        self.assertEqual(tuple(anatomy["focal_order"]), tuple(range(1, len(focal_registry) + 1)))

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
            self.assertEqual({path.name for path in outputs}, EXPECTED_PDFS | EXPECTED_PNGS)
            self.assertEqual({path.name for path in output_dir.glob("*.pdf")}, EXPECTED_PDFS)
            self.assertEqual({path.name for path in output_dir.glob("*.png")}, EXPECTED_PNGS)
            for path in outputs:
                self.assertTrue(path.is_file(), path)
                self.assertGreater(path.stat().st_size, 1_000, path)
                signature = b"%PDF-" if path.suffix == ".pdf" else b"\x89PNG\r\n\x1a\n"
                self.assertTrue(path.read_bytes().startswith(signature), path)


if __name__ == "__main__":
    unittest.main()
