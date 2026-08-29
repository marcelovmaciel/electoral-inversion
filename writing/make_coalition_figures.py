#!/usr/bin/env python3
"""
Generate the figures used in the coalition-inversions manuscripts.

Figure 2 and the decomposition-component figure consume full-precision inputs
computed by Julia. Python performs presentation-only date, percentage, and
layout formatting after validating the frozen empirical case lists.

Expected input tree:
  <artifact-root>/figure_data/party_vote_share_vs_seat_share.csv
  <artifact-root>/figure_data/observed_coalition_timeline.csv
  <artifact-root>/figure_data/ideological_interval_heatmap.csv
  <artifact-root>/figure_data/inversion_decomposition_components.csv

Outputs:
  party_vote_share_vs_seat_share.pdf
  observed_coalition_timeline.pdf
  ideological_interval_heatmap_2014.pdf
  ideological_interval_heatmap_2018.pdf
  ideological_interval_heatmap_2022.pdf
  inversion_decomposition_components.pdf
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.dates import DateFormatter


DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_ROOT = DEFAULT_REPO_ROOT / "processing" / "Processing" / "output" / "paper"
DEFAULT_FIGURE_DIR = DEFAULT_REPO_ROOT / "writing" / "figures"
EXPECTED_SEATS = 513
SEAT_MAJORITY = 257
ACCOUNTING_ATOL = 1e-9
ACCOUNTING_RTOL = 1e-12
ELECTION_LABELS = {
    2014: "2014 election",
    2018: "2018 election",
    2022: "2022 election",
}
EXPECTED_PERIOD_COUNTS = {2014: 8, 2018: 13, 2022: 3}
EXPECTED_INVERSION_KEYS = (
    (2014, "2016.2"),
    (2014, "2017.1"),
    (2018, "2021.3"),
    (2018, "2022.1"),
    (2022, "2023.1"),
)
EXPECTED_IDEOLOGICAL_COUNTS = {
    2014: (8, 4),
    2018: (0, 0),
    2022: (6, 2),
}
EXPECTED_IDEOLOGICAL_INTERVAL_COUNTS = {2014: 528, 2018: 630, 2022: 528}
DECOMPOSITION_COMPONENTS = ("A_C", "B_C", "d_C")

# Discrete palette for ideological-interval categories.
INTERVAL_COLORS = [
    "#f0f0f0",  # no seat majority
    "#8ecae6",  # seat majority without inversion
    "#fb8500",  # inversion
    "#c1121f",  # endpoint-minimal inversion
]
INTERVAL_LABELS = [
    "no seat majority",
    "seat majority",
    "inversion",
    "minimal inversion",
]


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required input CSV not found: {path}")
    return pd.read_csv(path)


def require_columns(data: pd.DataFrame, path: Path, columns: set[str]) -> None:
    missing = sorted(columns.difference(data.columns))
    if missing:
        raise ValueError(f"Required columns missing from {path}: {', '.join(missing)}")


def coerce_bool_column(data: pd.DataFrame, path: Path, column: str) -> pd.Series:
    """Return one strict boolean column, rejecting missing or ambiguous values."""
    values = data[column]
    if values.isna().any():
        raise ValueError(f"Boolean column {column!r} contains missing values in {path}")
    if pd.api.types.is_bool_dtype(values.dtype):
        return values.astype(bool)
    if pd.api.types.is_numeric_dtype(values.dtype):
        numeric = pd.to_numeric(values, errors="raise")
        unexpected = sorted(set(numeric).difference({0, 1}))
        if unexpected:
            raise ValueError(
                f"Boolean column {column!r} has non-binary values in {path}: {unexpected}"
            )
        return numeric.astype(bool)

    normalized = values.astype(str).str.strip().str.lower()
    unexpected = sorted(set(normalized).difference({"true", "false"}))
    if unexpected:
        raise ValueError(
            f"Boolean column {column!r} has invalid values in {path}: {unexpected}"
        )
    return normalized.map({"true": True, "false": False}).astype(bool)


def require_finite_numeric(data: pd.DataFrame, path: Path, columns: set[str]) -> None:
    for column in sorted(columns):
        converted = pd.to_numeric(data[column], errors="raise")
        if not np.isfinite(converted.to_numpy(dtype=float)).all():
            raise ValueError(f"Numeric column {column!r} contains non-finite values in {path}")
        data[column] = converted


def formatted_keys(keys: set[tuple[int, str]]) -> str:
    return ", ".join(f"{year}/{period}" for year, period in sorted(keys))


def load_party_vote_share_vs_seat_share(artifact_root: Path) -> pd.DataFrame:
    input_path = artifact_root / "figure_data" / "party_vote_share_vs_seat_share.csv"
    party = read_csv(input_path)
    require_columns(
        party,
        input_path,
        {"election_year", "party", "vote_share", "seat_share"},
    )
    require_finite_numeric(party, input_path, {"election_year", "vote_share", "seat_share"})
    party["election_year"] = party["election_year"].astype(int)
    if set(party["election_year"]) != set(ELECTION_LABELS):
        raise ValueError(
            f"Party figure years changed in {input_path}: {sorted(set(party['election_year']))}"
        )
    if party.duplicated(["election_year", "party"]).any():
        raise ValueError(f"Duplicate election/party rows in {input_path}")
    for year, rows in party.groupby("election_year"):
        if not np.isclose(rows["vote_share"].sum(), 1.0, atol=1e-6, rtol=0.0):
            raise ValueError(f"Party vote shares for {year} do not sum to one in {input_path}")
        if not np.isclose(rows["seat_share"].sum(), 1.0, atol=1e-6, rtol=0.0):
            raise ValueError(f"Party seat shares for {year} do not sum to one in {input_path}")
    return party


def load_observed_coalition_timeline(artifact_root: Path) -> pd.DataFrame:
    input_path = artifact_root / "figure_data" / "observed_coalition_timeline.csv"
    observed = read_csv(input_path)
    require_columns(
        observed,
        input_path,
        {
            "election_year",
            "period",
            "period_start",
            "period_end",
            "vote_share",
            "seat_share",
            "seats",
            "representation_ratio",
            "coalition_inversion",
        },
    )
    require_finite_numeric(
        observed,
        input_path,
        {"election_year", "vote_share", "seat_share", "seats", "representation_ratio"},
    )
    observed["election_year"] = observed["election_year"].astype(int)
    observed["period"] = observed["period"].astype(str)
    observed["coalition_inversion"] = coerce_bool_column(
        observed, input_path, "coalition_inversion"
    )

    if len(observed) != 24:
        raise ValueError(f"Expected 24 observed cabinet periods in {input_path}; found {len(observed)}")
    if observed.duplicated(["election_year", "period"]).any():
        raise ValueError(f"Duplicate election/period rows in {input_path}")
    actual_period_counts = observed.groupby("election_year").size().to_dict()
    if actual_period_counts != EXPECTED_PERIOD_COUNTS:
        raise ValueError(
            f"Observed cabinet period counts changed in {input_path}: "
            f"expected {EXPECTED_PERIOD_COUNTS}, found {actual_period_counts}"
        )

    inversion_rows = observed.loc[observed["coalition_inversion"]]
    actual_inversion_keys = set(
        zip(inversion_rows["election_year"], inversion_rows["period"], strict=True)
    )
    expected_inversion_keys = set(EXPECTED_INVERSION_KEYS)
    if actual_inversion_keys != expected_inversion_keys:
        raise ValueError(
            f"Observed inversion keys changed in {input_path}: expected "
            f"{formatted_keys(expected_inversion_keys)}; found "
            f"{formatted_keys(actual_inversion_keys)}"
        )
    if not (inversion_rows["vote_share"] < 0.5).all():
        raise ValueError(f"An observed inversion has at least 50% of votes in {input_path}")
    if not (inversion_rows["seats"] >= SEAT_MAJORITY).all():
        raise ValueError(f"An observed inversion has fewer than 257 seats in {input_path}")

    observed["period_start"] = pd.to_datetime(observed["period_start"], errors="raise")
    observed["period_end"] = pd.to_datetime(observed["period_end"], errors="raise")
    if (observed["period_end"] < observed["period_start"]).any():
        raise ValueError(f"Observed cabinet period ends before it starts in {input_path}")
    observed["midpoint"] = observed["period_start"] + (
        observed["period_end"] - observed["period_start"]
    ) / 2
    return observed


def load_ideological_interval_heatmap(artifact_root: Path) -> pd.DataFrame:
    input_path = artifact_root / "figure_data" / "ideological_interval_heatmap.csv"
    intervals = read_csv(input_path)
    require_columns(
        intervals,
        input_path,
        {
            "election_year",
            "start_index",
            "end_index",
            "seats",
            "coalition_inversion",
            "minimal_ideological_interval_inversion",
        },
    )
    require_finite_numeric(
        intervals,
        input_path,
        {"election_year", "start_index", "end_index", "seats"},
    )
    intervals["election_year"] = intervals["election_year"].astype(int)
    for column in ("coalition_inversion", "minimal_ideological_interval_inversion"):
        intervals[column] = coerce_bool_column(intervals, input_path, column)
    if intervals.duplicated(["election_year", "start_index", "end_index"]).any():
        raise ValueError(f"Duplicate ideological interval rows in {input_path}")
    if (
        intervals["minimal_ideological_interval_inversion"]
        & ~intervals["coalition_inversion"]
    ).any():
        raise ValueError(f"A minimal ideological inversion is not an inversion in {input_path}")

    actual_years = set(intervals["election_year"])
    if actual_years != set(EXPECTED_IDEOLOGICAL_COUNTS):
        raise ValueError(f"Ideological figure years changed in {input_path}: {sorted(actual_years)}")
    for year, (expected_inversions, expected_minimal) in EXPECTED_IDEOLOGICAL_COUNTS.items():
        rows = intervals.loc[intervals["election_year"] == year]
        expected_rows = EXPECTED_IDEOLOGICAL_INTERVAL_COUNTS[year]
        actual = (
            len(rows),
            int(rows["coalition_inversion"].sum()),
            int(rows["minimal_ideological_interval_inversion"].sum()),
        )
        expected = (expected_rows, expected_inversions, expected_minimal)
        if actual != expected:
            raise ValueError(
                f"Ideological counts changed for {year} in {input_path}: "
                f"expected intervals/inversions/minimal={expected}, found {actual}"
            )
    return intervals


def load_inversion_decomposition_components(artifact_root: Path) -> pd.DataFrame:
    input_path = artifact_root / "figure_data" / "inversion_decomposition_components.csv"
    components = read_csv(input_path)
    require_columns(
        components,
        input_path,
        {"coalition_id", "election_year", "cabinet_period", "component", "seats"},
    )
    require_finite_numeric(components, input_path, {"election_year", "seats"})
    components["election_year"] = components["election_year"].astype(int)
    components["cabinet_period"] = components["cabinet_period"].astype(str)
    components["coalition_id"] = components["coalition_id"].astype(str).str.strip()
    components["component"] = components["component"].astype(str).str.strip()
    if (components["coalition_id"] == "").any():
        raise ValueError(f"Blank coalition_id in {input_path}")
    if components.duplicated(
        ["coalition_id", "election_year", "cabinet_period", "component"]
    ).any():
        raise ValueError(f"Duplicate coalition/component rows in {input_path}")

    actual_keys = set(
        zip(components["election_year"], components["cabinet_period"], strict=True)
    )
    expected_keys = set(EXPECTED_INVERSION_KEYS)
    if actual_keys != expected_keys:
        raise ValueError(
            f"Decomposition case keys changed in {input_path}: expected "
            f"{formatted_keys(expected_keys)}; found {formatted_keys(actual_keys)}"
        )
    component_sets = components.groupby(["election_year", "cabinet_period"])["component"].agg(set)
    expected_components = set(DECOMPOSITION_COMPONENTS)
    invalid_component_sets = component_sets[component_sets != expected_components]
    if not invalid_component_sets.empty:
        raise ValueError(
            f"Every decomposition case must contain exactly {DECOMPOSITION_COMPONENTS} in {input_path}"
        )

    pivoted = components.pivot(
        index=["coalition_id", "election_year", "cabinet_period"],
        columns="component",
        values="seats",
    ).reset_index()
    pivoted.columns.name = None
    if len(pivoted) != len(EXPECTED_INVERSION_KEYS):
        raise ValueError(
            f"Expected five unique decomposition coalitions in {input_path}; found {len(pivoted)}"
        )
    residual = pivoted["A_C"] + pivoted["B_C"] - pivoted["d_C"]
    if not np.allclose(residual, 0.0, atol=ACCOUNTING_ATOL, rtol=ACCOUNTING_RTOL):
        failures = pivoted.loc[
            ~np.isclose(residual, 0.0, atol=ACCOUNTING_ATOL, rtol=ACCOUNTING_RTOL),
            ["coalition_id", "A_C", "B_C", "d_C"],
        ]
        raise ValueError(
            "Decomposition identity A_C + B_C = d_C failed in "
            f"{input_path}: {failures.to_dict(orient='records')}"
        )
    order = {key: index for index, key in enumerate(EXPECTED_INVERSION_KEYS)}
    pivoted["_order"] = [
        order[(year, period)]
        for year, period in zip(
            pivoted["election_year"], pivoted["cabinet_period"], strict=True
        )
    ]
    return pivoted.sort_values("_order").drop(columns="_order").reset_index(drop=True)


def save_party_vote_share_vs_seat_share(artifact_root: Path, figure_dir: Path) -> Path:
    party = load_party_vote_share_vs_seat_share(artifact_root)

    fig, ax = plt.subplots(figsize=(6.6, 4.8))
    for year, df in party.groupby("election_year"):
        ax.scatter(
            df["vote_share"] * 100,
            df["seat_share"] * 100,
            s=38,
            alpha=0.75,
            label=str(year),
        )

    upper = max(party["vote_share"].max(), party["seat_share"].max()) * 100 + 2
    lims = [0, upper]
    ax.plot(lims, lims, linestyle="--", linewidth=1, label="proportionality")
    ax.set_xlim(lims)
    ax.set_ylim(lims)
    ax.set_xlabel("Vote share (%)")
    ax.set_ylabel("Seat share (%)")
    ax.set_title("Party vote shares and Chamber seat shares")
    ax.legend(frameon=False, fontsize=8)
    ax.grid(True, linewidth=0.35, alpha=0.35)

    output = figure_dir / "party_vote_share_vs_seat_share.pdf"
    fig.tight_layout()
    fig.savefig(output)
    plt.close(fig)
    return output


def save_observed_coalition_timeline(artifact_root: Path, figure_dir: Path) -> Path:
    observed = load_observed_coalition_timeline(artifact_root)

    fig, (ax_vote, ax_seat, ax_ratio) = plt.subplots(1, 3, figsize=(10.8, 4.4), sharex=True)

    for year, df in observed.groupby("election_year"):
        df = df.sort_values("midpoint")
        ax_vote.plot(df["midpoint"], df["vote_share"] * 100, marker="o", label=ELECTION_LABELS[year])
        ax_seat.plot(df["midpoint"], df["seat_share"] * 100, marker="s", label=ELECTION_LABELS[year])
        ax_ratio.plot(df["midpoint"], df["representation_ratio"], marker="^", label=ELECTION_LABELS[year])

    ax_vote.axhline(50, linestyle="--", linewidth=1)
    ax_seat.axhline(SEAT_MAJORITY / EXPECTED_SEATS * 100, linestyle="--", linewidth=1)
    ax_ratio.axhline(1, linestyle="--", linewidth=1)

    ax_vote.set_title("Vote share")
    ax_seat.set_title("Seat share")
    ax_ratio.set_title("Representation ratio")
    ax_vote.set_ylabel("Coalition share (%)")
    ax_ratio.set_ylabel(r"$R_C$")
    ax_vote.set_xlabel("Cabinet period midpoint")
    ax_seat.set_xlabel("Cabinet period midpoint")
    ax_ratio.set_xlabel("Cabinet period midpoint")

    for ax in (ax_vote, ax_seat, ax_ratio):
        ax.xaxis.set_major_formatter(DateFormatter("%Y"))
        ax.grid(True, linewidth=0.35, alpha=0.35)

    handles, labels = ax_vote.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False)
    fig.suptitle(
        "Observed cabinet-period coalition vote shares, seat shares, and representation ratios",
        y=1.04,
    )

    output = figure_dir / "observed_coalition_timeline.pdf"
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.subplots_adjust(top=0.82, wspace=0.28)
    fig.savefig(output)
    plt.close(fig)
    return output


def interval_status_code(row: pd.Series) -> int:
    """Return matrix code for an ideological interval.

    0: no seat majority
    1: seat majority without inversion
    2: coalition inversion
    3: endpoint-minimal ideological interval inversion
    """
    if bool(row["minimal_ideological_interval_inversion"]):
        return 3
    if bool(row["coalition_inversion"]):
        return 2
    if int(row["seats"]) >= SEAT_MAJORITY:
        return 1
    return 0


def save_ideological_interval_heatmaps(artifact_root: Path, figure_dir: Path) -> list[Path]:
    intervals = load_ideological_interval_heatmap(artifact_root)
    outputs: list[Path] = []
    cmap = ListedColormap(INTERVAL_COLORS)
    norm = BoundaryNorm(np.arange(-0.5, 4.5, 1), cmap.N)

    for year, df in intervals.groupby("election_year"):
        n = int(max(df["start_index"].max(), df["end_index"].max()))
        matrix = np.full((n, n), np.nan)

        for _, row in df.iterrows():
            i = int(row["start_index"]) - 1
            j = int(row["end_index"]) - 1
            matrix[j, i] = interval_status_code(row)

        fig, ax = plt.subplots(figsize=(5.6, 5.2))
        ax.imshow(matrix, origin="lower", interpolation="nearest", aspect="auto", cmap=cmap, norm=norm)
        ax.set_title(f"Ideological interval status, {year}")
        ax.set_xlabel("Start index in ideology order")
        ax.set_ylabel("End index in ideology order")


        output = figure_dir / f"ideological_interval_heatmap_{year}.pdf"
        fig.tight_layout()
        fig.savefig(output, bbox_inches="tight")
        plt.close(fig)
        outputs.append(output)

    return outputs


def save_inversion_decomposition_components(artifact_root: Path, figure_dir: Path) -> Path:
    components = load_inversion_decomposition_components(artifact_root)
    labels = [
        f"{year} / {period}"
        for year, period in zip(
            components["election_year"], components["cabinet_period"], strict=True
        )
    ]
    positions = np.arange(len(components))
    offset = 0.18

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    ax.barh(
        positions - offset,
        components["A_C"],
        height=0.32,
        color="#315f7d",
        label=r"Within-district allocation ($A_C$)",
    )
    ax.barh(
        positions + offset,
        components["B_C"],
        height=0.32,
        color="#d17a22",
        label=r"Between-district weighting ($B_C$)",
    )
    ax.scatter(
        components["d_C"],
        positions,
        marker="D",
        s=28,
        color="black",
        label=r"Coalition differential ($d_C$)",
        zorder=3,
    )
    ax.axvline(0, color="0.45", linewidth=0.8)
    ax.set_yticks(positions, labels)
    ax.invert_yaxis()
    ax.set_xlabel("Seat contribution")
    ax.set_title("Accounting decomposition of observed coalition inversions")
    ax.grid(True, axis="x", linewidth=0.35, alpha=0.35)
    ax.legend(frameon=False, fontsize=8, loc="best")

    output = figure_dir / "inversion_decomposition_components.pdf"
    fig.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    plt.close(fig)
    return output


def generate_figures(artifact_root: Path, figure_dir: Path) -> list[Path]:
    figure_dir.mkdir(parents=True, exist_ok=True)
    outputs = [
        save_party_vote_share_vs_seat_share(artifact_root, figure_dir),
        save_observed_coalition_timeline(artifact_root, figure_dir),
        save_inversion_decomposition_components(artifact_root, figure_dir),
    ]
    outputs.extend(save_ideological_interval_heatmaps(artifact_root, figure_dir))
    return outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate figures for the coalition inversions manuscript.")
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=DEFAULT_ARTIFACT_ROOT,
        help=f"Path to processing/Processing/output/paper. Default: {DEFAULT_ARTIFACT_ROOT}",
    )
    parser.add_argument(
        "--figure-dir",
        type=Path,
        default=DEFAULT_FIGURE_DIR,
        help=f"Directory where figure PDFs should be written. Default: {DEFAULT_FIGURE_DIR}",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    outputs = generate_figures(args.artifact_root.expanduser(), args.figure_dir.expanduser())
    print("Generated figures:")
    for path in outputs:
        print(f"- {path}")


if __name__ == "__main__":
    main()
