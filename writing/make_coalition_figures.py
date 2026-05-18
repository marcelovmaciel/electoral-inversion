#!/usr/bin/env python3
"""
Generate the figures used in coalition_inversions_first_draft.tex.

Default paths target Marcelo's local electoral-inversion project. Override them
with --artifact-root and --figure-dir when running elsewhere.

Expected input tree:
  <artifact-root>/figure_data/party_vote_share_vs_seat_share.csv
  <artifact-root>/figure_data/observed_coalition_timeline.csv
  <artifact-root>/figure_data/ideological_interval_heatmap.csv

Outputs:
  party_vote_share_vs_seat_share.pdf
  observed_coalition_timeline.pdf
  ideological_interval_heatmap_2014.pdf
  ideological_interval_heatmap_2018.pdf
  ideological_interval_heatmap_2022.pdf
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.dates import DateFormatter


DEFAULT_REPO_ROOT = Path("/home/marcelo/Sync/Projects/electoral-inversion")
DEFAULT_ARTIFACT_ROOT = DEFAULT_REPO_ROOT / "processing" / "Processing" / "output" / "paper"
DEFAULT_FIGURE_DIR = DEFAULT_REPO_ROOT / "paper" / "figures"
EXPECTED_SEATS = 513
SEAT_MAJORITY = 257

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


def save_party_vote_share_vs_seat_share(artifact_root: Path, figure_dir: Path) -> Path:
    party = read_csv(artifact_root / "figure_data" / "party_vote_share_vs_seat_share.csv")

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
    observed = read_csv(artifact_root / "figure_data" / "observed_coalition_timeline.csv")
    observed["period_start"] = pd.to_datetime(observed["period_start"])
    observed["period_end"] = pd.to_datetime(observed["period_end"])
    observed["midpoint"] = observed["period_start"] + (observed["period_end"] - observed["period_start"]) / 2

    fig, (ax_vote, ax_seat) = plt.subplots(1, 2, figsize=(9.4, 4.4), sharex=True)

    for year, df in observed.groupby("election_year"):
        df = df.sort_values("midpoint")
        ax_vote.plot(df["midpoint"], df["vote_share"] * 100, marker="o", label=str(year))
        ax_seat.plot(df["midpoint"], df["seat_share"] * 100, marker="s", label=str(year))

    ax_vote.axhline(50, linestyle="--", linewidth=1)
    ax_seat.axhline(SEAT_MAJORITY / EXPECTED_SEATS * 100, linestyle="--", linewidth=1)

    ax_vote.set_title("Vote share")
    ax_seat.set_title("Seat share")
    ax_vote.set_ylabel("Coalition share (%)")
    ax_vote.set_xlabel("Cabinet period midpoint")
    ax_seat.set_xlabel("Cabinet period midpoint")

    for ax in (ax_vote, ax_seat):
        ax.xaxis.set_major_formatter(DateFormatter("%Y"))
        ax.grid(True, linewidth=0.35, alpha=0.35)

    handles, labels = ax_vote.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False)
    fig.suptitle("Observed cabinet-period coalition vote shares and seat shares", y=1.04)

    output = figure_dir / "observed_coalition_timeline.pdf"
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.subplots_adjust(top=0.82, wspace=0.18)
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
    intervals = read_csv(artifact_root / "figure_data" / "ideological_interval_heatmap.csv")
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


def generate_figures(artifact_root: Path, figure_dir: Path) -> list[Path]:
    figure_dir.mkdir(parents=True, exist_ok=True)
    outputs = [
        save_party_vote_share_vs_seat_share(artifact_root, figure_dir),
        save_observed_coalition_timeline(artifact_root, figure_dir),
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
