#!/usr/bin/env python3
"""
Generate the figures used in the coalition-inversions manuscripts.

Figure 2 and the accounting figures consume full-precision inputs computed by
Julia. Python performs presentation-only date, percentage, and layout
formatting after validating the current empirical registries. The cross-domain
figure uses the shared production decomposition extractor to aggregate audited
party/district accounting against the cabinet and minimal-winning registries.
Its accounting inputs live under the sibling output/decomposition directory.

Expected input tree:
  <artifact-root>/figure_data/party_vote_share_vs_seat_share.csv
  <artifact-root>/raw/cabinet_party_sets.csv
  <artifact-root>/figure_data/ideological_interval_heatmap.csv
  <artifact-root>/figure_data/inversion_decomposition_components.csv
  <artifact-root>/figure_data/accounting_state_weighting_anatomy.csv
  <artifact-root>/figure_data/accounting_district_electoral_weight.csv
  <artifact-root>/raw/ideology_order_{2014,2018,2022}.csv

Outputs:
  party_vote_share_vs_seat_share.pdf
  observed_coalition_timeline.pdf
  ideological_interval_heatmap_2014.pdf
  ideological_interval_heatmap_2018.pdf
  ideological_interval_heatmap_2022.pdf
  ideological_interval_heatmap_legend.pdf
  minimal_connected_winning_inversions_3x1_diamond.pdf
  minimal_connected_winning_inversions_3x1_diamond.png
  inversion_decomposition_components.pdf
  accounting_state_weighting_anatomy.pdf
  district_electoral_weight_by_magnitude.pdf
  cross_domain_components.pdf

Generate only the minimal-connected interval figure (PDF and 300-dpi PNG):
  python3 writing/make_coalition_figures.py --minimal-connected-only
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.dates import DateFormatter

import make_district_electoral_weight_diagnostic as district_weight_diagnostic
from make_cross_domain_components import save_cross_domain_components


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
DECOMPOSITION_COMPONENTS = ("A_C", "B_C", "d_C")

STATE_WEIGHTING_MAGNITUDE_COLUMNS = (
    "b_positive_eight_seat",
    "b_positive_other",
    "b_negative_sp",
    "b_negative_other",
)

# Discrete palette for ideological-interval categories.
INTERVAL_COLORS = [
    "#f0f0f0",  # no seat majority
    "#8ecae6",  # seat majority without inversion
    "#fb8500",  # inversion
    "#c1121f",  # endpoint-minimal inversion
]
INTERVAL_LABELS = [
    "no seat majority",
    "vote + seat majority",
    "inversion",
    "minimal inversion",
]


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required input CSV not found: {path}")
    # Period labels are identifiers: 2015.10 must not collapse to 2015.1.
    return pd.read_csv(path, dtype={"period": str, "cabinet_period": str})


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
    input_path = artifact_root / "raw" / "cabinet_party_sets.csv"
    observed = read_csv(input_path)
    if observed.duplicated(["cabinet_party_set_id"]).any():
        raise ValueError("Duplicate cabinet party set in shared registry")
    require_columns(
        observed,
        input_path,
        {
            "election_year",
            "first_observed",
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
        {"election_year", "vote_share", "seat_share", "seats"},
    )
    observed["representation_ratio"] = pd.to_numeric(observed["representation_ratio"], errors="raise")
    positive_quota = observed.vote_share.gt(0)
    if not np.isfinite(observed.loc[positive_quota, "representation_ratio"]).all():
        raise ValueError("Positive-quota cabinet ratios must be finite")
    if observed.loc[~positive_quota, "representation_ratio"].notna().any():
        raise ValueError("Zero-quota cabinet ratios must be unavailable")
    observed["election_year"] = observed["election_year"].astype(int)
    observed["period"] = observed["period"].astype(str)
    observed["coalition_inversion"] = coerce_bool_column(
        observed, input_path, "coalition_inversion"
    )

    if observed.duplicated(["election_year", "period"]).any():
        raise ValueError(f"Duplicate election/period rows in {input_path}")
    inversion_rows = observed.loc[observed["coalition_inversion"]]
    derived = (observed["vote_share"] < .5) & (observed["seats"] >= SEAT_MAJORITY)
    if not (derived == observed["coalition_inversion"]).all():
        raise ValueError("Cabinet inversion flags disagree with votes and seats")
    if not (inversion_rows["vote_share"] < 0.5).all():
        raise ValueError(f"An observed inversion has at least 50% of votes in {input_path}")
    if not (inversion_rows["seats"] >= SEAT_MAJORITY).all():
        raise ValueError(f"An observed inversion has fewer than 257 seats in {input_path}")

    observed["first_observed"] = pd.to_datetime(observed["first_observed"], errors="raise")
    if observed["first_observed"].isna().any():
        raise ValueError(f"Missing first-observed date in {input_path}")
    observed["period_start"] = pd.to_datetime(observed["period_start"], errors="raise")
    observed["period_end"] = pd.to_datetime(observed["period_end"], errors="raise")
    if (observed["period_end"] < observed["period_start"]).any():
        raise ValueError(f"Observed cabinet period ends before it starts in {input_path}")
    observed["midpoint"] = observed["period_start"] + (
        observed["period_end"] - observed["period_start"]
    ) / 2
    return observed


def cabinet_calendar_has_bounded_dates(artifact_root: Path) -> bool:
    path = artifact_root / "raw" / "cabinet_calendar_status.csv"
    if not path.exists():
        return False
    data = read_csv(path)
    return any(data[column].fillna("").astype(str).str.strip().isin(["", "[]", "false", "0"]).eq(False).any()
               for column in ("bounded_affiliation_ids", "bounded_service_ids") if column in data)


def load_cabinet_unidentified_intervals(artifact_root: Path) -> pd.DataFrame:
    """Return explicit composition gaps; the ordinary metrics contain full sets only."""
    path = artifact_root / "raw" / "cabinet_unidentified_intervals.csv"
    if not path.exists():
        # Legacy synthetic plotting fixtures predate the release adapter.
        return pd.DataFrame(columns=["start_inclusive", "end_exclusive", "days"])
    data = read_csv(path)
    require_columns(data, path, {"start_inclusive", "end_exclusive", "days"})
    for column in ("start_inclusive", "end_exclusive"):
        data[column] = pd.to_datetime(data[column], errors="raise")
    require_finite_numeric(data, path, {"days"})
    expected = (data.end_exclusive - data.start_inclusive).dt.days
    if not (expected.gt(0) & expected.eq(data.days)).all():
        raise ValueError(f"Invalid unidentified interval durations in {path}")
    return data


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

    require_columns(intervals, input_path, {"ideological_universe", "ideological_party_count", "vote_share"})
    if set(intervals["ideological_universe"]) != {"seat_winning"}:
        raise ValueError("Main interval figure requires the seat_winning universe")
    if set(intervals["election_year"]) != set(ELECTION_LABELS):
        raise ValueError("Ideological figure must contain all three election years")
    for year, rows in intervals.groupby("election_year"):
        order = read_csv(artifact_root / "raw" / f"ideology_order_{year}.csv")
        n = len(order)
        expected = {(i, j) for i in range(1, n + 1) for j in range(i, n + 1)}
        actual = set(zip(rows["start_index"], rows["end_index"]))
        if actual != expected or set(rows["ideological_party_count"]) != {n}:
            raise ValueError(f"Interval figure does not cover the parliamentary order for {year}")
        inverse = (rows["vote_share"] < 0.5) & (rows["seats"] >= 257)
        if not (inverse == rows["coalition_inversion"]).all():
            raise ValueError(f"Incorrect inversion flags in {input_path}")
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
    observed = load_observed_coalition_timeline(artifact_root)
    inversions = observed.loc[observed.coalition_inversion]
    expected_keys = set(zip(inversions.election_year, inversions.period))
    if actual_keys != expected_keys:
        raise ValueError(
            f"Decomposition case keys changed in {input_path}: expected "
            f"{formatted_keys(expected_keys)}; found {formatted_keys(actual_keys)}"
        )
    if components.empty:
        return pd.DataFrame(columns=["coalition_id", "election_year", "cabinet_period", *DECOMPOSITION_COMPONENTS])
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
    order = {key: index for index, key in enumerate(zip(inversions.election_year, inversions.period))}
    pivoted["_order"] = [
        order[(year, period)]
        for year, period in zip(
            pivoted["election_year"], pivoted["cabinet_period"], strict=True
        )
    ]
    return pivoted.sort_values("_order").drop(columns="_order").reset_index(drop=True)


def load_accounting_state_weighting_anatomy(artifact_root: Path) -> pd.DataFrame:
    """Load and validate all current focal state-weighting vectors.

    Negative component columns are stored by Julia as positive magnitudes. The
    loader verifies that gross positive minus gross negative contributions
    reproduces the reported net between-district component before plotting.
    """
    input_path = artifact_root / "figure_data" / "accounting_state_weighting_anatomy.csv"
    anatomy = read_csv(input_path)
    required = {
        "case_id",
        "case_display",
        "case_order",
        "focal_order",
        *STATE_WEIGHTING_MAGNITUDE_COLUMNS,
        "B_C",
        "largest_positive_state",
        "largest_positive_b_Cd",
    }
    require_columns(anatomy, input_path, required)
    require_finite_numeric(
        anatomy,
        input_path,
        {
            "case_order",
            "focal_order",
            *STATE_WEIGHTING_MAGNITUDE_COLUMNS,
            "B_C",
            "largest_positive_b_Cd",
        },
    )

    for column in ("case_id", "case_display", "largest_positive_state"):
        if anatomy[column].isna().any():
            raise ValueError(f"Text column {column!r} contains missing values in {input_path}")
        anatomy[column] = anatomy[column].astype(str).str.strip()
        if (anatomy[column] == "").any():
            raise ValueError(f"Text column {column!r} contains blank values in {input_path}")

    focal_order = anatomy["focal_order"].to_numpy(dtype=float)
    if not np.equal(focal_order, np.floor(focal_order)).all():
        raise ValueError(f"focal_order must contain integers in {input_path}")
    anatomy["focal_order"] = anatomy["focal_order"].astype(int)
    if anatomy["case_id"].duplicated().any():
        raise ValueError(f"Duplicate case_id rows in {input_path}")
    if anatomy["focal_order"].duplicated().any():
        raise ValueError(f"Duplicate focal_order rows in {input_path}")

    anatomy = anatomy.sort_values("focal_order").reset_index(drop=True)
    if tuple(anatomy["focal_order"]) != tuple(range(1, len(anatomy) + 1)):
        raise ValueError(f"Focal registry order is not contiguous in {input_path}")
    registry = read_csv(artifact_root / "tables" / "table_accounting_focal_cases.csv")
    if "case_id" in registry and set(anatomy["case_id"]) != set(registry["case_id"]):
        raise ValueError(f"State anatomy differs from the generated focal registry in {input_path}")

    magnitudes = anatomy.loc[:, STATE_WEIGHTING_MAGNITUDE_COLUMNS]
    if (magnitudes < -ACCOUNTING_ATOL).any().any():
        raise ValueError(
            f"State-weighting gross components must be nonnegative magnitudes in {input_path}"
        )
    if (anatomy["largest_positive_b_Cd"] <= 0).any():
        raise ValueError(f"Largest positive state contributions must be positive in {input_path}")

    gross_positive = anatomy["b_positive_eight_seat"] + anatomy["b_positive_other"]
    gross_negative = anatomy["b_negative_sp"] + anatomy["b_negative_other"]
    residual = gross_positive - gross_negative - anatomy["B_C"]
    if not np.allclose(residual, 0.0, atol=ACCOUNTING_ATOL, rtol=ACCOUNTING_RTOL):
        failures = anatomy.loc[
            ~np.isclose(residual, 0.0, atol=ACCOUNTING_ATOL, rtol=ACCOUNTING_RTOL),
            ["case_id", *STATE_WEIGHTING_MAGNITUDE_COLUMNS, "B_C"],
        ]
        raise ValueError(
            "State-weighting gross components do not reproduce B_C in "
            f"{input_path}: {failures.to_dict(orient='records')}"
        )
    if (
        anatomy["largest_positive_b_Cd"].to_numpy(dtype=float)
        > gross_positive.to_numpy(dtype=float) + ACCOUNTING_ATOL
    ).any():
        raise ValueError(
            f"A largest positive state contribution exceeds gross positive B in {input_path}"
        )

    return anatomy


def load_district_electoral_weight(artifact_root: Path) -> pd.DataFrame:
    """Load the validated neutral state-year district-weight diagnostic data."""
    input_path = (
        artifact_root
        / "figure_data"
        / "accounting_district_electoral_weight.csv"
    )
    return district_weight_diagnostic.load_district_electoral_weight(input_path)


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


ELECTION_COLORS = {2014: "#1f77b4", 2018: "#ff7f0e", 2022: "#2ca02c"}

def plot_observed_coalition_starts(axes, observed: pd.DataFrame) -> None:
    """One point per distinct set at its first observed calendar date."""
    series = (("vote_share", 100, "o"), ("seat_share", 100, "s"),
              ("representation_ratio", 1, "^"))
    for year, rows in observed.groupby("election_year", sort=True):
        for ax, (column, scale, marker) in zip(axes, series, strict=True):
            ax.plot(rows["first_observed"], rows[column] * scale, linestyle="none",
                    marker=marker, color=ELECTION_COLORS[year], label=ELECTION_LABELS[year], markersize=4)
    inverted = observed.loc[observed.coalition_inversion]
    for ax, (column, scale, _) in zip(axes, series, strict=True):
        ax.plot(inverted["first_observed"], inverted[column] * scale, linestyle="none", marker="o",
                markersize=10, markerfacecolor="none", markeredgecolor="black",
                markeredgewidth=1.1, label="Inversion", zorder=5)


def save_observed_coalition_timeline(artifact_root: Path, figure_dir: Path) -> Path:
    # Filename retained for manuscript label compatibility; chronology input
    # remains separately exported as figure_data/observed_coalition_timeline.csv.
    observed = load_observed_coalition_timeline(artifact_root).sort_values(
        ["first_observed", "cabinet_party_set_id"]).reset_index(drop=True)
    observed.to_csv(artifact_root / "figure_data/cabinet_party_set_comparison.csv", index=False)
    fig, axes = plt.subplots(1, 3, figsize=(9.2, 3.4), sharex=True)
    plot_observed_coalition_starts(axes, observed)
    annual_ticks = pd.date_range("2015-01-01", "2026-01-01", freq="YS")
    date_padding = pd.Timedelta(days=90)
    date_limits = (min(annual_ticks[0], observed.first_observed.min()) - date_padding,
                   max(annual_ticks[-1], observed.first_observed.max()) + date_padding)
    for ax, threshold, title, ylabel in zip(axes, (50, SEAT_MAJORITY / EXPECTED_SEATS * 100, 1),
            ("Vote share", "Seat share", "Representation ratio"), ("Vote (%)", "Seats (%)", r"$R_C$")):
        ax.axhline(threshold, linestyle="--", color="#666666", linewidth=.8)
        ax.set_title(title, loc="left", fontsize=10)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", linewidth=.35, alpha=.35)
        ax.set_xticks(annual_ticks)
        ax.xaxis.set_major_formatter(DateFormatter("%Y"))
        ax.set_xlim(date_limits)
        plt.setp(ax.get_xticklabels(), rotation=90, ha="center", fontsize=7)
    fig.supxlabel("First observed", y=.03, fontsize=10)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4, frameon=False, bbox_to_anchor=(.53,1.0))
    fig.subplots_adjust(top=.80,bottom=.25,left=.065,right=.99,wspace=.42)
    output = figure_dir / "observed_coalition_timeline.pdf"
    fig.savefig(output, metadata={"CreationDate":None,"ModDate":None})
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


def save_ideological_interval_legend(figure_dir: Path) -> Path:
    """Save one compact legend for the separately included heatmap panels."""
    handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor=color, edgecolor="none", label=label)
        for color, label in zip(INTERVAL_COLORS, INTERVAL_LABELS, strict=True)
    ]
    fig = plt.figure(figsize=(6.4, 0.28))
    fig.legend(
        handles=handles, loc="center", ncol=4, frameon=False, fontsize=9,
        handlelength=1.1, handletextpad=0.45, columnspacing=1.25,
        borderpad=0, borderaxespad=0,
    )
    output = figure_dir / "ideological_interval_heatmap_legend.pdf"
    fig.savefig(output, bbox_inches="tight", pad_inches=0.035)
    plt.close(fig)
    return output


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
        ax.imshow(matrix, origin="lower", interpolation="nearest", aspect="auto", cmap=cmap, norm=norm, extent=(0.5, n + 0.5, 0.5, n + 0.5))
        ax.set_title(f"Ideological interval status, {year}")
        ax.set_xlabel("Start index in ideology order")
        ax.set_ylabel("End index in ideology order")
        ticks = sorted(set([1, n] + list(range(5, n, 5))))
        ax.set_xticks(ticks)
        ax.set_yticks(ticks)


        output = figure_dir / f"ideological_interval_heatmap_{year}.pdf"
        fig.tight_layout()
        fig.savefig(output, bbox_inches="tight")
        plt.close(fig)
        outputs.append(output)

    outputs.append(save_ideological_interval_legend(figure_dir))
    return outputs



def load_seat_winning_order(artifact_root: Path, year: int) -> pd.DataFrame:
    """Return the validated ordinal party order used by Figure 3."""
    order_path = artifact_root / "raw" / f"ideology_order_{year}.csv"
    order = read_csv(order_path)
    require_columns(
        order, order_path,
        {"ideological_universe", "election_year", "ordinal_position", "party"},
    )
    order = order.loc[
        order["ideological_universe"].eq("seat_winning")
        & order["election_year"].eq(year)
    ].sort_values("ordinal_position")
    require_finite_numeric(order, order_path, {"ordinal_position"})
    if (order["party"].isna().any() or order["party"].duplicated().any()
            or not np.array_equal(order["ordinal_position"], np.arange(1, len(order) + 1))
            or order["party"].eq("PT").sum() != 1):
        raise ValueError(f"Invalid seat-winning party order in {order_path}")
    return order


def build_minimal_connected_winning_inversions(artifact_root: Path) -> plt.Figure:
    """Plot the exact-connected minimal inversions in the seat-winning order.

    Both interval membership and annotation values come from the unrounded
    figure-data export; the raw ideology-order CSVs determine party positions.
    Each interval gets its own bracket row, in increasing start-position order.
    """
    input_path = artifact_root / "figure_data" / "ideological_interval_heatmap.csv"
    intervals = load_ideological_interval_heatmap(artifact_root)
    require_columns(intervals, input_path, {"start_party", "end_party", "vote_share"})
    require_finite_numeric(intervals, input_path, {"vote_share"})
    minimal = intervals.loc[
        intervals["ideological_universe"].eq("seat_winning")
        & intervals["coalition_inversion"]
        & intervals["minimal_ideological_interval_inversion"]
    ].sort_values(["election_year", "start_index", "end_index"])

    years = sorted(ELECTION_LABELS)
    orders = {}
    for year in years:
        order = load_seat_winning_order(artifact_root, year)
        positions = order.set_index("party")["ordinal_position"]
        rows = minimal.loc[minimal["election_year"].eq(year)]
        if rows.empty:
            raise ValueError(f"No minimal connected winning inversions for {year}")
        for endpoint in ("start", "end"):
            if not np.array_equal(
                rows[f"{endpoint}_party"].map(positions), rows[f"{endpoint}_index"]
            ):
                raise ValueError(f"Interval endpoints disagree with party order for {year}")
        if not (rows["seats"] == rows["seats"].astype(int)).all():
            raise ValueError(f"Non-integer interval seat counts for {year}")
        orders[year] = order

    counts = [int(minimal["election_year"].eq(year).sum()) for year in years]
    with plt.rc_context({"font.family": "DejaVu Sans", "pdf.fonttype": 42}):
        fig, axes = plt.subplots(
            3, 1, figsize=(6.4, 6.2), layout="constrained",
            gridspec_kw={"height_ratios": [count + 0.75 for count in counts]},
        )
        for ax, year, count in zip(axes, years, counts, strict=True):
            order = orders[year]
            positions = order.set_index("party")["ordinal_position"]
            ordinary = order.loc[order["party"].ne("PT"), "ordinal_position"]
            ax.plot(ordinary, np.zeros(len(ordinary)), linestyle="none",
                    marker="o", markersize=2.7, color="black", zorder=3)
            ax.plot(positions["PT"], 0, linestyle="none", marker="D",
                    markersize=4.2, color="black", zorder=4)

            # Put tick labels immediately below the party baseline.
            for spine in ax.spines.values():
                spine.set_visible(False)
            ax.spines["bottom"].set_visible(True)
            ax.spines["bottom"].set_position(("data", 0))
            ax.spines["bottom"].set_bounds(1, len(order))
            ax.spines["bottom"].set_linewidth(0.6)
            ax.set_xticks(order["ordinal_position"], order["party"],
                          rotation=90, fontsize=8, ha="center", va="top")
            ax.tick_params(axis="x", length=0, pad=5)
            ax.set_yticks([])
            ax.grid(False)
            ax.set_xlim(0.35, len(order) + 0.65)
            ax.set_ylim(-0.12, count + 0.75)
            ax.set_title(str(year), fontsize=10.5, pad=4)

            rows = minimal.loc[minimal["election_year"].eq(year)]
            for lane, row in enumerate(rows.itertuples()):
                left = positions[row.start_party]
                right = positions[row.end_party]
                height = count - lane
                ax.plot([left, left, right, right],
                        [height - 0.18, height, height, height - 0.18],
                        color="black", linewidth=0.65, solid_capstyle="butt")
                ax.annotate(f"{100 * row.vote_share:.2f}% votes, {int(row.seats)} seats",
                            ((left + right) / 2, height), xytext=(0, 3),
                            textcoords="offset points", ha="center", va="bottom",
                            fontsize=8.5)

        axes[-1].set_xlabel("Seat-winning parties ordered from left to right",
                            fontsize=9, labelpad=8)
    return fig


def save_minimal_connected_winning_inversions(
    artifact_root: Path, figure_dir: Path
) -> list[Path]:
    """Save matching vector PDF and 300-dpi PNG versions of the three panels."""
    figure_dir.mkdir(parents=True, exist_ok=True)
    fig = build_minimal_connected_winning_inversions(artifact_root)
    stem = "minimal_connected_winning_inversions_3x1_diamond"
    outputs = [figure_dir / f"{stem}.{extension}" for extension in ("pdf", "png")]
    try:
        with plt.rc_context({"pdf.fonttype": 42}):
            for output in outputs:
                fig.savefig(output, dpi=300, facecolor="white")
    finally:
        plt.close(fig)
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

    fig, ax = plt.subplots(figsize=(7.2, max(3.2, .52 * len(components) + 1.6)))
    if components.empty:
        ax.text(.5, .5, "No identified cabinet inversions", ha="center", va="center", transform=ax.transAxes)
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
    ax.set_title("Accounting decomposition of inverted cabinet party sets")
    ax.grid(True, axis="x", linewidth=0.35, alpha=0.35)
    ax.legend(frameon=False, fontsize=8, loc="upper center",
              bbox_to_anchor=(0.5, -0.20), ncol=2)

    output = figure_dir / "inversion_decomposition_components.pdf"
    fig.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    plt.close(fig)
    return output


def save_accounting_state_weighting_anatomy(artifact_root: Path, figure_dir: Path) -> Path:
    anatomy = load_accounting_state_weighting_anatomy(artifact_root)
    positions = np.arange(len(anatomy))
    positive_eight = anatomy["b_positive_eight_seat"].to_numpy(dtype=float)
    positive_other = anatomy["b_positive_other"].to_numpy(dtype=float)
    negative_sp = anatomy["b_negative_sp"].to_numpy(dtype=float)
    negative_other = anatomy["b_negative_other"].to_numpy(dtype=float)
    positive_total = positive_eight + positive_other
    negative_total = negative_sp + negative_other

    fig, ax = plt.subplots(figsize=(10.2, 5.8))
    bar_options = {"height": 0.62, "edgecolor": "white", "linewidth": 0.45, "zorder": 2}
    ax.barh(
        positions,
        positive_eight,
        color="#0072B2",
        label="Positive: eight-seat districts",
        **bar_options,
    )
    ax.barh(
        positions,
        positive_other,
        left=positive_eight,
        color="#56B4E9",
        label="Positive: other districts",
        **bar_options,
    )
    ax.barh(
        positions,
        -negative_sp,
        color="#D55E00",
        label="Negative: São Paulo",
        **bar_options,
    )
    ax.barh(
        positions,
        -negative_other,
        left=-negative_sp,
        color="#E69F00",
        label="Negative: other districts",
        **bar_options,
    )
    ax.scatter(
        anatomy["B_C"],
        positions,
        marker="D",
        s=34,
        facecolor="black",
        edgecolor="white",
        linewidth=0.45,
        label=r"Net $B_C$",
        zorder=4,
    )

    extent = max(
        float(positive_total.max()),
        float(negative_total.max()),
        float(np.abs(anatomy["B_C"]).max()),
        1.0,
    )
    for position, positive_endpoint, state, value in zip(
        positions,
        positive_total,
        anatomy["largest_positive_state"],
        anatomy["largest_positive_b_Cd"],
        strict=True,
    ):
        ax.annotate(
            f"{state} +{value:.2f}",
            xy=(positive_endpoint, position),
            xytext=(4, 0),
            textcoords="offset points",
            ha="left",
            va="center",
            fontsize=7.5,
            color="#005A8C",
            clip_on=False,
        )

    ax.axvline(0, color="0.25", linewidth=0.8, zorder=3)
    ax.set_xlim(
        -float(negative_total.max()) - 0.05 * extent,
        float(positive_total.max()) + 0.24 * extent,
    )
    ax.set_yticks(positions, anatomy["case_display"])
    ax.invert_yaxis()
    ax.set_xlabel(r"Between-district accounting contribution (seats; $b_{Cd}$ and net $B_C$)")
    ax.grid(True, axis="x", linewidth=0.4, alpha=0.35, zorder=0)
    ax.legend(
        frameon=False,
        fontsize=8,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.01),
        ncol=3,
    )

    output = figure_dir / "accounting_state_weighting_anatomy.pdf"
    fig.tight_layout()
    fig.savefig(output, bbox_inches="tight")
    plt.close(fig)
    return output


def save_district_electoral_weight_by_magnitude(
    artifact_root: Path, figure_dir: Path
) -> Path:
    """Render the neutral district-weight diagnostic into the requested figure tree."""
    district_weights = load_district_electoral_weight(artifact_root)
    output = figure_dir / "district_electoral_weight_by_magnitude.pdf"
    return district_weight_diagnostic.render_district_electoral_weight(
        district_weights, output
    )


def generate_figures(artifact_root: Path, figure_dir: Path) -> list[Path]:
    figure_dir.mkdir(parents=True, exist_ok=True)
    outputs = [
        save_party_vote_share_vs_seat_share(artifact_root, figure_dir),
        save_observed_coalition_timeline(artifact_root, figure_dir),
        save_inversion_decomposition_components(artifact_root, figure_dir),
        save_accounting_state_weighting_anatomy(artifact_root, figure_dir),
    ]
    outputs.extend(save_ideological_interval_heatmaps(artifact_root, figure_dir))
    outputs.extend(save_minimal_connected_winning_inversions(artifact_root, figure_dir))
    outputs.append(
        save_district_electoral_weight_by_magnitude(artifact_root, figure_dir)
    )
    outputs.append(save_cross_domain_components(artifact_root, figure_dir))
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
        help=f"Directory where figures should be written. Default: {DEFAULT_FIGURE_DIR}",
    )
    parser.add_argument(
        "--minimal-connected-only",
        action="store_true",
        help="Generate only the 3x1 minimal-connected interval figure (PDF and PNG).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generator = (save_minimal_connected_winning_inversions
                 if args.minimal_connected_only else generate_figures)
    outputs = generator(args.artifact_root.expanduser(), args.figure_dir.expanduser())
    print("Generated figures:")
    for path in outputs:
        print(f"- {path}")


if __name__ == "__main__":
    main()
