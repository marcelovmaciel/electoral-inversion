#!/usr/bin/env python3
"""Build the deterministic post-PSC baseline SHA-256 inventory."""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path


REPAIR_DIR = Path(__file__).resolve().parent
REPO_ROOT = REPAIR_DIR.parents[2]
OUTPUT_PATH = REPAIR_DIR / "post_psc_baseline_manifest.csv"


EXPLICIT_FILES = {
    "cabinet_source": [
        "scraping/data/cabinet_source_snapshot.json",
        "scraping/data/cabinet_party_affiliation_spells.csv",
        "scraping/reconstruct_cabinet_timeline.py",
        "scraping/output/cabinet_timeline_dashboard.json",
        "scraping/output/ministerios_nomeacoes_intervalos.csv",
        "scraping/output/partidos_por_periodo.csv",
        "scraping/output/partidos_por_periodo.json",
    ],
    "election_source": [
        "data/raw/electionsBR/2014/candidate.csv",
        "data/raw/electionsBR/2014/party_mun_zone.csv",
        "data/raw/electionsBR/2018/candidate.csv",
        "data/raw/electionsBR/2018/party_mun_zone.csv",
        "data/raw/electionsBR/2022/candidate.csv",
        "data/raw/electionsBR/2022/party_mun_zone.csv",
    ],
    "pipeline": [
        "processing/Processing/Project.toml",
        "processing/Processing/Manifest.toml",
        "processing/Processing/src/Processing.jl",
        "processing/Processing/src/PartyNames.jl",
        "processing/Processing/src/analysis_runner_core.jl",
        "processing/Processing/src/code.jl",
        "processing/Processing/running/running.jl",
    ],
    "regression_test": [
        "scraping/tests/test_psc_cabinet_chronology.py",
        "processing/Processing/test/runtests.jl",
        "processing/Processing/test/test_psc_baseline_repair.jl",
    ],
    "stage1_provenance": [
        "processing/Processing/decomposition/REPO_MAP.md",
        "processing/Processing/decomposition/MATHEMATICAL_SPEC.md",
        "processing/Processing/decomposition/BASELINE_BLOCKER.md",
        "processing/Processing/decomposition/BASELINE_WAIVER.md",
        "processing/Processing/output/decomposition/baseline/baseline_manifest.csv",
        "processing/Processing/output/decomposition/baseline/baseline_test_failure.txt",
        "processing/Processing/output/decomposition/baseline/preexisting_git_status.txt",
        "processing/Processing/output/decomposition/baseline/protected_hashes_before.csv",
        "processing/Processing/output/decomposition/baseline/PRE_PSC_REPAIR_PROVENANCE.md",
    ],
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def add_file(rows: list[dict[str, str | int]], category: str, relative: str) -> None:
    path = REPO_ROOT / relative
    if not path.is_file():
        raise FileNotFoundError(f"Required baseline file is missing: {relative}")
    rows.append(
        {
            "category": category,
            "path": relative,
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        }
    )


def main() -> None:
    rows: list[dict[str, str | int]] = []
    for category, paths in EXPLICIT_FILES.items():
        for relative in paths:
            add_file(rows, category, relative)

    for path in sorted((REPO_ROOT / "processing/Processing/output/paper").rglob("*")):
        if path.is_file():
            add_file(rows, "paper_artifact", path.relative_to(REPO_ROOT).as_posix())

    excluded = {OUTPUT_PATH}
    for path in sorted(REPAIR_DIR.rglob("*")):
        if path.is_file() and path not in excluded and "pre_repair" not in path.parts:
            add_file(rows, "repair_artifact", path.relative_to(REPO_ROOT).as_posix())

    unique = {(row["category"], row["path"]): row for row in rows}
    ordered = [unique[key] for key in sorted(unique)]
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["category", "path", "bytes", "sha256"])
        writer.writeheader()
        writer.writerows(ordered)
    print(f"Wrote {len(ordered)} hashes to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
