#!/usr/bin/env python3
"""Build deterministic review-manuscript figure, table, and source archives."""

from __future__ import annotations

import hashlib
import csv
import re
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
REVIEW_ROOT = REPO_ROOT / "writing" / "submission_inversions_review"
MANUSCRIPT_ROOT = REVIEW_ROOT / "manuscript"
MAIN_TEX = MANUSCRIPT_ROOT / "main_rw_again.tex"
FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)
PUBLICATION_AUDIT_PATH = (
    REPO_ROOT
    / "processing"
    / "Processing"
    / "output"
    / "decomposition"
    / "audit"
    / "publication_artifact_manifest.csv"
)


def referenced_assets() -> tuple[list[Path], list[Path]]:
    source = MAIN_TEX.read_text(encoding="utf-8")
    table_names = sorted(set(re.findall(r"\\input\{([^{}]+\.tex)\}", source)))
    figure_names = sorted(
        set(re.findall(r"\\includegraphics(?:\[[^]]*\])?\{([^{}]+\.pdf)\}", source))
    )

    # This plot remains a generated repository diagnostic even though the
    # manuscript now uses the representation-profile rendering in its place.
    figure_names.append("party_vote_share_vs_seat_share.pdf")
    figure_names = sorted(set(figure_names))
    if "manuscript_values.tex" not in table_names:
        raise ValueError("The manuscript must consume the generated manuscript-value registry.")
    if "accounting_numeric_macros.tex" in table_names or re.search(r"\\Acct[A-Za-z]+", source):
        raise ValueError("Legacy accounting prose macros cannot enter the submission package.")
    tables = [MANUSCRIPT_ROOT / name for name in table_names]
    figures = [MANUSCRIPT_ROOT / name for name in figure_names]
    return tables, figures


def require_files(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError("Missing submission asset(s): " + ", ".join(missing))


def manuscript_tex_sources() -> list[Path]:
    # Package only the authoritative source and its generated inputs. Historical
    # local drafts are not publication inputs and may contain stale results.
    tables, _ = referenced_assets()
    return sorted(set([MAIN_TEX, *tables]))


def write_deterministic_zip(destination: Path, paths: list[Path]) -> str:
    require_files(paths)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(destination, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(paths, key=lambda item: item.name):
            info = ZipInfo(path.name, date_time=FIXED_ZIP_TIME)
            info.compress_type = ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, path.read_bytes(), compress_type=ZIP_DEFLATED, compresslevel=9)
    return hashlib.sha256(destination.read_bytes()).hexdigest()


def main() -> int:
    tables, figures = referenced_assets()
    core = [MAIN_TEX, MAIN_TEX.with_suffix(".pdf"), MANUSCRIPT_ROOT / "refs2.bib"]
    manuscript_files = core + tables + figures
    outputs = [
        (MANUSCRIPT_ROOT / "figures.zip", figures),
        (MANUSCRIPT_ROOT / "tables.zip", tables),
        (MANUSCRIPT_ROOT / "manuscript.zip", manuscript_files),
        (REVIEW_ROOT / "manuscript.zip", manuscript_files),
    ]
    publication_records: list[dict[str, object]] = []
    for destination, files in outputs:
        digest = write_deterministic_zip(destination, files)
        print(f"{destination.relative_to(REPO_ROOT)}: {len(files)} files, sha256={digest}")
        publication_records.append(
            {
                "path": destination.relative_to(REPO_ROOT).as_posix(),
                "artifact_type": "submission_archive",
                "members": len(files),
                "bytes": destination.stat().st_size,
                "sha256": digest,
            }
        )

    for path, artifact_type in (
        (MAIN_TEX, "manuscript_source"),
        (MAIN_TEX.with_suffix(".pdf"), "compiled_manuscript"),
        (MANUSCRIPT_ROOT / "manuscript_values.tex", "generated_manuscript_prose_values"),
    ):
        require_files([path])
        publication_records.append(
            {
                "path": path.relative_to(REPO_ROOT).as_posix(),
                "artifact_type": artifact_type,
                "members": "",
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )

    PUBLICATION_AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with PUBLICATION_AUDIT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("path", "artifact_type", "members", "bytes", "sha256"),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(sorted(publication_records, key=lambda row: str(row["path"])))
    print(f"Wrote {PUBLICATION_AUDIT_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
