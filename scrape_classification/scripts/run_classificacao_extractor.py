"""Entrypoint simples para executar a pipeline de classificação ideológica."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.classificacao_extractor.main import run_pipeline


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Executa pipeline de classificação ideológica.")
    parser.add_argument(
        "--pdf-filename",
        dest="pdf_filename",
        default=None,
        help="Filtra execução para um único PDF (ex.: codato_2023.pdf).",
    )
    parser.add_argument(
        "--output-subdir",
        dest="output_subdir",
        default="classificacao",
        help="Subdiretório de saída dentro de output/.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(
        run_pipeline(
            target_pdf_filename=args.pdf_filename,
            output_subdir=args.output_subdir,
        )
    )
