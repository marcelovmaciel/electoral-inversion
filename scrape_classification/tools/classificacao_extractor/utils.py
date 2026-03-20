"""Utilitários compartilhados da pipeline de extração."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def ensure_dir(path: Path) -> None:
    """Cria diretório (e pais) se não existir."""
    path.mkdir(parents=True, exist_ok=True)


def ensure_parent_dir(path: Path) -> None:
    """Cria diretório pai do arquivo, se necessário."""
    ensure_dir(path.parent)


def collapse_spaces(text: str) -> str:
    """Normaliza espaços internos para um único espaço."""
    return " ".join(text.split())


def shrink_text(text: str, max_chars: int) -> str:
    """Limita tamanho do texto preservando conteúdo inicial."""
    compact = text.strip()
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 3].rstrip() + "..."


def markdown_anchor(text: str) -> str:
    """Gera âncora simples de Markdown para títulos."""
    chars: list[str] = []
    for ch in text.strip().lower():
        if ch.isalnum() or ch == "_":
            chars.append(ch)
    return "".join(chars)


def now_iso() -> str:
    """Retorna timestamp ISO em UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def command_exists(command: str) -> bool:
    """Indica se um comando de shell está disponível no PATH."""
    return shutil.which(command) is not None


def run_command(command: list[str]) -> tuple[int, str, str]:
    """Executa comando e retorna (returncode, stdout, stderr)."""
    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
        errors="replace",
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def split_pdftotext_pages(text: str) -> list[str]:
    """Separa saída do pdftotext por página usando form-feed."""
    pages = text.split("\f")
    while pages and not pages[-1].strip():
        pages.pop()
    return pages


def json_dumps_pretty(data: object) -> str:
    """Serialização JSON estável para arquivos de auditoria."""
    return json.dumps(data, ensure_ascii=False, indent=2)
