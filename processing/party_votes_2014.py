#!/usr/bin/env python3
from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


DATA_PATH = Path("data/raw/electionsBR/2014/party_mun_zone.csv")
OUTPUT_PATH = Path("scraping/output/partidos_votos_validos_2014.csv")
TARGET_CARGO = "Deputado Federal"


def sniff_delimiter(path: Path) -> str:
    with path.open("r", encoding="latin1", newline="") as handle:
        sample = handle.read(4096)
    try:
        return csv.Sniffer().sniff(sample).delimiter
    except csv.Error:
        return ","


def parse_int(value: str | None) -> int:
    if value is None:
        return 0
    cleaned = value.strip()
    if cleaned in {"", "#NULO", "#NE"}:
        return 0
    try:
        return int(cleaned)
    except ValueError:
        return 0


def main() -> None:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {DATA_PATH}")

    delimiter = sniff_delimiter(DATA_PATH)
    votes_by_party: dict[str, int] = defaultdict(int)
    total_valid_votes = 0

    with DATA_PATH.open("r", encoding="latin1", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        for row in reader:
            if row.get("DS_CARGO") != TARGET_CARGO:
                continue
            nominal = parse_int(row.get("QT_VOTOS_NOMINAIS"))
            legenda = parse_int(row.get("QT_VOTOS_LEGENDA"))
            valid_votes = nominal + legenda
            party = (row.get("SG_PARTIDO") or "").strip()
            if not party:
                continue
            votes_by_party[party] += valid_votes
            total_valid_votes += valid_votes

    rows = sorted(votes_by_party.items(), key=lambda item: item[1], reverse=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["SG_PARTIDO", "QT_VOTOS_VALIDOS"])
        writer.writerows(rows)

    try:
        import pandas as pd  # type: ignore
    except ImportError:
        pd = None

    if pd:
        df = pd.DataFrame(rows, columns=["SG_PARTIDO", "QT_VOTOS_VALIDOS"])
        print(df.to_string(index=False))
    else:
        party_width = max([len("SG_PARTIDO")] + [len(party) for party, _ in rows])
        print(f"{'SG_PARTIDO':<{party_width}}  QT_VOTOS_VALIDOS")
        for party, votes in rows:
            print(f"{party:<{party_width}}  {votes}")

    print(f"\nSOMA_VOTOS_VALIDOS_TOTAL={total_valid_votes}")
    print(f"\nCSV gerado em: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
