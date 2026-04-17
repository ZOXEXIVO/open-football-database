#!/usr/bin/env python3
"""Split old-format Saudi player files into per-player files inside each club's players/ folder."""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path

SRC = Path(r"F:/Rust/open-football-database_2/buffer/sa")
DEST_ROOT = Path(r"F:/Rust/open-football-database/data/sa")


def slug(name: str) -> str:
    # Strip diacritics (NFKD then drop combining marks)
    normalized = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in normalized if not unicodedata.combining(c))
    # Lowercase, replace any non-alphanumeric run with a single underscore
    lower = stripped.lower()
    # Drop apostrophes entirely (they're not separators)
    lower = lower.replace("'", "").replace("\u2019", "")
    cleaned = re.sub(r"[^a-z0-9]+", "_", lower).strip("_")
    return cleaned


def build_club_index() -> dict[int, Path]:
    index: dict[int, Path] = {}
    for club_json in DEST_ROOT.glob("*/*/club.json"):
        data = json.loads(club_json.read_text(encoding="utf-8"))
        cid = int(data["id"])
        index[cid] = club_json.parent
    return index


def filename_for(player: dict) -> str:
    pid = player["id"]
    first = slug(player.get("first_name", "") or "")
    last = slug(player.get("last_name", "") or "")
    if last and first:
        return f"{pid}-{last}-{first}.json"
    if last:
        return f"{pid}-{last}.json"
    if first:
        return f"{pid}-{first}.json"
    return f"{pid}.json"


def main() -> int:
    club_index = build_club_index()
    total_players = 0
    missing_clubs: list[int] = []

    for src_file in sorted(SRC.glob("*.json")):
        stem = src_file.stem
        if not stem.isdigit():
            continue  # skip names.json etc.
        club_id = int(stem)
        dest_club = club_index.get(club_id)
        if dest_club is None:
            missing_clubs.append(club_id)
            print(f"no destination club for id {club_id} (source: {src_file.name})", file=sys.stderr)
            continue

        players_dir = dest_club / "players"
        players_dir.mkdir(exist_ok=True)

        data = json.loads(src_file.read_text(encoding="utf-8"))
        for player in data:
            fname = filename_for(player)
            out_path = players_dir / fname
            out_path.write_text(
                json.dumps(player, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            total_players += 1

        print(f"{src_file.name} -> {dest_club.relative_to(DEST_ROOT)} ({len(data)} players)")

    print(f"\nTotal players written: {total_players}")
    if missing_clubs:
        print(f"Missing destination clubs: {missing_clubs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
