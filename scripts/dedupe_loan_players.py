#!/usr/bin/env python3
"""Find duplicate player files across clubs.

For each player id that appears in more than one club folder, verify the
duplicate is a loan (a `loan` entry exists and its `to_club_id` matches the
duplicate location), keep the file that lives in the owner's folder, and
delete the copy stored in the loan destination.

Any duplicate that doesn't fit the owner+loan pattern is reported and skipped
(no deletion), so they can be resolved manually.

Run with --dry-run to preview without deleting.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

DATA_ROOT = Path(r"F:/Rust/open-football-database/data")


def build_club_index() -> dict[int, Path]:
    index: dict[int, Path] = {}
    for club_json in DATA_ROOT.glob("*/*/*/club.json"):
        try:
            data = json.loads(club_json.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"bad club.json: {club_json}: {e}", file=sys.stderr)
            continue
        cid = int(data["id"])
        index[cid] = club_json.parent
    return index


def folder_club_id(player_file: Path, club_index_by_path: dict[Path, int]) -> int | None:
    return club_index_by_path.get(player_file.parent.parent)


def collect_players() -> dict[int, list[tuple[Path, dict]]]:
    groups: dict[int, list[tuple[Path, dict]]] = defaultdict(list)
    for player_file in DATA_ROOT.glob("*/*/*/players/*.json"):
        try:
            data = json.loads(player_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"bad player json: {player_file}: {e}", file=sys.stderr)
            continue
        pid = data.get("id")
        if pid is None:
            continue
        groups[int(pid)].append((player_file, data))
    return groups


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="report only, do not delete")
    args = parser.parse_args()

    club_index = build_club_index()
    club_index_by_path = {path: cid for cid, path in club_index.items()}

    groups = collect_players()

    duplicates = {pid: copies for pid, copies in groups.items() if len(copies) > 1}
    print(f"Found {len(duplicates)} duplicated player id(s) across {sum(len(v) for v in duplicates.values())} files")

    removed = 0
    unresolved: list[tuple[int, list[Path]]] = []

    for pid, copies in duplicates.items():
        owner_copies: list[tuple[Path, dict]] = []
        loan_copies: list[tuple[Path, dict]] = []
        stray: list[tuple[Path, dict]] = []

        for path, data in copies:
            folder_cid = folder_club_id(path, club_index_by_path)
            owner_cid = data.get("club_id")
            loan = data.get("loan") or {}
            loan_to = loan.get("to_club_id")

            if folder_cid == owner_cid:
                owner_copies.append((path, data))
            elif loan_to is not None and folder_cid == loan_to:
                loan_copies.append((path, data))
            else:
                stray.append((path, data))

        first_names = {data.get("first_name", "") for _, data in copies}
        last_names = {data.get("last_name", "") for _, data in copies}
        name = f"{next(iter(last_names), '')}, {next(iter(first_names), '')}".strip(", ")

        # Require: exactly one owner copy, and every non-owner copy has a loan
        # entry pointing to its folder, AND loan is present in the owner copy too.
        owner_ok = len(owner_copies) == 1
        owner_has_loan = owner_ok and bool(owner_copies[0][1].get("loan"))
        loans_consistent = all(bool(data.get("loan")) for _, data in loan_copies)

        if stray or not owner_ok or not owner_has_loan or not loans_consistent:
            unresolved.append((pid, [p for p, _ in copies]))
            print(f"\n[UNRESOLVED] id={pid} ({name})")
            for path, data in copies:
                folder_cid = folder_club_id(path, club_index_by_path)
                loan_to = (data.get("loan") or {}).get("to_club_id")
                print(
                    f"  folder_club={folder_cid} owner_club={data.get('club_id')} "
                    f"loan_to={loan_to}  {path.relative_to(DATA_ROOT)}"
                )
            continue

        for path, _ in loan_copies:
            print(f"[REMOVE] id={pid} ({name})  {path.relative_to(DATA_ROOT)}")
            if not args.dry_run:
                path.unlink()
            removed += 1

    print(f"\nRemoved {removed} duplicate file(s){' (dry-run)' if args.dry_run else ''}")
    if unresolved:
        print(f"{len(unresolved)} duplicate group(s) left unresolved — review above.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
