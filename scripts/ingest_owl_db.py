"""
Owl Unit Harvester v4.0 — TOML-based ORBAT Extraction
=====================================================
Fetches the full unit hierarchy from the owlmaps/units GitHub repo.
The repo uses a folder-based structure with _meta.toml files:
  data/units-{ru|ua}/{Branch}/_subunits/{Unit}/_subunits/{SubUnit}/...

This script:
  1. Uses the GitHub Tree API to get the full recursive file listing
  2. Parses the folder hierarchy to extract unit names, sides, and parents
  3. Fetches _meta.toml blobs for social media links (Telegram, etc.)
  4. Outputs orbat_full.json and owl_telegram_sources.json
"""

import requests
import json
import os
import sys
import re
import base64
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8')

# Configuration
TREE_API_URL = "https://api.github.com/repos/owlmaps/units/git/trees/main?recursive=1"
BLOB_API_BASE = "https://api.github.com/repos/owlmaps/units/git/blobs/"
OUTPUT_ORBAT = os.path.join("assets", "data", "orbat_full.json")
OUTPUT_SOURCES = os.path.join("ingestion", "owl_telegram_sources.json")

GITHUB_HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "ImpactAtlas-OSINT/4.0"
}

# Check for optional GitHub token in environment
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
if GITHUB_TOKEN:
    GITHUB_HEADERS["Authorization"] = f"token {GITHUB_TOKEN}"


def parse_toml_simple(content: str) -> dict:
    """
    Minimal TOML parser for the simple key=value format used by owlmaps.
    Handles: key = "value" and key = "" patterns.
    """
    result = {}
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        match = re.match(r'^(\w+)\s*=\s*"(.*)"', line)
        if match:
            result[match.group(1)] = match.group(2)
    return result


def extract_side_from_path(path: str) -> str:
    """Extracts faction side from the path."""
    lower = path.lower()
    if "units-ua" in lower:
        return "UA"
    elif "units-ru" in lower:
        return "RU"
    return "UNKNOWN"


def build_orbat_from_tree(tree_entries: list) -> tuple:
    """
    Builds the ORBAT hierarchy from the GitHub API tree response.
    Returns (orbat_db, toml_blob_map) where toml_blob_map maps
    unit paths to their _meta.toml blob SHA for fetching socials.
    """
    # Collect all _meta.toml entries and directory structure
    toml_entries = {}
    directories = set()

    for entry in tree_entries:
        path = entry.get("path", "")
        entry_type = entry.get("type", "")

        if not path.startswith("data/units-"):
            continue

        if entry_type == "tree":
            directories.add(path)
        elif path.endswith("_meta.toml") and entry_type == "blob":
            # Map the parent directory to this toml entry
            parent_dir = os.path.dirname(path).replace("\\", "/")
            toml_entries[parent_dir] = entry.get("sha", "")

    # Build unit entries from the directory structure
    orbat_db = []
    unit_paths = set()

    for dir_path in sorted(directories):
        # Skip non-unit directories (like _subunits itself, .github, etc.)
        parts = dir_path.replace("\\", "/").split("/")
        if len(parts) < 3:
            continue

        # Skip _subunits directories themselves (they're containers)
        basename = parts[-1]
        if basename in ("_subunits", "_template", ".github", "data"):
            continue
        if basename.startswith("."):
            continue

        # Determine side
        side = extract_side_from_path(dir_path)
        if side == "UNKNOWN":
            continue

        # Extract unit name from folder name
        unit_name = basename

        # Determine parent from path
        parent = None
        # Walk up to find parent unit (skip _subunits containers)
        parent_parts = parts[:-1]
        while parent_parts and parent_parts[-1] == "_subunits":
            parent_parts = parent_parts[:-1]
        if len(parent_parts) >= 3:
            parent = parent_parts[-1]

        # Determine unit type/depth from path depth
        # data/units-{side}/{Branch} = depth 3 = Branch
        # data/units-{side}/{Branch}/_subunits/{Unit} = depth 5 = Division/Army
        depth = len(parts)
        if depth <= 3:
            unit_type = "Branch"
        elif depth <= 5:
            unit_type = "Formation"
        elif depth <= 7:
            unit_type = "Division"
        elif depth <= 9:
            unit_type = "Brigade/Regiment"
        else:
            unit_type = "Battalion/Unit"

        unit_entry = {
            "name": unit_name,
            "side": side,
            "type": unit_type,
            "parent": parent,
            "path": dir_path,
            "has_meta": dir_path in toml_entries
        }

        orbat_db.append(unit_entry)
        unit_paths.add(dir_path)

    return orbat_db, toml_entries


def fetch_toml_socials(toml_entries: dict, max_fetch: int = 0) -> dict:
    """
    Fetches _meta.toml blobs to extract social media links.
    Disabled by default (max_fetch=0) to avoid GitHub API rate limits.
    Set GITHUB_TOKEN env var and pass max_fetch > 0 to enable.
    """
    if max_fetch <= 0 or not GITHUB_TOKEN:
        print("   ⏭️  Skipping blob fetches (no GITHUB_TOKEN or max_fetch=0)")
        return {}

    socials_map = {}
    fetched = 0

    for unit_path, blob_sha in toml_entries.items():
        if fetched >= max_fetch:
            break
        if not blob_sha:
            continue

        try:
            r = requests.get(
                f"{BLOB_API_BASE}{blob_sha}",
                headers=GITHUB_HEADERS,
                timeout=10
            )
            if r.status_code == 200:
                blob_data = r.json()
                content = base64.b64decode(blob_data.get("content", "")).decode("utf-8", errors="replace")
                parsed = parse_toml_simple(content)
                socials_map[unit_path] = parsed
                fetched += 1
            elif r.status_code == 403:
                print(f"   ⚠️  Rate limited after {fetched} fetches, stopping.")
                break
        except Exception:
            continue

    return socials_map


def harvest_owl_units():
    print("🦅 Owl Unit Harvester v4.0 (TOML Edition) started...")
    print(f"   Timestamp: {datetime.now(timezone.utc).isoformat()}")

    # Step 1: Fetch the full repo tree
    print("\n1. Fetching repository tree via GitHub API...")
    try:
        r = requests.get(TREE_API_URL, headers=GITHUB_HEADERS, timeout=30)
        r.raise_for_status()
        tree_data = r.json()
        tree_entries = tree_data.get("tree", [])
        truncated = tree_data.get("truncated", False)
        print(f"   Retrieved {len(tree_entries)} tree entries (truncated: {truncated})")
    except Exception as e:
        print(f"❌ Error fetching tree: {e}")
        return

    # Step 2: Build ORBAT from tree structure
    print("\n2. Parsing unit hierarchy from folder structure...")
    orbat_db, toml_entries = build_orbat_from_tree(tree_entries)

    ru_units = sum(1 for u in orbat_db if u["side"] == "RU")
    ua_units = sum(1 for u in orbat_db if u["side"] == "UA")
    print(f"   Total units found: {len(orbat_db)}")
    print(f"   RU units: {ru_units}")
    print(f"   UA units: {ua_units}")
    print(f"   _meta.toml files found: {len(toml_entries)}")

    if not orbat_db:
        print("❌ No units found. Something is wrong.")
        return

    # Step 3: Fetch social media data from _meta.toml files (requires GITHUB_TOKEN)
    max_socials = 100 if GITHUB_TOKEN else 0
    print(f"\n3. Social media extraction (max_fetch={max_socials})...")
    socials_map = fetch_toml_socials(toml_entries, max_fetch=max_socials)
    print(f"   Fetched socials for {len(socials_map)} units")

    # Enrich orbat_db with socials
    telegram_sources = set()
    for unit in orbat_db:
        path = unit["path"]
        if path in socials_map:
            meta = socials_map[path]
            unit["socials"] = {
                "telegram": meta.get("telegram", ""),
                "facebook": meta.get("facebook", ""),
                "youtube": meta.get("youtube", ""),
                "twitter": meta.get("twitter", "")
            }
            unit["description"] = meta.get("description", "")

            # Extract Telegram channel
            tg = meta.get("telegram", "").strip()
            if tg:
                clean = tg.split("/")[-1].replace("@", "").strip()
                if clean:
                    telegram_sources.add(clean)

        # Remove internal path from output
        del unit["path"]
        del unit["has_meta"]

    # Step 4: Save outputs
    print("\n4. Saving outputs...")

    os.makedirs(os.path.dirname(OUTPUT_ORBAT), exist_ok=True)
    with open(OUTPUT_ORBAT, 'w', encoding='utf-8') as f:
        json.dump(orbat_db, f, indent=2, ensure_ascii=False)
    print(f"   ✅ ORBAT saved: {OUTPUT_ORBAT} ({len(orbat_db)} units)")

    os.makedirs(os.path.dirname(OUTPUT_SOURCES), exist_ok=True)
    with open(OUTPUT_SOURCES, 'w', encoding='utf-8') as f:
        json.dump(sorted(list(telegram_sources)), f, indent=2)
    print(f"   ✅ Telegram sources saved: {OUTPUT_SOURCES} ({len(telegram_sources)} channels)")

    # Summary
    print(f"\n{'='*60}")
    print(f"  ORBAT EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"  RU Units: {ru_units}")
    print(f"  UA Units: {ua_units}")
    print(f"  Total:    {len(orbat_db)}")
    print(f"  Telegram Channels: {len(telegram_sources)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    harvest_owl_units()