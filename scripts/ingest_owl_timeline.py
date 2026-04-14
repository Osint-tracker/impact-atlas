"""
Owl Timeline Data Ingestor v1.0
===============================
Fetches pre-processed data from the owlmaps/timeline-data repository:
  - frontline.json: Historical frontline layers (since mid-Dec 2022)
  - latestposition.json: All event positions with metadata and Point geometries

These are auto-generated every 15 minutes by the owlmaps team.
"""

import requests
import json
import os
import sys
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8')

# Configuration
BASE_URL = "https://raw.githubusercontent.com/owlmaps/timeline-data/main/data"
FRONTLINE_URL = f"{BASE_URL}/frontline.json"
POSITIONS_URL = f"{BASE_URL}/latestposition.json"

OUTPUT_DIR = os.path.join("assets", "data")
FRONTLINE_OUTPUT = os.path.join(OUTPUT_DIR, "owl_frontline_history.json")
POSITIONS_OUTPUT = os.path.join(OUTPUT_DIR, "owl_positions.json")

HEADERS = {
    "User-Agent": "ImpactAtlas-OSINT/4.0"
}


def fetch_and_save(url: str, output_path: str, label: str) -> bool:
    """Fetches a JSON file from a URL and saves it locally."""
    print(f"   Fetching {label}...")
    print(f"   URL: {url}")

    try:
        r = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        r.raise_for_status()

        # Stream to file to handle large responses
        total_bytes = 0
        with open(output_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                total_bytes += len(chunk)

        size_mb = total_bytes / (1024 * 1024)
        print(f"   ✅ Saved {label}: {output_path} ({size_mb:.2f} MB)")
        return True

    except requests.exceptions.Timeout:
        print(f"   ❌ Timeout fetching {label}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Error fetching {label}: {e}")
        return False


def analyze_frontline_data(path: str):
    """Quick analysis of the downloaded frontline history."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, list):
            print(f"   📊 Frontline history: {len(data)} snapshots")
            if data:
                first = data[0]
                last = data[-1]
                first_date = first.get("date", first.get("timestamp", "unknown"))
                last_date = last.get("date", last.get("timestamp", "unknown"))
                print(f"   📅 Range: {first_date} → {last_date}")
        elif isinstance(data, dict):
            features = data.get("features", [])
            print(f"   📊 Frontline data: {len(features)} features")
    except Exception as e:
        print(f"   ⚠️  Could not analyze: {e}")


def analyze_positions_data(path: str):
    """Quick analysis of the downloaded positions data."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, dict):
            features = data.get("features", [])
            print(f"   📊 Positions data: {len(features)} features")

            # Count by side
            sides = {}
            for f_item in features:
                props = f_item.get("properties", {})
                side = props.get("side", props.get("faction", "unknown"))
                sides[side] = sides.get(side, 0) + 1

            for side, count in sorted(sides.items()):
                print(f"      {side}: {count}")
        elif isinstance(data, list):
            print(f"   📊 Positions data: {len(data)} entries")
    except Exception as e:
        print(f"   ⚠️  Could not analyze: {e}")


def ingest_timeline_data():
    print("🦉 Owl Timeline Data Ingestor v1.0")
    print(f"   Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    results = {}

    # 1. Fetch frontline history
    print("\n[1/2] FRONTLINE HISTORY")
    ok = fetch_and_save(FRONTLINE_URL, FRONTLINE_OUTPUT, "frontline.json")
    results["frontline"] = ok
    if ok:
        analyze_frontline_data(FRONTLINE_OUTPUT)

    # 2. Fetch latest positions
    print("\n[2/2] LATEST POSITIONS")
    ok = fetch_and_save(POSITIONS_URL, POSITIONS_OUTPUT, "latestposition.json")
    results["positions"] = ok
    if ok:
        analyze_positions_data(POSITIONS_OUTPUT)

    # Summary
    print(f"\n{'='*60}")
    print(f"  TIMELINE DATA INGESTION COMPLETE")
    print(f"{'='*60}")
    for key, success in results.items():
        status = "✅ OK" if success else "❌ FAILED"
        print(f"  {key}: {status}")
    print(f"{'='*60}")


if __name__ == "__main__":
    ingest_timeline_data()
