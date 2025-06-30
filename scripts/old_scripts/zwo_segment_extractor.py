#script used to extract and match zwo activities and stravaId. Export in template_blocks_zwo.json

import os
import sys
import argparse
import json
from pathlib import Path

# Fix sys.path before other imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from pymongo import MongoClient
from utils.fit_engine.zwo_matcher import match_zwo_file_to_activity
from utils.fit_engine.zwo_parser import parse_zwo_schedule
from athlete_zones.zone_utils import resolve_athlete_zones, estimate_ftp_from_zones

# Load Mongo credentials
load_dotenv()
USER_ID = os.getenv("FIT_MATCH_USER_ID")
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "test")
client = MongoClient(MONGO_URL)
collection = client[DB_NAME]["stravaactivities"]

def analyze_zwo_folder(folder_path: str, output_path: str):
    folder = Path(folder_path)
    all_zwo_files = list(folder.rglob("*.zwo"))  # limit to 5 for testing

    print(f"🔍 Found {len(all_zwo_files)} .zwo files to analyze.")
    output = []
    global_unmatched = set()

    from athlete_zones.zone_utils import load_athlete_zones

    def format_activity_name_from_filename(filename):
        base = Path(filename).stem
        parts = base.split("-")
        # Rebuild name from everything after the date
        name_part = "-".join(parts[3:]) if len(parts) > 3 else parts[-1]
        # Remove leading 'c' if present
        if name_part.lower().startswith("c"):
            name_part = name_part[1:]
        # Split into alpha and numeric suffix
        prefix = ''.join([c for c in name_part if not c.isdigit()])
        suffix = ''.join([c for c in name_part if c.isdigit()])
        chunks = ["C", prefix.lower(), suffix] if suffix else ["C", prefix.lower()]
        return "-".join([chunk.capitalize() for chunk in chunks])

    for zwo_path in all_zwo_files:
        print(f"\n📂 Analyzing {zwo_path.name} at {zwo_path.resolve()}...")

        try:
            print("📁 Parsing ZWO schedule...")
            planned_segments = parse_zwo_schedule(str(zwo_path))

            print("🔍 Attempting to match with Strava activity...")
            activity = match_zwo_file_to_activity(str(zwo_path), user_id=USER_ID, fallback_sport="VirtualRide")
            if not activity:
                print(f"❌ No matching activity found for {zwo_path.name}")
                continue

            strava_id_clean = str(int(float(activity["stravaId"])))
            start_date_local = activity.get("startDate").isoformat() if activity.get("startDate") else None

            athlete_zones = load_athlete_zones().get(USER_ID, {})
            ride_zone_entries = athlete_zones.get("zones", {}).get("Ride", [])
            zone_types_to_try = [z.get("zone_type", "classic") for z in ride_zone_entries if isinstance(z, dict)]

            ftp_cache = {}
            zones_cache = {}
            best_zone_type = None
            best_match_count = -1

            unmatched_ranges = []

            for zone_type in zone_types_to_try:
                match_count = 0
                resolved = resolve_athlete_zones(USER_ID, "Ride", start_date_local, zone_type)
                zones = resolved.get("zones", {})
                ftp = resolved.get("ftp")

                if not ftp or not zones:
                    continue

                for segment in planned_segments:
                    power_low = segment.get("powerLow")
                    power_high = segment.get("powerHigh")

                    if power_low is None or power_high is None:
                        continue

                    watts_low = round(power_low * ftp)
                    watts_high = round(power_high * ftp)
                    segment["watts"] = [watts_low, watts_high]
                    segment["zone"] = ""

                    for key, z in zones.items():
                        z_range = z.get("watts", [])
                        if len(z_range) == 2 and not (watts_high < z_range[0] or watts_low > z_range[1]):
                            match_count += 1
                            break

                if match_count > best_match_count:
                    best_match_count = match_count
                    best_zone_type = zone_type
                    ftp_cache[zone_type] = ftp
                    zones_cache[zone_type] = zones

            ftp = ftp_cache.get(best_zone_type)
            zones = zones_cache.get(best_zone_type, {})

            for segment in planned_segments:
                power_low = segment.get("powerLow")
                power_high = segment.get("powerHigh")
                matched = False

                segment["zone"] = ""
                segment["watts"] = []

                if power_low is None or power_high is None or not ftp:
                    continue

                watts_low = round(power_low * ftp)
                watts_high = round(power_high * ftp)
                segment["watts"] = [watts_low, watts_high]

                for key, z in zones.items():
                    z_range = z.get("watts", [])
                    if len(z_range) == 2 and not (watts_high < z_range[0] or watts_low > z_range[1]):
                        segment["zone"] = z.get("name", key)
                        segment["matched_zone_key"] = key
                        segment["matched_zone_type"] = best_zone_type or "unknown"
                        matched = True
                        break

                if not matched:
                    unmatched = (power_low, power_high)
                    unmatched_ranges.append(unmatched)
                    global_unmatched.add(unmatched)

            if unmatched_ranges:
                print("⚠️ Unmatched power ranges:")
                for low, high in unmatched_ranges:
                    print(f"  - powerLow: {low}, powerHigh: {high}")

            print(f"✅ Parsed {len(planned_segments)} planned segments.")

            formatted = {
                "stravaId": strava_id_clean,
                "activity_type": "VirtualRide",
                "start_date_local": start_date_local,
                "activity_name": format_activity_name_from_filename(zwo_path.name),
                "zone_type_used": best_zone_type or "unknown",
                "planned_segments": planned_segments
            }
            print(f"✅ Match found. Strava ID: {strava_id_clean}, Start: {start_date_local}")
            output.append(formatted)

        except Exception as e:
            print(f"❌ Failed to parse {zwo_path}: {e}")

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    if global_unmatched:
        print("\n🛡️ Summary of unmatched power ranges across all files:")
        for low, high in sorted(global_unmatched):
            print(f"  - powerLow: {low}, powerHigh: {high}")

    print(f"\n✅ Done. Saved {len(output)} entries to {output_path}")






if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze planned workout blocks from .zwo files")
    parser.add_argument("--folder", type=str, default="fit_data/VirtualRide/ZWO files", help="Folder with .zwo files")
    parser.add_argument("--output", type=str, default="template_blocks_zwo.json", help="Output JSON path")
    args = parser.parse_args()

    analyze_zwo_folder(args.folder, args.output)
