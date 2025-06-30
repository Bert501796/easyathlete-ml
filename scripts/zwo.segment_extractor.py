import os
import argparse
import json
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from utils.fit_engine.zwo_matcher import match_zwo_file_to_activity
from utils.fit_engine.zwo_parser import parse_zwo_schedule
from utils.zone_utils import resolve_athlete_zones, estimate_ftp_from_zones
from pathlib import Path

# Load Mongo credentials
load_dotenv()
USER_ID = os.getenv("FIT_MATCH_USER_ID")
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "test")
client = MongoClient(MONGO_URL)
collection = client[DB_NAME]["stravaactivities"]

def map_power_zone(power_low, power_high):
    if power_low is None or power_high is None:
        return None
    if 0.38 <= power_low <= 0.41 and 0.49 <= power_high <= 0.53:
        return "Recovery"
    if 0.60 <= power_low <= 0.64 and 0.70 <= power_high <= 0.76:
        return "Base"
    if 0.75 <= power_low <= 0.77 and 0.79 <= power_high <= 0.81:
        return "Zone2"
    if 0.79 <= power_low <= 0.81 and 0.89 <= power_high <= 0.91:
        return "Tempo"
    if 0.92 <= power_low <= 0.95 and 1.07 <= power_high <= 1.09:
        return "Anaerobic threshold"
    if 1.42 <= power_low <= 1.44 and 1.72 <= power_high <= 1.74:
        return "High Anaerobic"
    return None

def format_activity_name_from_filename(filename):
    base = Path(filename).stem
    parts = base.split("-")[1:]  # skip date part
    return "-".join(parts).capitalize().replace("ctempo", "C-Tempo").replace("caerobic", "C-Aerobic").replace("cvo2", "C-VO2")  # simple substitutions

def analyze_zwo_folder(folder_path: str, output_path: str):
    folder = Path(folder_path)
    all_zwo_files = list(folder.rglob("*.zwo"))[:5]  # recursively find all .zwo files

    print(f"🔍 Found {len(all_zwo_files)} .zwo files to analyze.")
    output = []
    global_unmatched = set()

    for zwo_path in all_zwo_files:
        print(f"\n📂 Analyzing {zwo_path.name} at {zwo_path.resolve()}...")

        try:
            print("📑 Parsing ZWO schedule...")
            planned_segments = parse_zwo_schedule(str(zwo_path))

            print("🔍 Attempting to match with Strava activity...")
            activity = match_zwo_file_to_activity(str(zwo_path), user_id=USER_ID, fallback_sport="VirtualRide")
            if not activity:
                print(f"❌ No matching activity found for {zwo_path.name}")
                continue

            strava_id_clean = str(int(float(activity["stravaId"])))
            start_date_local = activity.get("startDate").isoformat() if activity.get("startDate") else None

            zones = resolve_athlete_zones(USER_ID, "Ride", start_date_local)
            ftp = estimate_ftp_from_zones(USER_ID)

            if not ftp:
                print("⚠️ FTP not found. Skipping watt conversion.")

            unmatched_ranges = []
            for segment in planned_segments:
                power_low = segment.get("powerLow")
                power_high = segment.get("powerHigh")
                zone = map_power_zone(power_low, power_high)
                if zone:
                    segment["zone"] = zone
                else:
                    unmatched = (power_low, power_high)
                    unmatched_ranges.append(unmatched)
                    global_unmatched.add(unmatched)

                if ftp:
                    segment["watts"] = [
                        round(power_low * ftp) if power_low else None,
                        round(power_high * ftp) if power_high else None
                    ]

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
                "planned_segments": planned_segments
            }
            print(f"✅ Match found. Strava ID: {strava_id_clean}, Start: {start_date_local}")
            output.append(formatted)

        except Exception as e:
            print(f"❌ Failed to parse {zwo_path}: {e}")

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    if global_unmatched:
        print("\n🟡 Summary of unmatched power ranges across all files:")
        for low, high in sorted(global_unmatched):
            print(f"  - powerLow: {low}, powerHigh: {high}")

    print(f"\n✅ Done. Saved {len(output)} entries to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze planned workout blocks from .zwo files")
    parser.add_argument("--folder", type=str, default="fit_data/VirtualRide/ZWO files", help="Folder with .zwo files")
    parser.add_argument("--output", type=str, default="template_blocks_zwo.json", help="Output JSON path")
    args = parser.parse_args()

    analyze_zwo_folder(args.folder, args.output)
