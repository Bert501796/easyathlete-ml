import os
import argparse
import json
from pathlib import Path
from xml.etree import ElementTree as ET
from dotenv import load_dotenv
from pymongo import MongoClient
from utils.fit_engine.fit_matcher import match_fit_file_to_activity

# Load Mongo credentials
load_dotenv()
USER_ID = os.getenv("FIT_MATCH_USER_ID")
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "test")
client = MongoClient(MONGO_URL)
collection = client[DB_NAME]["stravaactivities"]


def parse_zwo_segments(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    workout = root.find("workout")
    segments = []

    for elem in workout:
        duration = float(elem.attrib.get("Duration", 0))
        power_low = elem.attrib.get("PowerLow")
        power_high = elem.attrib.get("PowerHigh")
        power = elem.attrib.get("Power")
        cadence = elem.attrib.get("Cadence")

        segment = {
            "segment_type": elem.tag,
            "duration_sec": duration,
            "powerLow": float(power_low) if power_low else None,
            "powerHigh": float(power_high) if power_high else None,
            "power": float(power) if power else None,
            "cadence": int(cadence) if cadence else None,
            "notes": ""
        }
        segments.append(segment)

    return segments


def analyze_zwo_folder(folder_path: str, output_path: str):
    folder = Path(folder_path)
    #all_zwo_files = list(folder.rglob("*.zwo")) #analyze entire folder
    all_zwo_files = [Path("fit_data/VirtualRide/ZWO files/2025-06-14-caerobic-intervals2.zwo")] #analyze one activity


    print(f"🔍 Found {len(all_zwo_files)} .zwo files to analyze.")
    output = []

    for zwo_path in all_zwo_files:
        print(f"\n📂 Analyzing {zwo_path.name}...")

        try:
            planned_segments = parse_zwo_segments(str(zwo_path))
            activity_type = zwo_path.parent.name

            if not USER_ID:
                raise ValueError("❌ FIT_MATCH_USER_ID not set in your .env file")

            activity = match_fit_file_to_activity(zwo_path, user_id=USER_ID, fallback_sport=activity_type)
            if not activity:
                print(f"❌ No matching activity found for {zwo_path.name}")
                continue

            strava_id_clean = str(int(float(activity["stravaId"])))
            mongo_doc = collection.find_one({"stravaId": activity["stravaId"]})
            start_date_local = mongo_doc.get("startDate").isoformat() if mongo_doc and mongo_doc.get("startDate") else None

            formatted = {
                "stravaId": strava_id_clean,
                "activity_type": activity_type,
                "start_date_local": start_date_local,
                "planned_segments": planned_segments
            }
            output.append(formatted)

        except Exception as e:
            print(f"❌ Failed to parse {zwo_path}: {e}")

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Done. Saved {len(output)} entries to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze planned workout blocks from .zwo files")
    parser.add_argument("--folder", type=str, default="fit_data/VirtualRide/ZWO files", help="Folder with .zwo files")
    parser.add_argument("--output", type=str, default="template_blocks_zwo.json", help="Output JSON path")
    args = parser.parse_args()

    analyze_zwo_folder(args.folder, args.output)
