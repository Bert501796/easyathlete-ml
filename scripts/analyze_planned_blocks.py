import os
import argparse
import json
from pathlib import Path
from fitparse import FitFile
from dotenv import load_dotenv
from pymongo import MongoClient
from utils.fit_engine.fit_parser import parse_fit_schedule
from utils.fit_engine.fit_matcher import match_fit_file_to_activity

# Load Mongo credentials
load_dotenv()
USER_ID = os.getenv("FIT_MATCH_USER_ID")
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "test")
client = MongoClient(MONGO_URL)
collection = client[DB_NAME]["stravaactivities"]

def format_block(block):
    result = {
        "segment_type": block["type"],
        "duration_sec": block["duration_sec"],
        "notes": ""
    }
    if block.get("repeat"):
        result["repeats"] = block["repeat"]
    if block.get("recovery_sec"):
        result["rest_between_sec"] = block["recovery_sec"]
    return result

def analyze_fit_folder(folder_path: str, output_path: str):
    folder = Path(folder_path)
    all_fit_files = list(folder.rglob("*.fit")) #process all files
    #all_fit_files = list(folder.rglob("*.fit"))[:10]  # Limit to first 10 files


    print(f"🔍 Found {len(all_fit_files)} .fit files to analyze.")
    output = []

    for fit_path in all_fit_files:
        print(f"\n📂 Analyzing {fit_path}...")

        try:
            fitfile = FitFile(str(fit_path))
            planned_blocks = parse_fit_schedule(str(fit_path))
            activity_type = fit_path.parent.name

            if not USER_ID:
                raise ValueError("❌ FIT_MATCH_USER_ID not set in your .env file")

            activity = match_fit_file_to_activity(fitfile, user_id=USER_ID, fallback_sport=activity_type)
            if not activity:
                print(f"❌ No matching activity found for {fit_path.name}")
                continue

            # Trim .0 from stravaId and get startDate from Mongo
            strava_id_clean = str(int(float(activity["stravaId"])))
            mongo_doc = collection.find_one({"stravaId": activity["stravaId"]})
            start_date_local = mongo_doc.get("startDate").isoformat() if mongo_doc and mongo_doc.get("startDate") else None

            formatted = {
                "stravaId": strava_id_clean,
                "activity_type": activity_type,
                "start_date_local": start_date_local,
                "planned_segments": [format_block(b) for b in planned_blocks]
            }
            output.append(formatted)

        except Exception as e:
            print(f"❌ Failed to parse {fit_path}: {e}")

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Done. Saved {len(output)} entries to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze planned workout blocks from .fit files")
    parser.add_argument("--folder", type=str, default="fit_data", help="Folder with .fit files")
    parser.add_argument("--output", type=str, default="template_blocks.json", help="Output JSON path")
    args = parser.parse_args()

    analyze_fit_folder(args.folder, args.output)
