import os
import argparse
from pathlib import Path
from fitparse import FitFile
import json
from dotenv import load_dotenv
from bson import ObjectId
from datetime import datetime
from pymongo import MongoClient

from utils.fit_engine.fit_parser import parse_fit_schedule
from utils.fit_engine.fit_matcher import match_fit_file_to_activity
from utils.fit_engine.segment_aligner import align_planned_to_detected, score_segment_accuracy
from utils.enrichment_helpers import (
    parse_streams,
    detect_segments,
    prepare_activity_for_storage,
    extract_aggregated_features,
    convert_numpy_types,
)

# Load Mongo credentials
load_dotenv()
USER_ID = os.getenv("FIT_MATCH_USER_ID")
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "test")
client = MongoClient(MONGO_URL)
collection = client[DB_NAME]["stravaactivities"]

def run_fit_alignment(fit_folder: str, output_path: str = "fit_alignment_results.jsonl"):
    fit_files = list(Path(fit_folder).rglob("*.fit"))
    total = len(fit_files)
    print(f"🔍 Found {total} .fit file(s) in {fit_folder}")

    if total == 0:
        print("❌ No .fit files found.")
        return

    results = []
    for i, fit_path in enumerate(fit_files):
        sport_from_folder = fit_path.parent.name
        print(f"\n[{i+1}/{total}] 📂 Processing {fit_path.name} (sport: {sport_from_folder})")

        try:
            fitfile = FitFile(str(fit_path))
            planned_blocks = parse_fit_schedule(str(fit_path))

            sport_type = sport_from_folder
            print(f"🏷️ Detected sport type from folder: {sport_type}")

            if not USER_ID:
                raise ValueError("❌ FIT_MATCH_USER_ID not set in your .env file")

            activity = match_fit_file_to_activity(fitfile, user_id=USER_ID, fallback_sport=sport_type)
            if not activity:
                print(f"❌ No activity match found for {fit_path.name}")
                continue

            # 🧪 If missing enrichment, perform enrichment inline
            if not activity.get("segmentSequence") or not activity.get("segments"):
                print(f"⚠️ Missing enrichment for activity {activity.get('stravaId')} → running inline enrichment...")
                df = parse_streams(activity)
                if df.empty:
                    print(f"❌ Enrichment failed: no valid stream data for {activity.get('stravaId')}")
                    continue

                activity = prepare_activity_for_storage(activity, df)
                segments_result = detect_segments(df, activity)

                aggregated = extract_aggregated_features(activity)

                activity.update({
                    "aggregatedFeatures": convert_numpy_types(aggregated),
                    "segments": convert_numpy_types(segments_result["segments"]),
                    "segmentSummary": convert_numpy_types(segments_result["summary"]),
                    "segmentSequence": convert_numpy_types(segments_result["segments"]),
                    "enriched": True,
                    "enrichmentVersion": 1.4,
                    "updatedAt": datetime.utcnow()
                })

                collection.update_one({"_id": activity["_id"]}, {"$set": activity})
                print(f"✅ Enrichment completed for stravaId={activity.get('stravaId')}")

            actual_blocks = activity.get("segmentSequence", [])
            all_segments = activity.get("segments", [])

            if not actual_blocks:
                print(f"⚠️ No segmentSequence found even after enrichment.")
                continue

            alignment = align_planned_to_detected(planned_blocks, actual_blocks)
            metrics = score_segment_accuracy(alignment)

            entry = {
                "file": str(fit_path),
                "sport_type": sport_type,
                "stravaId": activity.get("stravaId"),
                "planned_blocks": planned_blocks,
                "matched_segments": actual_blocks,
                "raw_segments": all_segments,
                "alignment": alignment,
                "score": metrics,
            }

            results.append(entry)

            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "a") as out:
                out.write(json.dumps(entry) + "\n")

        except Exception as e:
            print(f"❌ Error processing {fit_path.name}: {e}")

    print("\n✅ Alignment complete.")
    print(f"📄 Results saved to: {output_path}")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run alignment between .fit and Strava segmentSequence")
    parser.add_argument("--folder", type=str, default="fit_data", help="Path to folder with .fit files")
    parser.add_argument("--output", type=str, default="fit_alignment_results.jsonl", help="Output file for results")
    args = parser.parse_args()

    run_fit_alignment(args.folder, args.output)
