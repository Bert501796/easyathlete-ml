import os
import argparse
from pathlib import Path
from fitparse import FitFile
import json
from dotenv import load_dotenv

from utils.fit_engine.fit_parser import parse_fit_schedule
from utils.fit_engine.fit_matcher import match_fit_file_to_activity
from utils.fit_engine.segment_aligner import align_planned_to_detected, score_segment_accuracy

# Load Mongo credentials
load_dotenv()
USER_ID = os.getenv("FIT_MATCH_USER_ID")

def run_fit_alignment(fit_folder: str, output_path: str = "fit_alignment_results.jsonl"):
    fit_files = list(Path(fit_folder).rglob("*.fit"))
    total = len(fit_files)
    print(f"🔍 Found {total} .fit file(s) in {fit_folder}")
    
    if total == 0:
        print("❌ No .fit files found.")
        return

    results = []
    for i, fit_path in enumerate(fit_files):
        sport_from_folder = fit_path.parent.name  # Extract sport from folder
        print(f"\n[{i+1}/{total}] 📂 Processing {fit_path.name} (sport: {sport_from_folder})")

        try:
            fitfile = FitFile(str(fit_path))
            planned_blocks = parse_fit_schedule(str(fit_path))

            # ⬇️ Derive sport_type from parent folder name (e.g. VirtualRide, Run)
            sport_type = fit_path.parent.name
            print(f"🏷️ Detected sport type from folder: {sport_type}")

            if not USER_ID:
                raise ValueError("❌ FIT_MATCH_USER_ID not set in your .env file")

            activity = match_fit_file_to_activity(
                fitfile,
                user_id=USER_ID,
                fallback_sport=sport_type
            )

            if not activity:
                print(f"❌ No activity match found for {fit_path.name}")
                continue

            actual_blocks = activity.get("segmentSequence", [])
            if not actual_blocks:
                print(f"⚠️ No segmentSequence found in activity {activity.get('stravaId')}")
                continue

            alignment = align_planned_to_detected(planned_blocks, actual_blocks)
            metrics = score_segment_accuracy(alignment)

            entry = {
                "file": fit_path.name,
                "stravaId": activity.get("stravaId"),
                "planned_blocks": planned_blocks,
                "matched_segments": actual_blocks,
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
