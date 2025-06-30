### segment_linker.py

import json
import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from athlete_zones.zone_utils import resolve_athlete_zones
from utils.enrichment_helpers import parse_streams
import pandas as pd
import numpy as np

# Load environment variables
load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "test")

# Setup MongoDB connection
client = MongoClient(MONGO_URL)
collection = client[DB_NAME]["stravaactivities"]

# Load training templates with planned zones
def load_planned_templates(path="training_templates_zones.json"):
    with open(path, "r") as f:
        return json.load(f)

# Flatten planned segments into sequential timeline
def expand_planned_segments(planned):
    sequence = []
    cursor = 0

    for seg in planned:
        if "repeats" in seg and "rest_between_sec" in seg:
            for _ in range(seg["repeats"]):
                sequence.append({
                    "start": cursor,
                    "end": cursor + seg["duration_sec"],
                    "zone": seg["zone"],
                    "type": "effort"
                })
                cursor += seg["duration_sec"]

                sequence.append({
                    "start": cursor,
                    "end": cursor + seg["rest_between_sec"],
                    "zone": seg["rest_zone"],
                    "type": "recovery"
                })
                cursor += seg["rest_between_sec"]
        else:
            sequence.append({
                "start": cursor,
                "end": cursor + seg["duration_sec"],
                "zone": seg["zone"],
                "type": "steady"
            })
            cursor += seg["duration_sec"]
    return sequence

# Extract metrics per segment
def extract_metrics(df, start, end):
    segment_df = df[(df["time_sec"] >= start) & (df["time_sec"] < end)]

    print(f"🕎 Segment {start}-{end} → Data points: {len(segment_df)}")
    if "heart_rate" in segment_df:
        print(segment_df[["time_sec", "heart_rate"]].head())
    else:
        print("⚠️ 'heart_rate' not found in segment data")

    if "speed" in df.columns and "pace" not in df.columns:
        df["pace"] = df["speed"].apply(lambda x: 1000 / x if x > 0 else None)
        segment_df = df[(df["time_sec"] >= start) & (df["time_sec"] < end)]

    if segment_df.empty:
        return {
            "duration": end - start,
            "avg_hr": None,
            "max_hr": None,
            "min_hr": None,
            "avg_pace": None,
            "max_pace": None,
            "min_pace": None,
            "avg_cadence": None,
            "max_hr": None,
            "avg_watts": None,
            "segment_data_points": 0,
            "delta_hr": None,
            "delta_pace": None,
            "delta_watts": None,
            "altitude_gain": None,
            "altitude_loss": None,
            "altitude_range": None,
            "distance_covered": None
        }

    def delta(col):
        if col in segment_df:
            values = segment_df[col].dropna()
            if not values.empty:
                return values.iloc[-1] - values.iloc[0]
        return None

    altitude_diff = segment_df["altitude"].dropna().diff()
    altitude_gain = altitude_diff[altitude_diff > 0].sum() if not altitude_diff.empty else None
    altitude_loss = -altitude_diff[altitude_diff < 0].sum() if not altitude_diff.empty else None
    altitude_range = segment_df["altitude"].max() - segment_df["altitude"].min() if "altitude" in segment_df else None

    distance_covered = None
    if "distance" in segment_df:
        d = segment_df["distance"].dropna()
        if len(d) > 1:
            distance_covered = float(d.iloc[-1] - d.iloc[0])

    return {
        "duration": int(end - start),
        "avg_hr": float(segment_df["heart_rate"].dropna().mean()) if "heart_rate" in segment_df else None,
        "max_hr": int(segment_df["heart_rate"].dropna().max()) if "heart_rate" in segment_df else None,
        "min_hr": int(segment_df["heart_rate"].dropna().min()) if "heart_rate" in segment_df else None,
        "avg_pace": float(segment_df["pace"].dropna().mean()) if "pace" in segment_df else None,
        "max_pace": float(segment_df["pace"].dropna().max()) if "pace" in segment_df else None,
        "min_pace": float(segment_df["pace"].dropna().min()) if "pace" in segment_df else None,
        "avg_cadence": float(segment_df["cadence"].dropna().mean()) if "cadence" in segment_df else None,
        "avg_watts": float(segment_df["watts"].dropna().mean()) if "watts" in segment_df else None,
        "segment_data_points": int(len(segment_df)),
        "delta_hr": float(delta("heart_rate")) if delta("heart_rate") is not None else None,
        "delta_pace": float(delta("pace")) if delta("pace") is not None else None,
        "delta_watts": float(delta("watts")) if delta("watts") is not None else None,
        "altitude_gain": float(altitude_gain) if altitude_gain is not None else None,
        "altitude_loss": float(altitude_loss) if altitude_loss is not None else None,
        "altitude_range": float(altitude_range) if altitude_range is not None else None,
        "distance_covered": distance_covered
    }

# Main runner
def run_segment_linking(strava_id):
    templates = load_planned_templates()
    planned = next((item for item in templates if item["stravaId"] == str(strava_id)), None)

    if not planned:
        print(f"❌ No planned segments found for stravaId={strava_id}")
        return

    print(f"✅ Found planned template for {strava_id}")
    sequence = expand_planned_segments(planned["planned_segments"])

    activity = collection.find_one({"stravaId": int(strava_id)})
    if not activity:
        print(f"❌ No activity found in MongoDB for stravaId={strava_id}")
        return

    if "stream_data_full" not in activity:
        print(f"❌ No stream data available for stravaId={strava_id}")
        return

    df = parse_streams(activity)

    # Normalize cadence if running
    if activity.get("type") == "Run" and "cadence" in df.columns:
        df["cadence"] = df["cadence"] * 2

    activity_date = activity.get("startDate") or activity.get("start_date_local")
    athlete_zones = resolve_athlete_zones(user_id=activity["userId"], sport=activity["type"], activity_date=activity_date)

    print("\n📊 Planned Segment Timeline:")
    enriched_segments = []
    for seg in sequence:
        metrics = extract_metrics(df, seg["start"], seg["end"])
        enriched = {**seg, **metrics}
        enriched_segments.append(enriched)
        print(f"{seg['type'].capitalize():<10} | {seg['zone']:<8} | {seg['start']:>5} → {seg['end']:>5} sec | avg_hr: {metrics['avg_hr'] if metrics['avg_hr'] is not None else 'n/a'} bpm")

    # Save to MongoDB
    collection.update_one(
        {"stravaId": int(strava_id)},
        {"$set": {"planned_segment_analysis": enriched_segments}}
    )
    print(f"📂 Saved {len(enriched_segments)} segments to MongoDB under 'planned_segment_analysis'")

    return enriched_segments

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Link planned segments to actual activity streams")
    parser.add_argument("--stravaId", type=str, help="Strava activity ID")
    parser.add_argument("--processAll", action="store_true", help="Process all Run-type planned activities")
    args = parser.parse_args()

    if args.processAll:
        templates = load_planned_templates()
        run_ids = [tpl["stravaId"] for tpl in templates if tpl.get("activity_type") == "Run"]
        print(f"📋 Processing {len(run_ids)} activities with type=Run")
        for i, sid in enumerate(run_ids, start=1):
            try:
                print(f"\n➡️  [{i}/{len(run_ids)}] Processing stravaId={sid}")
                run_segment_linking(sid)
            except Exception as e:
                print(f"⚠️ Error processing stravaId={sid}: {e}")
    elif args.stravaId:
        run_segment_linking(args.stravaId)
