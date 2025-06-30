import pandas as pd
from collections import defaultdict
from datetime import datetime
from dateutil import parser
import numpy as np
from typing import List, Optional

def compute_kpi_trends(activities: List[dict], start_date: Optional[str] = None, end_date: Optional[str] = None, activity_type: Optional[str] = None):
    segment_rows = []
    total_segments = 0
    valid_segments = 0
    logged_example = False

    for activity in activities:
        if activity_type and activity.get("type") != activity_type:
            continue

        start_date_raw = activity.get("startDate")
        date = start_date_raw if isinstance(start_date_raw, datetime) else parser.parse(start_date_raw)

        if start_date and date < parser.parse(start_date):
            continue
        if end_date and date > parser.parse(end_date):
            continue

        week = date.strftime("%Y-W%U")
        for seg in activity.get("segments", []):
            total_segments += 1

            if not logged_example:
                print("🔎 Example segment keys:", seg.keys())
                print("🔎 Example segment content:", seg)
                logged_example = True

            hr_avg = seg.get("avg_heart_rate")
            pace = seg.get("avg_speed")  # assumed to be speed in m/min
            watts_avg = seg.get("avg_watts")
            duration_min = seg.get("duration_sec", 0) / 60
            distance_km = seg.get("avg_distance", 0) / 1000
            zone_score = seg.get("zone_match_score")  # optional
            segment_type = seg.get("type") or "unknown"

            segment_rows.append({
                "week": week,
                "segment_type": segment_type,
                "hr": hr_avg,
                "watts": watts_avg,
                "pace": pace,
                "distance_km": distance_km,
                "duration_min": duration_min,
                "zone_score": zone_score,
                "activity_type": activity.get("type")
            })
            valid_segments += 1

    print(f"✅ Processed {len(segment_rows)} segment rows from {len(activities)} activities.")
    print(f"🔎 Total segments scanned: {total_segments}, valid segments used: {valid_segments}")

    df = pd.DataFrame(segment_rows)
    if df.empty:
        print("⚠️ No segment rows found after filtering. Returning empty trends.")
        return {"debug": "no_rows"}

    df_by_type_week = df.groupby(["segment_type", "week"])
    print(f"📊 KPI groups by segment type and week: {df_by_type_week.size().to_dict()}")

    trends = []

    for (segment_type, week), group in df_by_type_week:
        if (group["distance_km"] > 0).any():
            trends.append({
                "segment_type": segment_type,
                "metric": "hr_efficiency",
                "week": week,
                "value": (group["hr"] / group["distance_km"]).mean()
            })

        trends.append({
            "segment_type": segment_type,
            "metric": "pace_consistency",
            "week": week,
            "value": group["pace"].std()
        })

        if (group["zone_score"] > 0).any():
            trends.append({
                "segment_type": segment_type,
                "metric": "zone_compliance",
                "week": week,
                "value": group["zone_score"].mean()
            })

        trends.append({
            "segment_type": segment_type,
            "metric": "effort_matching",
            "week": week,
            "value": group["zone_score"].mean()
        })
        # Segment Completion Delta — placeholder
        # High-Intensity Improvement — placeholder
        # Training Load Efficiency — placeholder
    if not trends:
        print("⚠️ No KPI trends calculated from grouped data.")

    return trends



 
