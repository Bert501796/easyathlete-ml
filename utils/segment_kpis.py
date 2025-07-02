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
        activity_id = activity.get("stravaId") or activity.get("_id")

        for seg in activity.get("segments", []):
            total_segments += 1

            if not logged_example:
                print("🔎 Example segment keys:", seg.keys())
                print("🔎 Example segment content:", seg)
                logged_example = True

            hr_avg = seg.get("avg_heart_rate")
            pace = seg.get("avg_speed")
            watts_avg = seg.get("avg_watts")
            duration_min = seg.get("duration_sec", 0) / 60
            distance_km = seg.get("avg_distance", 0) / 1000
            zone_score = seg.get("zone_match_score")
            start_index = seg.get("start_index")
            end_index = seg.get("end_index")
            hr_recovery = seg.get("hr_recovery_60s")
            hr_drift = seg.get("hr_drift_ratio")

            segment_rows.append({
                "week": week,
                "hr": hr_avg,
                "watts": watts_avg,
                "pace": pace,
                "distance_km": distance_km,
                "duration_min": duration_min,
                "zone_score": zone_score,
                "activity_type": activity.get("type"),
                "activity_id": activity_id,
                "start_index": start_index,
                "end_index": end_index,
                "hr_recovery_60s": hr_recovery,
                "hr_drift_ratio": hr_drift,
            })
            valid_segments += 1

    print(f"✅ Processed {len(segment_rows)} segment rows from {len(activities)} activities.")
    print(f"🔎 Total segments scanned: {total_segments}, valid segments used: {valid_segments}")

    df = pd.DataFrame(segment_rows)
    if df.empty:
        print("⚠️ No segment rows found after filtering. Returning empty trends.")
        return {"debug": "no_rows"}

    df_by_week = df.groupby("week")
    print(f"📊 KPI groups by week: {df_by_week.size().to_dict()}")

    all_trends = []
    metric_values = defaultdict(list)

    for week, group in df_by_week:
        metrics = {}

        if (group["distance_km"] > 0).any():
            hr_eff = (group["hr"] / group["distance_km"]).mean()
            metrics["hr_efficiency"] = hr_eff

        metrics["pace_consistency"] = group["pace"].std()

        if (group["zone_score"] > 0).any():
            metrics["zone_compliance"] = group["zone_score"].mean()

        metrics["effort_matching"] = group["zone_score"].mean()
        metrics["completion_delta"] = group["duration_min"].std()

        high_intensity = group[group["duration_min"] < 5]
        if not high_intensity.empty:
            metrics["high_intensity_watts"] = high_intensity["watts"].mean()

        if (group["duration_min"] > 0).any():
            metrics["load_efficiency"] = (group["watts"] / group["duration_min"]).mean()

        if (group["hr_recovery_60s"].notna()).any():
            metrics["hr_recovery_60s"] = group["hr_recovery_60s"].dropna().mean()

        if (group["hr_drift_ratio"].notna()).any():
            metrics["hr_drift_ratio"] = group["hr_drift_ratio"].dropna().mean()

        for metric, value in metrics.items():
            all_trends.append({
                "segment_type": "all",
                "metric": metric,
                "week": week,
                "value": value
            })
            metric_values[("all", metric)].append((week, value))

    segment_counts = df.groupby("week").size().reset_index(name='count')
    for _, row in segment_counts.iterrows():
        all_trends.append({
            "segment_type": "all",
            "metric": "segment_frequency",
            "week": row["week"],
            "value": row["count"]
        })
        metric_values[("all", "segment_frequency")].append((row["week"], row["count"]))

    normalized_trends = []
    for (segment_type, metric), week_values in metric_values.items():
        values = np.array([v for _, v in week_values])
        if len(set(values)) == 1:
            norm_values = [0.5 for _ in values]
        else:
            min_v, max_v = values.min(), values.max()
            norm_values = [(v - min_v) / (max_v - min_v) for v in values]

        for (week, _), norm in zip(week_values, norm_values):
            normalized_trends.append({
                "segment_type": "all",
                "metric": metric + "_norm",
                "week": week,
                "value": norm
            })

    fitness_index = defaultdict(lambda: defaultdict(float))
    weights = {
        "hr_efficiency_norm": 0.4,
        "zone_compliance_norm": 0.3,
        "completion_delta_norm": 0.3
    }

    for row in normalized_trends:
        metric = row["metric"]
        if metric in weights:
            fitness_index[(row["segment_type"], row["week"])]["sum"] += row["value"] * weights[metric]
            fitness_index[(row["segment_type"], row["week"])]["weight"] += weights[metric]

    for (segment_type, week), val in fitness_index.items():
        score = val["sum"] / val["weight"]
        normalized_trends.append({
            "segment_type": segment_type,
            "metric": "fitness_index",
            "week": week,
            "value": score
        })

    return normalized_trends
