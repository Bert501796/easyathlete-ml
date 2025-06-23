# utils/segment_trends.py

import pandas as pd
from scipy.stats import linregress
from datetime import datetime
from typing import List, Dict


def extract_segments(activities: List[Dict]) -> pd.DataFrame:
    """
    Flatten segment data from a list of activities into a DataFrame
    """
    segments = []
    for activity in activities:
        try:
            activity_date = pd.to_datetime(activity.get("startDate"))
        except Exception:
            continue

        for seg in activity.get("segments", []):
            if not isinstance(seg, dict):
                continue

            segments.append({
                "activity_date": activity_date,
                "segment_type": seg.get("type"),
                "avg_hr": seg.get("avg_hr"),
                "avg_speed": seg.get("avg_speed"),
                "avg_watts": seg.get("avg_watts"),
                "avg_cadence": seg.get("avg_cadence"),
                "duration_sec": seg.get("duration_sec"),
                "elevation_gain": seg.get("elevation_gain"),
                "distance_m": seg.get("distance_m"),
                "hr_efficiency": _compute_hr_efficiency(seg),
                "pace": _compute_pace(seg),
            })

    return pd.DataFrame(segments)


def _compute_pace(seg: Dict) -> float:
    speed = seg.get("avg_speed")
    if speed and speed > 0:
        return 1000 / speed  # min/km if speed is m/min
    return None


def _compute_hr_efficiency(seg: Dict) -> float:
    speed = seg.get("avg_speed")
    hr = seg.get("avg_hr")
    if speed and hr and speed > 0 and hr > 0:
        pace = 1000 / speed
        return pace / hr
    return None


def compute_metric_trend(df: pd.DataFrame, metric: str, time_col: str = "activity_date") -> Dict:
    df = df.dropna(subset=[metric])
    df = df.sort_values(time_col)
    if len(df) < 3:
        return None

    x = df[time_col].astype("int64") // 10**9  # UNIX timestamps
    y = df[metric]

    slope, intercept, r_value, p_value, std_err = linregress(x, y)
    return {
        "slope": round(slope, 5),
        "trend_direction": "improving" if slope < 0 else "regressing",
        "r_squared": round(r_value**2, 3),
        "p_value": round(p_value, 4)
    }


def analyze_segment_trends(activities: List[Dict]) -> List[Dict]:
    df = extract_segments(activities)
    if df.empty:
        return []

    trends = []
    metrics = ["pace", "avg_hr", "hr_efficiency", "avg_watts", "avg_cadence"]

    for segment_type in df["segment_type"].dropna().unique():
        segment_df = df[df["segment_type"] == segment_type]
        for metric in metrics:
            result = compute_metric_trend(segment_df, metric)
            if result:
                trends.append({
                    "segment_type": segment_type,
                    "metric": metric,
                    **result
                })

    return trends
