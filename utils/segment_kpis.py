import pandas as pd
from collections import defaultdict
from datetime import datetime
from dateutil import parser
import numpy as np
from typing import List, Optional

def compute_kpi_trends(activities: List[dict], start_date: Optional[str] = None, end_date: Optional[str] = None, activity_type: Optional[str] = None):
    segment_rows = []
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
            if not seg.get("planned_segment_analysis"):
                continue

            ps = seg["planned_segment_analysis"]
            effort = seg.get("effort", {})

            duration_min = (effort.get("duration") or 0) / 60
            distance_km = (effort.get("distance") or 0) / 1000
            hr_avg = effort.get("hr", {}).get("avg")
            watts_avg = effort.get("watts", {}).get("avg")
            pace = (effort.get("pace", {}).get("avg") or 0)
            zone_score = seg.get("zone_match_score")

            segment_rows.append({
                "week": week,
                "hr": hr_avg,
                "watts": watts_avg,
                "pace": pace,
                "distance_km": distance_km,
                "duration_min": duration_min,
                "zone_score": zone_score,
                "activity_type": activity.get("type")
            })

    df = pd.DataFrame(segment_rows)
    if df.empty:
        return {}

    df_by_week = df.groupby("week")
    trends = defaultdict(list)

    for week, group in df_by_week:
        # Heart Rate Efficiency (bpm/km)
        if (group["distance_km"] > 0).any():
            trends["hr_efficiency"].append({
                "week": week,
                "value": (group["hr"] / group["distance_km"]).mean()
            })

        # Pacing Consistency (std of pace)
        trends["pace_consistency"].append({
            "week": week,
            "value": group["pace"].std()
        })

        # Recovery Slope — not applicable here yet

        # Zone Compliance
        if (group["zone_score"] > 0).any():
            trends["zone_compliance"].append({
                "week": week,
                "value": group["zone_score"].mean()
            })

        # Segment Completion Delta — placeholder
        # High-Intensity Improvement — placeholder

        # Training Load Efficiency — placeholder

        # Effort Matching Score — reuse zone_match_score for now
        trends["effort_matching"].append({
            "week": week,
            "value": group["zone_score"].mean()
        })

    return trends
