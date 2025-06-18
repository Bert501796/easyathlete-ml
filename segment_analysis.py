import numpy as np
import pandas as pd
from scipy.signal import find_peaks

def parse_streams(activity):
    hr = activity.get("heartRateStream", [])
    alt = activity.get("altitudeStream", [])
    dist = activity.get("distanceStream", [])
    length = min(len(hr), len(alt), len(dist))
    return pd.DataFrame({
        "heart_rate": hr[:length],
        "altitude": alt[:length],
        "distance": dist[:length]
    })

def detect_segments(df, min_hr_rise=15, min_hr_drop=10):
    segments = []
    if df.empty:
        return { "segments": segments, "summary": {} }

    df["speed"] = df["distance"].diff().fillna(0)  # rough speed approximation
    df["hr_delta"] = df["heart_rate"].diff().fillna(0)

    # --- ACCELERATION: look for steep speed increases
    accel_peaks, _ = find_peaks(df["speed"], height=np.percentile(df["speed"], 90))
    for i in accel_peaks:
        if i + 10 < len(df):
            segment = df.iloc[i:i+10]
            hr_rise = segment["heart_rate"].max() - segment["heart_rate"].min()
            if hr_rise >= min_hr_rise:
                segments.append({
                    "type": "acceleration",
                    "start": int(i),
                    "end": int(i + 10),
                    "delta_hr": float(hr_rise),
                    "avg_speed": float(segment["speed"].mean())
                })

    # --- RECOVERY: look for rapid HR drops
    for i in range(10, len(df) - 10):
        hr_before = df["heart_rate"].iloc[i-5]
        hr_after = df["heart_rate"].iloc[i+5]
        hr_drop = hr_before - hr_after
        if hr_drop >= min_hr_drop:
            segments.append({
                "type": "recovery",
                "center": int(i),
                "hr_drop": float(hr_drop),
                "slope": float(hr_drop / 10.0)
            })

    return {
        "segments": segments,
        "summary": {
            "total_segments": len(segments),
            "mean_hr": float(df["heart_rate"].mean()),
            "max_speed": float(df["speed"].max())
        }
    }
