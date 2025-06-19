import numpy as np
import pandas as pd
from scipy.signal import find_peaks

def parse_streams(activity):
    hr = activity.get("heartRateStream", [])
    alt = activity.get("altitudeStream", [])
    dist = activity.get("distanceStream", [])
    cad = activity.get("cadenceStream", [])
    speed = activity.get("speedStream", [])
    watts = activity.get("wattsStream", [])

    # Only include streams that are non-empty
    data = {
        "heart_rate": hr,
        "altitude": alt,
        "distance": dist,
        "cadence": cad,
        "speed": speed,
        "watts": watts
    }
    min_length = min(len(v) for v in data.values() if len(v) > 0)

    # Pad missing/empty streams with NaNs
    for k in data:
        if len(data[k]) < min_length:
            data[k] = data[k] + [np.nan] * (min_length - len(data[k]))
        data[k] = data[k][:min_length]

    return pd.DataFrame(data)

def detect_segments(df, min_hr_rise=15, min_hr_drop=10):
    segments = []
    if df.empty:
        return {"segments": segments, "summary": {}}

    df["speed"] = df["distance"].diff().fillna(0) * 60  # rough m/s -> m/min
    df["hr_delta"] = df["heart_rate"].diff().fillna(0)
    df["altitude_gain"] = df["altitude"].diff().clip(lower=0)
    df["gradient"] = df["altitude"].diff() / df["distance"].diff().replace(0, 1)

    # --- ACCELERATION SEGMENTS ---
    accel_peaks, _ = find_peaks(df["speed"].fillna(0), height=np.percentile(df["speed"].dropna(), 90))
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
                    "avg_speed": float(segment["speed"].mean()),
                    "avg_cadence": float(segment["cadence"].mean()),
                    "elevation_gain": float(segment["altitude_gain"].sum()),
                    "avg_gradient": float(segment["gradient"].mean()),
                    "avg_watts": float(segment["watts"].mean()) if "watts" in segment else None
                })

    # --- RECOVERY SEGMENTS ---
    for i in range(10, len(df) - 10):
        hr_before = df["heart_rate"].iloc[i-5]
        hr_after = df["heart_rate"].iloc[i+5]
        hr_drop = hr_before - hr_after
        if hr_drop >= min_hr_drop:
            segments.append({
                "type": "recovery",
                "center": int(i),
                "hr_drop": float(hr_drop),
                "slope": float(hr_drop / 10.0),
                "cadence": float(df["cadence"].iloc[i]),
                "gradient": float(df["gradient"].iloc[i]),
                "watts": float(df["watts"].iloc[i]) if "watts" in df else None
            })

    # --- CLIMB SEGMENTS ---
    climb_window = 20
    for i in range(len(df) - climb_window):
        segment = df.iloc[i:i+climb_window]
        alt_gain = segment["altitude"].iloc[-1] - segment["altitude"].iloc[0]
        if alt_gain > 5:  # meters climbed
            segments.append({
                "type": "climb",
                "start": int(i),
                "end": int(i + climb_window),
                "elevation_gain": float(alt_gain),
                "avg_hr": float(segment["heart_rate"].mean()),
                "avg_speed": float(segment["speed"].mean()),
                "avg_cadence": float(segment["cadence"].mean()),
                "avg_watts": float(segment["watts"].mean()) if "watts" in segment else None
            })

    return {
        "segments": segments,
        "summary": {
            "total_segments": len(segments),
            "mean_hr": float(df["heart_rate"].mean()),
            "max_speed": float(df["speed"].max()),
            "total_elevation": float(df["altitude_gain"].sum()),
            "avg_cadence": float(df["cadence"].mean()),
            "avg_watts": float(df["watts"].mean()) if "watts" in df else None
        }
    }
