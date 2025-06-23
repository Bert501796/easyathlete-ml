import pandas as pd
import numpy as np

def parse_streams(activity):
    streams = activity.get("stream_data_full", {})

    if not isinstance(streams, dict):
        # Fallback: rebuild from individual raw streams if stream_data_full is malformed
        print("⚠️ stream_data_full is malformed or missing, rebuilding from raw streams...")
        fallback_keys = [
            ("watts", "wattsStream"),
            ("heart_rate", "heartRateStream"),
            ("cadence", "cadenceStream"),
            ("altitude", "altitudeStream"),
            ("distance", "distanceStream"),
            ("time_sec", "timeStream"),
            ("speed", "speedStream")
        ]
        rebuilt = {
            alias: activity.get(orig)
            for alias, orig in fallback_keys
            if isinstance(activity.get(orig), list) and len(activity.get(orig)) > 0
        }
        if len(rebuilt) >= 2:
            min_len = min(len(v) for v in rebuilt.values())
            rebuilt = {k: v[:min_len] for k, v in rebuilt.items()}
            df = pd.DataFrame(rebuilt)
        else:
            return pd.DataFrame()
    else:
        df = pd.DataFrame(streams)

    # Compute deltas for all available streams
    for col in df.columns:
        if col != "time_sec":
            df[f"delta_{col}"] = df[col].diff()

    # Compute rolling metrics over a 30-second window
    window = 30
    for col in df.columns:
        if col.startswith("delta_"):
            base_col = col.replace("delta_", "")
            if base_col in df:
                df[f"rolling_{base_col}_trend"] = df[col].rolling(window, min_periods=1).mean()
        elif col not in df.columns or col == "time_sec":
            continue
        else:
            df[f"rolling_{col}_mean"] = df[col].rolling(window, min_periods=1).mean()

    return df

def extract_aggregated_features(activity):
    return {
        "distanceKm": activity.get("distanceKm", 0),
        "movingTimeMin": activity.get("movingTimeMin", 0),
        "paceMinPerKm": activity.get("paceMinPerKm", 0),
        "hrEfficiency": activity.get("hrEfficiency", 0),
        "elevationPerKm": activity.get("elevationPerKm", 0),
        "estimatedLoad": activity.get("estimatedLoad", 0),
        "averageHeartrate": activity.get("averageHeartrate", 0),
        "maxHeartrate": activity.get("maxHeartrate", 0),
    }

def detect_segments(df, activity):
    # Dummy logic — replace with actual segment detection later
    segments = []
    summary = {
        "count": 0,
        "avg_duration_sec": 0,
    }
    return {"segments": segments, "summary": summary}

def generate_ml_windows(df, segments):
    # Dummy logic — placeholder for real ML feature windowing
    return []

def convert_numpy_types(data):
    if isinstance(data, dict):
        return {k: convert_numpy_types(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_types(v) for v in data]
    elif isinstance(data, np.generic):
        return data.item()
    else:
        return data
