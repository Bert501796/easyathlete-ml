import pandas as pd
import numpy as np

def parse_streams(activity):
    streams = activity.get("stream_data_full", {})
    if not streams or not isinstance(streams, dict):
        return pd.DataFrame()

    df = pd.DataFrame(streams)
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
