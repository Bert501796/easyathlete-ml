import pandas as pd
import numpy as np
from utils.segment_rules import (
    detect_warmup,
    detect_intervals,
    detect_acceleration_blocks,
    detect_recovery_blocks,
    detect_steady_state_blocks,
    detect_cooldown,
    detect_swimming_blocks,  # ✅ imported, not redefined
)

def parse_streams(activity):
    print("🔍 parse_streams() was called")
    streams = activity.get("stream_data_full", {})

    if not isinstance(streams, dict):
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

        rebuilt = {}
        for alias, orig in fallback_keys:
            stream = activity.get(orig)
            if isinstance(stream, list) and len(stream) > 0:
                print(f"✅ Found {orig}: length {len(stream)}")
                rebuilt[alias] = stream
            else:
                print(f"⚠️ Missing or invalid {orig}")

        if len(rebuilt) >= 2:
            min_len = min(len(v) for v in rebuilt.values())
            print(f"🧪 Trimming all streams to min length: {min_len}")
            rebuilt = {k: v[:min_len] for k, v in rebuilt.items()}
            df = pd.DataFrame(rebuilt)
            print(f"✅ Rebuilt stream shape: {len(df)} rows, columns: {list(df.columns)}")
        else:
            print("❌ Not enough valid fallback streams to rebuild DataFrame.")
            return pd.DataFrame()
    else:
        df = pd.DataFrame(streams)
        if df.empty or df.shape[0] < 30:
            print("⚠️ stream_data_full was present but empty or insufficient — falling back to raw streams.")
            return parse_streams_from_raw(activity)
        print(f"✅ stream_data_full used directly: {len(df)} rows, columns: {list(df.columns)}")

    # 🔐 Ensure all data is numeric
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 🧹 Drop rows with any NaNs (optional, but safe)
    df.dropna(inplace=True)

    window = 30
    delta_cols = {}
    rolling_means = {}
    rolling_deltas = {}

    for col in df.columns:
        if col == "time_sec":
            continue

        delta = df[col].diff()
        delta_cols[f"delta_{col}"] = delta
        rolling_means[f"rolling_{col}_mean"] = df[col].rolling(window, min_periods=1).mean()
        rolling_deltas[f"rolling_{col}_trend"] = delta.rolling(window, min_periods=1).mean()

    df = pd.concat([df, pd.DataFrame(delta_cols), pd.DataFrame(rolling_means), pd.DataFrame(rolling_deltas)], axis=1)

    return df


def merge_close_segments(segments, min_gap_sec=10):
    """
    Merge segments of the same type that are very close to each other (within min_gap_sec).
    """
    merged = []
    segments = sorted(segments, key=lambda s: s["start_index"])

    for seg in segments:
        if not merged:
            merged.append(seg)
        else:
            last = merged[-1]
            if seg["type"] == last["type"] and seg["start_index"] - last["end_index"] <= min_gap_sec:
                last["end_index"] = seg["end_index"]
                last["duration_sec"] = seg["end_index"] - last["start_index"]
            else:
                merged.append(seg)

    return merged


def parse_streams_from_raw(activity):
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
    print(f"🧪 Fallback stream keys found: {list(rebuilt.keys())}")
    if len(rebuilt) >= 2:
        min_len = min(len(v) for v in rebuilt.values())
        rebuilt = {k: v[:min_len] for k, v in rebuilt.items()}
        df = pd.DataFrame(rebuilt)
        print(f"✅ Fallback rebuilt stream shape: {len(df)} rows")
        return df
    else:
        print("❌ Not enough fallback data to rebuild streams.")
        return pd.DataFrame()

def detect_segments(df, activity):
    if activity.get("type") == "Swim":
        return {"segments": detect_swimming_blocks(df), "summary": {"swim_mode": True}}

    segments = []
    segments += detect_warmup(df)
    segments += detect_intervals(df)
    segments += detect_acceleration_blocks(df)
    segments += detect_recovery_blocks(df)
    segments += detect_steady_state_blocks(df)
    segments += detect_cooldown(df)

    segments = merge_close_segments(segments, min_gap_sec=10)
    segments = [s for s in segments if s.get("duration_sec", 0) >= 30]


    segments.sort(key=lambda seg: df["time_sec"].iloc[seg["start_index"]] if "start_index" in seg else 0)

    summary = {
        "count": len(segments),
        "avg_duration_sec": int(np.mean([s["duration_sec"] for s in segments])) if segments else 0
    }

    for seg in segments:
        if "start_index" in seg and seg["start_index"] > 0:
            prior_block = df.iloc[:seg["start_index"]]
            seg["effort_before"] = {
                "duration_sec": float(prior_block["time_sec"].iloc[-1]) if not prior_block.empty else 0,
                "distance_m": float(prior_block["distance"].iloc[-1]) if "distance" in prior_block and not prior_block.empty else 0,
                "altitude_m": float(prior_block["altitude"].iloc[-1]) if "altitude" in prior_block and not prior_block.empty else 0,
                "avg_hr": float(prior_block["heart_rate"].mean()) if "heart_rate" in prior_block else None,
                "avg_speed": float(prior_block["speed"].mean()) if "speed" in prior_block else None,
                "avg_cadence": float(prior_block["cadence"].mean()) if "cadence" in prior_block else None,
                "avg_watts": float(prior_block["watts"].mean()) if "watts" in prior_block else None
            }

        seg_df = df.iloc[seg["start_index"]:seg["end_index"] + 1]
        for col in df.columns:
            if not col.startswith("delta_") and not col.startswith("rolling_") and col in seg_df:
                seg[f"avg_{col}"] = float(seg_df[col].mean())

    return {"segments": segments, "summary": summary}
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

def generate_ml_windows(df, segments):
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
    
def trim_stream_df(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only raw streams (e.g., no delta_ or rolling_ columns)"""
    return df[[col for col in df.columns if not col.startswith("delta_") and not col.startswith("rolling_")]]


def prepare_activity_for_storage(activity: dict, df: pd.DataFrame) -> dict:
    """
    Prepare the enriched activity for MongoDB storage.
    Drops legacy fields and stores only necessary raw stream data.
    """
    trimmed = trim_stream_df(df).round(3)

    # Store only raw stream data
    activity["stream_data_full"] = trimmed.to_dict(orient="list")

    # Remove raw legacy streams
    for key in [
        "wattsStream", "heartRateStream", "cadenceStream", "altitudeStream",
        "distanceStream", "timeStream", "speedStream"
    ]:
        activity.pop(key, None)

    # Optional: add lightweight summary
    activity["stream_summary"] = {
        "duration_sec": float(trimmed["time_sec"].iloc[-1]) if "time_sec" in trimmed else None,
        "avg_hr": float(trimmed["heart_rate"].mean()) if "heart_rate" in trimmed else None,
        "avg_speed": float(trimmed["speed"].mean()) if "speed" in trimmed else None,
        "avg_watts": float(trimmed["watts"].mean()) if "watts" in trimmed else None,
    }

    return activity

