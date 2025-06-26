import pandas as pd
import numpy as np
from utils.segment_rules import (
    detect_warmup,
    detect_intervals,
    detect_acceleration_blocks,
    detect_recovery_blocks,
    detect_steady_state_blocks,
    detect_cooldown,
    detect_swimming_blocks,
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
            print(f"💫 Trimming all streams to min length: {min_len}")
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

    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

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
    df = df.apply(pd.to_numeric, errors="coerce")

    return df

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
    print(f"💫 Fallback stream keys found: {list(rebuilt.keys())}")
    if len(rebuilt) >= 2:
        min_len = min(len(v) for v in rebuilt.values())
        rebuilt = {k: v[:min_len] for k, v in rebuilt.items()}
        df = pd.DataFrame(rebuilt)
        print(f"✅ Fallback rebuilt stream shape: {len(df)} rows")
        return df
    else:
        print("❌ Not enough fallback data to rebuild streams.")
        return pd.DataFrame()

def merge_close_segments(segments, min_gap_sec=10):
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

def apply_rule(fn, df, activity_type, *args, **kwargs):
    try:
        return fn(df, activity_type=activity_type, *args, **kwargs)
    except Exception as e:
        print(f"❌ Error applying rule {fn.__name__} for sport {activity_type}: {e}")
        return []

def detect_segments(df, activity):
    activity_type = activity.get("type", "default")

    if activity_type == "Swim":
        return {"segments": detect_swimming_blocks(df), "summary": {"swim_mode": True}}

    segments = []
    segments += apply_rule(detect_warmup, df, activity_type)
    segments += apply_rule(detect_intervals, df, activity_type)
    segments += apply_rule(detect_acceleration_blocks, df, activity_type)
    segments += apply_rule(detect_recovery_blocks, df, activity_type)
    segments += apply_rule(detect_steady_state_blocks, df, activity_type)
    segments += apply_rule(detect_cooldown, df, activity_type)

    segments = merge_close_segments(segments, min_gap_sec=10)
    segments = [s for s in segments if s.get("duration_sec", 0) >= 30]

    segments.sort(key=lambda seg: df["time_sec"].iloc[seg["start_index"]] if "start_index" in seg else 0)

    summary = {
        "count": len(segments),
        "avg_duration_sec": int(np.mean([s["duration_sec"] for s in segments])) if segments else 0
    }

    for seg in segments:
        try:
            # Effort BEFORE
            if "start_index" in seg and seg["start_index"] > 0:
                prior_block = df.iloc[:seg["start_index"]]
                effort_before = {
                    "duration_sec": float(prior_block["time_sec"].iloc[-1]) if not prior_block.empty else 0,
                    "distance_m": float(prior_block["distance"].iloc[-1]) if "distance" in prior_block and not prior_block.empty else 0,
                    "altitude_m": float(prior_block["altitude"].iloc[-1]) if "altitude" in prior_block and not prior_block.empty else 0,
                }

                for key in ["heart_rate", "speed", "cadence", "watts"]:
                    if key in prior_block:
                        try:
                            numeric = pd.to_numeric(prior_block[key], errors="coerce")
                            if not numeric.dropna().empty:
                                effort_before[f"avg_{key}"] = float(numeric.mean())

                                seg_df = df.iloc[seg["start_index"]:seg["end_index"] + 1]
                                if key in seg_df:
                                    seg_numeric = pd.to_numeric(seg_df[key], errors="coerce")
                                    if not seg_numeric.dropna().empty:
                                        seg_avg = float(seg_numeric.mean())
                                        effort_before[f"delta_{key}"] = float(effort_before[f"avg_{key}"] - seg_avg)

                                slope_col = f"rolling_{key}_trend"
                                if slope_col in prior_block:
                                    slope_vals = pd.to_numeric(prior_block[slope_col], errors="coerce").dropna()
                                    if not slope_vals.empty:
                                        effort_before[f"{key}_slope"] = float(slope_vals.iloc[-1])
                        except Exception as sub_e:
                            print(f"⚠️ Failed metric calc for effort_before[{key}]: {repr(sub_e)}")

                if "time_sec" in df.columns and seg["start_index"] > 0:
                    t_now = df["time_sec"].iloc[seg["start_index"]]
                    t_prev = df["time_sec"].iloc[seg["start_index"] - 1]
                    effort_before["time_gap_sec"] = float(t_now - t_prev) if t_now and t_prev else 0

                seg["effort_before"] = effort_before

            # Effort AFTER
            if "end_index" in seg and seg["end_index"] < len(df) - 2:
                after_start = seg["end_index"] + 1
                end_time = df["time_sec"].iloc[after_start] + 60  # 60-second window
                after_block = df[(df["time_sec"] > df["time_sec"].iloc[after_start]) & (df["time_sec"] <= end_time)]

                effort_after = {
                    "duration_sec": float(after_block["time_sec"].iloc[-1] - after_block["time_sec"].iloc[0]) if not after_block.empty else 0,
                    "distance_m": float(after_block["distance"].iloc[-1] - after_block["distance"].iloc[0]) if "distance" in after_block and not after_block.empty else 0,
                    "altitude_m": float(after_block["altitude"].iloc[-1] - after_block["altitude"].iloc[0]) if "altitude" in after_block and not after_block.empty else 0,
                }

                for key in ["heart_rate", "speed", "cadence", "watts"]:
                    if key in after_block:
                        try:
                            numeric = pd.to_numeric(after_block[key], errors="coerce")
                            if not numeric.dropna().empty:
                                effort_after[f"avg_{key}"] = float(numeric.mean())

                                seg_df = df.iloc[seg["start_index"]:seg["end_index"] + 1]
                                if key in seg_df:
                                    seg_numeric = pd.to_numeric(seg_df[key], errors="coerce")
                                    if not seg_numeric.dropna().empty:
                                        seg_avg = float(seg_numeric.mean())
                                        effort_after[f"delta_{key}"] = float(effort_after[f"avg_{key}"] - seg_avg)

                                slope_col = f"rolling_{key}_trend"
                                if slope_col in after_block:
                                    slope_vals = pd.to_numeric(after_block[slope_col], errors="coerce").dropna()
                                    if not slope_vals.empty:
                                        effort_after[f"{key}_slope"] = float(slope_vals.iloc[0])
                        except Exception as sub_e:
                            print(f"⚠️ Failed metric calc for effort_after[{key}]: {repr(sub_e)}")

                if "time_sec" in df.columns and seg["end_index"] < len(df) - 2:
                    t_now = df["time_sec"].iloc[seg["end_index"]]
                    t_next = df["time_sec"].iloc[seg["end_index"] + 1]
                    effort_after["time_gap_sec"] = float(t_next - t_now) if t_next and t_now else 0

                seg["effort_after"] = effort_after

            # Segment metrics
            seg_df = df.iloc[seg["start_index"]:seg["end_index"] + 1]
            for col in df.columns:
                if col.startswith("delta_") or col.startswith("rolling_"):
                    continue
                if col in seg_df:
                    try:
                        numeric_col = pd.to_numeric(seg_df[col], errors="coerce")
                        if isinstance(numeric_col, (pd.Series, np.ndarray)) and not numeric_col.dropna().empty:
                            seg[f"avg_{col}"] = float(numeric_col.mean())
                    except Exception as seg_e:
                        print(f"⚠️ Failed avg calc for seg[{col}]: {repr(seg_e)}")
        except Exception as seg_outer:
            print(f"❌ Failed to process segment: {repr(seg_outer)}")

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
    return df[[col for col in df.columns if not col.startswith("delta_") and not col.startswith("rolling_")]]

def prepare_activity_for_storage(activity: dict, df: pd.DataFrame, segment_result=None) -> dict:
    trimmed = trim_stream_df(df).round(3)
    activity["stream_data_full"] = trimmed.to_dict(orient="list")
    for key in [
        "wattsStream", "heartRateStream", "cadenceStream", "altitudeStream",
        "distanceStream", "timeStream", "speedStream"
    ]:
        activity.pop(key, None)

    activity["stream_summary"] = {
        "duration_sec": (
            float(trimmed["time_sec"].iloc[-1])
            if "time_sec" in trimmed and not trimmed["time_sec"].dropna().empty
            else None
        ),
        "avg_hr": (
            float(trimmed["heart_rate"].mean())
            if "heart_rate" in trimmed and not trimmed["heart_rate"].dropna().empty
            else None
        ),
        "avg_speed": (
            float(trimmed["speed"].mean())
            if "speed" in trimmed and not trimmed["speed"].dropna().empty
            else None
        ),
        "avg_watts": (
            float(trimmed["watts"].mean())
            if "watts" in trimmed and not trimmed["watts"].dropna().empty
            else None
        ),
    }

    if segment_result:
        activity["segments"] = convert_numpy_types(segment_result.get("segments", []))
        activity["raw_segments"] = convert_numpy_types(segment_result.get("raw_segments", []))

    return activity
