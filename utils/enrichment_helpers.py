import pandas as pd
import numpy as np

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
            if isinstance(stream, list):
                print(f"✅ Found {orig}: length {len(stream)}")
                if len(stream) > 0:
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


    # Continue processing
    for col in df.columns:
        if col != "time_sec":
            df[f"delta_{col}"] = df[col].diff()

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


def detect_warmup(df):
    if "time_sec" not in df or df.shape[0] < 60:
        return []

    warmup_end_idx = None
    threshold_duration = 60

    for idx in range(threshold_duration, len(df)):
        segment = df.iloc[:idx]

        avg_hr = segment["heart_rate"].mean() if "heart_rate" in segment else 0
        avg_watts = segment["watts"].mean() if "watts" in segment else 0
        avg_speed = segment["speed"].mean() if "speed" in segment else 0
        max_time = segment["time_sec"].max()

        if max_time > 600:
            break

        if "rolling_heart_rate_mean" in segment and segment["rolling_heart_rate_mean"].iloc[-1] < 0.8 * df["heart_rate"].max():
            warmup_end_idx = idx
        elif "rolling_speed_mean" in segment and segment["rolling_speed_mean"].iloc[-1] < 0.7 * df["speed"].max():
            warmup_end_idx = idx

    if warmup_end_idx:
        return [{
            "type": "warmup",
            "start_index": 0,
            "end_index": warmup_end_idx,
            "duration_sec": int(df["time_sec"].iloc[warmup_end_idx] - df["time_sec"].iloc[0]),
            "avg_hr": float(df["heart_rate"].iloc[:warmup_end_idx].mean()) if "heart_rate" in df else None,
            "avg_watts": float(df["watts"].iloc[:warmup_end_idx].mean()) if "watts" in df else None,
            "avg_speed": float(df["speed"].iloc[:warmup_end_idx].mean()) if "speed" in df else None
        }]
    return []

def detect_intervals(df):
    if "rolling_watts_trend" not in df or df.shape[0] < 100:
        return []

    segments = []
    in_interval = False
    start = 0
    high = df["rolling_watts_trend"].quantile(0.85)
    low = df["rolling_watts_trend"].quantile(0.15)

    for i in range(1, len(df)):
        watts = df["rolling_watts_trend"].iloc[i]

        if watts > high and not in_interval:
            start = i
            in_interval = True
        elif watts < low and in_interval:
            end = i
            duration = df["time_sec"].iloc[end] - df["time_sec"].iloc[start]
            if 20 <= duration <= 120:
                segments.append({
                    "type": "interval",
                    "start_index": start,
                    "end_index": end,
                    "duration_sec": int(duration),
                    "avg_watts": float(df["watts"].iloc[start:end].mean()),
                    "avg_hr": float(df["heart_rate"].iloc[start:end].mean()) if "heart_rate" in df else None
                })
            in_interval = False

    return segments

def detect_acceleration_blocks(df):
    if "rolling_speed_mean" not in df or df.shape[0] < 100:
        return []

    segments = []
    block_start = None
    speed_threshold = df["rolling_speed_mean"].quantile(0.7)

    for i in range(1, len(df)):
        is_accel = df["rolling_speed_mean"].iloc[i] > speed_threshold

        if is_accel and block_start is None:
            block_start = i
        elif not is_accel and block_start is not None:
            duration = df["time_sec"].iloc[i] - df["time_sec"].iloc[block_start]
            if duration > 120:
                segments.append({
                    "type": "acceleration",
                    "start_index": block_start,
                    "end_index": i,
                    "duration_sec": int(duration),
                    "avg_speed": float(df["speed"].iloc[block_start:i].mean()),
                    "avg_hr": float(df["heart_rate"].iloc[block_start:i].mean()) if "heart_rate" in df else None
                })
            block_start = None

    return segments

def detect_recovery_blocks(df):
    if "rolling_heart_rate_mean" not in df or df.shape[0] < 100:
        return []

    segments = []
    block_start = None
    hr_trend = df["rolling_heart_rate_mean"]
    threshold = hr_trend.quantile(0.3)

    for i in range(1, len(df)):
        is_recovery = hr_trend.iloc[i] < threshold

        if is_recovery and block_start is None:
            block_start = i
        elif not is_recovery and block_start is not None:
            duration = df["time_sec"].iloc[i] - df["time_sec"].iloc[block_start]
            if duration >= 30:
                segments.append({
                    "type": "recovery",
                    "start_index": block_start,
                    "end_index": i,
                    "duration_sec": int(duration),
                    "avg_hr": float(df["heart_rate"].iloc[block_start:i].mean()) if "heart_rate" in df else None,
                    "avg_speed": float(df["speed"].iloc[block_start:i].mean()) if "speed" in df else None
                })
            block_start = None

    return segments

def detect_steady_state_blocks(df):
    if "rolling_speed_mean" not in df or df.shape[0] < 100:
        return []

    segments = []
    window = 30
    min_duration = 180
    tolerance = 0.05
    block_start = 0

    for i in range(window, len(df)):
        current_block = df.iloc[block_start:i]
        speed_std = current_block["speed"].std() if "speed" in current_block else 0
        hr_std = current_block["heart_rate"].std() if "heart_rate" in current_block else 0

        if speed_std > tolerance * current_block["speed"].mean():
            if (df["time_sec"].iloc[i] - df["time_sec"].iloc[block_start]) >= min_duration:
                segments.append({
                    "type": "steady",
                    "start_index": block_start,
                    "end_index": i,
                    "duration_sec": int(df["time_sec"].iloc[i] - df["time_sec"].iloc[block_start]),
                    "avg_speed": float(current_block["speed"].mean()),
                    "avg_hr": float(current_block["heart_rate"].mean()) if "heart_rate" in df else None
                })
            block_start = i

    return segments

def detect_cooldown(df):
    if "time_sec" not in df or df.shape[0] < 60:
        return []

    duration = df["time_sec"].iloc[-1] - df["time_sec"].iloc[0]
    start_idx = int(len(df) * 0.85)
    end_idx = len(df) - 1
    segment = df.iloc[start_idx:]

    return [{
        "type": "cooldown",
        "start_index": start_idx,
        "end_index": end_idx,
        "duration_sec": int(segment["time_sec"].iloc[-1] - segment["time_sec"].iloc[0]),
        "avg_hr": float(segment["heart_rate"].mean()) if "heart_rate" in segment else None,
        "avg_speed": float(segment["speed"].mean()) if "speed" in segment else None,
        "avg_watts": float(segment["watts"].mean()) if "watts" in segment else None
    }]

def detect_segments(df, activity):
    segments = []
    segments += detect_warmup(df)
    segments += detect_intervals(df)
    segments += detect_acceleration_blocks(df)
    segments += detect_recovery_blocks(df)
    segments += detect_steady_state_blocks(df)
    segments += detect_cooldown(df)

    # ✅ Sort segments chronologically by time_sec of start_index
    segments.sort(key=lambda seg: df["time_sec"].iloc[seg["start_index"]] if "start_index" in seg else 0)

    summary = {
        "count": len(segments),
        "avg_duration_sec": int(np.mean([s["duration_sec"] for s in segments])) if segments else 0
    }
    # Add effort context to each segment
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
