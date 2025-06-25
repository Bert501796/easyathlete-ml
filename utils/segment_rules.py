import numpy as np
import pandas as pd

# Default rule set by sport type
rules_by_sport = {
    "Run": {
        "warmup": {"fraction": 0.1},
        "interval": {"min_duration_sec": 30},
        "acceleration": {"delta_threshold_std": 1.0},
        "steady": {"threshold_pct": 0.1, "min_len": 30},
        "cooldown": {"fraction": 0.1},
        "recovery": {"hr": 0.85, "speed": 0.85, "watts": 0.75, "min_duration": 30, "max_duration": 900},
    },
    "VirtualRide": {
        "warmup": {"fraction": 0.1},
        "interval": {"min_duration_sec": 30},
        "acceleration": {"delta_threshold_std": 1.0},
        "steady": {"threshold_pct": 0.1, "min_len": 30},
        "cooldown": {"fraction": 0.1},
        "recovery": {"hr": 0.85, "speed": 0.85, "watts": 0.75, "min_duration": 30, "max_duration": 900},
    },
    "Swim": {}
}

def safe_series(data, name=""):
    try:
        series = pd.to_numeric(data, errors="coerce")
        if not isinstance(series, (pd.Series, np.ndarray)) or series.dropna().empty:
            print(f"⚠️ {name} is not a valid non-empty Series.")
            return None
        return series
    except Exception as e:
        print(f"❌ Failed to coerce {name}: {e}")
        return None

def detect_warmup(df, activity_type="Run"):
    rule = rules_by_sport.get(activity_type, {}).get("warmup", {})
    if "time_sec" not in df or df.shape[0] < 30:
        return []
    max_time = df["time_sec"].max()
    warmup_duration = int(rule.get("fraction", 0.1) * max_time)
    warmup_end = df[df["time_sec"] <= warmup_duration].index.max()
    if warmup_end is None or warmup_end <= 0:
        return []
    duration = df["time_sec"].iloc[warmup_end] - df["time_sec"].iloc[0]
    if duration <= 0:
        return []
    return [{"type": "warmup", "start_index": 0, "end_index": int(warmup_end), "duration_sec": int(duration)}]

def detect_intervals(df, activity_type="Run"):
    print("🔍 Running detect_intervals")
    rule = rules_by_sport.get(activity_type, {}).get("interval", {})
    min_duration = rule.get("min_duration_sec", 30)
    speed = safe_series(df.get("rolling_speed_mean"), "rolling_speed_mean")
    if speed is None:
        return []
    mean_speed = speed.mean()
    std_speed = speed.std()
    threshold = mean_speed + std_speed
    intervals = []
    in_interval = False
    start_idx = 0
    for i, val in enumerate(speed):
        if val > threshold and not in_interval:
            start_idx = i
            in_interval = True
        elif val <= threshold and in_interval:
            end_idx = i
            duration = df["time_sec"].iloc[end_idx] - df["time_sec"].iloc[start_idx]
            if duration >= min_duration:
                intervals.append({"type": "interval", "start_index": start_idx, "end_index": end_idx, "duration_sec": int(duration)})
            in_interval = False
    return intervals

def detect_acceleration_blocks(df, activity_type="Run"):
    print("🔍 Running detect_acceleration_blocks")
    rule = rules_by_sport.get(activity_type, {}).get("acceleration", {})
    delta = safe_series(df.get("delta_speed"), "delta_speed")
    if delta is None:
        return []
    threshold = delta.mean() + rule.get("delta_threshold_std", 1.0) * delta.std()
    acc_blocks = []
    for i in range(1, len(df)):
        if delta.iloc[i] > threshold:
            start_idx = max(0, i - 5)
            end_idx = min(len(df) - 1, i + 5)
            duration = df["time_sec"].iloc[end_idx] - df["time_sec"].iloc[start_idx]
            if duration > 0:
                acc_blocks.append({"type": "acceleration", "start_index": start_idx, "end_index": end_idx, "duration_sec": int(duration)})
    return acc_blocks

def detect_steady_state_blocks(df, activity_type="Run"):
    print("🔍 Running detect_steady_state_blocks")
    rule = rules_by_sport.get(activity_type, {}).get("steady", {})
    threshold = rule.get("threshold_pct", 0.1)
    min_len = rule.get("min_len", 30)
    speed = safe_series(df.get("rolling_speed_mean"), "rolling_speed_mean")
    hr = safe_series(df.get("rolling_heart_rate_mean"), "rolling_heart_rate_mean")
    if speed is None or hr is None:
        return []
    mask = (speed > speed.mean() * (1 - threshold)) & (speed < speed.mean() * (1 + threshold)) & \
           (hr > hr.mean() * (1 - threshold)) & (hr < hr.mean() * (1 + threshold))
    blocks = []
    current = []
    for i, val in enumerate(mask):
        if val:
            current.append(i)
        elif current:
            if len(current) > min_len:
                blocks.append({"type": "steady", "start_index": current[0], "end_index": current[-1],
                               "duration_sec": int(df["time_sec"].iloc[current[-1]] - df["time_sec"].iloc[current[0]])})
            current = []
    return blocks

def detect_recovery_blocks(df, known_segments=None, activity_type="Run"):
    print("🔍 Running detect_recovery_blocks")
    rule = rules_by_sport.get(activity_type, {}).get("recovery", {})
    hr = safe_series(df.get("rolling_heart_rate_mean"), "heart rate")
    speed = safe_series(df.get("rolling_speed_mean"), "speed")
    watts = safe_series(df.get("rolling_power_mean"), "power")
    thresholds = {}
    if hr is not None:
        thresholds["hr"] = hr.mean() * rule.get("hr", 0.85)
    if speed is not None:
        thresholds["speed"] = speed.mean() * rule.get("speed", 0.85)
    if watts is not None:
        thresholds["watts"] = watts.mean() * rule.get("watts", 0.75)
    def is_below(i):
        count = 0
        if hr is not None and hr.iloc[i] < thresholds["hr"]: count += 1
        if speed is not None and speed.iloc[i] < thresholds["speed"]: count += 1
        if watts is not None and watts.iloc[i] < thresholds["watts"]: count += 1
        return count >= 2
    recovery = []
    in_block = False
    start_idx = 0
    for i in range(len(df)):
        if is_below(i):
            if not in_block:
                start_idx = i
                in_block = True
        elif in_block:
            end_idx = i
            duration = df["time_sec"].iloc[end_idx] - df["time_sec"].iloc[start_idx]
            if rule.get("min_duration", 30) <= duration <= rule.get("max_duration", 900):
                recovery.append({"type": "recovery", "start_index": start_idx, "end_index": end_idx, "duration_sec": int(duration)})
            in_block = False
    return recovery

def detect_cooldown(df, activity_type="Run"):
    print("🔍 Running detect_cooldown")
    rule = rules_by_sport.get(activity_type, {}).get("cooldown", {})
    if "time_sec" not in df or df.shape[0] < 30:
        return []
    end_time = df["time_sec"].iloc[-1]
    cooldown_duration = int(rule.get("fraction", 0.1) * end_time)
    cooldown_start = df[df["time_sec"] >= (end_time - cooldown_duration)].index.min()
    if cooldown_start is None or cooldown_start >= len(df):
        return []
    duration = df["time_sec"].iloc[-1] - df["time_sec"].iloc[cooldown_start]
    if duration <= 0:
        return []
    return [{"type": "cooldown", "start_index": int(cooldown_start), "end_index": len(df) - 1, "duration_sec": int(duration)}]

def detect_swimming_blocks(df, activity_type="Swim"):
    print("🔍 Running detect_swimming_blocks")
    if "time_sec" not in df or df.shape[0] < 30:
        return []
    duration = df["time_sec"].iloc[-1] - df["time_sec"].iloc[0]
    segment = {"type": "steady_swim", "start_index": 0, "end_index": len(df) - 1, "duration_sec": int(duration)}
    seg_df = df.copy()
    for col in seg_df.columns:
        if col.startswith("delta_") or col.startswith("rolling_"): continue
        try:
            numeric_col = pd.to_numeric(seg_df[col], errors="coerce")
            if numeric_col.isna().all(): continue
            segment[f"avg_{col}"] = float(numeric_col.mean())
        except Exception as e:
            print(f"⚠️ Failed to process {col}: {e}")
    return [segment]

def is_valid_recovery_position(block, known_segments, total_len):
    preceding = {"interval", "acceleration"}
    following = {"cooldown"}
    for seg in known_segments:
        if seg["end_index"] < block["start_index"] and seg["type"] in preceding:
            return True
        if seg["start_index"] > block["end_index"] and seg["type"] in following:
            return True
    tenth = total_len // 10
    if block["start_index"] < tenth or block["end_index"] > total_len - tenth:
        return False
    return False
