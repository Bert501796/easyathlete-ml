import numpy as np
import pandas as pd

def detect_warmup(df):
    if "time_sec" not in df or df.shape[0] < 30:
        return []

    max_time = df["time_sec"].max()
    warmup_duration = int(0.1 * max_time)
    warmup_end = df[df["time_sec"] <= warmup_duration].index.max()

    if warmup_end is None or warmup_end <= 0:
        return []

    duration = df["time_sec"].iloc[warmup_end] - df["time_sec"].iloc[0]
    if duration <= 0:
        return []

    return [{
        "type": "warmup",
        "start_index": 0,
        "end_index": int(warmup_end),
        "duration_sec": int(duration)
    }]

def detect_intervals(df):
    if "rolling_speed_mean" not in df:
        return []

    df["rolling_speed_mean"] = pd.to_numeric(df["rolling_speed_mean"], errors="coerce")
    mean_speed = df["rolling_speed_mean"].mean()
    std_speed = df["rolling_speed_mean"].std()
    if pd.isna(mean_speed) or pd.isna(std_speed):
        return []

    threshold = mean_speed + std_speed
    intervals = []
    in_interval = False
    start_idx = 0

    for i, val in enumerate(df["rolling_speed_mean"]):
        if val > threshold and not in_interval:
            start_idx = i
            in_interval = True
        elif val <= threshold and in_interval:
            end_idx = i
            duration = df["time_sec"].iloc[end_idx] - df["time_sec"].iloc[start_idx]
            if duration >= 30:
                intervals.append({
                    "type": "interval",
                    "start_index": start_idx,
                    "end_index": end_idx,
                    "duration_sec": int(duration)
                })
            in_interval = False

    return intervals

def detect_acceleration_blocks(df):
    if "delta_speed" not in df:
        return []

    df["delta_speed"] = pd.to_numeric(df["delta_speed"], errors="coerce")
    mean_delta = df["delta_speed"].mean()
    std_delta = df["delta_speed"].std()
    if pd.isna(mean_delta) or pd.isna(std_delta):
        return []

    acc_threshold = mean_delta + std_delta
    acc_blocks = []

    for i in range(1, len(df)):
        if df["delta_speed"].iloc[i] > acc_threshold:
            start_idx = max(0, i - 5)
            end_idx = min(len(df) - 1, i + 5)
            duration = df["time_sec"].iloc[end_idx] - df["time_sec"].iloc[start_idx]
            if duration > 0:
                acc_blocks.append({
                    "type": "acceleration",
                    "start_index": start_idx,
                    "end_index": end_idx,
                    "duration_sec": int(duration)
                })

    return acc_blocks

def detect_steady_state_blocks(df):
    if "rolling_speed_mean" not in df or "rolling_heart_rate_mean" not in df:
        return []

    try:
        rolling_speed = pd.to_numeric(df["rolling_speed_mean"], errors="coerce")
        rolling_hr = pd.to_numeric(df["rolling_heart_rate_mean"], errors="coerce")

        if rolling_speed.isna().all() or rolling_hr.isna().all():
            return []

        speed_mean = rolling_speed.mean()
        hr_mean = rolling_hr.mean()

        mask = (rolling_speed > speed_mean * 0.9) & (rolling_speed < speed_mean * 1.1) & \
               (rolling_hr > hr_mean * 0.9) & (rolling_hr < hr_mean * 1.1)

        steady_blocks = []
        current_block = []
        for i, val in enumerate(mask):
            if val:
                current_block.append(i)
            elif current_block:
                if len(current_block) > 30:
                    steady_blocks.append({
                        "type": "steady",
                        "start_index": current_block[0],
                        "end_index": current_block[-1],
                        "duration_sec": int(df["time_sec"].iloc[current_block[-1]] - df["time_sec"].iloc[current_block[0]])
                    })
                current_block = []

        return steady_blocks
    except Exception as e:
        print("⚠️ Error in detect_steady_state_blocks:", e)
        return []


def detect_recovery_blocks(df):
    if "rolling_heart_rate_mean" not in df:
        return []

    df["rolling_heart_rate_mean"] = pd.to_numeric(df["rolling_heart_rate_mean"], errors="coerce")
    mean_hr = df["rolling_heart_rate_mean"].mean()
    if pd.isna(mean_hr):
        return []

    recovery_threshold = mean_hr * 0.85
    recovery_blocks = []
    in_recovery = False
    start_idx = 0

    for i, val in enumerate(df["rolling_heart_rate_mean"]):
        if val < recovery_threshold and not in_recovery:
            start_idx = i
            in_recovery = True
        elif val >= recovery_threshold and in_recovery:
            end_idx = i
            duration = df["time_sec"].iloc[end_idx] - df["time_sec"].iloc[start_idx]
            if duration >= 30:
                recovery_blocks.append({
                    "type": "recovery",
                    "start_index": start_idx,
                    "end_index": end_idx,
                    "duration_sec": int(duration)
                })
            in_recovery = False

    return recovery_blocks

def detect_cooldown(df):
    if "time_sec" not in df or df.shape[0] < 30:
        return []

    end_time = df["time_sec"].iloc[-1]
    cooldown_duration = int(0.1 * end_time)
    cooldown_start = df[df["time_sec"] >= (end_time - cooldown_duration)].index.min()

    if cooldown_start is None or cooldown_start >= len(df):
        return []

    duration = df["time_sec"].iloc[-1] - df["time_sec"].iloc[cooldown_start]
    if duration <= 0:
        return []

    return [{
        "type": "cooldown",
        "start_index": int(cooldown_start),
        "end_index": len(df) - 1,
        "duration_sec": int(duration)
    }]

def detect_swimming_blocks(df):
    if "time_sec" not in df or df.shape[0] < 30:
        return []

    duration = df["time_sec"].iloc[-1] - df["time_sec"].iloc[0]
    segment = {
        "type": "steady_swim",
        "start_index": 0,
        "end_index": len(df) - 1,
        "duration_sec": int(duration),
    }
    for col in df.columns:
        if not col.startswith("delta_") and not col.startswith("rolling_"):
            segment[f"avg_{col}"] = float(pd.to_numeric(df[col], errors="coerce").mean())

    return [segment]
