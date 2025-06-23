import numpy as np

def detect_warmup(df):
    if "time_sec" not in df or df.shape[0] < 30:
        return []

    max_time = df["time_sec"].max()
    warmup_duration = int(0.1 * max_time)
    warmup_end = df[df["time_sec"] <= warmup_duration].index.max()

    return [{
        "type": "warmup",
        "start_index": 0,
        "end_index": int(warmup_end),
        "duration_sec": int(df["time_sec"].iloc[warmup_end] - df["time_sec"].iloc[0])
    }] if warmup_end and warmup_end > 0 else []


def detect_intervals(df):
    if "rolling_speed_mean" not in df:
        return []

    intervals = []
    threshold = df["rolling_speed_mean"].mean() + df["rolling_speed_mean"].std()

    in_interval = False
    start_idx = 0

    for i, val in enumerate(df["rolling_speed_mean"]):
        if val > threshold and not in_interval:
            start_idx = i
            in_interval = True
        elif val <= threshold and in_interval:
            end_idx = i
            duration = df["time_sec"].iloc[end_idx] - df["time_sec"].iloc[start_idx]
            if duration >= 30:  # only count meaningful intervals
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

    acc_blocks = []
    acc_threshold = df["delta_speed"].mean() + df["delta_speed"].std()

    for i in range(1, len(df)):
        if df["delta_speed"].iloc[i] > acc_threshold:
            acc_blocks.append({
                "type": "acceleration",
                "start_index": max(0, i - 5),
                "end_index": min(len(df) - 1, i + 5),
                "duration_sec": int(df["time_sec"].iloc[i + 5] - df["time_sec"].iloc[i - 5])
            })

    return acc_blocks


def detect_steady_state_blocks(df):
    if "rolling_speed_mean" not in df or "rolling_heart_rate_mean" not in df:
        return []

    rolling_speed = df["rolling_speed_mean"]
    rolling_hr = df["rolling_heart_rate_mean"]
    mask = (rolling_speed > rolling_speed.mean() * 0.9) & (rolling_speed < rolling_speed.mean() * 1.1) & \
           (rolling_hr > rolling_hr.mean() * 0.9) & (rolling_hr < rolling_hr.mean() * 1.1)

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


def detect_recovery_blocks(df):
    if "rolling_heart_rate_mean" not in df:
        return []

    recovery_blocks = []
    recovery_threshold = df["rolling_heart_rate_mean"].mean() * 0.85
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

    return [{
        "type": "cooldown",
        "start_index": int(cooldown_start),
        "end_index": len(df) - 1,
        "duration_sec": int(df["time_sec"].iloc[-1] - df["time_sec"].iloc[cooldown_start])
    }] if cooldown_start and cooldown_start < len(df) else []


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
            segment[f"avg_{col}"] = float(df[col].mean())

    return [segment]
