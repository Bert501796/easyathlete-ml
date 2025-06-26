from fitparse import FitFile

def parse_fit_schedule(fit_path):
    """
    Parses a .fit file and extracts scheduled workout blocks.
    Returns a list like:
    [
        {"type": "warmup", "duration_sec": 600},
        {"type": "sprint_interval", "duration_sec": 30, "repeat": 4, "recovery_sec": 90},
        {"type": "cooldown", "duration_sec": 300}
    ]
    """
    fitfile = FitFile(fit_path)
    steps = list(fitfile.get_messages("workout_step"))
    blocks = []

    warmup_count = 0
    i = 0
    while i < len(steps):
        msg = steps[i]
        intensity = msg.get_value("intensity")
        duration_type = msg.get_value("duration_type")
        duration_value = msg.get_value("duration_value")

        # Skip unsupported steps
        if duration_type != "time" or duration_value is None:
            i += 1
            continue

        duration_sec = int(duration_value)
        block_type = map_detailed_intensity_to_type(intensity, duration_sec)

        # Convert second warmup into cooldown if misclassified
        if block_type == "warmup":
            warmup_count += 1
            if warmup_count > 1:
                block_type = "cooldown"

        repeat_count = msg.get_value("repeat_count")
        if repeat_count and repeat_count > 1:
            recovery_step = steps[i + 1] if i + 1 < len(steps) else None
            recovery_duration = None
            if recovery_step:
                rec_dur_type = recovery_step.get_value("duration_type")
                rec_dur_value = recovery_step.get_value("duration_value")
                rec_intensity = recovery_step.get_value("intensity")
                if rec_dur_type == "time" and rec_intensity == "rest":
                    recovery_duration = int(rec_dur_value)

            blocks.append({
                "type": block_type,
                "duration_sec": duration_sec,
                "repeat": repeat_count,
                "recovery_sec": recovery_duration
            })
            i += 2  # Skip recovery step
        else:
            blocks.append({
                "type": block_type,
                "duration_sec": duration_sec
            })
            i += 1

    return blocks


def map_detailed_intensity_to_type(intensity, duration_sec):
    """
    Maps intensity + duration to a specific block type.
    """
    if intensity == "warmup":
        return "warmup"
    elif intensity == "cooldown":
        return "cooldown"
    elif intensity == "rest":
        return "recovery"
    elif intensity == "active":
        if duration_sec < 40:
            return "sprint_interval"
        elif duration_sec < 180:
            return "tempo_interval"
        elif duration_sec < 600:
            return "steady"
        elif duration_sec < 1200:
            return "tempo_run"
        else:
            return "long_effort"
    return "unknown"
