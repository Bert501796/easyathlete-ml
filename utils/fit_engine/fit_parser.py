from fitparse import FitFile

def parse_fit_schedule(fit_path):
    """
    Parses a .fit file and extracts scheduled workout blocks.
    Returns a list like:
    [
        {"type": "warmup", "duration_sec": 600},
        {"type": "interval", "duration_sec": 180, "repeat": 4, "recovery_sec": 120},
        {"type": "cooldown", "duration_sec": 300}
    ]
    """
    fitfile = FitFile(fit_path)
    steps = list(fitfile.get_messages("workout_step"))
    blocks = []

    i = 0
    while i < len(steps):
        msg = steps[i]
        intensity = msg.get_value("intensity")
        duration_type = msg.get_value("duration_type")
        duration_value = msg.get_value("duration_value")

        # Skip unsupported durations
        if duration_type != "time" or duration_value is None:
            i += 1
            continue

        duration_sec = int(duration_value)
        block_type = map_intensity_to_type(intensity)

        # Check for repeat blocks
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
            i += 2  # skip the recovery step too
        else:
            blocks.append({
                "type": block_type,
                "duration_sec": duration_sec
            })
            i += 1

    return blocks

def map_intensity_to_type(intensity):
    """
    Maps Garmin/Coachbox intensity label to EasyAthlete block type
    """
    if intensity == "warmup":
        return "warmup"
    elif intensity == "cooldown":
        return "cooldown"
    elif intensity == "rest":
        return "recovery"
    elif intensity == "active":
        return "interval"
    return "unknown"
