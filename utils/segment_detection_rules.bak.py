rules_by_sport = {
    "Run": {
        "interval": {"min_duration_sec": 30, "max_duration_sec": 60, "min_hr": 140},
        "tempo": {"min_duration_sec": 90, "max_duration_sec": 600, "min_hr": 135},
        "steady": {"min_duration_sec": 300, "min_hr": 130},
        "recovery": {"min_duration_sec": 30, "max_hr": 120},
        "warmup": {"max_fraction": 0.15},
        "cooldown": {"min_duration_sec": 120, "max_hr": 130}
    },
    "VirtualRide": {
        "interval": {"min_duration_sec": 45, "max_duration_sec": 75, "min_hr": 145, "min_watts": 150},
        "tempo": {"min_duration_sec": 120, "max_duration_sec": 900, "min_hr": 140, "min_watts": 130},
        "steady": {"min_duration_sec": 300, "min_hr": 135, "min_watts": 120},
        "recovery": {"min_duration_sec": 30, "max_hr": 125, "max_watts": 80},
        "warmup": {"max_fraction": 0.1},
        "cooldown": {"min_duration_sec": 180, "max_hr": 130, "max_watts": 90}
    },
    "Ride": {
        "interval": {"min_duration_sec": 60, "max_duration_sec": 90, "min_hr": 150, "min_watts": 170},
        "tempo": {"min_duration_sec": 150, "max_duration_sec": 1000, "min_hr": 145, "min_watts": 140},
        "steady": {"min_duration_sec": 300, "min_hr": 140, "min_watts": 130},
        "recovery": {"min_duration_sec": 30, "max_hr": 130, "max_watts": 90},
        "warmup": {"max_fraction": 0.12},
        "cooldown": {"min_duration_sec": 150, "max_hr": 135, "max_watts": 100}
    },
    "Swim": {
        "interval": {"min_duration_sec": 40, "max_duration_sec": 60, "min_hr": 130},
        "tempo": {"min_duration_sec": 120, "max_duration_sec": 800, "min_hr": 125},
        "steady": {"min_duration_sec": 300, "min_hr": 120},
        "recovery": {"min_duration_sec": 30, "max_hr": 110},
        "warmup": {"max_fraction": 0.1},
        "cooldown": {"min_duration_sec": 120, "max_hr": 115}
    }
}
