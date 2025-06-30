# athlete_zones/base_zones.py

# Default training zones per sport type (can be overridden per athlete)

default_zones = {
    "Run": [
        {
            "timestamp": "default",
            "zone_type": "classic",
            "data": {
                "Z1": {"heart_rate_bpm": [100, 135], "pace_min_per_km": [7.0, 9.5], "power_w": [125, 202], "name": "Recovery", "abbreviation": "REC"},
                "Z2": {"heart_rate_bpm": [136, 150], "pace_min_per_km": [6.0, 7.0], "power_w": [229, 278], "name": "Base", "abbreviation": "BASE"},
                "Z3": {"heart_rate_bpm": [151, 165], "pace_min_per_km": [5.0, 6.0], "power_w": [176, 228], "name": "FatMax", "abbreviation": "FATMAX"},
                "Z4": {"heart_rate_bpm": [166, 180], "pace_min_per_km": [4.0, 5.0], "power_w": [303, 318], "name": "Tempo", "abbreviation": "TEMPO"},
                "Z5": {"heart_rate_bpm": [181, 195], "pace_min_per_km": [3.5, 4.0], "power_w": [338, 352], "name": "Anaerobic Threshold", "abbreviation": "ANT"}
            }
        }
    ],
    "Ride": [
        {
            "timestamp": "default",
            "zone_type": "classic",
            "data": {
                "Z1": {"watts": [0, 140], "heart_rate_bpm": [90, 125], "speed_kph": [18, 24], "name": "Recovery", "abbreviation": "REC"},
                "Z2": {"watts": [141, 180], "heart_rate_bpm": [126, 140], "speed_kph": [25, 30], "name": "Base", "abbreviation": "BASE"},
                "Z3": {"watts": [181, 220], "heart_rate_bpm": [141, 155], "speed_kph": [31, 36], "name": "Medio", "abbreviation": "MEDIO"},
                "Z4": {"watts": [221, 250], "heart_rate_bpm": [156, 165], "speed_kph": [36, 38], "name": "FatMax", "abbreviation": "FATMAX"},
                "Z5": {"watts": [251, 290], "heart_rate_bpm": [166, 180], "speed_kph": [38, 42], "name": "Anaerobic Threshold", "abbreviation": "ANT"}
            }
        }
    ],
    "Swim": [
        {
            "timestamp": "default",
            "zone_type": "aerobic",
            "data": {
                "Z1": {"pace_sec_per_100m": [130, 150], "heart_rate_bpm": [90, 120], "name": "Aerobic Low Intensity", "abbreviation": "A1"},
                "Z2": {"pace_sec_per_100m": [110, 129], "heart_rate_bpm": [121, 140], "name": "Aerobic Maintenance", "abbreviation": "A2"},
                "Z3": {"pace_sec_per_100m": [100, 109], "heart_rate_bpm": [141, 155], "name": "Aerobic Development", "abbreviation": "A3"},
                "Z4": {"pace_sec_per_100m": [90, 99], "heart_rate_bpm": [156, 170], "name": "Anaerobic Threshold", "abbreviation": "ANT"},
                "Z5": {"pace_sec_per_100m": [75, 89], "heart_rate_bpm": [171, 185], "name": "VO2 Max", "abbreviation": "VO2"}
            }
        }
    ]
}
