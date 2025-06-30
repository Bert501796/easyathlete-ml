import json
from datetime import datetime
from pathlib import Path

from athlete_zones.base_zones import default_zones

ZONES_FILE = Path("athlete_zones_store.json")

def load_athlete_zones():
    if ZONES_FILE.exists():
        with open(ZONES_FILE) as f:
            data = json.load(f)
            return {entry["user_id"]: entry for entry in data}
    return {}

def resolve_athlete_zones(user_id: str, sport: str, activity_date: str, zone_type: str = None):
    athlete_data = load_athlete_zones().get(user_id)
    default_entry = default_zones.get(sport, [{}])[0]
    default = default_entry.get("data", {})

    if not athlete_data:
        return {"zones": default, "ftp": None}

    sport_zones = athlete_data.get("zones", {}).get(sport)
    if not isinstance(sport_zones, list):
        return {"zones": default, "ftp": None}

    activity_dt = datetime.fromisoformat(activity_date.replace("Z", ""))

    filtered = [
        z for z in sport_zones
        if (not zone_type or z.get("zone_type") == zone_type)
    ]

    sorted_entries = sorted(
        filtered,
        key=lambda x: datetime.fromisoformat(x["timestamp"].replace("Z", "")),
        reverse=True
    )

    for entry in sorted_entries:
        if datetime.fromisoformat(entry["timestamp"].replace("Z", "")) <= activity_dt:
            return {
                "zones": entry.get("data", {}),
                "ftp": entry.get("ftp")
            }

    return {"zones": default, "ftp": None}


def get_zones_for_athlete(user_id: str, sport: str, zone_type: str = None):
    """
    Returns the latest zones for a given sport and optional zone_type.
    """
    athlete_data = load_athlete_zones().get(user_id)
    default = default_zones.get(sport, [{}])[0]
    if not athlete_data:
        return {"zones": default["data"], "last_updated": default.get("timestamp")}

    sport_zones = athlete_data.get("zones", {}).get(sport)
    if not isinstance(sport_zones, list):
        return {"zones": default["data"], "last_updated": default.get("timestamp")}

    if zone_type:
        sport_zones = [z for z in sport_zones if z.get("zone_type") == zone_type]

    if not sport_zones:
        return {"zones": default["data"], "last_updated": default.get("timestamp")}

    latest = max(
        sport_zones,
        key=lambda x: datetime.fromisoformat(x["timestamp"].replace("Z", ""))
    )
    return {
        "zones": latest.get("data", {}),
        "last_updated": latest.get("timestamp")
    }

def estimate_ftp_from_zones(user_id: str, activity_date: str = None, zone_type: str = "classic"):
    """
    Estimate FTP using resolved zones at a given activity_date and optional zone_type.
    """
    zones = resolve_athlete_zones(user_id, "Ride", activity_date, zone_type) if activity_date else \
            get_zones_for_athlete(user_id, "Ride", zone_type).get("zones", {})

    z4 = zones.get("Z4", {}).get("watts", [])
    z5 = zones.get("Z5", {}).get("watts", [])

    if len(z4) == 2 and len(z5) == 2:
        return round((z4[1] + z5[0]) / 2)

    return None
