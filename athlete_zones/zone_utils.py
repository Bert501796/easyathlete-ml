### zone_utils.py

import json
import os
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


def resolve_athlete_zones(user_id: str, sport: str, activity_date: str):
    """
    Returns the most recent set of zones (by timestamp) for the sport type,
    at or before the given activity date.
    """
    athlete_data = load_athlete_zones().get(user_id)
    if not athlete_data:
        return default_zones.get(sport, {})

    sport_zones = athlete_data.get("zones", {}).get(sport)
    if isinstance(sport_zones, dict):
        # Legacy format fallback
        return sport_zones

    if not isinstance(sport_zones, list):
        return default_zones.get(sport, {})

    # Convert activity_date to datetime
    activity_dt = datetime.fromisoformat(activity_date.replace("Z", ""))

    # Sort available entries by timestamp descending
    sorted_entries = sorted(
        sport_zones,
        key=lambda x: datetime.fromisoformat(x["timestamp"].replace("Z", "")),
        reverse=True
    )

    for entry in sorted_entries:
        entry_dt = datetime.fromisoformat(entry["timestamp"].replace("Z", ""))
        if entry_dt <= activity_dt:
            return entry.get("data", {})

    return default_zones.get(sport, {})


def get_zones_for_athlete(user_id: str, sport: str):
    """
    Return the most recent zones (ignores activity date), useful for display or debugging.
    """
    athlete_data = load_athlete_zones().get(user_id)
    if not athlete_data:
        return {
            "zones": default_zones.get(sport, {}),
            "last_updated": None
        }

    sport_zones = athlete_data.get("zones", {}).get(sport)
    if isinstance(sport_zones, dict):
        return {
            "zones": sport_zones,
            "last_updated": None
        }

    if not sport_zones:
        return {
            "zones": default_zones.get(sport, {}),
            "last_updated": None
        }

    latest = max(
        sport_zones,
        key=lambda x: datetime.fromisoformat(x["timestamp"].replace("Z", ""))
    )
    return {
        "zones": latest.get("data", {}),
        "last_updated": latest["timestamp"]
    }


def estimate_ftp_from_zones(user_id: str):
    """
    Estimate FTP as midpoint between Z4 upper and Z5 lower bounds for Ride zones.
    """
    athlete_data = load_athlete_zones().get(user_id)
    if not athlete_data:
        return None

    ride_zones = athlete_data.get("zones", {}).get("Ride")
    if not ride_zones:
        return None

    # Use latest ride zones
    latest = max(
        ride_zones,
        key=lambda x: datetime.fromisoformat(x["timestamp"].replace("Z", ""))
    )
    z4 = latest["data"].get("Z4", {}).get("watts", [])
    z5 = latest["data"].get("Z5", {}).get("watts", [])

    if len(z4) == 2 and len(z5) == 2:
        return round((z4[1] + z5[0]) / 2)

    return None
