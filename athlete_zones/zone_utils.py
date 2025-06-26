### zone_utils.py

import json
import os
from datetime import datetime
from pathlib import Path

from base_zones import default_zones

ZONES_FILE = Path("athlete_zones_store.json")


def load_athlete_zones():
    if ZONES_FILE.exists():
        with open(ZONES_FILE) as f:
            return json.load(f)
    return {}


def get_zones_for_athlete(user_id: str, sport: str):
    zones_data = load_athlete_zones()
    athlete_key = f"user_{user_id}"

    if athlete_key in zones_data:
        athlete_entry = zones_data[athlete_key]
        return {
            "zones": athlete_entry["zones"].get(sport, default_zones.get(sport, {})),
            "last_updated": athlete_entry["last_updated"]
        }

    return {
        "zones": default_zones.get(sport, {}),
        "last_updated": None
    }
