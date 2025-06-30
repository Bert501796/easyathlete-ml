#script to match watts for an athlete into planned segments. 


import json
from datetime import datetime

# Load templates
with open("training_templates_zones/bike_training_templates_zones.json") as f:
    templates = json.load(f)

# Load athlete zones
with open("athlete_zones/athlete_zones_store.json") as f:
    zones_store = json.load(f)

# Default user and sport
DEFAULT_USER_ID = "68527a64ebe020183134aab4"
SPORT = "Ride"  # or "VirtualRide"

# Resolve the most recent zone block active at the activity date
def resolve_zone_block_by_date(user_id, sport, zones_store, activity_date: str):
    user_zones = next((z for z in zones_store if z["user_id"] == user_id), None)
    if not user_zones:
        return None

    sport_zones = user_zones.get("zones", {}).get(sport)
    if not sport_zones:
        return None

    activity_dt = datetime.fromisoformat(activity_date).replace(tzinfo=None)

    # Filter valid zone blocks by date and presence of watts
    valid_blocks = [
        z for z in sport_zones
        if datetime.fromisoformat(z["timestamp"]).replace(tzinfo=None) <= activity_dt and
           any("watts" in zd and zd["watts"] for zd in z["data"].values())
    ]

    if not valid_blocks:
        return None

    # Sort descending and return the most recent
    valid_blocks.sort(key=lambda z: z["timestamp"], reverse=True)
    return valid_blocks[0]["data"]

# Apply watts to all templates
updated_count = 0
activity_updated = 0

for tpl in templates:
    activity_date = tpl.get("start_date_local")
    if not activity_date:
        print(f"⚠️ Missing start_date_local for stravaId={tpl['stravaId']}")
        continue

    zone_data = resolve_zone_block_by_date(DEFAULT_USER_ID, SPORT, zones_store, activity_date)
    if not zone_data:
        print(f"⚠️ No valid zone data found for stravaId={tpl['stravaId']} on {activity_date}")
        continue

    updated_this_activity = 0
    for segment in tpl["planned_segments"]:
        zone_name = segment.get("zone")
        if not zone_name:
            continue

        # Match by abbreviation or key
        matched_zone = None
        for key, info in zone_data.items():
            if info.get("abbreviation") == zone_name or key == zone_name:
                matched_zone = info
                break

        if matched_zone:
            watts_range = matched_zone.get("watts") or matched_zone.get("power_w")
            if watts_range:
                segment["watts"] = watts_range
                updated_count += 1
                updated_this_activity += 1
        else:
            print(f"❌ No zone match for '{zone_name}' in stravaId={tpl['stravaId']}")

    if updated_this_activity > 0:
        activity_updated += 1
        print(f"✅ Updated {updated_this_activity} segments in {tpl['activity_name']} ({tpl['stravaId']})")

# Save result
with open("training_templates_zones/bike_training_templates_zones_enriched.json", "w") as f:
    json.dump(templates, f, indent=2)

print(f"\n✅ Updated watts for {updated_count} segments.")
print("💾 Saved to bike_training_templates_zones_enriched.json")
