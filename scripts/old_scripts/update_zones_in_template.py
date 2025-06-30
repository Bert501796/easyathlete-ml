#script used to update disntinct power ranges originating from template_block_zwo and generating a file with teplate_blocks_zwo_with_zones.

import json

TEMPLATE_FILE = "template_blocks_zwo.json"
ZONES_FILE = "distinct_power_ranges.json"
OUTPUT_FILE = "template_blocks_zwo_with_zones.json"

# Load zone definitions
with open(ZONES_FILE, "r") as f:
    zone_defs = json.load(f)

# Create lookup dictionary for numeric ranges
zone_lookup = {}
null_zone = None

for z in zone_defs:
    plow = z["powerLow"]
    phigh = z["powerHigh"]
    zone_name = z["zone"]

    if plow is None and phigh is None:
        null_zone = zone_name
    elif plow is not None and phigh is not None:
        zone_lookup[(round(plow, 5), round(phigh, 5))] = zone_name

# Load the template
with open(TEMPLATE_FILE, "r") as f:
    template = json.load(f)

# Update zones
updated_count = 0

for activity in template:
    for segment in activity.get("planned_segments", []):
        power_low = segment.get("powerLow")
        power_high = segment.get("powerHigh")

        if power_low is None and power_high is None and null_zone:
            segment["zone"] = null_zone
            updated_count += 1
        elif power_low is not None and power_high is not None:
            key = (round(power_low, 5), round(power_high, 5))
            if key in zone_lookup:
                segment["zone"] = zone_lookup[key]
                updated_count += 1

# Save updated output
with open(OUTPUT_FILE, "w") as f:
    json.dump(template, f, indent=2)

print(f"✅ Updated {updated_count} segments with zone info.")
print(f"💾 Saved to: {OUTPUT_FILE}")
