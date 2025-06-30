#Script used to extract different power ranges used in zwo files. 


import json

# Adjust if needed
INPUT_FILE = "template_blocks_zwo.json"
OUTPUT_FILE = "distinct_power_ranges.json"

# Load the input file
with open(INPUT_FILE, "r") as f:
    data = json.load(f)

# Extract unique power ranges
power_ranges = set()

for activity in data:
    for segment in activity.get("planned_segments", []):
        low = segment.get("powerLow")
        high = segment.get("powerHigh")
        if low is not None and high is not None:
            power_ranges.add((round(low, 5), round(high, 5)))

# Sort and format with zone placeholder
output_data = [
    {
        "powerLow": low,
        "powerHigh": high,
        "zone": ""
    }
    for (low, high) in sorted(power_ranges)
]

# Save to JSON
with open(OUTPUT_FILE, "w") as out:
    json.dump(output_data, out, indent=2)

print(f"Saved {len(output_data)} distinct power ranges to {OUTPUT_FILE}")
