import json

# Load the JSON file
with open("template_blocks_zwo.json") as f:
    data = json.load(f)

# Collect all unique (powerLow, powerHigh) tuples
power_ranges = set()

for activity in data:
    for segment in activity.get("planned_segments", []):
        low = segment.get("powerLow")
        high = segment.get("powerHigh")
        if low is not None and high is not None:
            power_ranges.add((low, high))

# Sort and print them
sorted_ranges = sorted(power_ranges)
for low, high in sorted_ranges:
    print(f"powerLow: {low:.5f}, powerHigh: {high:.5f}")
