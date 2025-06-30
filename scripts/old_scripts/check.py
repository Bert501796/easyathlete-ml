#Script used to parse an example fit file. 

# from fitparse import FitFile

# fit = FitFile("fit_data/VirtualRide/2024-08-03-09-52-58.fit")
# fit.parse()

# for record in fit.get_messages():
#     print(f"\n🔹 {record.name}")
#     for field in record:
#         print(f"  {field.name}: {field.value}")

# fitfile = FitFile("fit_data/VirtualRide/2024-08-03-09-52-58.fit")

# has_workout_steps = False
# for msg in fitfile.get_messages("workout_step"):
#     has_workout_steps = True
#     print("🔹 Workout step:", msg.get_values())

# if not has_workout_steps:
#     print("ℹ️ No structured workout steps found.")

import json
from fitparse import FitFile

fit = FitFile("fit_data/VirtualRide/2024-08-03-09-52-58.fit")
fit.parse()

output = []

for record in fit.get_messages():
    record_data = {"type": record.name, "fields": {}}
    for field in record:
        record_data["fields"][field.name] = field.value
    output.append(record_data)

with open("check.json", "w") as f:
    json.dump(output, f, indent=2, default=str)

print("✅ Output written to check.json")
