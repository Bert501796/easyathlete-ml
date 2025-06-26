from fitparse import FitFile

fit = FitFile("fit_data/Run/2025-06-18-rfoundation3.fit")
fit.parse()

for record in fit.get_messages():
    print(f"\n🔹 {record.name}")
    for field in record:
        print(f"  {field.name}: {field.value}")

# fitfile = FitFile("fit_data/raw_fit/2025-05-28-08-22-08.fit")

# has_workout_steps = False
# for msg in fitfile.get_messages("workout_step"):
#     has_workout_steps = True
#     print("🔹 Workout step:", msg.get_values())

# if not has_workout_steps:
#     print("ℹ️ No structured workout steps found.")
