from fitparse import FitFile

fit = FitFile("fit_data/2025-06-04-cspeed-play8.fit")
fit.parse()

for record in fit.get_messages():
    print(f"\n🔹 {record.name}")
    for field in record:
        print(f"  {field.name}: {field.value}")
