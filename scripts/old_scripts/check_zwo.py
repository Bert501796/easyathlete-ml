#script to analyze a fit file to check if ftp level is stored

import json
from fitparse import FitFile
from pathlib import Path

fit_path = Path("fit_data/VirtualRide/FIT files/2024-03-15-20-13-02.fit")

fit = FitFile(str(fit_path))
fit.parse()

ftp = None
messages = []

# Scan for user_profile or workout FTP
for record in fit.get_messages():
    msg = {"type": record.name, "fields": {}}
    for field in record:
        msg["fields"][field.name] = field.value

    # FTP is often stored in these fields
    if record.name == "user_profile":
        ftp = msg["fields"].get("ftp", ftp)

    if record.name == "workout":
        # Sometimes 'ftp' is here or implied
        ftp = msg["fields"].get("ftp", ftp)

    messages.append(msg)

# Save to JSON
output = {
    "filename": fit_path.name,
    "ftp_found": ftp is not None,
    "ftp_value": ftp,
    "message_types": sorted(set(m["type"] for m in messages))
}

with open("fit_check.json", "w") as f:
    json.dump(output, f, indent=2)

print("✅ Parsed", fit_path.name)
print("📦 FTP:", ftp if ftp else "Not found")