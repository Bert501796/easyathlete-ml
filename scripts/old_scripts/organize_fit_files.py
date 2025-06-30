#Script used to sort different fit files in different folders

import os
from pathlib import Path
from fitparse import FitFile
import shutil

RAW_DIR = Path("fit_data/raw_fit")

def detect_activity_type(fitfile):
    """Try to detect the sport/activity type from the .fit file."""
    for record in fitfile.get_messages("sport"):
        sport = record.get_value("sport")
        if sport:
            return sport.capitalize()
    for session in fitfile.get_messages("session"):
        sport = session.get_value("sport")
        if sport:
            return sport.capitalize()
    return "Unknown"

def organize_fit_files():
    if not RAW_DIR.exists():
        print(f"❌ Source folder {RAW_DIR} does not exist.")
        return

    fit_files = [f for f in RAW_DIR.glob("*.fit")]
    if not fit_files:
        print("📂 No .fit files found directly in raw_fit.")
        return

    for fit_path in fit_files:
        try:
            with open(fit_path, "rb") as f:
                fitfile = FitFile(f)
                activity_type = detect_activity_type(fitfile)

            dest_folder = RAW_DIR / activity_type
            dest_folder.mkdir(parents=True, exist_ok=True)

            new_path = dest_folder / fit_path.name
            shutil.move(str(fit_path), str(new_path))
            print(f"✅ Moved {fit_path.name} → raw_fit/{activity_type}/")

        except Exception as e:
            print(f"❌ Failed to process {fit_path.name}: {e}")

if __name__ == "__main__":
    organize_fit_files()
