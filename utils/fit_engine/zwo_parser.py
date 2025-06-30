import xml.etree.ElementTree as ET

def parse_zwo_schedule(file_path: str):
    tree = ET.parse(file_path)
    root = tree.getroot()
    workout = root.find("workout")
    segments = []

    for elem in workout:
        duration = float(elem.attrib.get("Duration", 0))
        power_low = elem.attrib.get("PowerLow")
        power_high = elem.attrib.get("PowerHigh")
        power = elem.attrib.get("Power")
        cadence = elem.attrib.get("Cadence")

        description = elem.attrib.get("OnText", "") or elem.attrib.get("Text", "")
        note_parts = []
        if power_low and power_high:
            note_parts.append(f"@ {round(float(power_low)*100)}–{round(float(power_high)*100)}% FTP")
        elif power:
            note_parts.append(f"@ {round(float(power)*100)}% FTP")
        if cadence:
            note_parts.append(f"Cadence: {cadence} rpm")
        if description:
            note_parts.append(description)

        segment = {
            "segment_type": elem.tag,
            "duration_sec": duration,
            "powerLow": float(power_low) if power_low else None,
            "powerHigh": float(power_high) if power_high else None,
            "power": float(power) if power else None,
            "cadence": int(cadence) if cadence else None,
            "notes": ", ".join(note_parts)
        }
        segments.append(segment)

    return segments
