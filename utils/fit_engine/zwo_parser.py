import xml.etree.ElementTree as ET

def parse_zwo_schedule(file_path: str):
    tree = ET.parse(file_path)
    root = tree.getroot()
    workout = root.find("workout")
    segments = []

    for elem in workout:
        segment = {
            "segment_type": elem.tag,
            "duration_sec": float(elem.attrib.get("Duration", 0)),
            "powerLow": float(elem.attrib.get("PowerLow", 0)) if "PowerLow" in elem.attrib else None,
            "powerHigh": float(elem.attrib.get("PowerHigh", 0)) if "PowerHigh" in elem.attrib else None,
            "power": float(elem.attrib.get("Power", 0)) if "Power" in elem.attrib else None,
            "cadence": int(elem.attrib.get("Cadence", 0)) if "Cadence" in elem.attrib else None,
            "notes": ""
        }
        segments.append(segment)

    return segments
