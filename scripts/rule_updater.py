import json
from collections import defaultdict
from pathlib import Path
import statistics
from datetime import datetime

from utils.segment_detection_rules import rules_by_sport

SEGMENT_RULES_PATH = Path("utils/segment_detection_rules.py")
ALIGNMENT_RESULTS_PATH = Path("fit_alignment_results.jsonl")

def load_alignment_results(path):
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]

def analyze_failures_and_unmatched(results):
    """
    Returns:
    {
        sport_type: {
            segment_type: [list of missed + unmatched segments]
        }
    }
    """
    failures = defaultdict(lambda: defaultdict(list))

    for r in results:
        sport = r.get("sport_type") or infer_sport_from_filename(r.get("file"))
        alignment = r.get("alignment", [])
        segments = r.get("raw_segments", [])

        # Collect missed segments: planned but unmatched
        for align in alignment:
            if not align.get("matched") and align.get("planned_type"):
                matching = [s for s in segments if s.get("type") == align["planned_type"]]
                failures[sport][align["planned_type"]].extend(matching)

        # Collect unmatched raw segments (not planned at all)
        planned_types = set(a.get("planned_type") for a in alignment if a.get("planned_type"))
        matched_detected_ids = set(a.get("detected_id") for a in alignment if a.get("matched") and a.get("detected_id"))
        for seg in segments:
            seg_type = seg.get("type")
            seg_id = seg.get("id") or id(seg)
            if seg_type and seg_type not in planned_types and seg_id not in matched_detected_ids:
                failures[sport][seg_type].append(seg)

    return failures

def infer_sport_from_filename(filename):
    try:
        path = Path(filename)
        if "fit_data" in path.parts:
            idx = path.parts.index("fit_data")
            return path.parts[idx + 1]  # fit_data/<sport>/...
        return path.parent.name
    except Exception:
        return "Unknown"

def extract_stat_bounds(values):
    if not values:
        return None, None
    avg = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0
    return max(0, avg - std), avg + std

def suggest_threshold_updates(failures):
    updates = {}

    for sport, seg_types in failures.items():
        if sport not in updates:
            updates[sport] = {}

        for seg_type, segments in seg_types.items():
            new_rule = {}

            def collect(metric):
                return [s.get(metric) for s in segments if s.get(metric) is not None]

            durations = collect("duration_sec")
            hr = collect("avg_heart_rate")
            watts = collect("avg_watts")
            cadence = collect("avg_cadence")
            speed = collect("avg_speed")

            delta_hr = [abs(s.get("avg_heart_rate") - s.get("effort_before", {}).get("avg_heart_rate", 0))
                        for s in segments if s.get("avg_heart_rate") and s.get("effort_before", {}).get("avg_heart_rate")]

            delta_speed = [abs(s.get("avg_speed") - s.get("effort_before", {}).get("avg_speed", 0))
                           for s in segments if s.get("avg_speed") and s.get("effort_before", {}).get("avg_speed")]

            if durations:
                min_dur, max_dur = extract_stat_bounds(durations)
                new_rule["min_duration_sec"] = int(min_dur or 0)
                new_rule["max_duration_sec"] = int(max_dur or 0)
                print(f"🕒 {sport}/{seg_type}: duration {int(min_dur)}–{int(max_dur)}")

            if hr:
                hr_min, hr_max = extract_stat_bounds(hr)
                new_rule["hr_range"] = [int(hr_min), int(hr_max)]
                print(f"📉 {sport}/{seg_type}: HR range = {int(hr_min)}–{int(hr_max)}")

            if watts:
                w_min, w_max = extract_stat_bounds(watts)
                new_rule["watts_range"] = [int(w_min), int(w_max)]
                print(f"⚡ {sport}/{seg_type}: watts range = {int(w_min)}–{int(w_max)}")

            if cadence:
                c_min, c_max = extract_stat_bounds(cadence)
                new_rule["cadence_range"] = [int(c_min), int(c_max)]
                print(f"⚙️ {sport}/{seg_type}: cadence range = {int(c_min)}–{int(c_max)}")

            if speed:
                s_min, s_max = extract_stat_bounds(speed)
                new_rule["speed_range"] = [round(s_min, 2), round(s_max, 2)]
                print(f"🚴 {sport}/{seg_type}: speed range = {round(s_min, 2)}–{round(s_max, 2)}")

            if delta_hr:
                _, max_delta_hr = extract_stat_bounds(delta_hr)
                new_rule["max_delta_hr"] = int(max_delta_hr)
                print(f"📉 {sport}/{seg_type}: max_delta_hr = {int(max_delta_hr)}")

            if delta_speed:
                _, max_delta_speed = extract_stat_bounds(delta_speed)
                new_rule["max_delta_speed"] = int(max_delta_speed)
                print(f"💨 {sport}/{seg_type}: max_delta_speed = {int(max_delta_speed)}")

            updates[sport][seg_type] = new_rule

    return updates

def calculate_summary_score(results):
    total = 0
    matched = 0
    for r in results:
        for align in r.get("alignment", []):
            total += 1
            if align.get("matched"):
                matched += 1
    return {
        "last_updated": datetime.utcnow().isoformat(),
        "score": {
            "total_planned": total,
            "matched": matched,
            "match_rate": round(matched / total, 4) if total else 0
        }
    }

def apply_rule_updates(updates, original_path, score_metadata):
    backup_path = original_path.with_suffix(".bak.py")
    if not backup_path.exists():
        original_path.rename(backup_path)

    with open(original_path, "w") as f:
        f.write("metadata = ")
        json.dump(score_metadata, f, indent=2)
        f.write("\n\nrules_by_sport = ")
        json.dump(updates, f, indent=2)
        f.write("\n")

    print(f"✅ Updated rules and score saved to {original_path} (backup created at {backup_path})")

if __name__ == "__main__":
    results = load_alignment_results(ALIGNMENT_RESULTS_PATH)
    failures = analyze_failures_and_unmatched(results)
    updates = suggest_threshold_updates(failures)
    score_metadata = calculate_summary_score(results)
    apply_rule_updates(updates, SEGMENT_RULES_PATH, score_metadata)
