import json
from collections import defaultdict
from pathlib import Path
from utils.segment_rules import rules_by_sport  # must be refactored to support this

SEGMENT_RULES_PATH = Path("utils/segment_rules.py")
ALIGNMENT_RESULTS_PATH = Path("fit_alignment_results.jsonl")


def load_alignment_results(path):
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]


def analyze_failures(results):
    failures = defaultdict(lambda: defaultdict(list))  # sport -> segment_type -> list of durations/values

    for r in results:
        sport = r.get("sport_type") or infer_sport_from_filename(r.get("file"))
        alignment = r.get("alignment", [])
        segments = r.get("matched_segments", [])

        for align in alignment:
            if not align["matched"] and align["planned_type"]:
                failures[sport][align["planned_type"]].append(align.get("planned_duration"))

    return failures


def infer_sport_from_filename(filename):
    # e.g. "fit_data/VirtualRide/2025-06-04-blah.fit" -> "VirtualRide"
    return Path(filename).parts[0] if filename else "Unknown"


def suggest_threshold_updates(failures, current_rules):
    updates = {}

    for sport, seg_types in failures.items():
        sport_rules = current_rules.get(sport, {})
        updates[sport] = {}

        for seg_type, missed_durations in seg_types.items():
            avg_missed = sum(missed_durations) / len(missed_durations)
            min_duration = max(int(avg_missed * 0.8), 20)

            existing = sport_rules.get(seg_type, {})
            old_min = existing.get("min_duration_sec", 30)

            if min_duration < old_min:
                print(f"⬇️  Suggest lowering {sport}/{seg_type} min_duration_sec from {old_min} to {min_duration}")
                updates[sport][seg_type] = {**existing, "min_duration_sec": min_duration}
            else:
                print(f"✅ {sport}/{seg_type} duration already appropriate")

    return updates


def apply_rule_updates(updates, original_path):
    backup_path = original_path.with_suffix(".bak.py")
    original_path.rename(backup_path)

    with open(original_path, "w") as f:
        f.write("rules_by_sport = \
")
        json.dump(updates, f, indent=2)
        f.write("\n")

    print(f"✅ Updated rules saved to {original_path} (backup created at {backup_path})")


if __name__ == "__main__":
    results = load_alignment_results(ALIGNMENT_RESULTS_PATH)
    failures = analyze_failures(results)
    updates = suggest_threshold_updates(failures, rules_by_sport)
    apply_rule_updates(updates, SEGMENT_RULES_PATH)
