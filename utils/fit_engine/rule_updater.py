import json
import re
import shutil
from pathlib import Path
from collections import defaultdict

# Path to the segment rules file you want to update
SEGMENT_RULES_PATH = Path("utils/segment_rules.py")
BACKUP_PATH = SEGMENT_RULES_PATH.with_suffix(".bak.py")

# Path to the alignment results
ALIGNMENT_RESULTS_PATH = Path("fit_alignment_results.jsonl")


def load_alignment_data(path):
    data = []
    with open(path, "r") as f:
        for line in f:
            data.append(json.loads(line.strip()))
    return data


def analyze_alignment(data):
    type_stats = defaultdict(lambda: {"total": 0, "matched": 0, "avg_iou": 0.0})

    for item in data:
        for row in item["alignment"]:
            seg_type = row["planned_type"]
            type_stats[seg_type]["total"] += 1
            if row["matched"]:
                type_stats[seg_type]["matched"] += 1
                type_stats[seg_type]["avg_iou"] += row["match_iou"]

    for seg_type, stats in type_stats.items():
        if stats["matched"]:
            stats["avg_iou"] /= stats["matched"]
        stats["match_rate"] = round(stats["matched"] / stats["total"], 2) if stats["total"] else 0.0

    return type_stats


def update_segment_rules(segment_stats, rules_path=SEGMENT_RULES_PATH):
    print("📄 Updating segment_rules.py based on accuracy data...")

    # Backup current version
    shutil.copy(rules_path, BACKUP_PATH)
    with open(rules_path, "r") as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        modified = line

        # Example: adjust recovery threshold if match rate is poor
        if "recovery_threshold" in line:
            stat = segment_stats.get("recovery")
            if stat and stat["match_rate"] < 0.7:
                # Increase HR threshold (e.g. from 0.85 to 0.9)
                new_val = min(0.95, 0.85 + (1 - stat["match_rate"]) * 0.1)
                modified = re.sub(r"(recovery_threshold\s*=\s*)[\d.]+", f"\\1{round(new_val, 3)}", line)
                print(f"🔧 Adjusted recovery_threshold to {round(new_val, 3)}")

        # You could add similar blocks for intervals, acceleration, etc.

        new_lines.append(modified)

    # Overwrite rules file
    with open(rules_path, "w") as f:
        f.writelines(new_lines)

    print("✅ segment_rules.py updated.")
    print("🛡️ Backup saved to:", BACKUP_PATH)


def run_rule_update():
    if not ALIGNMENT_RESULTS_PATH.exists():
        print(f"❌ No alignment file found at {ALIGNMENT_RESULTS_PATH}")
        return

    data = load_alignment_data(ALIGNMENT_RESULTS_PATH)
    stats = analyze_alignment(data)

    print("\n📊 Segment detection stats:")
    for t, s in stats.items():
        print(f"  {t:12} match_rate={s['match_rate']} avg_iou={round(s['avg_iou'], 2)}")

    update_segment_rules(stats)


if __name__ == "__main__":
    run_rule_update()
