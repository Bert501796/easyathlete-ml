from typing import List, Dict
import numpy as np


def iou_range(range_a, range_b):
    """Compute the Intersection over Union of two index ranges."""
    set_a = set(range(range_a[0], range_a[1] + 1))
    set_b = set(range(range_b[0], range_b[1] + 1))
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0


def align_planned_to_detected(planned_blocks: List[Dict], detected_blocks: List[Dict]) -> List[Dict]:
    """
    Attempts to align planned training blocks (from .fit) with actual detected segmentSequence (from Strava).
    Returns list of aligned pairs with match quality info.
    """
    results = []
    detected_used = set()

    current_index = 0  # Index into detected_blocks

    for i, planned in enumerate(planned_blocks):
        best_match = None
        best_iou = 0
        planned_duration = planned.get("duration_sec", 0)

        # Try to find best matching detected block
        for j in range(current_index, len(detected_blocks)):
            detected = detected_blocks[j]
            if j in detected_used:
                continue

            if planned["type"] != detected["type"]:
                continue

            detected_duration = detected["duration_sec"]
            duration_diff = abs(planned_duration - detected_duration)

            # Only consider segments that are not too far off in duration
            if duration_diff > 0.5 * planned_duration:
                continue

            iou = iou_range(
                (current_index, current_index + planned_duration),
                (detected["start_index"], detected["end_index"])
            )

            if iou > best_iou:
                best_match = detected
                best_iou = iou

        results.append({
            "planned_type": planned["type"],
            "planned_duration": planned.get("duration_sec"),
            "matched": bool(best_match),
            "match_iou": round(best_iou, 2) if best_match else 0.0,
            "detected_start_index": best_match.get("start_index") if best_match else None,
            "detected_duration": best_match.get("duration_sec") if best_match else None
        })

        if best_match:
            detected_used.add(detected_blocks.index(best_match))
            current_index = best_match["end_index"]

    return results


def score_segment_accuracy(alignment: List[Dict]) -> Dict:
    """
    Computes basic accuracy metrics from aligned blocks.
    """
    matched = [r for r in alignment if r["matched"]]
    unmatched = [r for r in alignment if not r["matched"]]
    avg_iou = np.mean([r["match_iou"] for r in matched]) if matched else 0

    return {
        "planned_total": len(alignment),
        "matched": len(matched),
        "unmatched": len(unmatched),
        "match_rate": round(len(matched) / len(alignment), 2) if alignment else 0,
        "avg_iou": round(avg_iou, 2)
    }
