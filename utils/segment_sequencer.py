SEGMENT_PRIORITY = {
    "interval": 5,
    "steady": 4,
    "recovery": 3,
    "acceleration": 2,
    "warmup": 1,
    "cooldown": 0,
}

EXPECTED_FLOW = {
    None: ["warmup", "steady"],
    "warmup": ["steady", "acceleration", "interval"],
    "steady": ["interval", "acceleration", "recovery"],
    "interval": ["recovery", "interval", "acceleration"],
    "recovery": ["interval", "steady", "cooldown"],
    "cooldown": [],
}

def infer_segment_sequence(segments, df):
    """
    Constructs a non-overlapping, logically ordered sequence of training segments.
    Uses transition rules and prioritization to reflect actual workout structure.
    """
    # Sort by start_index and prioritize by strength
    sorted_segments = sorted(
        segments,
        key=lambda s: (s["start_index"], -SEGMENT_PRIORITY.get(s["type"], 0))
    )

    sequence = []
    occupied = set()
    last_type = None

    for seg in sorted_segments:
        seg = seg.copy()
        seg_range = set(range(seg["start_index"], seg["end_index"] + 1))
        seg_type = seg["type"]

        # Skip if overlapping with previous accepted segment
        if not seg_range.isdisjoint(occupied):
            continue

        # Check logical transition
        if last_type is not None and seg_type not in EXPECTED_FLOW.get(last_type, []):
            continue

        # Accept the segment
        seg["primary"] = True
        if last_type:
            seg["after"] = last_type
        last_type = seg_type
        occupied.update(seg_range)
        sequence.append(seg)

    return sequence
