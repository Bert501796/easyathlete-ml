def infer_segment_sequence(segments, df):
    """
    Converts overlapping raw segments into a logical, non-overlapping training sequence.
    Prioritizes the most likely 'primary' segments.
    """
    segments = sorted(segments, key=lambda s: s["start_index"])
    sequence = []
    occupied = set()
    last_primary_type = None

    for seg in segments:
        seg = seg.copy()

        # Check if overlaps any accepted primary segment
        current_range = set(range(seg["start_index"], seg["end_index"] + 1))
        overlap = not current_range.isdisjoint(occupied)

        if not overlap:
            seg["primary"] = True
            if last_primary_type:
                seg["after"] = last_primary_type
            last_primary_type = seg["type"]
            occupied.update(current_range)
        else:
            seg["primary"] = False
            # Optionally annotate nesting
            for prior in sequence:
                if prior["primary"] and prior["start_index"] <= seg["start_index"] <= prior["end_index"]:
                    seg["nested_within"] = prior["type"]
                    break

        sequence.append(seg)

    # Return only primary segments as the clean sequence
    return [s for s in sequence if s["primary"]]
