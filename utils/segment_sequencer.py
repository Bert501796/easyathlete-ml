def infer_segment_sequence(segments, df):
    """
    Converts overlapping raw segments into a logical training sequence with annotations.
    """
    # Step 1: Sort by start time
    segments = sorted(segments, key=lambda s: s["start_index"])

    # Step 2: Init structures
    sequence = []
    last_primary_type = None

    for seg in segments:
        seg = seg.copy()  # avoid mutating original

        # Assign primary if it's not nested
        is_nested = any(
            s["start_index"] <= seg["start_index"] <= s["end_index"] and
            s["end_index"] >= seg["end_index"]
            for s in sequence if s.get("primary")
        )
        if not is_nested:
            seg["primary"] = True
            if last_primary_type:
                seg["after"] = last_primary_type
            last_primary_type = seg["type"]
        else:
            seg["primary"] = False
            # Optional: track what it's nested in
            for s in sequence:
                if s.get("primary") and s["start_index"] <= seg["start_index"] <= s["end_index"]:
                    seg["nested_within"] = s["type"]
                    break

        sequence.append(seg)

    return sequence
