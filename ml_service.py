import json
from mongo_utils import get_db_connection, fetch_activity_by_strava_id
from segment_analysis import parse_streams, detect_segments

# Load config
with open("config.json") as f:
    config = json.load(f)

# MongoDB setup
db = get_db_connection(config["mongo_uri"], config["db_name"])

# Set the stravaId you want to analyze
strava_id_to_test = config.get("test_strava_id", 14825780607)

# Fetch single activity
activity = fetch_activity_by_strava_id(db, strava_id_to_test)

# Run parsing + analysis
if activity:
    df = parse_streams(activity)
    analysis = detect_segments(df)
    result = {
        "stravaId": activity.get("stravaId"),
        "analysis": analysis
    }
    print(json.dumps(result, indent=2))
else:
    print(f"No activity found with stravaId {strava_id_to_test}")
