import json
from mongo_utils import get_db_connection, fetch_activity_by_strava_id
from segment_analysis import parse_streams, detect_segments

# Load config
with open("config.json") as f:
    config = json.load(f)

# Use the correct key name
db = get_db_connection(config["mongo_url"], config["db_name"])

def run_analysis(strava_id):
    """
    Analyze a specific activity by Strava ID.
    """
    activity = fetch_activity_by_strava_id(db, strava_id)

    if not activity:
        return {"error": f"No activity found with stravaId {strava_id}"}

    try:
        df = parse_streams(activity)
        analysis = detect_segments(df)

        result = {
            "stravaId": strava_id,
            "analysis": analysis
        }
        return result

    except Exception as e:
        return {"error": str(e)}
