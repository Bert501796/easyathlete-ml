import os
from dotenv import load_dotenv
from mongo_utils import get_db_connection, fetch_activity_by_strava_id
from segment_analysis import parse_streams, detect_segments

# ✅ Load environment variables
load_dotenv()

# ✅ Use .env values
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME")

if not MONGO_URL or not MONGO_URL.startswith("mongodb"):
    raise RuntimeError(f"❌ Invalid MONGO_URL: {MONGO_URL}")

# ✅ Connect to DB
db = get_db_connection(MONGO_URL, DB_NAME)

def run_analysis(strava_id):
    print(f"🔍 Starting analysis for stravaId: {strava_id}")
    activity = fetch_activity_by_strava_id(db, strava_id)

    if not activity:
        print("⚠️ Activity not found in DB.")
        return {"error": f"No activity found with stravaId {strava_id}"}

    try:
        df = parse_streams(activity)
        print("✅ Streams parsed.")
        analysis = detect_segments(df)
        print("✅ Segments detected.")

        return {
            "stravaId": strava_id,
            "analysis": analysis
        }

    except Exception as e:
        return {"error": str(e)}
