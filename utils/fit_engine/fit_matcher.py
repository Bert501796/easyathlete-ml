from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import os
from dotenv import load_dotenv

# Load .env settings
load_dotenv()
mongo_url = os.getenv("MONGO_URL")
db_name = os.getenv("DB_NAME", "test")
client = MongoClient(mongo_url)
db = client[db_name]
collection = db["stravaactivities"]


def match_fit_to_activity(fit_start_time: datetime, user_id: str, max_time_diff_min=15):
    """
    Find a matching activity in MongoDB within a time window (±15 minutes by default).
    """
    start_range = fit_start_time - timedelta(minutes=max_time_diff_min)
    end_range = fit_start_time + timedelta(minutes=max_time_diff_min)

    query = {
        "userId": user_id,
        "startDate": {
            "$gte": start_range,
            "$lte": end_range
        }
    }

    matches = list(collection.find(query).sort("startDate", 1))

    if not matches:
        print(f"❌ No match found for user {user_id} around {fit_start_time.isoformat()}")
        return None

    print(f"✅ Found {len(matches)} candidate(s) for user {user_id} at {fit_start_time.date()}")
    return matches[0]  # return best match (closest by default)


def extract_fit_start_time(fitfile):
    """
    Extract start timestamp from a FitFile object.
    """
    for msg in fitfile.get_messages("session"):
        ts = msg.get_value("start_time")
        if isinstance(ts, datetime):
            return ts
    return None


def match_fit_file_to_activity(fitfile, user_id: str):
    """
    High-level helper to go from FitFile to matched activity.
    """
    fit_start_time = extract_fit_start_time(fitfile)
    if not fit_start_time:
        print("❌ No start_time found in .fit file")
        return None

    return match_fit_to_activity(fit_start_time, user_id)
