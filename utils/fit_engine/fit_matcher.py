from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import os
from dotenv import load_dotenv
import re

# Load .env settings
load_dotenv()
mongo_url = os.getenv("MONGO_URL")
db_name = os.getenv("DB_NAME", "test")
client = MongoClient(mongo_url)
db = client[db_name]
collection = db["stravaactivities"]

def match_fit_to_activity(fit_start_time: datetime, user_id: str, sport_type: str = None, max_time_diff_min=15):
    start_range = fit_start_time - timedelta(minutes=max_time_diff_min)
    end_range = fit_start_time + timedelta(minutes=max_time_diff_min)

    query = {
        "userId": user_id,
        "startDate": {
            "$gte": start_range,
            "$lte": end_range
        }
    }

    if sport_type:
        query["type"] = sport_type  # e.g. "VirtualRide"

    matches = list(collection.find(query).sort("startDate", 1))

    if not matches:
        print(f"❌ No match found for user {user_id} around {fit_start_time.isoformat()} with sport={sport_type}")
        return None

    print(f"✅ Found {len(matches)} candidate(s) for user {user_id} at {fit_start_time.date()} with type={sport_type}")
    return matches[0]


def extract_fit_start_time_and_type(fitfile):
    """
    Extracts start_time and sport type from the FIT file.
    """
    for msg in fitfile.get_messages("session"):
        ts = msg.get_value("start_time")
        sport = msg.get_value("sport")
        if isinstance(ts, datetime):
            return ts, sport

    # Fallback to workout name
    for msg in fitfile.get_messages("workout"):
        name = msg.get_value("wkt_name")
        sport = msg.get_value("sport")
        if isinstance(name, str):
            match = re.search(r"\d{4}-\d{2}-\d{2}", name)
            if match:
                try:
                    ts = datetime.strptime(match.group(0), "%Y-%m-%d")
                    return ts, sport
                except Exception as e:
                    print(f"❌ Failed to parse fallback date: {e}")
    return None, None

def match_fit_file_to_activity(fitfile, user_id: str, fallback_sport: str = None):
    """
    High-level helper to go from FitFile to matched activity.
    """
    fit_start_time, sport = extract_fit_start_time_and_type(fitfile)
    if not fit_start_time:
        print("❌ No start_time found in .fit file — using fallback if available.")

    sport_to_use = sport or fallback_sport
    return match_fit_to_activity(fit_start_time, user_id, sport_type=sport_to_use)
