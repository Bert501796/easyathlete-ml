from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import re
from pathlib import Path

# Load .env settings
load_dotenv()
mongo_url = os.getenv("MONGO_URL")
db_name = os.getenv("DB_NAME", "test")
client = MongoClient(mongo_url)
db = client[db_name]
collection = db["stravaactivities"]

def extract_date_from_filename(filename):
    match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
    if match:
        try:
            return datetime.strptime(match.group(0), "%Y-%m-%d")
        except Exception as e:
            print(f"❌ Failed to parse date from filename: {e}")
    return None

def match_zwo_file_to_activity(filename: str, user_id: str, fallback_sport: str = "VirtualRide"):
    base_name = Path(filename).stem
    date_estimate = extract_date_from_filename(base_name)

    if not date_estimate:
        print(f"❌ Could not extract date from filename: {filename}")
        return None

    start_of_day = datetime(date_estimate.year, date_estimate.month, date_estimate.day)
    end_of_day = start_of_day + timedelta(days=1)

    query = {
        "userId": user_id,
        "startDate": {"$gte": start_of_day, "$lt": end_of_day},
        "type": fallback_sport
    }

    print(f"🔍 ZWO date-based query: {query}")
    matches = list(collection.find(query).sort("startDate", 1))

    if not matches:
        print(f"❌ No match found on {date_estimate.date()} for sport={fallback_sport}")
        return None

    print(f"✅ Found {len(matches)} match(es) for {date_estimate.date()} with type={fallback_sport}")
    return matches[0]
