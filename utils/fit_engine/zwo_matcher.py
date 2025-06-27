from pymongo import MongoClient
from dotenv import load_dotenv
import os
import re

load_dotenv()
mongo_url = os.getenv("MONGO_URL")
db_name = os.getenv("DB_NAME", "test")
client = MongoClient(mongo_url)
collection = client[db_name]["stravaactivities"]

def match_zwo_file_to_activity(filename: str, user_id: str, fallback_sport: str = None):
    """
    Match based on filename and user_id. Optionally filter by sport.
    """
    base_name = Path(filename).stem  # strip extension
    query = {
        "userId": user_id,
        "name": {"$regex": base_name, "$options": "i"}
    }
    if fallback_sport:
        query["type"] = fallback_sport

    print(f"🔍 ZWO filename-based query: {query}")
    match = collection.find_one(query)
    if match:
        print(f"✅ Match found: {match['name']} ({match['stravaId']})")
    else:
        print(f"❌ No match found for {base_name}")
    return match
