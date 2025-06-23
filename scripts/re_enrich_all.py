import requests
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
import time
import os

# ✅ Load environment variables
load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "test")
ML_API_URL = os.getenv("ML_API_URL", "https://easyathlete-ml-production.up.railway.app")  # ✅ Fixed default

# ✅ Connect to MongoDB
client = MongoClient(MONGO_URL)
db = client[DB_NAME]
collection = db["stravaactivities"]

#✅ Query: activities that needs enrichment
query = {
    #"enrichmentVersion": {"$ne": 1.4},
    "type": {"$ne": "WeightTraining"},
    #"stream_data_full": {"$exists": True}
}

# query = {
#     "stravaId": 14866675543,  # ✅ or whichever activity ID you want to test
# }


activities = list(collection.find(query))
total = len(activities)

print(f"🔄 Found {total} activities to re-enrich.\n")

for idx, activity in enumerate(activities, 1):
    try:
        res = requests.post(f"{ML_API_URL}/ml/enrich-activity", json={
            "activity_id": str(activity["_id"]),
            "user_id": activity["userId"]
        })
        print(f"✅ [{idx}/{total}] {activity['stravaId']} enriched →", res.json())
    except Exception as e:
        print(f"❌ [{idx}/{total}] Failed to enrich {activity['stravaId']} →", str(e))

    time.sleep(0.2)  # protect your server