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

# ✅ Query: Only one activity that needs enrichment
query = {
    "enrichmentVersion": {"$ne": 1.4},
    "type": {"$ne": "WeightTraining"},
    "stream_data_full": {"$exists": True}
}

activity = collection.find_one(query)

if not activity:
    print("⚠️ No activity found that matches the enrichment criteria.")
else:
    try:
        res = requests.post(f"{ML_API_URL}/ml/enrich-activity", json={
            "activity_id": str(activity["_id"]),
            "user_id": activity["userId"]
        })
        print(f"✅ {activity['stravaId']} enriched →", res.json())
    except Exception as e:
        print(f"❌ Failed to enrich {activity['stravaId']} →", str(e))

    time.sleep(0.2)
