from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from bson import ObjectId
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, UTC  # ✅ Use UTC from datetime


from utils.enrichment_helpers import (
    parse_streams,
    extract_aggregated_features,
    detect_segments,
    generate_ml_windows,
    convert_numpy_types,
    prepare_activity_for_storage

)

# ✅ Load environment variables
load_dotenv()

# ✅ Get MongoDB configuration
mongo_url = os.getenv("MONGO_URL")
db_name = os.getenv("DB_NAME", "test")

if not mongo_url or not mongo_url.startswith("mongodb"):
    raise ValueError(f"❌ Invalid or missing MONGO_URL: {mongo_url}")

# ✅ Set up MongoDB client
client = MongoClient(mongo_url)
db = client[db_name]
collection = db["stravaactivities"]

# ✅ FastAPI router
router = APIRouter()

class EnrichmentRequest(BaseModel):
    activity_id: str
    user_id: str

@router.post("/ml/enrich-activity")
async def enrich_activity(request: EnrichmentRequest):
    strava_id = None
    print(f"🚀 Starting enrichment for activity_id={request.activity_id}, user_id={request.user_id}")

    try:
        activity = collection.find_one({
            "_id": ObjectId(request.activity_id),
            "userId": request.user_id
        })
        if not activity:
            raise HTTPException(status_code=404, detail="Activity not found for this user")

        strava_id = activity.get("stravaId")
        print(f"📌 Processing stravaId={strava_id}")

        if activity.get("type") == "WeightTraining":
            print("⏭ Skipping WeightTraining activity")
            return {"skipped": True, "reason": "WeightTraining activity"}

        # STEP 1: parse streams
        print("📊 Parsing streams...")
        df = parse_streams(activity)
        print(f"✅ Parsed stream shape: {df.shape}")

        if df.empty or df.shape[0] < 30:
            print(f"⏭ Skipping {strava_id}: insufficient stream data.")
            return {"skipped": True, "reason": "Insufficient stream data"}

        # STEP 2: extract aggregated features
        print("📈 Extracting aggregated features...")
        aggregated = extract_aggregated_features(activity)

        # STEP 3: detect segments
        print("🔍 Detecting segments...")
        segments_result = detect_segments(df, activity)

        # STEP 4: cleanup stream and legacy fields
        print("🧹 Preparing activity for storage...")
        activity = prepare_activity_for_storage(activity, df, segment_result)

        # STEP 5: Add metadata
        print("🧬 Injecting metadata...")
        activity.update({
            "aggregatedFeatures": convert_numpy_types(aggregated),
            "segments": convert_numpy_types(segments_result["segments"]),
            "segmentSummary": convert_numpy_types(segments_result["summary"]),
            "enriched": True,
            "enrichmentVersion": 1.4,
            "updatedAt": datetime.now(UTC)
        })

        # STEP 6: Write to DB
        print("💾 Writing updated activity to MongoDB...")
        result = collection.update_one({"_id": activity["_id"]}, {"$set": activity})
        if result.modified_count == 0:
            print(f"⚠️ MongoDB update failed or document unchanged for stravaId={strava_id}")
        else:
            print(f"✅ MongoDB update complete for stravaId={strava_id}")

        return {"success": True, "stravaId": strava_id}

    except Exception as e:
        print(f"❌ ERROR during enrichment of stravaId={strava_id}: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))