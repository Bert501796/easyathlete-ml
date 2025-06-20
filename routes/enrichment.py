from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from bson import ObjectId
import os
from pymongo import MongoClient
from dotenv import load_dotenv

from utils.enrichment_helpers import (
    parse_streams,
    extract_aggregated_features,
    detect_segments,
    generate_ml_windows,
    convert_numpy_types
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
    try:
        activity = collection.find_one({
            "_id": ObjectId(request.activity_id),
            "userId": request.user_id
        })

        if not activity:
            raise HTTPException(status_code=404, detail="Activity not found for this user")

        strava_id = activity.get("stravaId")
        if activity.get("type") == "WeightTraining":
            return {"skipped": True, "reason": "WeightTraining activity"}

        df = parse_streams(activity)
        if df.empty or df.shape[0] < 30:
            return {"skipped": True, "reason": "Insufficient stream data"}

        aggregated = extract_aggregated_features(activity)
        segments_result = detect_segments(df, activity)
        ml_windows = generate_ml_windows(df, segments_result["segments"])

        update_fields = {
            "aggregatedFeatures": convert_numpy_types(aggregated),
            "segments": convert_numpy_types(segments_result["segments"]),
            "segmentSummary": convert_numpy_types(segments_result["summary"]),
            "mlWindows": convert_numpy_types(ml_windows),
            "stream_data_full": convert_numpy_types({
                "time_sec": df["time_sec"].tolist(),
                "heart_rate": df["heart_rate"].tolist(),
                "watts": df["watts"].tolist(),
                "speed": df["speed"].tolist(),
                "cadence": df["cadence"].tolist(),
                "altitude": df["altitude"].tolist(),
                "distance": df["distance"].tolist(),
            }),
            "enriched": True,
            "enrichmentVersion": 1.4
        }

        collection.update_one({"_id": activity["_id"]}, {"$set": update_fields})
        return {"success": True, "stravaId": strava_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
