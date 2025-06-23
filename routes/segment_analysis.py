from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from bson import ObjectId
from pymongo import MongoClient
import os
from dotenv import load_dotenv

from utils.segment_trends import analyze_segment_trends

load_dotenv()
router = APIRouter()

mongo_url = os.getenv("MONGO_URL")
db_name = os.getenv("DB_NAME", "test")
client = MongoClient(mongo_url)
db = client[db_name]
collection = db["stravaactivities"]

class TrendRequest(BaseModel):
    user_id: str

@router.post("/ml/analyze-trends")
async def analyze_trends(request: TrendRequest):
    try:
        activities = list(collection.find({
            "userId": request.user_id,
            "segments": {"$exists": True, "$ne": []}
        }))

        if not activities:
            raise HTTPException(status_code=404, detail="No enriched activities found for this user")

        trends = analyze_segment_trends(activities)
        return {"user_id": request.user_id, "trend_count": len(trends), "trends": trends}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
