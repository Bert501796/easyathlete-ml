from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from utils.segment_trends import analyze_segment_trends

load_dotenv()

router = APIRouter()

class TrendAnalysisRequest(BaseModel):
    user_id: str
    activity_type: Optional[str] = None

# Connect to MongoDB
client = MongoClient(os.getenv("MONGO_URL"))
db = client[os.getenv("DB_NAME", "test")]
collection = db["stravaactivities"]

@router.post("/ml/analyze-trends")
async def analyze_trends(request: TrendAnalysisRequest):
    query = {
        "userId": request.user_id,
        "segments": {"$exists": True, "$ne": []}
    }
    if request.activity_type:
        query["type"] = request.activity_type

    activities = list(collection.find(query))

    if not activities:
        raise HTTPException(status_code=404, detail="No activities with segments found for this user.")

    trends = analyze_segment_trends(activities)
    return {"user_id": request.user_id, "activity_type": request.activity_type, "trend_summary": trends}
