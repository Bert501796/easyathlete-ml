from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from utils.segment_trends import analyze_segment_trends
import math


load_dotenv()

router = APIRouter()

class TrendAnalysisRequest(BaseModel):
    user_id: str
    activity_type: Optional[str] = None

# Connect to MongoDB
client = MongoClient(os.getenv("MONGO_URL"))
db = client[os.getenv("DB_NAME", "test")]
collection = db["stravaactivities"]

def clean_nan_values(data):
    if isinstance(data, dict):
        return {k: clean_nan_values(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_nan_values(i) for i in data]
    elif isinstance(data, float) and (math.isnan(data) or math.isinf(data)):
        return None
    return data

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
    return {"user_id": request.user_id, "trends": clean_nan_values(trends)}
