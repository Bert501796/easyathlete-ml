from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from utils.segment_kpis import compute_kpi_trends_with_sessions as compute_kpi_trends

import math

load_dotenv()

router = APIRouter()

class TrendAnalysisRequest(BaseModel):
    user_id: str
    activity_type: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

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

    activities = list(collection.find(query, max_time_ms=5000))

    if not activities:
        raise HTTPException(status_code=404, detail="No activities with segments found for this user.")

    trends = compute_kpi_trends(
        activities,
        start_date=request.start_date,
        end_date=request.end_date,
        activity_type=request.activity_type
    )

    # ✅ FINAL FIX: align with frontend expectation
    return {"version": "v1", "data": clean_nan_values(trends)}
