from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd
from typing import Dict

# ✅ Load environment variables
load_dotenv()

# ✅ Get MongoDB config from .env
mongo_url = os.getenv("MONGO_URL")
db_name = os.getenv("DB_NAME")

if not mongo_url or not mongo_url.startswith("mongodb"):
    raise RuntimeError(f"❌ Invalid or missing MONGO_URL: {mongo_url}")

client = MongoClient(mongo_url)
db = client[db_name]
collection = db["stravaactivities"]

router = APIRouter()

class PredictRequest(BaseModel):
    user_id: str

def detect_best_efforts(df: pd.DataFrame) -> Dict:
    best_efforts = {}
    targets = {
        "5k": 5000,
        "10k": 10000,
        "half_marathon": 21097,
        "marathon": 42195
    }

    for name, dist_target in targets.items():
        best_time = None
        for i in range(len(df) - 1):
            for j in range(i + 5, len(df)):
                dist_diff = df["distance"].iloc[j] - df["distance"].iloc[i]
                if dist_diff >= dist_target:
                    time_diff = df["time_sec"].iloc[j] - df["time_sec"].iloc[i]
                    if not best_time or time_diff < best_time:
                        best_time = time_diff
                    break
        if best_time:
            best_efforts[name] = {"type": "measured", "time_sec": best_time}
    return best_efforts

def estimate_remaining_efforts(known: Dict) -> Dict:
    predictions = known.copy()
    targets = {
        "5k": 5000,
        "10k": 10000,
        "half_marathon": 21097,
        "marathon": 42195
    }

    if len(known) == 0:
        return predictions

    base_key = min(known, key=lambda k: targets[k])
    T1 = known[base_key]["time_sec"]
    D1 = targets[base_key]

    for k, D2 in targets.items():
        if k not in predictions:
            T2 = T1 * (D2 / D1) ** 1.06
            predictions[k] = {"type": "predicted", "time_sec": round(T2)}

    return predictions

@router.post("/ml/predict-user")
async def predict_user(payload: PredictRequest):
    try:
        docs = collection.find({
            "userId": payload.user_id,
            "stream_data_full": {"$exists": True}
        })

        dfs = []
        for doc in docs:
            stream = doc.get("stream_data_full")
            if isinstance(stream, dict):
                df = pd.DataFrame(stream)
            elif isinstance(stream, list):
                df = pd.DataFrame(stream)
            else:
                continue

            if not df.empty and "time_sec" in df and "distance" in df:
                dfs.append(df)

        if not dfs:
            return {"error": "No stream data found."}

        full_df = pd.concat(dfs, ignore_index=True).sort_values("time_sec")
        best_efforts = detect_best_efforts(full_df)
        predictions = estimate_remaining_efforts(best_efforts)

        readable = {}
        for k, v in predictions.items():
            minutes = int(v["time_sec"] // 60)
            seconds = int(v["time_sec"] % 60)
            readable[k] = {
                "type": v["type"],
                "time_sec": v["time_sec"],
                "formatted": f"{minutes}m {seconds}s"
            }

        return {
            "user_id": payload.user_id,
            "predictions": readable
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
