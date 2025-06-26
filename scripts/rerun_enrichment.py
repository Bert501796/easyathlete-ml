import os
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from bson import ObjectId

from utils.enrichment_helpers import (
    parse_streams,
    detect_segments,
    prepare_activity_for_storage,
    extract_aggregated_features,
    convert_numpy_types,
)

load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "test")
client = MongoClient(MONGO_URL)
collection = client[DB_NAME]["stravaactivities"]

def enrich_activity_by_id(activity_id):
    doc = collection.find_one({"_id": ObjectId(activity_id)})
    if not doc:
        print(f"❌ Activity {activity_id} not found.")
        return None

    df = parse_streams(doc)
    if df.empty:
        print(f"❌ Skipping {activity_id}: empty stream data.")
        return None

    doc = prepare_activity_for_storage(doc, df)
    segments_result = detect_segments(df, doc)
    aggregated = extract_aggregated_features(doc)

    doc.update({
        "aggregatedFeatures": convert_numpy_types(aggregated),
        "segments": convert_numpy_types(segments_result["segments"]),
        "segmentSummary": convert_numpy_types(segments_result["summary"]),
        "segmentSequence": convert_numpy_types(segments_result["segments"]),
        "enriched": True,
        "enrichmentVersion": 1.4,
        "updatedAt": datetime.utcnow()
    })

    collection.update_one({"_id": doc["_id"]}, {"$set": doc})
    print(f"✅ Re-enriched {doc.get('stravaId')} ({doc['_id']})")
    return doc

def rerun_enrichment(user_id=None, limit=None):
    query = {"stream_data_full": {"$exists": True}}
    if user_id:
        query["userId"] = user_id

    cursor = collection.find(query).limit(limit or 0)
    total = cursor.count()

    print(f"🔁 Re-enriching {total} activities...")
    success = 0
    for doc in cursor:
        if enrich_activity_by_id(doc["_id"]):
            success += 1

    print(f"✅ Finished: {success}/{total} successfully re-enriched.")

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--activity", type=str, help="Single activity ID to reprocess")
    parser.add_argument("--user", type=str, help="User ID to filter activities")
    parser.add_argument("--limit", type=int, help="Limit number of activities")
    args = parser.parse_args()

    if args.activity:
        enrich_activity_by_id(args.activity)
    else:
        rerun_enrichment(user_id=args.user, limit=args.limit)
