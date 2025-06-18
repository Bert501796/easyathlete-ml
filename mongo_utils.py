from pymongo import MongoClient

def get_db_connection(uri, db_name):
    client = MongoClient(uri)
    return client[db_name]

def fetch_activity_by_strava_id(db, strava_id):
    return db.stravaactivities.find_one({"stravaId": strava_id})

