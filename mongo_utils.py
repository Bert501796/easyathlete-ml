from pymongo import MongoClient

from dotenv import load_dotenv
import os

load_dotenv()  # Loads variables from .env

MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME")

def get_db_connection(uri, db_name):
    client = MongoClient(uri)
    return client[db_name]

def fetch_activity_by_strava_id(db, strava_id):
    # Ensure we are comparing using an integer
    strava_id = int(strava_id)
    return db.stravaactivities.find_one({"stravaId": strava_id})
