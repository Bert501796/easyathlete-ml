from fastapi import FastAPI, Query
from ml_service import run_analysis  # you already have this logic
import uvicorn

app = FastAPI()

@app.get("/analyze")
def analyze(stravaId: int = Query(...)):
    result = run_analysis(stravaId)
    return result
