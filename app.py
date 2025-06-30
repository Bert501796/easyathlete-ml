from fastapi import FastAPI, Query
from ml_service import run_analysis
from routes import enrichment, prediction, segment_analysis  
import uvicorn

app = FastAPI()

# ✅ Legacy analyze endpoint
@app.get("/analyze")
def analyze(stravaId: int = Query(...)):
    result = run_analysis(stravaId)
    return result

# ✅ Include routers
app.include_router(enrichment.router)
app.include_router(prediction.router)
app.include_router(segment_analysis.router)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
