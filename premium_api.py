"""Minimal FastAPI service for premium scoring outputs."""

from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException


app = FastAPI(title="Telematics Insurance Premium API")
PREMIUM_HISTORY = Path("data/premium_history/premium_history.csv")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/drivers/{driver_id}/premium")
def driver_premium(driver_id: str) -> dict:
    if not PREMIUM_HISTORY.exists():
        raise HTTPException(status_code=404, detail="premium_history.csv not found; run score_premiums.py first")
    history = pd.read_csv(PREMIUM_HISTORY)
    driver_rows = history[history["driver_id"] == driver_id].sort_values("scored_at")
    if driver_rows.empty:
        raise HTTPException(status_code=404, detail=f"No premium for driver {driver_id}")
    return driver_rows.tail(1).iloc[0].to_dict()
