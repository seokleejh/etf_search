"""
KOSPI Fundamentals API

Run:
    uvicorn api:app --host 0.0.0.0 --port 8000

Endpoints:
    GET /api/health          - service health + cached date
    GET /api/sectors         - list of available KRX sectors
    GET /api/fundamentals    - KOSPI stock fundamentals (PER, PBR, market cap, sector)
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pykrx import stock


# ── Cache ─────────────────────────────────────────────────────────────────────

class DataCache:
    def __init__(self):
        self.date: str | None = None
        self.df: pd.DataFrame | None = None

    def is_stale(self) -> bool:
        return self.date != _latest_business_day()

    def refresh(self) -> None:
        date = _latest_business_day()
        print(f"Fetching KOSPI data for {date}...")
        self.df = _fetch(date)
        self.date = date
        print(f"Cache refreshed: {len(self.df)} stocks loaded.")


_cache = DataCache()


# ── Data helpers ──────────────────────────────────────────────────────────────

def _latest_business_day() -> str:
    d = datetime.today()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def _fetch(date: str) -> pd.DataFrame:
    df_sector = stock.get_market_sector_classifications(date, market="KOSPI")
    df_fund = stock.get_market_fundamental(date, market="KOSPI")

    df = df_sector[["종목명", "업종명", "시가총액"]].join(
        df_fund[["PER", "PBR", "EPS", "BPS"]], how="inner"
    )
    df = df.rename(columns={"업종명": "섹터"})
    df["시가총액(억)"] = (df["시가총액"] / 1e8).round(0).astype(int)
    df = df.drop(columns=["시가총액"])
    df.index.name = "티커"
    return df.reset_index()


def _get_data() -> pd.DataFrame:
    if _cache.is_stale():
        _cache.refresh()
    return _cache.df


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Preload on startup so the first request is instant
    _cache.refresh()
    yield


app = FastAPI(title="KOSPI Fundamentals API", version="1.0.0", lifespan=lifespan)

# Allow all origins in development.
# In production, replace "*" with your WordPress domain, e.g.:
#   allow_origins=["https://your-blog.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "cached_date": _cache.date}


@app.get("/api/sectors")
def get_sectors():
    df = _get_data()
    sectors = sorted(df["섹터"].dropna().unique().tolist())
    return {"sectors": sectors}


@app.get("/api/fundamentals")
def get_fundamentals(
    sector: str | None = Query(None, description="Filter by sector name (partial match)"),
    limit: int = Query(1000, ge=1, le=2000, description="Max number of rows to return"),
):
    df = _get_data()
    df = df[(df["PER"] > 0) & (df["PBR"] > 0)].copy()

    if sector:
        df = df[df["섹터"].str.contains(sector, case=False, na=False)]
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No stocks found for sector '{sector}'"
            )

    # Return by market cap descending as default order
    df = df.sort_values("시가총액(억)", ascending=False).head(limit)
    df = df.reset_index(drop=True)
    df.index += 1

    return {
        "date": _cache.date,
        "total": len(df),
        "data": df.to_dict(orient="records"),
    }


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)
