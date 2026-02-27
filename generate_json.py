"""
Generate kospi.json for GitHub Pages.

This script is run daily by GitHub Actions.
It fetches KOSPI fundamentals from KRX and saves the result
to docs/kospi.json, which is then served via GitHub Pages.
"""

import json
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock


def get_latest_business_day() -> str:
    d = datetime.today()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def main():
    date = get_latest_business_day()
    print(f"Fetching KOSPI data for {date}...")

    df_sector = stock.get_market_sector_classifications(date, market="KOSPI")
    df_fund = stock.get_market_fundamental(date, market="KOSPI")

    if df_sector is None or df_sector.empty or df_fund is None or df_fund.empty:
        print("Error: no data returned from KRX. Aborting.")
        sys.exit(1)

    df = df_sector[["종목명", "업종명", "시가총액"]].join(
        df_fund[["PER", "PBR", "EPS", "BPS"]], how="inner"
    )
    df = df.rename(columns={"업종명": "섹터"})
    df["시가총액(억)"] = (df["시가총액"] / 1e8).round(0).astype(int)
    df = df.drop(columns=["시가총액"])
    df.index.name = "티커"
    df = df.reset_index()

    # Keep only stocks with valid PER and PBR
    df = df[(df["PER"] > 0) & (df["PBR"] > 0)]
    df = df.sort_values("시가총액(억)", ascending=False).reset_index(drop=True)

    output = {
        "date": date,
        "total": len(df),
        "data": df.to_dict(orient="records"),
    }

    os.makedirs("docs", exist_ok=True)
    with open("docs/kospi.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(df)} stocks to docs/kospi.json")


if __name__ == "__main__":
    main()