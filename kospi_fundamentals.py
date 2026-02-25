"""List KOSPI-listed stocks with PER, PBR, market cap, and KRX industry sector.

Usage:
    python kospi_fundamentals.py [--date YYYYMMDD] [--sector NAME] [--sort PER|PBR|시가총액] [--top N] [--output FILE]

Examples:
    python kospi_fundamentals.py
    python kospi_fundamentals.py --sector 전기전자
    python kospi_fundamentals.py --sort PBR --top 30
    python kospi_fundamentals.py --sort 시가총액 --sector 제약
    python kospi_fundamentals.py --output result.csv
"""

import argparse
import sys
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock
from tabulate import tabulate


def get_latest_business_day() -> str:
    """Return the most recent business day as YYYYMMDD string."""
    date = datetime.today()
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    return date.strftime("%Y%m%d")


def load_data(date: str) -> pd.DataFrame:
    """
    Fetch and merge sector/market-cap data with fundamental data for all KOSPI stocks.

    - get_market_sector_classifications: 종목명, 업종명(sector), 시가총액 in one call
    - get_market_fundamental: PER, PBR, EPS, BPS, DIV in one call
    """
    print("Loading sector and market cap data...")
    try:
        df_sector = stock.get_market_sector_classifications(date, market="KOSPI")
    except Exception as e:
        print(f"Error fetching sector data: {e}")
        sys.exit(1)

    print("Loading fundamental data (PER, PBR)...")
    try:
        df_fund = stock.get_market_fundamental(date, market="KOSPI")
    except Exception as e:
        print(f"Error fetching fundamental data: {e}")
        sys.exit(1)

    if df_sector is None or df_sector.empty or df_fund is None or df_fund.empty:
        print("Error: no data returned.")
        sys.exit(1)

    # Merge on ticker index
    df = df_sector[["종목명", "업종명", "시가총액"]].join(df_fund[["PER", "PBR", "EPS", "BPS"]], how="inner")
    df = df.rename(columns={"업종명": "섹터"})

    # Convert 시가총액 to 억원 for readability
    df["시가총액(억)"] = (df["시가총액"] / 1e8).round(0).astype(int)
    df = df.drop(columns=["시가총액"])

    df.index.name = "티커"
    return df.reset_index()


def main():
    parser = argparse.ArgumentParser(
        description="List KOSPI stocks with PER, PBR, market cap, and sector."
    )
    parser.add_argument(
        "--date", default=None, help="Reference date YYYYMMDD (default: latest business day)"
    )
    parser.add_argument(
        "--sector", default=None,
        help="Filter by sector name, e.g. 전기전자, 금융, 제약 (partial match supported)"
    )
    parser.add_argument(
        "--sort", choices=["PER", "PBR", "시가총액"], default="시가총액",
        help="Sort metric (default: 시가총액 descending; PER/PBR ascending)"
    )
    parser.add_argument(
        "--top", type=int, default=50, help="Show top N results (default: 50)"
    )
    parser.add_argument(
        "--output", default=None, help="Save full results to this CSV file"
    )
    args = parser.parse_args()

    date = args.date or get_latest_business_day()
    print(f"Reference date: {date}\n")

    df = load_data(date)
    print(f"  {len(df)} stocks loaded.\n")

    # Drop stocks with no valid PER/PBR (loss-making or data unavailable)
    df_valid = df[(df["PER"] > 0) & (df["PBR"] > 0)].copy()

    # Sector filter
    if args.sector:
        filtered = df_valid[df_valid["섹터"].str.contains(args.sector, case=False, na=False)]
        if filtered.empty:
            available = sorted(df_valid["섹터"].unique())
            print(f"No stocks found for sector '{args.sector}'.")
            print(f"Available sectors: {', '.join(available)}")
            return
        df_valid = filtered

    # Sort: 시가총액 descending, PER/PBR ascending
    ascending = args.sort != "시가총액"
    sort_col = "시가총액(억)" if args.sort == "시가총액" else args.sort
    df_valid = df_valid.sort_values(sort_col, ascending=ascending).reset_index(drop=True)
    df_valid.index += 1

    display_df = df_valid.head(args.top) if args.top else df_valid

    sector_label = f" | 섹터: {args.sector}" if args.sector else ""
    sort_dir = "내림차순" if args.sort == "시가총액" else "오름차순"
    print(f"KOSPI 종목 (기준일: {date}{sector_label}) — {args.sort} {sort_dir}:\n")
    print(tabulate(display_df, headers="keys", tablefmt="rounded_outline", floatfmt=".2f"))
    print(f"\n표시: {len(display_df)}개 / 전체 유효 종목: {len(df_valid)}개")

    csv_path = args.output or f"kospi_fundamentals_{date}.csv"
    df_valid.to_csv(csv_path, index=True, encoding="utf-8-sig")
    print(f"Results saved to: {csv_path}")


if __name__ == "__main__":
    main()
