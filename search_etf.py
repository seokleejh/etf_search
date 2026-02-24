"""
Search KRX-listed ETFs by exposure to a specific stock.

Usage:
    python search_etf.py <stock_name_or_ticker> [--date YYYYMMDD] [--top N] [--output FILE]

Examples:
    python search_etf.py SK하이닉스
    python search_etf.py 000660
    python search_etf.py SK하이닉스 --top 20 --output results.csv
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock
from tabulate import tabulate


def get_latest_business_day() -> str:
    """Return the most recent business day as YYYYMMDD string."""
    date = datetime.today()
    # Step back until we hit a weekday (Mon-Fri)
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    return date.strftime("%Y%m%d")


def resolve_ticker(query: str, date: str) -> tuple[str, str]:
    """
    Given a stock name or ticker, return (ticker, name).
    Raises SystemExit if not found.
    """
    # If it looks like a ticker (numeric), validate it
    if query.isdigit():
        try:
            name = stock.get_market_ticker_name(query)
            return query, name
        except Exception:
            print(f"Error: ticker '{query}' not found on KRX.")
            sys.exit(1)

    # Otherwise search by name across all tickers
    tickers = stock.get_market_ticker_list(date, market="ALL")
    for ticker in tickers:
        name = stock.get_market_ticker_name(ticker)
        if query in name:
            return ticker, name

    print(f"Error: could not find a stock matching '{query}'.")
    sys.exit(1)


def fetch_single_etf(etf_ticker: str, target_ticker: str, target_name: str, date: str) -> dict | None:
    """Fetch portfolio for one ETF and return a result dict if target stock is found."""
    try:
        portfolio = stock.get_etf_portfolio_deposit_file(etf_ticker, date)
        if portfolio is None or portfolio.empty:
            return None
        if target_ticker in portfolio.index:
            row = portfolio.loc[target_ticker]
            weight = float(row.get("비중", 0))
            etf_name = stock.get_etf_ticker_name(etf_ticker)
            return {
                "ETF 티커": etf_ticker,
                "ETF 명": etf_name,
                f"{target_name} 비중 (%)": weight,
            }
    except Exception:
        pass
    return None


def fetch_etf_exposure(target_ticker: str, target_name: str, date: str, workers: int = 20) -> pd.DataFrame:
    """
    Scan all KRX ETFs in parallel and return a DataFrame of ETFs
    that hold the target stock, sorted by weight descending.
    """
    etf_tickers = stock.get_etf_ticker_list(date)
    total = len(etf_tickers)
    print(f"Found {total} ETFs. Scanning portfolios with {workers} parallel workers...\n")

    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_single_etf, t, target_ticker, target_name, date): t
            for t in etf_tickers
        }
        for future in as_completed(futures):
            completed += 1
            print(f"\r[{completed}/{total}] Scanning...", end="", flush=True)
            result = future.result()
            if result:
                results.append(result)

    print()  # newline after progress
    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(f"{target_name} 비중 (%)", ascending=False).reset_index(drop=True)
        df.index += 1  # 1-based ranking
    return df


def main():
    parser = argparse.ArgumentParser(description="Search KRX ETFs by stock exposure.")
    parser.add_argument("query", help="Stock name (e.g. SK하이닉스) or ticker (e.g. 000660)")
    parser.add_argument("--date", default=None, help="Reference date YYYYMMDD (default: latest business day)")
    parser.add_argument("--top", type=int, default=None, help="Show only top N ETFs")
    parser.add_argument("--output", default=None, help="Save results to this CSV file")
    args = parser.parse_args()

    date = args.date or get_latest_business_day()
    print(f"Reference date: {date}")

    print(f"Resolving '{args.query}'...")
    ticker, name = resolve_ticker(args.query, date)
    print(f"Target stock: {name} ({ticker})\n")

    df = fetch_etf_exposure(ticker, name, date)

    if df.empty:
        print(f"No ETFs found holding '{name}'.")
        return

    display_df = df.head(args.top) if args.top else df
    weight_col = f"{name} 비중 (%)"
    display_df = display_df.copy()
    display_df[weight_col] = display_df[weight_col].map(lambda x: f"{x:.2f}%")

    print(f"\nETFs holding {name} ({ticker}) — sorted by weight:\n")
    print(tabulate(display_df, headers="keys", tablefmt="rounded_outline"))
    print(f"\nTotal: {len(df)} ETF(s) found.")

    # CSV output
    csv_path = args.output or f"etf_{ticker}_{date}.csv"
    df.to_csv(csv_path, index=True, encoding="utf-8-sig")
    print(f"Results saved to: {csv_path}")


if __name__ == "__main__":
    main()
