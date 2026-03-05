"""
Generate kospi.json for GitHub Pages using NAVER Finance.

KRX added mandatory login authentication to data.krx.co.kr in March 2026,
breaking pykrx and all libraries that relied on that API. This script uses
NAVER Finance instead, which provides the same data without authentication.
"""

import json
import os
import sys
import time
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

NAVER_BASE = "https://finance.naver.com"


def make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": NAVER_BASE,
    })
    # Configure NAVER Finance to show: market cap, PER, PBR, EPS columns
    session.post(f"{NAVER_BASE}/sise/field_submit.naver", data={
        "menu": "market_sum",
        "returnUrl": f"{NAVER_BASE}/sise/sise_market_sum.naver",
        "fieldIds": ["market_sum", "per", "pbr", "eps"],
    })
    return session


def get_total_pages(soup):
    pager = soup.find("td", class_="pgRR")
    if pager and pager.find("a"):
        return int(pager.find("a")["href"].split("page=")[-1])
    return 1


def fetch_market_page(session, page):
    r = session.get(
        f"{NAVER_BASE}/sise/sise_market_sum.naver",
        params={"sosok": "0", "page": str(page)},  # sosok=0 → KOSPI
    )
    soup = BeautifulSoup(r.text, "html.parser")

    # Extract ticker codes and current prices from row links
    table = soup.find("table", class_="type_2")
    ticker_map = {}
    price_map = {}
    for row in table.find_all("tr"):
        link = row.find("a", href=lambda h: h and "code=" in h)
        if not link:
            continue
        code = link["href"].split("code=")[-1]
        name = link.get_text(strip=True)
        ticker_map[name] = code
        # Current price is in the first <td> after the name cell
        cells = row.find_all("td")
        if len(cells) >= 2:
            try:
                price = float(cells[1].get_text(strip=True).replace(",", ""))
                price_map[name] = price
            except (ValueError, IndexError):
                pass

    df = pd.read_html(StringIO(r.text))[1]
    df = df.dropna(how="all")
    df = df[df["종목명"].notna()]
    df["티커"] = df["종목명"].map(ticker_map)
    df["현재가_raw"] = df["종목명"].map(price_map)

    return df, soup


def fetch_sectors(session):
    """Return a {ticker: sector_name} dict from NAVER Finance sector pages."""
    r = session.get(f"{NAVER_BASE}/sise/sise_group.naver", params={"type": "upjong"})
    soup = BeautifulSoup(r.text, "html.parser")

    sector_links = soup.find_all(
        "a", href=lambda h: h and "sise_group_detail" in h and "type=upjong" in h
    )
    ticker_to_sector = {}

    print(f"  Fetching {len(sector_links)} sectors...")
    for link in sector_links:
        sector_name = link.get_text(strip=True)
        no = link["href"].split("no=")[-1]
        r2 = session.get(
            f"{NAVER_BASE}/sise/sise_group_detail.naver",
            params={"type": "upjong", "no": no},
        )
        soup2 = BeautifulSoup(r2.text, "html.parser")
        for stock_link in soup2.find_all("a", href=lambda h: h and "code=" in h):
            code = stock_link["href"].split("code=")[-1]
            ticker_to_sector[code] = sector_name
        time.sleep(0.3)

    return ticker_to_sector


def main():
    today = datetime.today().strftime("%Y%m%d")
    print(f"Fetching KOSPI data for {today} from NAVER Finance...")

    session = make_session()

    # Fetch all KOSPI market summary pages
    df_first, soup = fetch_market_page(session, 1)
    total_pages = get_total_pages(soup)
    print(f"Total pages: {total_pages}")

    all_dfs = [df_first]
    for page in range(2, total_pages + 1):
        time.sleep(0.3)
        df_page, _ = fetch_market_page(session, page)
        all_dfs.append(df_page)
        if page % 10 == 0:
            print(f"  Fetched {page}/{total_pages} pages...")

    df = pd.concat(all_dfs, ignore_index=True)
    df = df.dropna(subset=["종목명", "티커"])

    # Rename EPS column (NAVER calls it 주당순이익)
    df = df.rename(columns={"주당순이익": "EPS", "시가총액": "시가총액(억)"})

    # Convert numeric columns
    for col in ["시가총액(억)", "PER", "PBR", "EPS", "현재가_raw"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ""), errors="coerce"
            )

    # Also try parsing 현재가 from the main numeric column if raw extraction failed
    if "현재가" in df.columns:
        df["현재가_raw"] = df["현재가_raw"].fillna(
            pd.to_numeric(df["현재가"].astype(str).str.replace(",", ""), errors="coerce")
        )

    # BPS = current price / PBR  (since PBR = Price / BPS)
    df["BPS"] = (df["현재가_raw"] / df["PBR"]).round(0)

    # Fetch sector mapping
    print("Fetching sector data...")
    sector_map = fetch_sectors(session)
    df["섹터"] = df["티커"].map(sector_map).fillna("")

    # Filter to valid rows only
    df = df[(df["PER"] > 0) & (df["PBR"] > 0)]
    df = df.sort_values("시가총액(억)", ascending=False).reset_index(drop=True)

    if df.empty:
        print("Error: no data returned. Aborting.")
        sys.exit(1)

    # Finalize types
    df["시가총액(억)"] = df["시가총액(억)"].round(0).astype(int)
    df["EPS"] = df["EPS"].round(0).astype("Int64")
    df["BPS"] = df["BPS"].round(0).astype("Int64")

    out_df = df[["티커", "종목명", "섹터", "시가총액(억)", "PER", "PBR", "EPS", "BPS"]]

    output = {
        "date": today,
        "total": len(out_df),
        "data": out_df.to_dict(orient="records"),
    }

    os.makedirs("docs", exist_ok=True)
    with open("docs/kospi.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(out_df)} stocks to docs/kospi.json")


if __name__ == "__main__":
    main()
