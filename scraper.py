import asyncio
from datetime import datetime, timedelta
import csv
import os
from typing import List, Dict
from flight_scraper import FlightURLBuilder, setup_browser, scrape_flight_info
from asyncio import Semaphore

# List of 10 countries (main airports) to/from Taiwan (TPE)
COUNTRIES = [
    ("JPN", "NRT"),  # Japan - Tokyo Narita
    ("KOR", "ICN"),  # South Korea - Incheon
    ("THA", "BKK"),  # Thailand - Bangkok
    ("DEU", "FRA"),  # Germany - Frankfurt
    ("USA", "LAX"),  # USA - Los Angeles
    ("AUS", "SYD"),  # Australia - Sydney
    ("VNM", "SGN"),  # Vietnam - Ho Chi Minh
    ("MYS", "KUL"),  # Malaysia - Kuala Lumpur
    ("HKG", "HKG"),  # Hong Kong
    ("EGY", "CAI"),  # Egypt - Cairo
]

TAIWAN = "TPE"  # Taipei Taoyuan

# Date range: June 1st to August 31st (every day)
def generate_daily_dates(start_year: int, start_month: int, end_year: int, end_month: int) -> list:
    dates = []
    current = datetime(start_year, start_month, 1)
    # Find the last day of the end_month
    if end_month == 12:
        next_month = datetime(end_year + 1, 1, 1)
    else:
        next_month = datetime(end_year, end_month + 1, 1)
    last_day = (next_month - timedelta(days=1)).day
    end = datetime(end_year, end_month, last_day)
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates

# Fix: get_best_price should accept a page and url
async def get_best_price(page, url: str) -> Dict:
    best = None
    try:
        print(f"Visiting: {url}")
        await page.goto(url, timeout=60000)
        await page.wait_for_selector(".pIav2d", timeout=40000)
        flights = await page.query_selector_all(".pIav2d")
        print(f"Found {len(flights)} flights for {url}")
        for flight in flights:
            info = await scrape_flight_info(flight)
            price_str = info.get("Price", "N/A").replace("$", "").replace(",", "").strip()
            try:
                price = float(price_str) if price_str != "N/A" else float('inf')
            except Exception:
                price = float('inf')
            if best is None or price < best["price"]:
                best = {"info": info, "price": price}
    except Exception as e:
        print(f"Error scraping {url}: {e}")
    if not best:
        print(f"No valid flights found for {url}")
    return best["info"] if best else None

async def get_best_price_with_semaphore(url: str, semaphore: Semaphore):
    async with semaphore:
        return await get_best_price(url)

async def process_month(month_dates, month_name, csv_file, write_header):
    p, browser, page = await setup_browser()
    keys = None
    try:
        for country_code, airport in COUNTRIES:
            for direction in [(airport, TAIWAN), (TAIWAN, airport)]:
                for date in month_dates:
                    url = FlightURLBuilder.build_url(direction[0], direction[1], date)
                    best = await get_best_price(page, url)
                    if best:
                        best["From"] = direction[0]
                        best["To"] = direction[1]
                        best["Date"] = date
                        if keys is None:
                            keys = ["From", "To", "Date"] + [k for k in best if k not in ("From", "To", "Date")]
                        with open(csv_file, "a", newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=keys)
                            if write_header[0]:
                                writer.writeheader()
                                write_header[0] = False
                            writer.writerow(best)
                        print(f"Written to CSV: {direction[0]} -> {direction[1]} on {date}")
                    else:
                        print(f"No best price found for {direction[0]} -> {direction[1]} on {date}")
    finally:
        await browser.close()
        await p.stop()

async def main():
    # Date range: June 1st to August 31st (every day)
    today = datetime.now()
    start_year = today.year
    end_year = today.year
    # Generate daily dates for each month
    june_dates = generate_daily_dates(start_year, 6, start_year, 6)
    july_dates = generate_daily_dates(start_year, 7, start_year, 7)
    august_dates = generate_daily_dates(start_year, 8, start_year, 8)
    month_date_lists = [june_dates, july_dates, august_dates]

    csv_file = "best_flight_prices.csv"
    write_header = [not os.path.exists(csv_file) or os.stat(csv_file).st_size == 0]  # mutable for all tasks

    await asyncio.gather(
        process_month(june_dates, "June", csv_file, write_header),
        process_month(july_dates, "July", csv_file, write_header),
        process_month(august_dates, "August", csv_file, write_header),
    )
    print(f"Done. Results saved to {csv_file}")

if __name__ == "__main__":
    asyncio.run(main())
