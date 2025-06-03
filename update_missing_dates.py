import asyncio
import csv
import os
from scraper import get_best_price, FlightURLBuilder, setup_browser
from check_date import check_missing_dates

CSV_FILE = "best_flight_prices.csv"

async def scrape_and_append(missing_entries):
    p, browser, page = await setup_browser()
    keys = None
    try:
        with open(CSV_FILE, "a", newline='', encoding='utf-8') as f:
            writer = None
            for from_airport, to_airport, date in missing_entries:
                url = FlightURLBuilder.build_url(from_airport, to_airport, date)
                best = await get_best_price(page, url)
                if best:
                    best["From"] = from_airport
                    best["To"] = to_airport
                    best["Date"] = date
                    if keys is None:
                        keys = ["From", "To", "Date"] + [k for k in best if k not in ("From", "To", "Date")]
                        writer = csv.DictWriter(f, fieldnames=keys)
                        if os.stat(CSV_FILE).st_size == 0:
                            writer.writeheader()
                    if writer:
                        writer.writerow(best)
                    print(f"Appended data: {from_airport} -> {to_airport} on {date}")
                else:
                    print(f"No data found for {from_airport} -> {to_airport} on {date}")
    finally:
        await browser.close()
        await p.stop()

async def main():
    # Adjust the date range as needed
    start_year = 2025
    start_month = 6
    end_year = 2025
    end_month = 8

    missing_entries = check_missing_dates(CSV_FILE, start_year, start_month, end_year, end_month)
    if not missing_entries:
        print("No missing dates to update.")
        return

    print(f"Found {len(missing_entries)} missing entries. Starting scraping...")
    await scrape_and_append(missing_entries)
    print("Update complete.")

if __name__ == "__main__":
    asyncio.run(main())
