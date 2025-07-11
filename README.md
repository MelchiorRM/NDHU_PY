# Google Flight Price Scraper

This project is a Python-based web scraper that retrieves the best flight prices from Google Flights for 10 countries to and from Taiwan (TPE) for every day in June, July, and August. The results are saved progressively to a CSV file for easy monitoring and analysis. I built it as a final project for the Intermediate Python programming course.

## Features
- Scrapes flight prices for 10 countries (main airports) to/from Taiwan (TPE)
- Searches every day in June, July, and August
- Uses Playwright for browser automation
- Runs 3 browsers in parallel, each handling a different month
- Results are written to `best_flight_prices.csv` as soon as they are scraped
- Proxy support for avoiding blocks

## How It Works
- The script launches 3 browsers (one for each month).
- Each browser searches all routes for its month, day by day, in order.
- For each search, the best price is extracted and written to the CSV file immediately.
- The process is fully asynchronous and efficient.

## Customization
- To change the countries or airports, edit the `COUNTRIES` list in `scraper.py`.
- To change the date range, modify the `generate_daily_dates` function and the month setup in `main()`.