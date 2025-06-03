import csv
from datetime import datetime, timedelta

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

def generate_daily_dates(start_year: int, start_month: int, end_year: int, end_month: int) -> list:
    dates = []
    current = datetime(start_year, start_month, 1)
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

def check_missing_dates(csv_file: str, start_year: int, start_month: int, end_year: int, end_month: int):
    # Generate all expected dates
    expected_dates = generate_daily_dates(start_year, start_month, end_year, end_month)
    # Create a set of all expected (country, date) pairs for both directions
    expected_pairs = set()
    for country_code, airport in COUNTRIES:
        for direction in [(airport, TAIWAN), (TAIWAN, airport)]:
            for date in expected_dates:
                expected_pairs.add((direction[0], direction[1], date))

    # Read existing data from CSV
    existing_pairs = set()
    try:
        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                from_airport = row.get("From")
                to_airport = row.get("To")
                date = row.get("Date")
                if from_airport and to_airport and date:
                    existing_pairs.add((from_airport, to_airport, date))
    except FileNotFoundError:
        print(f"CSV file {csv_file} not found.")
        return None

    # Find missing pairs
    missing = expected_pairs - existing_pairs
    missing_sorted = sorted(missing, key=lambda x: (x[0], x[1], x[2]))
    return missing_sorted

if __name__ == "__main__":
    csv_file = "best_flight_prices.csv"
    # Adjust the date range as needed
    start_year = 2025
    start_month = 6
    end_year = 2025
    end_month = 8

    missing_dates = check_missing_dates(csv_file, start_year, start_month, end_year, end_month)
    if missing_dates is None:
        print("No data to check.")
    elif not missing_dates:
        print("All dates have data for each country and direction.")
    else:
        print("Missing dates and routes:")
        for from_airport, to_airport, date in missing_dates:
            print(f"From {from_airport} to {to_airport} on {date}")
