import pandas as pd
import time
import re
from check_date import check_missing_dates
from scraper import COUNTRIES

CSV_FILE = "best_flight_prices.csv"

def convert_stops(stops_str):
    if pd.isna(stops_str) or stops_str == "":
        return pd.NA
    if isinstance(stops_str, (int, float)):
        return int(stops_str) if pd.notna(stops_str) else pd.NA
    if isinstance(stops_str, str):
        stops_str = stops_str.lower().strip()
        if "nonstop" in stops_str:
            return 0
        match = re.search(r'(\d+)', stops_str)
        if match:
            try:
                return int(match.group(1))
            except Exception as e:
                print(f"Error converting Stops value '{stops_str}': {e}")
                return pd.NA
        else:
            print(f"Failed to parse Stops value: '{stops_str}'")
            return pd.NA
    print(f"Invalid Stops input type: '{stops_str}' (type: {type(stops_str)})")
    return pd.NA

def convert_duration_to_minutes(duration_str):
    if pd.isna(duration_str) or duration_str == "":
        return pd.NA
    if isinstance(duration_str, (int, float)):
        return int(duration_str) if pd.notna(duration_str) else pd.NA
    if not isinstance(duration_str, str):
        print(f"Invalid Flight Duration input type: '{duration_str}' (type: {type(duration_str)})")
        return pd.NA
    duration_str = duration_str.lower().replace(" ", "")
    hours = 0
    minutes = 0
    try:
        if "hr" in duration_str:
            hours_part = duration_str.split("hr")[0]
            hours_match = re.search(r'\d+', hours_part)
            if hours_match:
                hours = int(hours_match.group(0))
            else:
                raise ValueError("No hours found")
            if "min" in duration_str:
                minutes_part = duration_str.split("hr")[1].replace("min", "")
                minutes_match = re.search(r'\d+', minutes_part)
                minutes = int(minutes_match.group(0)) if minutes_match else 0
        elif "min" in duration_str:
            minutes_match = re.search(r'\d+', duration_str.replace("min", ""))
            if minutes_match:
                minutes = int(minutes_match.group(0))
            else:
                raise ValueError("No minutes found")
        return hours * 60 + minutes
    except Exception as e:
        print(f"Error converting Flight Duration value '{duration_str}': {e}")
        return pd.NA

def convert_price(price_str):
    if pd.isna(price_str) or price_str == "":
        return pd.NA
    if isinstance(price_str, (int, float)):
        return int(price_str) if pd.notna(price_str) else pd.NA
    if isinstance(price_str, str):
        cleaned = re.sub(r'[^\d.]', '', price_str)
        try:
            return float(cleaned)
        except Exception as e:
            print(f"Error converting Price value '{price_str}': {e}")
            return pd.NA
    print(f"Invalid Price input type: '{price_str}' (type: {type(price_str)})")
    return pd.NA

def convert_co2(co2_str):
    if pd.isna(co2_str) or co2_str == "":
        return pd.NA
    if isinstance(co2_str, (int, float)):
        return int(co2_str) if pd.notna(co2_str) else pd.NA
    if isinstance(co2_str, str):
        no_commas = co2_str.replace(',', '')
        match = re.search(r'(\d+\.?\d*)', no_commas)
        if match:
            try:
                return float(match.group(0))
            except Exception as e:
                print(f"Error converting co2 emissions value '{co2_str}': {e}")
                return pd.NA
        else:
            print(f"Failed to parse co2 emissions value: '{co2_str}'")
            return pd.NA
    print(f"Invalid co2 emissions input type: '{co2_str}' (type: {type(co2_str)})")
    return pd.NA

def reading_raw_data():
    try:
        df = pd.read_csv(CSV_FILE, encoding="utf-8")
    except FileNotFoundError:
        print(f"CSV file {CSV_FILE} not found.")
        return None

    # Drop unnecessary columns early
    df = df.drop(columns=["Departure Time", "Arrival Time", "Airline Company", "emissions variation"], errors='ignore')

    # Clean unwanted characters in all string columns
    def clean_text(value):
        if isinstance(value, str):
            return value.encode('ascii', 'ignore').decode('ascii').strip()
        return value

    print("Starting to read and clean data...")
    cleaned_rows = []
    counter = 0

    # Debug: Print first few rows of raw data
    print("Sample of raw data:")
    print(df.head(5).to_string())

    for index, row in df.iterrows():
        print(f"Reading row {counter + 1}: From {row.get('From', '')} to {row.get('To', '')} on {row.get('Date', '')}")
        cleaned_row = {col: (clean_text(row[col]) if col in row else "") for col in df.columns}
        cleaned_rows.append(cleaned_row)
        counter += 1
        time.sleep(0.01)

    df_cleaned = pd.DataFrame(cleaned_rows)

    # Ensure all columns are present
    expected_columns = ["From", "To", "Date", "Flight Duration", "Stops", "Price", "co2 emissions"]
    for col in expected_columns:
        if col not in df_cleaned.columns:
            df_cleaned[col] = ""

    # Convert critical columns to appropriate types
    df_cleaned["Date"] = pd.to_datetime(df_cleaned["Date"], errors='coerce')
    df_cleaned["Stops"] = df_cleaned["Stops"].apply(convert_stops).astype('Int64')
    df_cleaned["Flight Duration"] = df_cleaned["Flight Duration"].apply(convert_duration_to_minutes).astype('Int64')
    df_cleaned["Price"] = df_cleaned["Price"].apply(convert_price).astype('Int64')
    df_cleaned["co2 emissions"] = df_cleaned["co2 emissions"].apply(convert_co2).astype('Int64')

    # Use check_date to get missing entries
    start_year = 2025
    start_month = 6
    end_year = 2025
    end_month = 8

    missing_entries = check_missing_dates(CSV_FILE, start_year, start_month, end_year, end_month)
    if missing_entries is None:
        print("No data to check.")
        return df_cleaned

    # Calculate monthly averages
    monthly_averages = df_cleaned.groupby(df_cleaned["Date"].dt.to_period("M")).agg({
        "Stops": "mean",
        "Flight Duration": "mean",
        "Price": "mean",
        "co2 emissions": "mean"
    }).rename_axis("Month").reset_index()

    def build_row_with_averages(from_airport, to_airport, date, monthly_averages):
        month_period = date.to_period("M")
        avg_row = monthly_averages[monthly_averages["Month"] == month_period]
        if not avg_row.empty:
            avg_stops = int(round(avg_row["Stops"].values[0])) if pd.notna(avg_row["Stops"].values[0]) else None
            avg_duration = int(round(avg_row["Flight Duration"].values[0])) if pd.notna(avg_row["Flight Duration"].values[0]) else None
            avg_price = int(round(avg_row["Price"].values[0])) if pd.notna(avg_row["Price"].values[0]) else None
            avg_co2 = int(round(avg_row["co2 emissions"].values[0])) if pd.notna(avg_row["co2 emissions"].values[0]) else None
        else:
            avg_stops = avg_duration = avg_price = avg_co2 = None

        new_row = {col: "" for col in expected_columns}
        new_row["From"] = from_airport
        new_row["To"] = to_airport
        new_row["Date"] = date
        new_row["Stops"] = avg_stops
        new_row["Flight Duration"] = avg_duration
        new_row["Price"] = avg_price
        new_row["co2 emissions"] = avg_co2

        return new_row

    # Build rows for missing dates
    existing_keys = set(zip(df_cleaned["From"], df_cleaned["To"], df_cleaned["Date"].dt.strftime('%Y-%m-%d')))
    added_count = 0
    for from_airport, to_airport, date_str in missing_entries:
        key = (from_airport, to_airport, date_str)
        if key not in existing_keys:
            date = pd.to_datetime(date_str)
            new_row = build_row_with_averages(from_airport, to_airport, date, monthly_averages)
            df_cleaned = pd.concat([df_cleaned, pd.DataFrame([new_row])], ignore_index=True)
            added_count += 1
            print(f"Added missing row with averages: From {from_airport} to {to_airport} on {date}")

    print(f"Data reading complete. Total raw rows in DataFrame: {len(df_cleaned)}")
    return df_cleaned

def clean_data(df=None):
    if df is None:
        try:
            df = pd.read_csv(CSV_FILE, encoding="utf-8")
        except FileNotFoundError:
            print(f"CSV file {CSV_FILE} not found.")
            return None

    # Drop unnecessary columns early
    df = df.drop(columns=["Departure Time", "Arrival Time", "Airline Company", "emissions variation"], errors='ignore')

    print("Starting data cleaning...")
    print(f"Columns before conversion: {df.columns.tolist()}")
    if "Date" not in df.columns:
        print("Error: 'Date' column missing in input dataframe.")
        return None

    # Convert critical columns
    df["Stops"] = df["Stops"].apply(convert_stops).astype('Int64')
    df["Flight Duration"] = df["Flight Duration"].apply(convert_duration_to_minutes).astype('Int64')
    df["Price"] = df["Price"].apply(convert_price).astype('Int64')
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df["co2 emissions"] = df["co2 emissions"].apply(convert_co2).astype('Int64')

    print(f"After converting Stops, NaNs count: {df['Stops'].isna().sum()}")
    print(f"After converting Flight Duration, NaNs count: {df['Flight Duration'].isna().sum()}")
    print(f"After converting Price, NaNs count: {df['Price'].isna().sum()}")
    print(f"After converting Date, NaTs count: {df['Date'].isna().sum()}")
    print(f"After converting co2 emissions, NaNs count: {df['co2 emissions'].isna().sum()}")


    # Debug: Print rows with NaN in critical columns
    critical_columns = ["Stops", "Flight Duration", "Price", "co2 emissions"]
    nan_rows = df[df[critical_columns].isna().any(axis=1)]
    if not nan_rows.empty:
        print(f"Rows with NaN in {critical_columns}:")
        print(nan_rows[["From", "To", "Date", "Stops", "Flight Duration", "Price", "co2 emissions"]].to_string())

    # Remove rows with NaN in critical columns
    critical_columns = ["From", "To", "Date"]
    df = df.dropna(subset=critical_columns)
    print(f"After dropping NaNs in {critical_columns}, remaining rows: {len(df)}")

    if df.empty:
        print("No data remaining after dropping NaNs in critical columns. Exiting clean_data.")
        return df

    # Remove duplicates
    df = df.drop_duplicates(subset=["From", "To", "Date"])
    print(f"After dropping duplicates, remaining rows: {len(df)}")

    # Filter by allowed airports
    allowed_airports = {airport for _, airport in COUNTRIES}
    def is_allowed_route(row):
        return (row.get("From") in allowed_airports) or (row.get("To") in allowed_airports)

    df = df[df.apply(is_allowed_route, axis=1)]
    print(f"After filtering allowed routes, remaining rows: {len(df)}")

    # Calculate monthly averages
    df["Month"] = df["Date"].dt.to_period("M")
    monthly_averages = df.groupby(["From", "To", "Month"]).agg({
        "Stops": "mean",
        "Flight Duration": "mean",
        "Price": "mean",
        "co2 emissions": "mean"
    }).reset_index()

    # Fill missing or zero values with monthly averages
    def is_missing_or_zero(row):
        return (
            pd.isna(row["Stops"]) or row["Stops"] == 0 or
            pd.isna(row["Flight Duration"]) or row["Flight Duration"] == 0 or
            pd.isna(row["Price"]) or row["Price"] == 0 or
            pd.isna(row["co2 emissions"]) or row["co2 emissions"] == 0
        )

    missing_mask = df.apply(is_missing_or_zero, axis=1)
    missing_rows = df[missing_mask]

    for idx, row in missing_rows.iterrows():
        from_airport = row["From"]
        to_airport = row["To"]
        month = row["Month"]
        avg_row = monthly_averages[
            (monthly_averages["From"] == from_airport) &
            (monthly_averages["To"] == to_airport) &
            (monthly_averages["Month"] == month)
        ]
        if not avg_row.empty:
            if pd.isna(row["Stops"]) or row["Stops"] == 0:
                df.at[idx, "Stops"] = int(round(avg_row["Stops"].values[0])) if pd.notna(avg_row["Stops"].values[0]) else pd.NA
            if pd.isna(row["Flight Duration"]) or row["Flight Duration"] == 0:
                df.at[idx, "Flight Duration"] = int(round(avg_row["Flight Duration"].values[0])) if pd.notna(avg_row["Flight Duration"].values[0]) else pd.NA
            if pd.isna(row["Price"]) or row["Price"] == 0:
                df.at[idx, "Price"] = int(round(avg_row["Price"].values[0])) if pd.notna(avg_row["Price"].values[0]) else pd.NA
            if pd.isna(row["co2 emissions"]) or row["co2 emissions"] == 0:
                df.at[idx, "co2 emissions"] = int(round(avg_row["co2 emissions"].values[0])) if pd.notna(avg_row["co2 emissions"].values[0]) else pd.NA

    # Drop Month column
    df = df.drop(columns=["Month"])

    # Sort by 'From' and 'Date'
    if "From" in df.columns and "Date" in df.columns:
        df = df.sort_values(by=["From", "Date"]).reset_index(drop=True)
    else:
        print("Warning: 'From' or 'Date' column missing, skipping sort.")

    # Save cleaned data
    df.to_csv("clean.csv", index=False)
    print("Cleaned data saved to clean.csv")
    print(f"Data cleaning complete. Total cleaned rows in DataFrame: {len(df)}")
    return df

if __name__ == "__main__":
    df_raw = reading_raw_data()
    if df_raw is not None:
        df_cleaned = clean_data(df_raw)