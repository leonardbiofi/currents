import requests
import datetime
import os
import pandas as pd
from dotenv import load_dotenv

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")

# === Load .env variables ===
load_dotenv()
API_KEY = os.getenv("IQAIR_API_KEY")

# Cities to query (city, state, country)
cities = [
    ("Los Angeles", "California", "USA"),
    ("Paris", "Ile-de-France", "France"),
    ("Delhi", "Delhi", "India"),
    ("Tokyo", "Tokyo", "Japan"),
    ("Zurich", "Zurich", "Switzerland"),
]

# Logging
# LOG_DIR = "aqair_logs"
# os.makedirs(LOG_DIR, exist_ok=True)
# LOG_FILE = os.path.join(LOG_DIR, "air_quality_log.txt")


def get_air_quality(city, state, country):
    url = "https://api.airvisual.com/v2/city"
    params = {"city": city, "state": state, "country": country, "key": API_KEY}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data["status"] == "success":
            pollution = data["data"]["current"]["pollution"]
            return {
                "city": city,
                "aqi": pollution["aqius"],
                "main_pollutant": pollution["mainus"],
                "timestamp": pollution["ts"],
            }
        else:
            return {"city": city, "error": data.get("data", "Unknown error")}
    except Exception as e:
        return {"city": city, "error": str(e)}


# def log_data(entries):
#     timestamp = datetime.datetime.utcnow().isoformat()
#     with open(LOG_FILE, "a") as f:
#         f.write(f"\n=== Data fetched at {timestamp} UTC ===\n")
#         for entry in entries:
#             if "error" in entry:
#                 f.write(f"{entry['city']}: ERROR - {entry['error']}\n")
#             else:
#                 f.write(
#                     f"{entry['city']}: AQI={entry['aqi']} | Main Pollutant={entry['main_pollutant']} | Time={entry['timestamp']}\n"
#                 )


def write_to_parquet(entry):
    city = entry["city"]
    city_dir = os.path.join(DATA_DIR, city.replace(" ", "_"))
    os.makedirs(city_dir, exist_ok=True)

    # Get current date for filename
    dt = datetime.datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
    file_date = dt.strftime("%Y-%m-%d")
    parquet_file = os.path.join(city_dir, f"{file_date}.parquet")

    # Convert entry to DataFrame
    df = pd.DataFrame([entry])

    # Append or create parquet file
    if os.path.exists(parquet_file):
        existing = pd.read_parquet(parquet_file)
        combined = pd.concat([existing, df], ignore_index=True)
        combined.to_parquet(parquet_file, index=False)
    else:
        df.to_parquet(parquet_file, index=False)


def main():
    results = []
    for city, state, country in cities:
        result = get_air_quality(city, state, country)
        results.append(result)
        if "error" not in result:
            write_to_parquet(result)

    # log_data(results)


if __name__ == "__main__":
    main()
