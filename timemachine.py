import requests
import time
import csv
import json
import datetime
from tqdm import tqdm  # Progress bar

# Load cities from JSON
with open("ville_traitement.json", "r", encoding="utf-8") as f:
    villes = json.load(f)["villes"]

# API parameters
API_KEY = "YOUR_API_KEY"  # Replace with your actual API key

# Define date range
start_date = datetime.datetime(1979, 1, 1, tzinfo=datetime.timezone.utc)
end_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=4)  # Up to 4 days ago

# CSV filename
csv_filename = "historical_weather_villes.csv"

# Create CSV file with headers
with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "ville", "date", "latitude", "longitude", "timezone", "temp", "feels_like",
        "pressure", "humidity", "dew_point", "uvi", "clouds", "visibility",
        "wind_speed", "wind_deg", "weather_main", "weather_description"
    ])

# Progress bar setup
total_days = (end_date - start_date).days
total_requests = total_days * len(villes)
pbar = tqdm(total=total_requests, desc="Fetching data")

# Fetch data for each city
for ville in villes:
    ville_nom = ville["nom"]
    lat = ville["lat"]
    lon = ville["lon"]
    
    current_date = start_date

    while current_date <= end_date:
        unix_timestamp = int(current_date.timestamp())  # Convert to UNIX timestamp
        url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={unix_timestamp}&appid={API_KEY}"

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            timezone_str = data.get("timezone", "N/A")

            for entry in data.get("data", []):
                row = [
                    ville_nom,
                    datetime.datetime.fromtimestamp(int(entry["dt"]), tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),  # 🔧 FIXED HERE
                    lat,
                    lon,
                    timezone_str,
                    entry["temp"],
                    entry["feels_like"],
                    entry["pressure"],
                    entry["humidity"],
                    entry["dew_point"],
                    entry["uvi"],
                    entry["clouds"],
                    entry["visibility"],
                    entry["wind_speed"],
                    entry["wind_deg"],
                    entry["weather"][0]["main"] if "weather" in entry else "N/A",
                    entry["weather"][0]["description"] if "weather" in entry else "N/A"
                ]

                # Append data to CSV
                with open(csv_filename, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(row)

            print(f"✅ {ville_nom} - Data fetched for {current_date.strftime('%Y-%m-%d')}")
        else:
            print(f"❌ {ville_nom} - Error ({response.status_code}) for {current_date.strftime('%Y-%m-%d')}")

        # Avoid rate limits
        time.sleep(1.5)

        # Move to next day
        current_date += datetime.timedelta(days=1)
        pbar.update(1)

pbar.close()
print(f"✅ Data retrieval complete! Data saved in {csv_filename}")
