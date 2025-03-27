import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime

# Charger les variables d'environnement
load_dotenv()

# Variables sensibles
AQ_API_KEY = os.getenv("AQ_API_KEY")
WM_API_KEY = os.getenv("WM_API_KEY")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# Charger les villes depuis le fichier JSON
with open("C:/Users/camil/OneDrive - Ifag Paris/Cours/MSPR_EID_BLOC_5/Projet_MSPR_5/ville_traitement.json", "r") as file:
    VILLES = json.load(file)["villes"]

# URLs des API
AQ_URL_TEMPLATE = "https://api.waqi.info/feed/{city}/?token={api_key}"
WM_URL_TEMPLATE = "https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat}&lon={lon}&date={date}&units=metric&appid={api_key}"

def fetch_air_quality(city):
    url = AQ_URL_TEMPLATE.format(city=city, api_key=AQ_API_KEY)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur API AirQuality pour {city}: {response.status_code}")
        return None

def fetch_weather_map(lat, lon, date):
    url = WM_URL_TEMPLATE.format(lat=lat, lon=lon, date=date, api_key=WM_API_KEY)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur API WeatherMap pour {lat}, {lon}: {response.status_code}")
        return None

def transform_air_quality(data, city):
    if not data or data.get("status") != "ok":
        return None
    
    aq_data = data["data"]
    df = pd.DataFrame([{  
        "city": city,
        "Nom complet": aq_data["city"]["name"],
        "aqi": aq_data["aqi"],
        "pm25": aq_data["iaqi"].get("pm25", {}).get("v", None),
        "pm10": aq_data["iaqi"].get("pm10", {}).get("v", None),
        "no2": aq_data["iaqi"].get("no2", {}).get("v", None),
        "so2": aq_data["iaqi"].get("so2", {}).get("v", None),
        "o3": aq_data["iaqi"].get("o3", {}).get("v", None),
        "temperature": aq_data["iaqi"].get("t", {}).get("v", None),
        "humidity": aq_data["iaqi"].get("h", {}).get("v", None),
        "wind": aq_data["iaqi"].get("w", {}).get("v", None),
        "date": aq_data["time"]["iso"]
    }])
    return df

def transform_weather_map(data, city, date):
    if not data:
        return None
    
    df = pd.DataFrame([{  
        "city": city,
        "date": date,
        "temperature_min": data["temperature"]["min"],
        "temperature_max": data["temperature"]["max"],
        "humidity": data["humidity"]["afternoon"],
        "pressure": data["pressure"]["afternoon"],
        "wind_speed": data["wind"]["max"]["speed"],
        "wind_direction": data["wind"]["max"]["direction"],
        "precipitation": data["precipitation"]["total"],
        "cloud_cover": data["cloud_cover"]["afternoon"]
    }])
    return df

def save_csv(df, filename):
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', header=False, index=False)
    else:
        df.to_csv(filename, index=False)
    print(f"Données sauvegardées en local : {filename}")

def save_to_gcs(df, filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(filename)
    
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f"Données sauvegardées dans GCS : {filename}")

def main():
    all_aq_data = []
    all_weather_data = []
    date_du_jour = datetime.today().strftime("%Y-%m-%d")

    save_path = "C:/Users/camil/OneDrive - Ifag Paris/Cours/MSPR_EID_BLOC_5/Projet_MSPR_5/"
    
    for ville in VILLES:
        city, lat, lon = ville["nom"], ville["lat"], ville["lon"]
        
        # Récupération des données
        aq_data = fetch_air_quality(city)
        weather_data = fetch_weather_map(lat, lon, "2025-02-12")  # Exemple avec une date fixe
        
        # Transformation des données
        print(f"Transformation des données de Air Quality pour : {city}")
        aq_df = transform_air_quality(aq_data, city)
        print(f"Transformation des données de WeatherMap pour : {city}")
        weather_df = transform_weather_map(weather_data, city, date_du_jour)
        
        # Ajout aux listes
        if aq_df is not None:
            print(f"Ajout aux listes de air quality pour {city}")
            all_aq_data.append(aq_df)
        if weather_df is not None:
            print(f"Ajout aux listes de weathermap pour {city}")
            all_weather_data.append(weather_df)
    
    # Sauvegarde finale
    if all_aq_data:
        final_aq_df = pd.concat(all_aq_data, ignore_index=True)
        save_csv(final_aq_df, f"{save_path}Qualite_Air.csv")
        # save_to_gcs(final_aq_df, "Qualite_Air.csv")
    
    if all_weather_data:
        final_weather_df = pd.concat(all_weather_data, ignore_index=True)
        save_csv(final_weather_df, f"{save_path}Ville_Meteo.csv")
        # save_to_gcs(final_weather_df, "Ville_Meteo.csv")

if __name__ == "__main__":
    main()