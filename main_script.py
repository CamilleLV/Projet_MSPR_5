import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime, timezone
import pytz

# Charger les variables d'environnement
load_dotenv()

# # Variables sensibles
AQ_API_KEY = os.getenv("AQ_API_KEY")
WM_API_KEY = os.getenv("WM_API_KEY")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# Charger les villes depuis le fichier JSON
with open("C:/Users/camil/OneDrive - Ifag Paris/Cours/MSPR_EID_BLOC_5/Projet_MSPR_5/ville_traitement.json", "r") as file:
    VILLES = json.load(file)["villes"]


# URLs des API
AQ_URL_TEMPLATE = "https://api.waqi.info/feed/{city}/?token={api_key}"
WM_URL_TEMPLATE = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&lang=fr&appid={api_key}"

def fetch_air_quality(city):
    url = AQ_URL_TEMPLATE.format(city=city, api_key=AQ_API_KEY)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur API AirQuality pour {city}: {response.status_code}")
        return None

def fetch_weather_map(lat, lon):

    # Définir le fuseau horaire (Paris, UTC+1 ou UTC+2 selon l'heure d'été)
    tz = pytz.timezone('Europe/Paris')

    # Obtenir la date actuelle avec fuseau horaire
    now = datetime.now(tz)

    date_du_jour = now.strftime('%Y-%m-%d')


    url = WM_URL_TEMPLATE.format(lat=lat, lon=lon, date=date_du_jour, api_key=WM_API_KEY)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur API WeatherMap pour {lat}, {lon}: {response.status_code}")
        return None

# Définir la fonction qui retourne le niveau de pollution et la description
def get_air_quality_level(aqi):

    if aqi <= 50:
        return 1
    elif 51 <= aqi <= 100:
        return 2
    elif 101 <= aqi <= 150:
        return 3
    elif 151 <= aqi <= 200:
        return 4
    elif 201 <= aqi <= 300:
        return 5
    else:  # aqi > 300
        return 6


def transform_air_quality(data, city):
    if not data or data.get("status") != "ok":
        return None
    
    aq_data = data["data"]

    # Ajouter le ":" dans le décalage horaire pour respecter le format ISO 8601
    date_formatee = datetime.today().strftime("%Y-%m-%d")
    df = pd.DataFrame([{  
        "Ville": city,
        # "Nom_Complet": aq_data["city"]["name"],
        "Information_Qualite_Air": aq_data["aqi"],
        "Indice_IQA_PM_25": aq_data["iaqi"].get("pm25", {}).get("v", None),
        "Indice_IQA_PM_10": aq_data["iaqi"].get("pm10", {}).get("v", None),
        "Indice_IQA_No2": aq_data["iaqi"].get("no2", {}).get("v", None),
        "Indice_IQA_So2": aq_data["iaqi"].get("so2", {}).get("v", None),
        "Indice_IQA_Ozone": aq_data["iaqi"].get("o3", {}).get("v", None),
        "Temperature (°C)": aq_data["iaqi"].get("t", {}).get("v", None),
        "Humidite_AQ": aq_data["iaqi"].get("h", {}).get("v", None),
        "Vent": aq_data["iaqi"].get("w", {}).get("v", None),
        "date": date_formatee
    }])
    return df

def transform_weather_map(data, city, date):
    if not data:
        return None
    
    sunrise = datetime.fromtimestamp(data["sys"]["sunrise"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    sunset = datetime.fromtimestamp(data["sys"]["sunset"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    # Création du DataFrame
    df = pd.DataFrame([{  
        "Ville": city,
        # "Date_Observation": date,
        # "Type_Temps": data["weather"][0]["main"],
        "Description_Temps": data["weather"][0]["description"],
        # "Température_Minimale": data["main"]["temp_min"],
        # "Température_Maximale": data["main"]["temp_max"],
        "Température_Ressentie (°C)": data["main"]["feels_like"],
        "Humidite_WM": data["main"]["humidity"],
        "Pression_Atmosphérique (hPa)": data["main"]["pressure"],
        "Niveau_Mer (hPa)": data["main"]["sea_level"],
        "Pression_Sol (hPa)": data["main"]["grnd_level"],
        "Vitesse_Vent (m/sec)": data["wind"]["speed"],
        "Direction_Vent (DEG)": data["wind"]["deg"],
        "Rafales_Vent (m/sec)": data["wind"].get("gust", 0),
        "Précipitations_1h (mm/h)": data.get("rain", {}).get("1h", 0),
        "Couverture_Nuageuse (%)": data["clouds"]["all"],
        "Heure_Lever_Soleil": sunrise,
        "Heure_Coucher_Soleil": sunset
    }])
    return df

def save_csv(df, filename):
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', header=False, index=False, encoding="utf-8-sig")
    else:
        df.to_csv(filename, mode='a', index=False, encoding="utf-8-sig")
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
    all_merged_df = []
    date_du_jour = datetime.today().strftime("%Y-%m-%d")

    save_path = "C:/Users/camil/OneDrive - Ifag Paris/Cours/MSPR_EID_BLOC_5/Projet_MSPR_5/"
    
    for ville in VILLES:
        city, lat, lon = ville["nom"], ville["lat"], ville["lon"]
        
        # Récupération des données
        aq_data = fetch_air_quality(city)
        weather_data = fetch_weather_map(lat, lon)  # Exemple avec une date fixe
        
        # Transformation des données
        print(f"Transformation des données de Air Quality pour : {city}")
        aq_df = transform_air_quality(aq_data, city)
        print(f"Transformation des données de WeatherMap pour : {city}")
        weather_df = transform_weather_map(weather_data, city, date_du_jour)
        
        # Fusion des DataFrames
        merged_df = pd.merge(aq_df, weather_df, on="Ville", how="outer")  # type: ignore


        # Remplacer les valeurs manquantes par None (NULL)
        cols_to_null = [
        "Information_Qualite_Air", 
        "Indice_IQA_PM_25", 
        "Indice_IQA_PM_10", 
        "Indice_IQA_No2", 
        "Indice_IQA_So2", 
        "Indice_IQA_Ozone", 
        "Temperature (°C)", 
        "Humidite_AQ", 
        "Vent", 
        "Température_Ressentie (°C)", 
        "Humidite_WM", 
        "Pression_Atmosphérique (hPa)", 
        "Niveau_Mer (hPa)", 
        "Pression_Sol (hPa)", 
        "Vitesse_Vent (m/sec)", 
        "Direction_Vent (DEG)", 
        "Rafales_Vent (m/sec)", 
        "Précipitations_1h (mm/h)", 
        "Couverture_Nuageuse (%)", 
        "Heure_Lever_Soleil", 
        "Heure_Coucher_Soleil"
    ]
        cols_avec_virgule = [
        "Information_Qualite_Air", 
        "Indice_IQA_PM_25", 
        "Indice_IQA_PM_10", 
        "Indice_IQA_No2", 
        "Indice_IQA_So2", 
        "Indice_IQA_Ozone", 
        "Temperature (°C)", 
        "Vent", 
        "Température_Ressentie (°C)", 
        "Pression_Atmosphérique (hPa)", 
        "Niveau_Mer (hPa)", 
        "Pression_Sol (hPa)", 
        "Vitesse_Vent (m/sec)", 
        "Direction_Vent (DEG)", 
        "Rafales_Vent (m/sec)", 
        "Précipitations_1h (mm/h)", 
        "Couverture_Nuageuse (%)",
        "Humidité (%)"
    ]


        print(merged_df.head())
        merged_df[cols_to_null] = merged_df[cols_to_null].fillna(pd.NA)

        # Formater les colonnes de lever et coucher du soleil en HH:MM:SS
        merged_df["Heure_Lever_Soleil"] = pd.to_datetime(merged_df["Heure_Lever_Soleil"]).dt.strftime('%H:%M:%S')
        merged_df["Heure_Coucher_Soleil"] = pd.to_datetime(merged_df["Heure_Coucher_Soleil"]).dt.strftime('%H:%M:%S')

        # Conversion des températures de Kelvin à Celsius (sauf Temperature qui est déjà en °C)
        temp_cols = ["Température_Ressentie (°C)"]
        merged_df[temp_cols] = merged_df[temp_cols].apply(lambda x: x - 273.15)

        # Arrondir la colonne "Température_Ressentie (°C)" à deux chiffres après la virgule
        merged_df["Température_Ressentie (°C)"] = merged_df["Température_Ressentie (°C)"].round(2)

        merged_df["Humidite_WM"] = pd.to_numeric(merged_df["Humidite_WM"])
        merged_df["Humidite_AQ"] = pd.to_numeric(merged_df["Humidite_AQ"])

        # Fusionner les colonnes "Humidite_WM" et "Humidite_AQ" en "Humidité (%)"
        merged_df["Humidité (%)"] = merged_df[["Humidite_WM", "Humidite_AQ"]].mean(axis=1)
        merged_df["Humidité (%)"] = merged_df["Humidité (%)"].round(2)

        # Si tu veux supprimer les colonnes originales après la fusion :
        merged_df = merged_df.drop(columns=["Humidite_WM", "Humidite_AQ"])

        # Ajouter la colonne Air_Quality_Level et Description
        merged_df[['Niveau_Qualite_Air']] = merged_df['Information_Qualite_Air'].apply(
            lambda x: pd.Series(get_air_quality_level(x))
        )
        
        ordered_columns = [
            "Ville", "date", "Description_Temps", "Niveau_Qualite_Air",
            "Temperature (°C)", "Température_Ressentie (°C)", "Humidité (%)", "Précipitations_1h (mm/h)", 
            "Couverture_Nuageuse (%)", "Information_Qualite_Air", "Indice_IQA_PM_25", "Indice_IQA_PM_10", 
            "Indice_IQA_No2", "Indice_IQA_So2", "Indice_IQA_Ozone", "Pression_Atmosphérique (hPa)", 
            "Niveau_Mer (hPa)", "Pression_Sol (hPa)", "Vent", "Vitesse_Vent (m/sec)", 
            "Direction_Vent (DEG)", "Rafales_Vent (m/sec)", "Heure_Lever_Soleil", "Heure_Coucher_Soleil"
        ]

        # Réorganiser le DataFrame selon l'ordre des colonnes
        merged_df = merged_df[ordered_columns]

        renamed_columns = [
            'Ville', 'date', 'Description_Temps', 'Niveau_Qualite_Air',
            'Temperature_C', 'Temperature_Ressentie_C', 'Humidite_pourcentage', 'Precipitations_1h_mmh',
            'Couverture_Nuageuse_pourcentage', 'Information_Qualite_Air', 'Indice_IQA_PM_25', 'Indice_IQA_PM_10',
            'Indice_IQA_No2', 'Indice_IQA_So2', 'Indice_IQA_Ozone', 'Pression_Atmospherique_hPa',
            'Niveau_Mer_hPa', 'Pression_Sol_hPa', 'Vent', 'Vitesse_Vent_msec', 'Direction_Vent_DEG',
            'Rafales_Vent_msec', 'Heure_Lever_Soleil', 'Heure_Coucher_Soleil'
        ]

        merged_df.columns = renamed_columns

        # Ajout aux listes
        if aq_df is not None:
            print(f"Ajout aux listes de air quality pour {city}")
            all_aq_data.append(aq_df)
        if weather_df is not None:
            print(f"Ajout aux listes de weathermap pour {city}")
            all_weather_data.append(weather_df)
        if merged_df is not None:
            print(f"Ajout aux listes de weathermap pour {city}")
            all_merged_df.append(merged_df)
    
    # Sauvegarde finale
    if all_aq_data:
        final_aq_df = pd.concat(all_aq_data, ignore_index=True)
        save_csv(final_aq_df, f"{save_path}Qualite_Air.csv")
        # save_to_gcs(final_aq_df, "Qualite_Air.csv")
    
    if all_weather_data:
        final_weather_df = pd.concat(all_weather_data, ignore_index=True)
        save_csv(final_weather_df, f"{save_path}Ville_Meteo.csv")
        # save_to_gcs(final_weather_df, "Ville_Meteo.csv")
    
    if all_merged_df:
        final_merged_df = pd.concat(all_merged_df, ignore_index=True)
        print(final_merged_df.columns.tolist())
        save_csv(final_merged_df, f"{save_path}Ville_Stat_Meteo_DEMO.csv")
        # save_to_gcs(final_merged_df, "Ville_Stat_Meteo.csv")


if __name__ == "__main__":
    main()