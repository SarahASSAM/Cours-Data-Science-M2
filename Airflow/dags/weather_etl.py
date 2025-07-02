from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import os

# Liste des villes avec coordonnées
CITIES = {
    "Paris": {"lat": 48.85, "lon": 2.35},
    "London": {"lat": 51.51, "lon": -0.13},
    "Berlin": {"lat": 52.52, "lon": 13.41},
}

# Chemin du fichier CSV final
DATA_FILE = "/opt/airflow/data/weather_data.csv"

# ----------- ETAPE 1 : EXTRACTION -----------
def extract():
    records = []
    for city, coords in CITIES.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        response = requests.get(url)
        data = response.json()
        weather = data["current_weather"]
        record = {
            "city": city,
            "timestamp": datetime.utcnow().isoformat(),
            "temperature": weather["temperature"],
            "windspeed": weather["windspeed"],
            "weathercode": weather["weathercode"]
        }
        records.append(record)
    
    # Enregistrement temporaire
    pd.DataFrame(records).to_csv("/opt/airflow/data/temp_weather.csv", index=False)

# ----------- ETAPE 2 : TRANSFORMATION & LOAD -----------
def transform_and_load():
    new_df = pd.read_csv("/opt/airflow/data/temp_weather.csv")

    if not os.path.exists(DATA_FILE):
        new_df.to_csv(DATA_FILE, index=False)
    else:
        old_df = pd.read_csv(DATA_FILE)
        combined_df = pd.concat([old_df, new_df])
        combined_df.drop_duplicates(subset=["city", "timestamp"], inplace=True)
        combined_df.to_csv(DATA_FILE, index=False)

# ----------- DEFINITION DU DAG -----------
with DAG(
    dag_id="weather_etl",
    start_date=datetime(2025, 7, 1),
    schedule="0 8 * * *",  # Tous les jours à 8h UTC
    catchup=False,
    tags=["weather", "ETL"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract,
    )

    t2 = PythonOperator(
        task_id="transform_and_load_weather_data",
        python_callable=transform_and_load,
    )

    t1 >> t2  # Dépendance : d'abord extract, ensuite transform/load
