"""
weather_api.py

Récupère les données météo historiques horaires (température, pluie,
humidité, vent) pour New York via l'API Open-Meteo (Archive API), et les
écrit telles quelles (JSON brut, aucune transformation) dans la couche
Bronze.

Toute la configuration passe par des variables d'environnement (.env) —
aucun paramètre en dur, aucun script shell.

Variables d'environnement utilisées (avec valeurs par défaut) :
    WEATHER_LAT           latitude du point de mesure   (40.7128 = NYC)
    WEATHER_LON           longitude du point de mesure  (-74.0060 = NYC)
    WEATHER_START_DATE    date de début (YYYY-MM-DD)    (2025-01-01)
    WEATHER_END_DATE      date de fin   (YYYY-MM-DD)    (2026-05-31)
    WEATHER_OUTPUT_DIR    dossier de sortie local       (data/bronze/weather)

Usage :
    python ingestion/weather_api.py
"""

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv optionnel : si absent, on lit directement os.environ

# --- Configuration (100% via .env, aucune valeur en dur obligatoire) ------

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"

LAT = os.environ.get("WEATHER_LAT", "40.7128")
LON = os.environ.get("WEATHER_LON", "-74.0060")
START_DATE = os.environ.get("WEATHER_START_DATE", "2024-01-01")
END_DATE = os.environ.get("WEATHER_END_DATE", "2026-05-31")
OUTPUT_DIR = Path(os.environ.get("WEATHER_OUTPUT_DIR", "data/bronze/weather"))

HOURLY_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "relative_humidity_2m",
    "wind_speed_10m",
]


def fetch_weather(lat: str, lon: str, start_date: str, end_date: str) -> dict:
    """Interroge l'API Open-Meteo Archive et retourne la réponse JSON brute."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "America/New_York",
    }

    print(f"Requête Open-Meteo : {start_date} -> {end_date} (lat={lat}, lon={lon})")
    response = requests.get(ARCHIVE_API_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def save_raw(payload: dict, output_dir: Path, start_date: str, end_date: str) -> Path:
    """Écrit la réponse brute en JSON, sans aucune transformation."""
    output_dir.mkdir(parents=True, exist_ok=True)
    fetched_at = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"weather_{start_date}_{end_date}_{fetched_at}.json"
    filepath = output_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    return filepath


def main():
    try:
        payload = fetch_weather(LAT, LON, START_DATE, END_DATE)
    except requests.RequestException as e:
        print(f"[Erreur] Échec de la requête Open-Meteo : {e}", file=sys.stderr)
        sys.exit(1)

    filepath = save_raw(payload, OUTPUT_DIR, START_DATE, END_DATE)

    nb_records = len(payload.get("hourly", {}).get("time", []))
    file_size_kb = filepath.stat().st_size / 1024

    print(f"Fichier écrit : {filepath}")
    print(f"Enregistrements horaires récupérés : {nb_records:,}")
    print(f"Taille du fichier : {file_size_kb:.1f} Ko")


if __name__ == "__main__":
    main()