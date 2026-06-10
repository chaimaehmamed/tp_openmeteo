"""
Module fetch — appels HTTP vers l'API Open-Meteo.
Aucune transformation ici : on retourne le JSON brut tel quel.
"""

import logging
import requests

logger = logging.getLogger(__name__)

DAILY_FIELDS = "temperature_2m_max,temperature_2m_min,precipitation_sum,sunrise,sunset"


def fetch_city(city_name: str, coords: dict, forecast_days: int) -> dict:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={coords['latitude']}"
        f"&longitude={coords['longitude']}"
        "&current_weather=true"
        f"&daily={DAILY_FIELDS}"
        "&timezone=auto"
        f"&forecast_days={forecast_days}"
    )
    logger.info(f"[fetch] Appel API : {city_name}")
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    logger.info(f"[fetch] {city_name} — HTTP {response.status_code} OK")
    return response.json()


def fetch_all_cities(cities: list, city_coords: dict, forecast_days: int) -> dict:
    raw_responses = {}
    for city in cities:
        if city not in city_coords:
            raise ValueError(f"Ville inconnue : '{city}'. Disponibles : {list(city_coords)}")
        raw_responses[city] = fetch_city(city, city_coords[city], forecast_days)
    logger.info(f"[fetch] Total : {len(raw_responses)} ville(s) récupérée(s)")
    return raw_responses
