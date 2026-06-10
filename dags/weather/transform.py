"""
Module transform — normalisation du JSON brut vers le schéma de la table cible.
current_weather (snapshot instantané) est attaché uniquement au jour J (index 0).
"""

import logging

logger = logging.getLogger(__name__)


def transform_city_day(city_name: str, data: dict, fetch_ts: str, day_index: int) -> dict:
    current = data["current_weather"]
    daily   = data["daily"]
    return {
        "city_name":             city_name,
        "fetch_timestamp":       fetch_ts,
        "forecast_date":         daily["time"][day_index],
        "temp_current_c":        current["temperature"] if day_index == 0 else None,
        "windspeed_current_kmh": current["windspeed"]   if day_index == 0 else None,
        "weathercode":           current["weathercode"] if day_index == 0 else None,
        "temp_max_c":            daily["temperature_2m_max"][day_index],
        "temp_min_c":            daily["temperature_2m_min"][day_index],
        "precipitation_sum_mm":  daily["precipitation_sum"][day_index],
        "sunrise_local":         daily["sunrise"][day_index],
        "sunset_local":          daily["sunset"][day_index],
    }


def transform_all(raw_responses: dict, fetch_ts: str, simulate_failure: bool = False) -> list:
    rows = []
    for city_name, data in raw_responses.items():
        for i in range(len(data["daily"]["time"])):
            rows.append(transform_city_day(city_name, data, fetch_ts, i))

    if simulate_failure and rows:
        logger.warning("[transform] Injection anomalie qualité (simulate_quality_failure=true)")
        rows[0]["temp_max_c"] = 999.0    # valeur impossible — déclenche le contrôle qualité
        rows[0]["temp_min_c"] = -999.0

    logger.info(f"[transform] {len(rows)} ligne(s) transformée(s)")
    return rows
