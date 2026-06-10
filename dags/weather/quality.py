"""
Module quality — contrôles qualité sur les lignes transformées.

Règles appliquées :
  - Champs obligatoires non null : city_name, forecast_date, temp_max_c, temp_min_c
  - temp_max_c et temp_min_c dans [-50, 60] °C
  - temp_current_c dans [-50, 60] °C (si renseigné)
  - windspeed_current_kmh dans [0, 200] km/h (si renseigné)
  - precipitation_sum_mm >= 0
  - temp_max_c >= temp_min_c
"""

import logging

logger = logging.getLogger(__name__)

TEMP_MIN    = -50.0
TEMP_MAX    =  60.0
WIND_MAX    = 200.0
PRECIP_MIN  =   0.0


def check_row(row: dict) -> list:
    issues = []
    label = f"{row.get('city_name','?')}/{row.get('forecast_date','?')}"

    for field in ["city_name", "forecast_date", "temp_max_c", "temp_min_c"]:
        if row.get(field) is None:
            issues.append(f"{label} — champ obligatoire manquant : {field}")

    for field in ["temp_max_c", "temp_min_c"]:
        val = row.get(field)
        if val is not None and not (TEMP_MIN <= val <= TEMP_MAX):
            issues.append(f"{label} — {field} hors plage [{TEMP_MIN},{TEMP_MAX}] : {val}°C")

    val = row.get("temp_current_c")
    if val is not None and not (TEMP_MIN <= val <= TEMP_MAX):
        issues.append(f"{label} — temp_current_c hors plage : {val}°C")

    val = row.get("windspeed_current_kmh")
    if val is not None and not (0 <= val <= WIND_MAX):
        issues.append(f"{label} — windspeed hors plage [0,{WIND_MAX}] : {val} km/h")

    val = row.get("precipitation_sum_mm")
    if val is not None and val < PRECIP_MIN:
        issues.append(f"{label} — precipitation négative : {val} mm")

    tmax = row.get("temp_max_c")
    tmin = row.get("temp_min_c")
    if tmax is not None and tmin is not None and tmax < tmin:
        issues.append(f"{label} — temp_max_c ({tmax}) < temp_min_c ({tmin})")

    return issues


def check_all_rows(rows: list) -> tuple:
    if not rows:
        return False, ["Aucune donnée à valider"]

    all_issues = []
    for row in rows:
        all_issues.extend(check_row(row))

    is_valid = len(all_issues) == 0

    if is_valid:
        logger.info(f"[quality] Contrôle PASSÉ ✓ — {len(rows)} lignes valides")
    else:
        logger.warning(f"[quality] Contrôle ÉCHOUÉ ✗ — {len(all_issues)} anomalie(s) sur {len(rows)} lignes")
        for issue in all_issues:
            logger.warning(f"  ✗ {issue}")

    return is_valid, all_issues
