"""
TP 5 — Pipeline industrialisé Open-Meteo

Fonctionnalités :
  retries / retry_delay / execution_timeout sur les tâches sensibles
  archivage des données brutes en JSON + référence en base
  contrôle qualité avec branchement conditionnel
  chargement PostgreSQL idempotent (ON CONFLICT DO NOTHING)
  traçabilité complète via ingestion_log
  paramétrage via Variables Airflow (villes, jours, simulation anomalie)

Variables Airflow requises (Admin → Variables) :
  weather_cities              : ["Paris","Lyon","Marseille","Bordeaux"]
  weather_forecast_days       : 1
  simulate_quality_failure    : false   ← passer à "true" pour simuler une anomalie

Connexion requise :
  weather_db (postgresql://weather:weather@postgres-weather:5432/weather)
  → injectée automatiquement via AIRFLOW_CONN_WEATHER_DB dans docker-compose
"""

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from weather.archive import save_archive
from weather.fetch import fetch_all_cities
from weather.quality import check_all_rows
from weather.transform import transform_all

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------- #
# Configuration                                                           #
# --------------------------------------------------------------------- #

WEATHER_CONN_ID = "weather_db"

CITY_COORDS = {
    "Paris":     {"latitude": 48.8566,  "longitude":  2.3522},
    "Lyon":      {"latitude": 45.7640,  "longitude":  4.8357},
    "Marseille": {"latitude": 43.2965,  "longitude":  5.3698},
    "Bordeaux":  {"latitude": 44.8378,  "longitude": -0.5792},
    "Lille":     {"latitude": 50.6292,  "longitude":  3.0573},
    "Nantes":    {"latitude": 47.2184,  "longitude": -1.5536},
}

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

# --------------------------------------------------------------------- #
# Tâche 1 — Récupération brute                                           #
# --------------------------------------------------------------------- #

def task_fetch_raw_weather(**context):
    cities        = Variable.get("weather_cities", deserialize_json=True,
                                 default_var=["Paris", "Lyon", "Marseille", "Bordeaux"])
    forecast_days = int(Variable.get("weather_forecast_days", default_var=1))

    logger.info(f"[fetch] Villes={cities} | forecast_days={forecast_days}")
    raw_responses = fetch_all_cities(cities, CITY_COORDS, forecast_days)

    context["ti"].xcom_push(key="raw_responses", value=raw_responses)
    context["ti"].xcom_push(key="cities", value=cities)


# --------------------------------------------------------------------- #
# Tâche 2 — Archivage des données brutes                                 #
# --------------------------------------------------------------------- #

def task_archive_raw_data(**context):
    raw_responses  = context["ti"].xcom_pull(key="raw_responses", task_ids="fetch_raw_weather")
    cities         = context["ti"].xcom_pull(key="cities",        task_ids="fetch_raw_weather")
    execution_date = context["ds"]
    run_id         = context["run_id"]

    archive_path = save_archive(raw_responses, execution_date, run_id)

    hook   = PostgresHook(postgres_conn_id=WEATHER_CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO raw_archive (run_id, execution_date, archive_path, cities)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (run_id) DO NOTHING
    """, (run_id, execution_date, archive_path, cities))
    conn.commit()
    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="archive_path", value=archive_path)


# --------------------------------------------------------------------- #
# Tâche 3 — Transformation                                               #
# --------------------------------------------------------------------- #

def task_transform_data(**context):
    raw_responses    = context["ti"].xcom_pull(key="raw_responses", task_ids="fetch_raw_weather")
    fetch_ts         = context["ts"]
    simulate_failure = Variable.get("simulate_quality_failure", default_var="false").lower() == "true"

    if simulate_failure:
        logger.warning("[transform] Mode anomalie activé — injection de valeurs invalides")

    rows = transform_all(raw_responses, fetch_ts, simulate_failure=simulate_failure)
    context["ti"].xcom_push(key="prepared_rows", value=rows)


# --------------------------------------------------------------------- #
# Tâche 4 — Contrôle qualité                                             #
# --------------------------------------------------------------------- #

def task_check_data_quality(**context):
    rows = context["ti"].xcom_pull(key="prepared_rows", task_ids="transform_data")

    is_valid, issues = check_all_rows(rows)
    context["ti"].xcom_push(key="quality_passed", value=is_valid)
    context["ti"].xcom_push(key="quality_issues",  value=issues)

    status = "PASSÉ ✓" if is_valid else f"ÉCHOUÉ ✗ ({len(issues)} anomalie(s))"
    logger.info(f"[quality] {status}")


# --------------------------------------------------------------------- #
# Tâche 5 — Branchement conditionnel                                     #
# --------------------------------------------------------------------- #

def task_branch_on_quality(**context):
    quality_passed = context["ti"].xcom_pull(key="quality_passed", task_ids="check_data_quality")
    next_task = "load_to_postgresql" if quality_passed else "log_quality_failure"
    logger.info(f"[branch] → {next_task}")
    return next_task


# --------------------------------------------------------------------- #
# Tâche 6a — Chargement PostgreSQL (branche nominale)                    #
# --------------------------------------------------------------------- #

def task_load_to_postgresql(**context):
    rows = context["ti"].xcom_pull(key="prepared_rows", task_ids="transform_data")

    hook   = PostgresHook(postgres_conn_id=WEATHER_CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT INTO weather_data (
            city_name, fetch_timestamp, forecast_date,
            temp_current_c, windspeed_current_kmh, weathercode,
            temp_max_c, temp_min_c, precipitation_sum_mm,
            sunrise_local, sunset_local
        ) VALUES (
            %(city_name)s, %(fetch_timestamp)s, %(forecast_date)s,
            %(temp_current_c)s, %(windspeed_current_kmh)s, %(weathercode)s,
            %(temp_max_c)s, %(temp_min_c)s, %(precipitation_sum_mm)s,
            %(sunrise_local)s, %(sunset_local)s
        )
        ON CONFLICT (city_name, forecast_date, fetch_timestamp) DO NOTHING;
    """, rows)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM weather_data")
    total_in_db = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="rows_inserted", value=len(rows))
    logger.info(f"[load] {len(rows)} ligne(s) insérée(s) | Total en base : {total_in_db}")


# --------------------------------------------------------------------- #
# Tâche 7a — Log succès ingestion                                        #
# --------------------------------------------------------------------- #

def task_log_ingestion_success(**context):
    cities       = context["ti"].xcom_pull(key="cities",        task_ids="fetch_raw_weather")
    rows_inserted = context["ti"].xcom_pull(key="rows_inserted", task_ids="load_to_postgresql")

    start    = context["ti"].start_date
    duration = round((datetime.now(timezone.utc) - start).total_seconds(), 2) if start else None

    hook   = PostgresHook(postgres_conn_id=WEATHER_CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO ingestion_log
            (dag_id, run_id, execution_date, cities_processed,
             rows_loaded, quality_status, quality_issues, pipeline_status, duration_sec)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO NOTHING
    """, (
        context["dag"].dag_id,
        context["run_id"],
        context["ts"],
        cities,
        rows_inserted or 0,
        "passed",
        [],
        "success",
        duration,
    ))
    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"[log] SUCCESS — {rows_inserted} ligne(s) chargée(s) en {duration}s")


# --------------------------------------------------------------------- #
# Tâche 6b — Log anomalie qualité (branche alternative)                  #
# --------------------------------------------------------------------- #

def task_log_quality_failure(**context):
    cities = context["ti"].xcom_pull(key="cities",         task_ids="fetch_raw_weather")
    issues = context["ti"].xcom_pull(key="quality_issues", task_ids="check_data_quality")

    start    = context["ti"].start_date
    duration = round((datetime.now(timezone.utc) - start).total_seconds(), 2) if start else None

    logger.error("[log] Pipeline BLOQUÉ — anomalie qualité, aucune donnée chargée")
    for issue in (issues or []):
        logger.error(f"  ✗ {issue}")

    hook   = PostgresHook(postgres_conn_id=WEATHER_CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO ingestion_log
            (dag_id, run_id, execution_date, cities_processed,
             rows_loaded, quality_status, quality_issues, pipeline_status, duration_sec)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO NOTHING
    """, (
        context["dag"].dag_id,
        context["run_id"],
        context["ts"],
        cities,
        0,
        "failed",
        issues or [],
        "quality_failure",
        duration,
    ))
    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"[log] QUALITY_FAILURE tracé en base ({len(issues or [])} anomalie(s))")


# --------------------------------------------------------------------- #
# Définition du DAG                                                       #
# --------------------------------------------------------------------- #

with DAG(
    dag_id="weather_pipeline_v2",
    description="Pipeline industrialisé Open-Meteo avec QC, branchement et traçabilité",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["meteo", "postgresql", "quality", "tp5"],
) as dag:

    fetch     = PythonOperator(
        task_id="fetch_raw_weather",
        python_callable=task_fetch_raw_weather,
        retries=3,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=5),
    )
    archive   = PythonOperator(
        task_id="archive_raw_data",
        python_callable=task_archive_raw_data,
    )
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform_data,
    )
    quality   = PythonOperator(
        task_id="check_data_quality",
        python_callable=task_check_data_quality,
    )
    branch    = BranchPythonOperator(
        task_id="branch_on_quality",
        python_callable=task_branch_on_quality,
    )
    load      = PythonOperator(
        task_id="load_to_postgresql",
        python_callable=task_load_to_postgresql,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    log_ok    = PythonOperator(
        task_id="log_ingestion_success",
        python_callable=task_log_ingestion_success,
    )
    log_fail  = PythonOperator(
        task_id="log_quality_failure",
        python_callable=task_log_quality_failure,
    )

    # Flux principal
    fetch >> archive >> transform >> quality >> branch
    # Branche nominale
    branch >> load >> log_ok
    # Branche anomalie
    branch >> log_fail
