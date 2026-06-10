# TP 5 — Industrialisation d'un pipeline Airflow Open-Meteo

## Description du pipeline

Conception d’un pipeline ETL orchestré par Apache Airflow exploitant l’API Open-Meteo pour l’acquisition automatisée de données météorologiques multi-villes. Le pipeline assure l’extraction, l’archivage, la transformation et le contrôle qualité des données avant leur intégration dans PostgreSQL. Toute anomalie détectée bloque le processus de chargement et déclenche une traçabilité des erreurs afin de garantir la qualité des données.

---

## Schéma du workflow

```
fetch_raw_weather
      │
      ▼
archive_raw_data
      │
      ▼
transform_data
      │
      ▼
check_data_quality
      │
      ▼
branch_on_quality
     ╱ ╲
    ╱   ╲
   ▼     ▼
load_to_  log_quality_
postgresql  failure
   │
   ▼
log_ingestion_
success
```

---

## Structure du projet

```
tp5/
├── docker-compose.yml
├── .env
├── sql/
│   └── init.sql                      #3 tables PostgreSQL
├── dags/
│   ├── weather_pipeline_v2.py        # DAG principal
│   └── weather/                      # Modules Python séparés
│       ├── __init__.py
│       ├── fetch.py                  # Appels API Open-Meteo
│       ├── transform.py              # Normalisation vers schéma cible
│       ├── quality.py                # Contrôles qualité
│       └── archive.py                # Sauvegarde JSON brut
├── logs/
├── plugins/
```

---

## Variables Airflow utilisées

| Variable | Valeur par défaut | Description |
|---|---|---|
| `weather_cities` | `["Paris","Lyon","Marseille","Bordeaux"]` | Liste des villes à ingérer |
| `weather_forecast_days` | `1` | Nombre de jours de prévision (1–7) |
| `simulate_quality_failure` | `false` | Passer à `"true"` pour simuler une anomalie qualité |

Définies automatiquement au démarrage via `airflow-init`.
Modifiables dans l'UI : **Admin → Variables**.

---

## Connexions Airflow utilisées

| Conn ID | Type | Valeur |
|---|---|---|
| `weather_db` | PostgreSQL | `postgresql://weather:weather@postgres-weather:5432/weather` |

Injectée via la variable d'environnement `AIRFLOW_CONN_WEATHER_DB` dans `docker-compose.yml`.

---

## Description des tâches du DAG

| Tâche | Type | Rôle |
|---|---|---|
| `fetch_raw_weather` | PythonOperator | Lit les Variables Airflow, appelle Open-Meteo pour chaque ville, pousse les JSON bruts dans XCom |
| `archive_raw_data` | PythonOperator | Sauvegarde les JSON bruts sur disque (`/tmp/weather_archive/`) et enregistre la référence dans `raw_archive` |
| `transform_data` | PythonOperator | Normalise les JSON vers le schéma de la table cible. Peut injecter une anomalie si `simulate_quality_failure=true` |
| `check_data_quality` | PythonOperator | Applique les règles qualité sur chaque ligne. Pousse `quality_passed` et `quality_issues` dans XCom |
| `branch_on_quality` | BranchPythonOperator | Lit `quality_passed` et retourne l'ID de la prochaine tâche |
| `load_to_postgresql` | PythonOperator | INSERT idempotent dans `weather_data` (branche nominale) |
| `log_ingestion_success` | PythonOperator | Écrit une ligne `status=success` dans `ingestion_log` |
| `log_quality_failure` | PythonOperator | Écrit une ligne `status=quality_failure` dans `ingestion_log` et loggue les anomalies (branche alternative) |

---

## Stratégie de robustesse

| Mécanisme | Configuration |
|---|---|
| Retries fetch | `retries=3`, `retry_delay=2min`, `execution_timeout=5min` |
| Retries load | `retries=2`, `retry_delay=1min` |
| Retries défaut | `retries=2`, `retry_delay=2min` (via `default_args`) |
| Timeout global | `execution_timeout=10min` (toutes les tâches) |
| Erreur API | `requests.raise_for_status()` → exception → retry automatique |
| Erreur ville inconnue | `ValueError` explicite dans `fetch.py` |

---

## Stratégie d'idempotence

| Table | Mécanisme |
|---|---|
| `weather_data` | `UNIQUE (city_name, forecast_date, fetch_timestamp)` + `ON CONFLICT DO NOTHING` |
| `raw_archive` | `UNIQUE (run_id)` + `ON CONFLICT DO NOTHING` |
| `ingestion_log` | `UNIQUE (run_id)` + `ON CONFLICT DO NOTHING` |

Rejouer le même DAG ne crée aucun doublon dans aucune table.

---

## Contrôles qualité mis en place

Définis dans `dags/weather/quality.py` :

| Règle | Condition |
|---|---|
| Champs obligatoires | `city_name`, `forecast_date`, `temp_max_c`, `temp_min_c` non null |
| Plage température max/min | Entre -50°C et 60°C |
| Plage température actuelle | Entre -50°C et 60°C (si renseignée) |
| Cohérence thermique | `temp_max_c >= temp_min_c` |
| Plage vent | Entre 0 et 200 km/h (si renseigné) |
| Précipitations | >= 0 mm |

---

## Règle de branchement conditionnel

`branch_on_quality` (BranchPythonOperator) :

```python
if quality_passed == True  →  "load_to_postgresql"
if quality_passed == False →  "log_quality_failure"
```

Si la branche `load_to_postgresql` est choisie, `log_quality_failure` est **skipped** automatiquement par Airflow.
Si la branche `log_quality_failure` est choisie, `load_to_postgresql` et `log_ingestion_success` sont **skipped**.

---

## Description des logs produits

Chaque tâche produit des logs applicatifs via `logging.getLogger(__name__)` :

| Préfixe | Tâche | Exemple |
|---|---|---|
| `[fetch]` | fetch_raw_weather | `[fetch] Paris — HTTP 200 OK` |
| `[archive]` | archive_raw_data | `[archive] JSON brut sauvegardé : /tmp/weather_archive/...` |
| `[transform]` | transform_data | `[transform] 4 ligne(s) transformée(s)` |
| `[quality]` | check_data_quality | `[quality] PASSÉ ✓ — 4 lignes valides` |
| `[branch]` | branch_on_quality | `[branch] → load_to_postgresql` |
| `[load]` | load_to_postgresql | `[load] 4 ligne(s) insérée(s) \| Total en base : 4` |
| `[log]` | log_ingestion_success | `[log] SUCCESS — 4 ligne(s) chargée(s) en 3.2s` |
| `[log]` | log_quality_failure | `[log] QUALITY_FAILURE tracé en base (2 anomalie(s))` |

---

## Tables PostgreSQL

### `weather_data` — données météo chargées

| Colonne | Type | Description |
|---|---|---|
| `city_name` | VARCHAR | Nom de la ville |
| `fetch_timestamp` | TIMESTAMPTZ | Horodatage du fetch |
| `forecast_date` | DATE | Date de la prévision |
| `temp_current_c` | NUMERIC | Température actuelle (jour J uniquement) |
| `windspeed_current_kmh` | NUMERIC | Vent actuel (jour J uniquement) |
| `weathercode` | SMALLINT | Code WMO |
| `temp_max_c` / `temp_min_c` | NUMERIC | Plage thermique journalière |
| `precipitation_sum_mm` | NUMERIC | Précipitations totales |
| `sunrise_local` / `sunset_local` | VARCHAR | Lever/coucher du soleil |

### `raw_archive` — références aux JSON bruts

| Colonne | Type | Description |
|---|---|---|
| `run_id` | VARCHAR (UNIQUE) | Identifiant du run Airflow |
| `execution_date` | DATE | Date d'exécution |
| `archive_path` | TEXT | Chemin du fichier JSON brut |
| `cities` | TEXT[] | Villes archivées |

### `ingestion_log` — traçabilité des exécutions

| Colonne | Type | Description |
|---|---|---|
| `run_id` | VARCHAR (UNIQUE) | Identifiant du run |
| `cities_processed` | TEXT[] | Villes traitées |
| `rows_loaded` | INTEGER | Lignes insérées (0 si anomalie) |
| `quality_status` | VARCHAR | `passed` ou `failed` |
| `quality_issues` | TEXT[] | Liste des anomalies détectées |
| `pipeline_status` | VARCHAR | `success` ou `quality_failure` |
| `duration_sec` | NUMERIC | Durée totale du pipeline |

---

## Lancer l'environnement

```powershell
cd tp5
docker compose up -d
```

Interface → http://localhost:8083 — `admin` / `admin`

### Cas nominal

Déclencher le DAG avec le bouton **▶** (Variables par défaut).

### Cas anomalie qualité

1. **Admin → Variables** → `simulate_quality_failure` → passer à `true`
2. Déclencher le DAG
3. Observer le branchement vers `log_quality_failure`
4. Remettre à `false` ensuite

### Vérifier les tables

```powershell
# Données chargées
docker exec tp5-postgres-weather-1 psql -U weather -d weather -c \
  "SELECT city_name, forecast_date, temp_max_c, temp_min_c FROM weather_data ORDER BY city_name;"

# Log de traçabilité
docker exec tp5-postgres-weather-1 psql -U weather -d weather -c \
  "SELECT pipeline_status, quality_status, rows_loaded, cities_processed, duration_sec FROM ingestion_log;"

# Archive
docker exec tp5-postgres-weather-1 psql -U weather -d weather -c \
  "SELECT run_id, execution_date, cities FROM raw_archive;"
```

---

### Cas relance — Preuve d'idempotence
docker exec tp5-postgres-weather-1 psql -U weather -d weather -c "SELECT city_name, forecast_date, COUNT(*) FROM weather_data GROUP BY city_name, forecast_date ORDER BY city_name;"
>> 
 city_name | forecast_date | count 
-----------+---------------+-------
 Bordeaux  | 2026-06-10    |     3
 Lyon      | 2026-06-10    |     3
 Marseille | 2026-06-10    |     3
 Paris     | 2026-06-10    |     3
(4 rows)



---

