-- ============================================================
-- TP 5 — Initialisation base weather_v2
-- ============================================================

-- Données météo transformées (table cible)
CREATE TABLE IF NOT EXISTS weather_data (
    id                      SERIAL PRIMARY KEY,
    city_name               VARCHAR(100)    NOT NULL,
    fetch_timestamp         TIMESTAMPTZ     NOT NULL,
    forecast_date           DATE            NOT NULL,
    temp_current_c          NUMERIC(5,1),
    windspeed_current_kmh   NUMERIC(5,1),
    weathercode             SMALLINT,
    temp_max_c              NUMERIC(5,1),
    temp_min_c              NUMERIC(5,1),
    precipitation_sum_mm    NUMERIC(6,1),
    sunrise_local           VARCHAR(25),
    sunset_local            VARCHAR(25),
    UNIQUE (city_name, forecast_date, fetch_timestamp)
);

-- Archive des JSON bruts (référence fichier + métadonnées)
CREATE TABLE IF NOT EXISTS raw_archive (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(250)    NOT NULL UNIQUE,
    execution_date  DATE            NOT NULL,
    archive_path    TEXT            NOT NULL,
    cities          TEXT[]          NOT NULL,
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- Traçabilité de chaque exécution du pipeline
CREATE TABLE IF NOT EXISTS ingestion_log (
    id                  SERIAL PRIMARY KEY,
    dag_id              VARCHAR(200)    NOT NULL,
    run_id              VARCHAR(250)    NOT NULL UNIQUE,
    execution_date      TIMESTAMPTZ     NOT NULL,
    cities_processed    TEXT[],
    rows_loaded         INTEGER         DEFAULT 0,
    quality_status      VARCHAR(20)     NOT NULL,   -- 'passed' | 'failed'
    quality_issues      TEXT[],
    pipeline_status     VARCHAR(20)     NOT NULL,   -- 'success' | 'quality_failure'
    duration_sec        NUMERIC(8,2),
    created_at          TIMESTAMPTZ     DEFAULT NOW()
);
