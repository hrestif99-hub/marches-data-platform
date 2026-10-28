-- Tables RAW + chargement depuis GCS (or, pétrole, EUR/USD)
-- Prérequis : 02_gcs_integration.sql, 03_stage.sql, stage MARCHES.RAW.GCS_RAW_STAGE
-- (racine du bucket gcs://marches-data-raw-hrestif/) et fichiers JSONL déjà dans le bucket.
USE ROLE MARCHES_ROLE;
USE WAREHOUSE MARCHES_WH;
USE SCHEMA MARCHES.RAW;

-- ---------------------------------------------------------------------------
-- 0. Droits nécessaires (optionnel, à lancer seulement si une erreur de droits apparaît)
-- ---------------------------------------------------------------------------
-- MARCHES_ROLE a reçu "GRANT ALL ON ALL SCHEMAS" dans marches_project_setup.sql
-- (donc CREATE TABLE sur RAW). Le stage a été créé hors repo : si un autre rôle
-- en est propriétaire, MARCHES_ROLE peut ne pas avoir USAGE dessus.
-- USE ROLE ACCOUNTADMIN;
-- GRANT CREATE TABLE ON SCHEMA MARCHES.RAW TO ROLE MARCHES_ROLE;
-- GRANT USAGE ON STAGE MARCHES.RAW.GCS_RAW_STAGE TO ROLE MARCHES_ROLE;
-- GRANT USAGE ON FILE FORMAT MARCHES.RAW.JSON_FORMAT TO ROLE MARCHES_ROLE;
-- USE ROLE MARCHES_ROLE;

-- ---------------------------------------------------------------------------
-- 1. Tables RAW : une par source, la ligne JSON est stockée telle quelle
-- ---------------------------------------------------------------------------
-- loaded_at : SYSDATE() renvoie l'heure courante en UTC (TIMESTAMP_NTZ), quel que
-- soit le paramètre TIMEZONE de la session, contrairement à CURRENT_TIMESTAMP().
-- Elle est posée en DEFAULT de colonne : le COPY ne la liste pas, Snowflake la remplit.

CREATE TABLE IF NOT EXISTS MARCHES.RAW.GOLD_BOURSORAMA (
  raw_data         VARIANT NOT NULL,
  file_name        VARCHAR,
  file_row_number  NUMBER,
  ingestion_date   DATE,
  loaded_at        TIMESTAMP_NTZ DEFAULT SYSDATE()
)
COMMENT = 'RAW - historique or (Boursorama, scraping unique). Une ligne JSONL par cotation, non transformée. Source : raw/gold/boursorama/';

CREATE TABLE IF NOT EXISTS MARCHES.RAW.OIL_EIA (
  raw_data         VARIANT NOT NULL,
  file_name        VARCHAR,
  file_row_number  NUMBER,
  ingestion_date   DATE,
  loaded_at        TIMESTAMP_NTZ DEFAULT SYSDATE()
)
COMMENT = 'RAW - prix spot pétrole (API EIA, séries WTI/Brent). Une ligne JSONL par cotation, non transformée. Source : raw/oil/eia/';

CREATE TABLE IF NOT EXISTS MARCHES.RAW.EURUSD_ECB (
  raw_data         VARIANT NOT NULL,
  file_name        VARCHAR,
  file_row_number  NUMBER,
  ingestion_date   DATE,
  loaded_at        TIMESTAMP_NTZ DEFAULT SYSDATE()
)
COMMENT = 'RAW - taux de change EUR/USD (API BCE). Une ligne JSONL par cotation, non transformée. Source : raw/eurusd/ecb/';

-- ---------------------------------------------------------------------------
-- 2. (Optionnel) Tester la lecture avant de charger
-- ---------------------------------------------------------------------------
-- VALIDATION_MODE n'est pas compatible avec un COPY qui transforme (SELECT ...).
-- Pour tester, on lit directement le stage (lecture seule) :
-- SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
-- FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/gold/boursorama/
--   (FILE_FORMAT => 'MARCHES.RAW.JSON_FORMAT',
--    PATTERN => '.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl')
-- LIMIT 10;
-- Ou un COPY sans transformation, qui ne charge rien et renvoie les erreurs :
-- COPY INTO MARCHES.RAW.GOLD_BOURSORAMA (raw_data)
--   FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/gold/boursorama/
--   FILE_FORMAT = (FORMAT_NAME = 'MARCHES.RAW.JSON_FORMAT')
--   PATTERN = '.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl'
--   VALIDATION_MODE = RETURN_ERRORS;

-- ---------------------------------------------------------------------------
-- 3. Chargement (COPY INTO)
-- ---------------------------------------------------------------------------
-- Idempotence : Snowflake garde pendant 64 jours les métadonnées de chargement de
-- chaque table (nom de fichier + ETag). Un fichier déjà chargé est ignoré
-- ("LOAD_SKIPPED"), donc relancer ces COPY ne crée pas de doublon.
-- Ne PAS utiliser FORCE = TRUE sur ces COPY : il recharge tous les fichiers et duplique les lignes.
-- Seule exception (flux quotidien, cf. CLAUDE.md) : FORCE limité à UN fichier via FILES = (...),
-- juste après un DELETE des lignes de ce même fichier, dans la même transaction.
-- Au-delà de 64 jours, le dédoublonnage se fait en STAGING (file_name + file_row_number).
--
-- PATTERN : dans une chaîne Snowflake, '\\d' devient \d pour le moteur regex.
-- [.] évite un échappement supplémentaire du point. Le motif doit couvrir tout le
-- chemin (d'où le .* initial) et n'attrape que les fichiers .jsonl d'une partition
-- ingestion_date=YYYY-MM-DD.
-- ingestion_date : extraite du chemin avec REGEXP_SUBSTR (paramètre 'e' = groupe 1).
-- ON_ERROR = ABORT_STATEMENT : la moindre ligne invalide annule le chargement du COPY.

COPY INTO MARCHES.RAW.GOLD_BOURSORAMA (raw_data, file_name, file_row_number, ingestion_date)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'ingestion_date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e'))
  FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/gold/boursorama/
)
FILE_FORMAT = (FORMAT_NAME = 'MARCHES.RAW.JSON_FORMAT')
PATTERN = '.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl'
ON_ERROR = ABORT_STATEMENT;

COPY INTO MARCHES.RAW.OIL_EIA (raw_data, file_name, file_row_number, ingestion_date)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'ingestion_date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e'))
  FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/oil/eia/
)
FILE_FORMAT = (FORMAT_NAME = 'MARCHES.RAW.JSON_FORMAT')
PATTERN = '.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl'
ON_ERROR = ABORT_STATEMENT;

COPY INTO MARCHES.RAW.EURUSD_ECB (raw_data, file_name, file_row_number, ingestion_date)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'ingestion_date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e'))
  FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/eurusd/ecb/
)
FILE_FORMAT = (FORMAT_NAME = 'MARCHES.RAW.JSON_FORMAT')
PATTERN = '.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl'
ON_ERROR = ABORT_STATEMENT;

-- ---------------------------------------------------------------------------
-- 4. Contrôles (lecture seule)
-- ---------------------------------------------------------------------------
-- Fichiers présents sous raw/
LIST @MARCHES.RAW.GCS_RAW_STAGE/raw/;

-- Comptage par table et par fichier. Attendu (fichiers du 2026-10-18) :
--   GOLD_BOURSORAMA : 5110 | OIL_EIA : 20271 | EURUSD_ECB : 7116
SELECT 'GOLD_BOURSORAMA' AS table_name, file_name, ingestion_date, COUNT(*) AS row_count
FROM MARCHES.RAW.GOLD_BOURSORAMA GROUP BY file_name, ingestion_date
UNION ALL
SELECT 'OIL_EIA', file_name, ingestion_date, COUNT(*)
FROM MARCHES.RAW.OIL_EIA GROUP BY file_name, ingestion_date
UNION ALL
SELECT 'EURUSD_ECB', file_name, ingestion_date, COUNT(*)
FROM MARCHES.RAW.EURUSD_ECB GROUP BY file_name, ingestion_date
ORDER BY table_name, file_name;

-- Plage de dates couverte par source (quote_date reste une chaîne en RAW)
SELECT 'GOLD_BOURSORAMA' AS table_name,
       MIN(raw_data:quote_date::STRING) AS min_quote_date,
       MAX(raw_data:quote_date::STRING) AS max_quote_date,
       MIN(loaded_at) AS first_loaded_at_utc
FROM MARCHES.RAW.GOLD_BOURSORAMA
UNION ALL
SELECT 'OIL_EIA', MIN(raw_data:quote_date::STRING), MAX(raw_data:quote_date::STRING), MIN(loaded_at)
FROM MARCHES.RAW.OIL_EIA
UNION ALL
SELECT 'EURUSD_ECB', MIN(raw_data:quote_date::STRING), MAX(raw_data:quote_date::STRING), MIN(loaded_at)
FROM MARCHES.RAW.EURUSD_ECB;

-- Historique des COPY sur 7 jours (statut, lignes chargées, erreurs)
SELECT table_name, file_name, status, row_count, row_parsed, first_error_message, last_load_time
FROM TABLE(MARCHES.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'MARCHES.RAW.GOLD_BOURSORAMA',
  START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())))
UNION ALL
SELECT table_name, file_name, status, row_count, row_parsed, first_error_message, last_load_time
FROM TABLE(MARCHES.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'MARCHES.RAW.OIL_EIA',
  START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())))
UNION ALL
SELECT table_name, file_name, status, row_count, row_parsed, first_error_message, last_load_time
FROM TABLE(MARCHES.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'MARCHES.RAW.EURUSD_ECB',
  START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;
