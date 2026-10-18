-- Table RAW + chargement depuis GCS (bitcoin, Binance Vision, BTCUSDT journalier)
-- Prérequis : 02_gcs_integration.sql, 03_stage.sql, stage MARCHES.RAW.GCS_RAW_STAGE
-- (racine du bucket gcs://marches-data-raw-hrestif/) et fichier JSONL déjà dans le bucket :
-- raw/bitcoin/binance_vision/ingestion_date=2026-10-18/bitcoin_binance_vision_2026-10-18.jsonl
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
-- 1. Table RAW : la ligne JSON est stockée telle quelle
-- ---------------------------------------------------------------------------
-- loaded_at : SYSDATE() renvoie l'heure courante en UTC (TIMESTAMP_NTZ), quel que
-- soit le paramètre TIMEZONE de la session, contrairement à CURRENT_TIMESTAMP().
-- Elle est posée en DEFAULT de colonne : le COPY ne la liste pas, Snowflake la remplit.
--
-- À CONFIRMER : SYSDATE() accepté comme DEFAULT de colonne et bien appliqué lors
-- d'un COPY INTO qui ne liste pas la colonne. Si ce n'est pas le cas :
--   - alternative 1 : DEFAULT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ ;
--   - alternative 2 : retirer le DEFAULT et ajouter loaded_at à la liste des colonnes
--     du COPY, avec SYSDATE() (ou CONVERT_TIMEZONE(...) ci-dessus) dans le SELECT.

CREATE TABLE IF NOT EXISTS MARCHES.RAW.BITCOIN_BINANCE_VISION (
  raw_data         VARIANT NOT NULL,
  file_name        VARCHAR,
  file_row_number  NUMBER,
  ingestion_date   DATE,
  loaded_at        TIMESTAMP_NTZ DEFAULT SYSDATE()
)
COMMENT = 'RAW - bitcoin BTCUSDT journalier (Binance Vision, data.binance.vision). Licence CC BY-NC-SA 4.0 : usage privé non commercial, pas de rediffusion. Prix en USDT, pas en USD. Une ligne JSONL par cotation, non transformée. Source : raw/bitcoin/binance_vision/';

-- ---------------------------------------------------------------------------
-- 2. (Optionnel) Tester la lecture avant de charger
-- ---------------------------------------------------------------------------
-- VALIDATION_MODE n'est pas compatible avec un COPY qui transforme (SELECT ...).
-- Pour tester, on lit directement le stage (lecture seule) :
-- SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
-- FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/bitcoin/binance_vision/
--   (FILE_FORMAT => 'MARCHES.RAW.JSON_FORMAT',
--    PATTERN => '.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl')
-- LIMIT 10;
-- Ou un COPY sans transformation, qui ne charge rien et renvoie les erreurs :
-- COPY INTO MARCHES.RAW.BITCOIN_BINANCE_VISION (raw_data)
--   FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/bitcoin/binance_vision/
--   FILE_FORMAT = (FORMAT_NAME = 'MARCHES.RAW.JSON_FORMAT')
--   PATTERN = '.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl'
--   VALIDATION_MODE = RETURN_ERRORS;

-- ---------------------------------------------------------------------------
-- 3. Chargement (COPY INTO)
-- ---------------------------------------------------------------------------
-- Idempotence : Snowflake garde pendant 64 jours les métadonnées de chargement de
-- chaque table (nom de fichier + ETag). Un fichier déjà chargé est ignoré
-- ("LOAD_SKIPPED"), donc relancer ce COPY ne crée pas de doublon.
-- Ne PAS utiliser FORCE = TRUE : il recharge tous les fichiers et duplique les lignes.
-- Au-delà de 64 jours, le dédoublonnage se fait en STAGING (file_name + file_row_number).
--
-- PATTERN : dans une chaîne Snowflake, '\\d' devient \d pour le moteur regex.
-- [.] évite un échappement supplémentaire du point. Le motif doit couvrir tout le
-- chemin (d'où le .* initial) et n'attrape que les fichiers .jsonl d'une partition
-- ingestion_date=YYYY-MM-DD.
-- ingestion_date : extraite du chemin avec REGEXP_SUBSTR (paramètre 'e' = groupe 1).
--
-- À CONFIRMER : REGEXP_SUBSTR accepté dans le SELECT d'un COPY de transformation
-- (Snowflake n'autorise qu'un sous-ensemble de fonctions dans ce cas). Si le COPY
-- est refusé :
--   - alternative 1 : SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'ingestion_date=', 2), '/', 1)
--     à la place de REGEXP_SUBSTR ;
--   - alternative 2 : charger sans ingestion_date (colonne NULL) et la dériver de
--     file_name en STAGING.
-- ON_ERROR = ABORT_STATEMENT : la moindre ligne invalide annule le chargement du COPY.

COPY INTO MARCHES.RAW.BITCOIN_BINANCE_VISION (raw_data, file_name, file_row_number, ingestion_date)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'ingestion_date=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e'))
  FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/bitcoin/binance_vision/
)
FILE_FORMAT = (FORMAT_NAME = 'MARCHES.RAW.JSON_FORMAT')
PATTERN = '.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl'
ON_ERROR = ABORT_STATEMENT;

-- ---------------------------------------------------------------------------
-- 4. Contrôles (lecture seule)
-- ---------------------------------------------------------------------------
-- Fichiers présents sous raw/bitcoin/binance_vision/
LIST @MARCHES.RAW.GCS_RAW_STAGE/raw/bitcoin/binance_vision/;

-- Comptage par fichier. Attendu (fichier du 2026-10-18) :
--   ingestion_date=2026-10-18 : 3349
SELECT 'BITCOIN_BINANCE_VISION' AS table_name, file_name, ingestion_date, COUNT(*) AS row_count
FROM MARCHES.RAW.BITCOIN_BINANCE_VISION
GROUP BY file_name, ingestion_date
ORDER BY file_name;

-- Plage de dates couverte (quote_date reste une chaîne en RAW)
-- Attendu : min_quote_date = 2017-08-17 | max_quote_date = 2026-10-17
SELECT 'BITCOIN_BINANCE_VISION' AS table_name,
       MIN(raw_data:quote_date::STRING) AS min_quote_date,
       MAX(raw_data:quote_date::STRING) AS max_quote_date,
       MIN(loaded_at) AS first_loaded_at_utc
FROM MARCHES.RAW.BITCOIN_BINANCE_VISION;

-- Historique des COPY sur 7 jours (statut, lignes chargées, erreurs)
SELECT table_name, file_name, status, row_count, row_parsed, first_error_message, last_load_time
FROM TABLE(MARCHES.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'MARCHES.RAW.BITCOIN_BINANCE_VISION',
  START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;
