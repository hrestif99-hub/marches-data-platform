-- Chargement quotidien EUR/USD : UN fichier GCS du jour -> MARCHES.RAW.EURUSD_ECB
-- Lancé chaque jour ouvré par Airflow (étape E), après la Cloud Function eurusd-daily.
-- Test manuel possible par le propriétaire du compte, avec le rôle MARCHES_LOADER
-- (07_airflow_loader.sql) ; jamais lancé par un agent.
--
-- Fichier chargé (écrit par la Cloud Function, cf. CLAUDE.md « Flux quotidien ») :
--   raw/eurusd/ecb/ingestion_date=<D>/eurusd_ecb_daily_<D>.jsonl
-- Les fichiers d'historique (eurusd_ecb_<D>.jsonl, sans _daily_) ne sont jamais touchés.
--
-- Idempotence (relance du même jour) : dans UNE transaction,
--   1. DELETE des lignes RAW venant de CE fichier précis (jamais toute l'ingestion_date) ;
--   2. COPY INTO de CE seul fichier, FILES = (...) + FORCE = TRUE.
-- FORCE est indispensable : le fichier du jour, réécrit à la relance, garde le même nom ;
-- sans FORCE, Snowflake pourrait le juger « déjà chargé » et ne rien recharger, alors que
-- ses lignes viennent d'être effacées. FORCE ne vaut ici que pour ce fichier (FILES) : sur
-- tout le préfixe, il rechargerait tous les fichiers et dupliquerait RAW.
-- Résultat : toujours une seule copie du fichier en RAW, quel que soit le nombre de relances.
--
-- Atomicité : dans Snowflake, une instruction qui échoue DANS une transaction n'annule pas
-- la transaction ; un COMMIT ultérieur validerait donc le DELETE seul (lignes perdues).
-- D'où le bloc EXCEPTION : toute erreur (COPY en échec, fichier absent, 0 ligne) fait un
-- ROLLBACK explicite, puis relève l'erreur pour qu'Airflow marque la tâche en échec.
--
-- Bloc anonyme Snowflake Scripting dans EXECUTE IMMEDIATE $$ ... $$ : forme acceptée à la
-- fois par Snowsight et par le connecteur Python (utilisé par Airflow).
-- La SEULE valeur à changer est la date du DECLARE.
--
-- Depuis Airflow (étape E), trois règles :
-- - n'envoyer QUE le bloc EXECUTE IMMEDIATE $$ ... $$ (un seul statement) : pas les USE
--   ni le SELECT de contrôle ; rôle et warehouse viennent des valeurs par défaut
--   d'AIRFLOW_USER (MARCHES_LOADER, MARCHES_WH) ;
-- - autocommit=True sur la tâche : sinon une transaction déjà ouverte par la session
--   entre en conflit avec le BEGIN TRANSACTION / COMMIT du bloc ;
-- - date = {{ data_interval_end | ds }}, la MÊME expression que celle envoyée à la Cloud
--   Function, sinon on chargerait un autre fichier que celui qui vient d'être écrit.
-- La date est collée textuellement dans le DECLARE : seule une valeur YYYY-MM-DD issue
-- d'Airflow doit y arriver. Ensuite, le typage DATE garantit que le COPY construit plus
-- bas ne reçoit qu'une date bien formée.

USE ROLE MARCHES_LOADER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE MARCHES_WH;

EXECUTE IMMEDIATE $$
DECLARE
  load_date DATE DEFAULT '2026-10-27';  -- date du fichier à charger (YYYY-MM-DD)
  date_str       VARCHAR;
  relative_path  VARCHAR;  -- chemin relatif au préfixe du FROM (pour FILES = ...)
  full_file_name VARCHAR;  -- chemin relatif à la racine du stage (= METADATA$FILENAME)
  copy_sql       VARCHAR;
  deleted_rows   INTEGER DEFAULT 0;
  loaded_rows    INTEGER DEFAULT 0;
  empty_load EXCEPTION (-20001, 'Aucune ligne chargée pour le fichier du jour : fichier absent ou vide. Rien n''a été modifié.');
BEGIN
  date_str       := TO_VARCHAR(load_date, 'YYYY-MM-DD');
  relative_path  := 'ingestion_date=' || date_str || '/eurusd_ecb_daily_' || date_str || '.jsonl';
  full_file_name := 'raw/eurusd/ecb/' || relative_path;

  -- ingestion_date est écrite directement (au lieu du REGEXP_SUBSTR de 04_raw_tables.sql) :
  -- c'est la même valeur, puisque le chemin est construit à partir d'elle.
  copy_sql := 'COPY INTO MARCHES.RAW.EURUSD_ECB (raw_data, file_name, file_row_number, ingestion_date) '
           || 'FROM ( SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, '
           || '''' || date_str || '''::DATE '
           || 'FROM @MARCHES.RAW.GCS_RAW_STAGE/raw/eurusd/ecb/ ) '
           || 'FILES = (''' || relative_path || ''') '
           || 'FILE_FORMAT = (FORMAT_NAME = ''MARCHES.RAW.JSON_FORMAT'') '
           || 'FORCE = TRUE '
           || 'ON_ERROR = ABORT_STATEMENT';

  BEGIN TRANSACTION;

  DELETE FROM MARCHES.RAW.EURUSD_ECB WHERE file_name = :full_file_name;
  deleted_rows := SQLROWCOUNT;

  EXECUTE IMMEDIATE :copy_sql;

  -- Contrôle : le fichier du jour doit avoir apporté au moins une ligne. Sinon (fichier
  -- absent du bucket, ou COPY qui n'a rien chargé) on annule, DELETE compris.
  SELECT COUNT(*) INTO :loaded_rows
  FROM MARCHES.RAW.EURUSD_ECB
  WHERE file_name = :full_file_name;

  IF (loaded_rows = 0) THEN
    RAISE empty_load;
  END IF;

  COMMIT;

  RETURN 'EURUSD_ECB : ' || loaded_rows || ' lignes chargées, ' || deleted_rows
      || ' lignes remplacées, fichier ' || full_file_name;

EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    RAISE;
END;
$$;

-- ---------------------------------------------------------------------------
-- Contrôles (lecture seule)
-- ---------------------------------------------------------------------------
-- Une ligne par fichier : le fichier quotidien ne doit apparaître qu'avec son nombre de
-- lignes (11 pour le 2026-10-27), même après plusieurs relances ; l'historique
-- (eurusd_ecb_2026-10-18.jsonl, 7116 lignes) doit être inchangé.
SELECT file_name, ingestion_date, COUNT(*) AS row_count,
       COUNT(DISTINCT file_row_number) AS distinct_rows, MAX(loaded_at) AS last_loaded_at_utc
FROM MARCHES.RAW.EURUSD_ECB
GROUP BY file_name, ingestion_date
ORDER BY file_name;
