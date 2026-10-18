USE ROLE MARCHES_ROLE;
USE WAREHOUSE MARCHES_WH;

-- Format de fichier JSON, réutilisable pour tout le projet
CREATE FILE FORMAT IF NOT EXISTS MARCHES.RAW.JSON_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

-- Nom exact du fichier dans le bucket
LIST @MARCHES.RAW.GCS_RAW_STAGE;

-- Lecture du JSON (adapte le nom si besoin)
SELECT $1
FROM @MARCHES.RAW.GCS_RAW_STAGE/test.json
  (FILE_FORMAT => 'MARCHES.RAW.JSON_FORMAT');