USE ROLE MARCHES_ROLE;
USE WAREHOUSE MARCHES_WH;

-- Stage externe sur la racine du bucket GCS.
-- Créé à la main à l'origine ; définition relevée par DESC STAGE le 2026-10-28 et versionnée
-- ici. Seuls l'URL et l'intégration diffèrent des valeurs par défaut : pas de FILE_FORMAT
-- attaché (le format JSON est passé à chaque COPY), copy options par défaut
-- (ON_ERROR = ABORT_STATEMENT, FORCE = FALSE, PURGE = FALSE), pas de table répertoire.
-- IF NOT EXISTS : sans effet sur le stage existant, sert seulement à le recréer à l'identique.
CREATE STAGE IF NOT EXISTS MARCHES.RAW.GCS_RAW_STAGE
  URL = 'gcs://marches-data-raw-hrestif/'
  STORAGE_INTEGRATION = GCS_MARCHES_INT;

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