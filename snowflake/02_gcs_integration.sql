USE ROLE ACCOUNTADMIN;

-- Connexion sécurisée entre Snowflake et ton bucket GCS
CREATE STORAGE INTEGRATION IF NOT EXISTS GCS_MARCHES_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://marches-data-raw-hrestif/');

-- Le rôle du projet peut utiliser cette connexion
GRANT USAGE ON INTEGRATION GCS_MARCHES_INT TO ROLE MARCHES_ROLE;

-- Affiche le "badge" Google généré par Snowflake
DESC STORAGE INTEGRATION GCS_MARCHES_INT;