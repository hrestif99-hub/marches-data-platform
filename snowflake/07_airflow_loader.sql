-- Chargement quotidien par Airflow : rôle MARCHES_LOADER + utilisateur de service AIRFLOW_USER
-- À lancer une fois, en entier, par le propriétaire du compte. N'est jamais lancé par dbt ni par un agent.
-- Prérequis : marches_project_setup.sql, 03_stage.sql (stage MARCHES.RAW.GCS_RAW_STAGE,
-- file format MARCHES.RAW.JSON_FORMAT), 04_raw_tables.sql (table MARCHES.RAW.EURUSD_ECB).
--
-- Périmètre volontairement étroit : MARCHES_LOADER ne fait QUE charger des fichiers du stage
-- dans les tables RAW du flux quotidien (COPY INTO) et effacer les lignes d'un fichier
-- rechargé (DELETE). Il ne crée, ne supprime ni ne modifie aucun objet (pas de CREATE,
-- DROP, TRUNCATE, ALTER), et n'a aucun accès à STAGING ni à MARTS.
-- dbt (lancé aussi par Airflow, étape E) aura besoin d'un autre rôle : décidé à l'étape E.

-- ---------------------------------------------------------------------------
-- 1. Rôle MARCHES_LOADER
-- ---------------------------------------------------------------------------
-- SECURITYADMIN : crée les rôles et utilisateurs, et a MANAGE GRANTS, donc peut donner des
-- droits sur des objets dont il n'est pas propriétaire (tables RAW de MARCHES_ROLE, stage
-- créé à la main).
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS MARCHES_LOADER
  COMMENT = 'Chargement quotidien GCS -> MARCHES.RAW (COPY INTO + DELETE par fichier). Utilisé par Airflow.';

-- Hiérarchie standard : SYSADMIN hérite des rôles applicatifs.
GRANT ROLE MARCHES_LOADER TO ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- 2. Droits
-- ---------------------------------------------------------------------------
-- Warehouse : USAGE seulement (AUTO_RESUME le démarre, pas besoin d'OPERATE).
GRANT USAGE ON WAREHOUSE MARCHES_WH TO ROLE MARCHES_LOADER;
GRANT USAGE ON DATABASE MARCHES TO ROLE MARCHES_LOADER;
GRANT USAGE ON SCHEMA MARCHES.RAW TO ROLE MARCHES_LOADER;

-- Lecture des fichiers GCS : USAGE sur le stage externe et sur le format de fichier.
-- (L'intégration GCS_MARCHES_INT n'est utile qu'à la CRÉATION du stage : pas de droit ici.)
GRANT USAGE ON STAGE MARCHES.RAW.GCS_RAW_STAGE TO ROLE MARCHES_LOADER;
GRANT USAGE ON FILE FORMAT MARCHES.RAW.JSON_FORMAT TO ROLE MARCHES_LOADER;

-- Table RAW du flux quotidien EUR/USD, table par table (pas de ALL TABLES ni de FUTURE) :
-- - INSERT : exigé par COPY INTO ;
-- - DELETE : effacer les lignes du fichier du jour avant de le recharger (08_daily_load_eurusd.sql) ;
-- - SELECT : contrôle du nombre de lignes chargées, et filtre WHERE du DELETE.
-- Pas de TRUNCATE : on n'efface jamais une table entière.
-- Pétrole puis bitcoin : ajouter les mêmes droits sur leur table au moment de leur flux quotidien.
GRANT SELECT, INSERT, DELETE ON TABLE MARCHES.RAW.EURUSD_ECB TO ROLE MARCHES_LOADER;

-- ---------------------------------------------------------------------------
-- 3. Utilisateur de service AIRFLOW_USER
-- ---------------------------------------------------------------------------
-- TYPE = SERVICE : pas de mot de passe ni de connexion interactive, authentification par
-- paire de clés uniquement. DEFAULT_SECONDARY_ROLES = () : la session n'a que son rôle
-- principal (plus PUBLIC), jamais l'union de tous ses rôles.
-- IF NOT EXISTS : si l'utilisateur existe déjà, ses propriétés ne sont PAS réappliquées ;
-- en cas de relance après modification, passer par ALTER USER AIRFLOW_USER SET ...
CREATE USER IF NOT EXISTS AIRFLOW_USER
  TYPE = SERVICE
  DEFAULT_ROLE = MARCHES_LOADER
  DEFAULT_SECONDARY_ROLES = ()
  DEFAULT_WAREHOUSE = MARCHES_WH
  DEFAULT_NAMESPACE = MARCHES.RAW
  COMMENT = 'Compte de service Airflow (local, Docker) : chargement quotidien RAW (rôle MARCHES_LOADER).';

GRANT ROLE MARCHES_LOADER TO USER AIRFLOW_USER;

-- Clé publique : générée en local par le propriétaire (script gen_airflow_key.py, hors repo,
-- dans C:/Users/hug56/.snowflake/), qui affiche la commande exacte à lancer ici :
-- ALTER USER AIRFLOW_USER SET RSA_PUBLIC_KEY = '<clé publique, une ligne, sans BEGIN/END>';
-- DESC USER AIRFLOW_USER;  -- RSA_PUBLIC_KEY_FP doit égaler l'empreinte affichée par le script

-- ---------------------------------------------------------------------------
-- 4. Vérification des droits (lecture seule)
-- ---------------------------------------------------------------------------
-- Attendu : USAGE (warehouse, base, schéma RAW, stage, file format) et
-- SELECT/INSERT/DELETE sur EURUSD_ECB, rien d'autre.
SHOW GRANTS TO ROLE MARCHES_LOADER;
SHOW GRANTS TO USER AIRFLOW_USER;

-- Pour tester le rôle sans la clé d'AIRFLOW_USER : on l'attribue aussi à l'utilisateur
-- administrateur, qui pourra lancer 08_daily_load_eurusd.sql avec ce seul rôle
-- (USE ROLE MARCHES_LOADER; USE SECONDARY ROLES NONE;). Retirer après vérification si souhaité :
-- REVOKE ROLE MARCHES_LOADER FROM USER "hrestif99";
GRANT ROLE MARCHES_LOADER TO USER "hrestif99";
