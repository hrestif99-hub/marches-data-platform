-- Accès BI en lecture seule (Power BI Desktop) : rôle BI_READER + utilisateur de service POWERBI_USER
-- À lancer une fois, en entier, par le propriétaire du compte. N'est jamais lancé par dbt ni par un agent.
-- Prérequis : marches_project_setup.sql (MARCHES_WH, base MARCHES, schéma MARCHES.MARTS).
--
-- Périmètre volontairement limité à MARTS : pas d'accès à RAW (données brutes, licences
-- des sources) ni à STAGING (intermédiaire, non stable pour l'analyse).

-- ---------------------------------------------------------------------------
-- 1. Rôle BI_READER
-- ---------------------------------------------------------------------------
-- Création de rôle et d'utilisateur : SECURITYADMIN suffit (moins de privilèges qu'ACCOUNTADMIN).
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS BI_READER
  COMMENT = 'Lecture seule sur MARCHES.MARTS pour les outils BI (Power BI).';

-- Hiérarchie standard : SYSADMIN hérite des rôles applicatifs (administration sans ACCOUNTADMIN).
GRANT ROLE BI_READER TO ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- 2. Droits : warehouse, base, schéma MARTS
-- ---------------------------------------------------------------------------
-- USAGE seulement : le rôle peut lancer des requêtes sur le warehouse, sans le modifier
-- (pas d'OPERATE ni de MODIFY : AUTO_RESUME suffit pour le démarrer).
GRANT USAGE ON WAREHOUSE MARCHES_WH TO ROLE BI_READER;
GRANT USAGE ON DATABASE MARCHES TO ROLE BI_READER;
GRANT USAGE ON SCHEMA MARCHES.MARTS TO ROLE BI_READER;

-- Tables et vues existantes de MARTS
GRANT SELECT ON ALL TABLES IN SCHEMA MARCHES.MARTS TO ROLE BI_READER;
GRANT SELECT ON ALL VIEWS IN SCHEMA MARCHES.MARTS TO ROLE BI_READER;

-- Tables et vues futures de MARTS.
-- Indispensable avec dbt : chaque `dbt build` fait CREATE OR REPLACE des tables MARTS,
-- ce qui crée de NOUVEAUX objets et efface les droits posés sur les anciens. Les future
-- grants sont appliqués automatiquement à chaque création, donc les droits survivent.
-- Ils exigent MANAGE GRANTS : SECURITYADMIN l'a.
-- Attention : des future grants posés au niveau de la BASE seraient ignorés pour MARTS,
-- ceux du schéma priment. Garder les droits BI au niveau du schéma.
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARCHES.MARTS TO ROLE BI_READER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MARCHES.MARTS TO ROLE BI_READER;

-- ---------------------------------------------------------------------------
-- 3. Utilisateur de service POWERBI_USER
-- ---------------------------------------------------------------------------
-- TYPE = SERVICE : pas de mot de passe ni de connexion interactive, authentification par
-- paire de clés uniquement. DEFAULT_SECONDARY_ROLES = () : aucun rôle secondaire, la
-- session n'a que BI_READER, plus PUBLIC dont tout rôle hérite (vérifié en section 4).
-- IF NOT EXISTS : si l'utilisateur existe déjà, ses propriétés ne sont PAS réappliquées ;
-- en cas de relance après modification, passer par ALTER USER POWERBI_USER SET ...
CREATE USER IF NOT EXISTS POWERBI_USER
  TYPE = SERVICE
  DEFAULT_ROLE = BI_READER
  DEFAULT_SECONDARY_ROLES = ()
  DEFAULT_WAREHOUSE = MARCHES_WH
  DEFAULT_NAMESPACE = MARCHES.MARTS
  COMMENT = 'Compte de service Power BI Desktop, lecture seule sur MARTS (rôle BI_READER).';

GRANT ROLE BI_READER TO USER POWERBI_USER;

-- Clé publique : générée en local par le propriétaire (script gen_powerbi_key.py, hors repo),
-- qui affiche la commande exacte à lancer ici :
-- ALTER USER POWERBI_USER SET RSA_PUBLIC_KEY = '<clé publique, une ligne, sans BEGIN/END>';
-- DESC USER POWERBI_USER;  -- RSA_PUBLIC_KEY_FP doit égaler l'empreinte affichée par le script

-- ---------------------------------------------------------------------------
-- 4. Vérification des droits (lecture seule)
-- ---------------------------------------------------------------------------
-- Pour tester le rôle BI_READER sans la clé de POWERBI_USER : on l'attribue aussi à
-- l'utilisateur administrateur, qui pourra ouvrir une session avec ce seul rôle
-- (USE ROLE BI_READER; USE SECONDARY ROLES NONE;). Retirer après vérification si souhaité :
-- REVOKE ROLE BI_READER FROM USER "hrestif99";
GRANT ROLE BI_READER TO USER "hrestif99";

SHOW GRANTS TO ROLE BI_READER;
SHOW FUTURE GRANTS IN SCHEMA MARCHES.MARTS;
SHOW GRANTS TO USER POWERBI_USER;
-- PUBLIC est hérité par tout rôle : il ne doit rien donner sur RAW ni STAGING.
SHOW GRANTS TO ROLE PUBLIC;
