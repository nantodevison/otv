-- New script in postgres.
-- Date: 4 oct. 2023
-- Time: 14:07:24

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*
 * Script de création de la table acceuillant les résultats de tests de RTest
*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/

/* ===============
 * Schema et table
 ================= */

-- Si beson créé le schéma
CREATE SCHEMA rtest ; 

--création du type énuméré de gestion des natures de reverse geocoding
CREATE TYPE rtest.reverse_geoloc_type_enum AS ENUM ('StreetAddress', 'CadastralParcel', 'PointOfInterest', 'Administratif', 'PasDeGeomSource') ;
--creation de la table
CREATE TABLE rtest.geoloc_inverse_ign 
 (id serial PRIMARY key,
  id_objet_test integer NOT NULL,
  "GeolocInverse.Numero" varchar,
  "GeolocInverse.Rue" varchar,
  "GeolocInverse.Commune" varchar,
  "GeolocInverse.Departement" varchar,
  "GeolocInverse.CodeCommuneInsee" varchar,
  "GeolocInverse.CodePostal" varchar,
  "GeolocInverse.DistanceObjet" real,
  "GeolocInverse.x_wgs84" real,
  "GeolocInverse.y_wgs84" real,
  "GeolocInverse.ParcelleComplete" varchar,
  "GeolocInverse.Feuille" varchar,
  "GeolocInverse.Section" varchar,
  "GeolocInverse.CommuneAbsorbee" varchar,
  "GeolocInverse.Arrondissement" varchar,
  "GeolocInverse.Type" varchar,
  "GeolocInverse.Nom" varchar,
  "GeolocInverse.Nature" varchar,
  "GeolocInverse.typeGeoloc" rtest.reverse_geoloc_type_enum) ;
  
SELECT coalesce("GeolocInverse.Numero", "GeolocInverse.Rue", "GeolocInverse.Commune", "GeolocInverse.Departement", "GeolocInverse.CodeCommuneInsee", "GeolocInverse.CodePostal", "GeolocInverse.ParcelleComplete", "GeolocInverse.Feuille", "GeolocInverse.Section", "GeolocInverse.CommuneAbsorbee", "GeolocInverse.Arrondissement", "GeolocInverse.Type", "GeolocInverse.Nom", "GeolocInverse.Nature") IS NULL AND coalesce("GeolocInverse.DistanceObjet", "GeolocInverse.x_wgs84", "GeolocInverse.y_wgs84") IS null
 FROM rtest.geoloc_inverse_ign
