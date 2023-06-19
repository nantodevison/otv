-- Active: 1682597730739@@127.0.0.1@5432@otv

/*===================================================================================================================
 * SCRIPTS DE MMIGRATION DES COMPTEURS, COMPTAGES, INDIC_AGREGE, INDIC_AGREGE, INDIC_AGREGE ET SCHEMA COMPTAGE_ASSOC 
 * DANS UN SCHEMA COVID SPECIFIQUE
 *===================================================================================================================*/

/* ------------------------
 * Vérifications préalables
 *-------------------------*/

/*
 * Schéma comptage_assoc
 */

-- Recherche des comptages_assoc.compteur où le comptage_asso et identique au comptage_ref : 10 résultats
with 
comptage2020 as (
select *
 from comptage_assoc.comptage 
 where annee = '2020')
select ce.id_cpteur_asso,
      (regexp_match (ce.id_cpteur_asso, '(.*(\.|\+))([0-9]+)$'))[1]||(regexp_match (ce.id_cpteur_asso, '(.*(\.|\+))([0-9]+)$'))[3]::int + 1
 from comptage_assoc.compteur ce join comptage2020 ca using(id_cpteur_asso)
 where ce.id_cpteur_asso = ce.id_cpteur_ref ;

-- on utilise ça pour modifier les nom de id_cpteur_asso sur la base du fichier créé:
-- Transfert_Donnees\Sql\Migration_annee_covid_comptageAssocMapping_id_cpteur_asso.txt
WITH
mappage as (
select *
 from (values 
  ('Angouleme-6_BD_ALLENDE-0.1671;45.6472', 'Angouleme-6_BD_ALLENDE-0.1671;45.6473'),
  ('Angouleme-bd_bury-0.1654;45.6476', 'Angouleme-bd_bury-0.1654;45.6477'),
  ('Angouleme-bd_liédot-0.1700;45.6464', 'Angouleme-bd_liédot-0.1700;45.6465'),
  ('Angouleme-av_gambetta-0.1622;45.6513', 'Angouleme-av_gambetta-0.1622;45.6514'),
  ('LimMet-avenue emile zola-1.3394;45.9199', 'LimMet-avenue emile zola-1.3394;45.9200'),
  ('LimMet-avenue maryse bastie-1.3294;45.8762', 'LimMet-avenue maryse bastie-1.3294;45.8763'),
  ('LimMet-charmeaux-1.3624;45.866', 'LimMet-charmeaux-1.3624;45.867'),
  ('LimMet-rue daniel gelin-1.2688;45.8878', 'LimMet-rue daniel gelin-1.2688;45.8879'),
  ('LimMet-rue de beaupuy-1.2506;45.8379', 'LimMet-rue de beaupuy-1.2506;45.8380'),
  ('LimMet-rue martin nadaud-1.3193;45.9013', 'LimMet-rue martin nadaud-1.3193;45.9014')) v(id_cpteur_asso, id_cpteur_asso_modif)),
comptage2020 as (
select *
 from comptage_assoc.comptage 
 where annee = '2020'),
compteur2020 as (
select ce.*
 from comptage_assoc.compteur ce join comptage2020 ca using(id_cpteur_asso)),
compteur2020_map as (
select m.id_cpteur_asso_modif id_comptag, c.geom, c.route, c.pr, c.abs, c.type_poste, c.techno, c.src_geo, c.obs_geo, c.obs_supl,
       c.src_cpt, c.convention, c.sens_cpt, c.id_cpt, c.id_sect
 from compteur2020 c join mappage m using(id_cpteur_asso)),
compteur2020_notmap as (
select c.id_cpteur_asso id_comptag, c.geom, c.route, c.pr, c.abs, c.type_poste, c.techno, c.src_geo, c.obs_geo, c.obs_supl,
       c.src_cpt, c.convention, c.sens_cpt, c.id_cpt, c.id_sect
 from compteur2020 c left join mappage m using(id_cpteur_asso)
 where m.id_cpteur_asso is null)
select *
 from compteur2020_notmap
union
select *
 from compteur2020_map ;

-- On applique ces modification d'id_cpteur_asso dans les données de comptage_assoc.comptage également
with 
comptage2020 as (
select *
 from comptage_assoc.comptage 
 where annee = '2020'),
mappage as (
select *
 from (values 
  ('Angouleme-6_BD_ALLENDE-0.1671;45.6472', 'Angouleme-6_BD_ALLENDE-0.1671;45.6473'),
  ('Angouleme-bd_bury-0.1654;45.6476', 'Angouleme-bd_bury-0.1654;45.6477'),
  ('Angouleme-bd_liédot-0.1700;45.6464', 'Angouleme-bd_liédot-0.1700;45.6465'),
  ('Angouleme-av_gambetta-0.1622;45.6513', 'Angouleme-av_gambetta-0.1622;45.6514'),
  ('LimMet-avenue emile zola-1.3394;45.9199', 'LimMet-avenue emile zola-1.3394;45.9200'),
  ('LimMet-avenue maryse bastie-1.3294;45.8762', 'LimMet-avenue maryse bastie-1.3294;45.8763'),
  ('LimMet-charmeaux-1.3624;45.866', 'LimMet-charmeaux-1.3624;45.867'),
  ('LimMet-rue daniel gelin-1.2688;45.8878', 'LimMet-rue daniel gelin-1.2688;45.8879'),
  ('LimMet-rue de beaupuy-1.2506;45.8379', 'LimMet-rue de beaupuy-1.2506;45.8380'),
  ('LimMet-rue martin nadaud-1.3193;45.9013', 'LimMet-rue martin nadaud-1.3193;45.9014')) v(id_cpteur_asso, id_cpteur_asso_modif)),
comptage2020_map as (
select m.id_cpteur_asso_modif id_comptag, c.periode, c.type_veh, c.src, c.obs, c.annee, c.suspect, c.id + 1000000000
 from comptage2020 c join mappage m using(id_cpteur_asso)),
comptage2020_notmap as (
select c.id_cpteur_asso id_comptag, c.periode, c.type_veh, c.src, c.obs, c.annee, c.suspect, c.id + 1000000000
 from comptage2020 c left join mappage m using(id_cpteur_asso)
 where m.id_cpteur_asso is null)
select * from comptage2020_notmap ;

-- et on va modifier les identififant des indicateurs concernés pour garder les relations
-- indic_agrege : 
with 
comptage2020 as (
select *
 from comptage_assoc.comptage 
 where annee = '2020')
select ia.id + 1000000000 id, ia.id_comptag_uniq + 1000000000 id_comptag_uniq, ia.indicateur, ia.valeur, ia.fichier
 from comptage_assoc.indic_agrege ia join comptage2020 c on ia.id_comptag_uniq = c.id ; 
 -- indic_mensuel : 
 with 
comptage2020 as (
select *
 from comptage_assoc.comptage 
 where annee = '2020')
select im.id + 1000000000 id, im.id_comptag_uniq + 1000000000 id_comptag_uniq, im.indicateur, im.mois, im.valeur, im.fichier
 from comptage_assoc.indic_mensuel im join comptage2020 c on im.id_comptag_uniq = c.id ; 
 -- indic_horaire : 
 with 
comptage2020 as (
select *
 from comptage_assoc.comptage 
 where annee = '2020')
select ih.id + 1000000000 id, ih.jour, ih.id_comptag_uniq + 1000000000 id_comptag_uniq, ih.indicateur, 
       ih.h0_1, ih.h1_2, ih.h2_3, ih.h3_4, ih.h4_5, ih.h5_6, ih.h6_7, ih.h7_8, ih.h8_9, ih.h9_10, ih.h10_11,
       ih.h11_12, ih.h12_13, ih.h13_14, ih.h14_15, ih.h15_16, ih.h16_17, ih.h17_18, ih.h18_19, ih.h19_20,
       ih.h20_21, ih.h21_22, ih.h22_23, ih.h23_24, ih.fichier
 from comptage_assoc.indic_horaire ih join comptage2020 c on ih.id_comptag_uniq = c.id ;

/*
 * Schéma comptage
 */

-- trouver les compteur qui n'ont que des valeurs pour 2020
with 
comptage2020 as (
select *
 from comptage.comptage 
 where annee = '2020'),
tt_comptag_2020_autre as (
select c.*, 
       max(c.annee) over(partition by c.id_comptag) annee_cptag_max, 
       min(c.annee) over(partition by c.id_comptag) annee_cptag_min
 from comptage.comptage c join comptage2020 c2 using(id_comptag)
 order by c.id_comptag)
select *
 from tt_comptag_2020_autre
 where  annee_cptag_max = annee_cptag_min; 

/* -----------------------------------
 * Création de la structure de données
 *------------------------------------*/

-- Schéma
create schema covid ;


--Table compteur
CREATE TABLE covid.compteur(
    id_comptag character varying(254) NOT NULL,
    geom public.geometry(point, 2154) NULL,
    dep character(2) NOT NULL,
    route character varying(254),
    pr smallint,
    abs smallint,
    reseau character varying(20) NOT NULL,
    gestionnai character varying(254) NOT NULL,
    concession boolean NOT NULL,
    type_poste character varying(20) NOT NULL,
    techno character varying(50),
    src_geo character varying(80),
    obs_geo text,
    x_l93 numeric,
    y_l93 numeric,
    obs_supl character varying(254),
    id_cpt character varying(254),
    last_ann_cpt character(4),
    id_sect character varying(80),
    last_ann_sect character(4),
    fictif boolean NOT NULL,
    src_cpt character varying(50) NOT NULL,
    convention boolean NOT NULL,
    sens_cpt character varying(11) NOT NULL,
    en_service boolean DEFAULT true,
    PRIMARY KEY(id_comptag)) ;


--Table comptage
CREATE TABLE covid.comptage (
	id int4 NOT NULL,
	id_comptag comptage.id_comptage NOT NULL,
	annee comptage.annee NOT NULL,
	periode comptage.periode NULL,
	src text NULL,
	obs text NULL,
	type_veh varchar(50) NOT NULL,
	suspect bool NOT NULL DEFAULT false,
	CONSTRAINT comptage_pkey PRIMARY KEY (id)) ; 

--Table indic_agrege
CREATE TABLE covid.indic_agrege (
	id int4 NOT NULL,
	id_comptag_uniq int8 NOT NULL,
	indicateur varchar(20) NOT NULL,
	valeur comptage.numeric_positif NOT NULL,
	fichier text NULL,
	CONSTRAINT indic_agrege_pkey PRIMARY KEY (id)) ;

--Table indic_mensuel
CREATE TABLE covid.indic_mensuel (
	id int4 NOT NULL,
	id_comptag_uniq int8 NOT NULL,
	indicateur varchar(20) NOT NULL,
	mois varchar(4) NOT NULL,
	valeur comptage.numeric_positif NOT NULL,
	fichier text NULL,
	CONSTRAINT indic_mensuel_pkey PRIMARY KEY (id)) ;

--Table indic_horaire
CREATE TABLE covid.indic_horaire (
	id int NOT NULL,
	jour date NOT NULL,
	id_comptag_uniq int8 NOT NULL,
	indicateur varchar(20) NOT NULL,
	h0_1 comptage.numeric_positif NULL,
	h1_2 comptage.numeric_positif NULL,
	h2_3 comptage.numeric_positif NULL,
	h3_4 comptage.numeric_positif NULL,
	h4_5 comptage.numeric_positif NULL,
	h5_6 comptage.numeric_positif NULL,
	h6_7 comptage.numeric_positif NULL,
	h7_8 comptage.numeric_positif NULL,
	h8_9 comptage.numeric_positif NULL,
	h9_10 comptage.numeric_positif NULL,
	h10_11 comptage.numeric_positif NULL,
	h11_12 comptage.numeric_positif NULL,
	h12_13 comptage.numeric_positif NULL,
	h13_14 comptage.numeric_positif NULL,
	h14_15 comptage.numeric_positif NULL,
	h15_16 comptage.numeric_positif NULL,
	h16_17 comptage.numeric_positif NULL,
	h17_18 comptage.numeric_positif NULL,
	h18_19 comptage.numeric_positif NULL,
	h19_20 comptage.numeric_positif NULL,
	h20_21 comptage.numeric_positif NULL,
	h21_22 comptage.numeric_positif NULL,
	h22_23 comptage.numeric_positif NULL,
	h23_24 comptage.numeric_positif NULL,
	fichier text NULL,
  CONSTRAINT indic_horaire_pkey PRIMARY KEY (id)) ;


/* ------------------------
 * Transfert des données
 *-------------------------*/

