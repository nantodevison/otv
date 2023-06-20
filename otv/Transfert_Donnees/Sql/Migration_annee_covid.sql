/*===================================================================================================================
 * SCRIPTS DE MIGRATION DES COMPTEURS, COMPTAGES, INDIC_AGREGE, INDIC_AGREGE, INDIC_AGREGE ET SCHEMA COMPTAGE_ASSOC 
 * DANS UN SCHEMA COVID SPECIFIQUE
 *===================================================================================================================*/;

/* ------------------------
 * Vérifications préalables
 *-------------------------*/

/*
 * Schéma comptage_assoc
 */;
-- Recherche des comptages_assoc.compteur où le comptage_asso et identique au comptage_ref : 10 résultats
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
)
SELECT
    ce.id_cpteur_asso,
      (
        regexp_match (
            ce.id_cpteur_asso,
            '(.*(\.|\+))([0-9]+)$'
        )
    )[1]||(
        regexp_match (
            ce.id_cpteur_asso,
            '(.*(\.|\+))([0-9]+)$'
        )
    )[3]::int + 1
FROM
    comptage_assoc.compteur ce
JOIN comptage2020 ca
        USING(id_cpteur_asso)
WHERE
    ce.id_cpteur_asso = ce.id_cpteur_ref ;
-- on utilise ça pour modifier les nom de id_cpteur_asso sur la base du fichier créé:
-- Transfert_Donnees\Sql\Migration_annee_covid_comptageAssocMapping_id_cpteur_asso.txt
WITH
mappage AS (
    SELECT
        *
    FROM
        (
        VALUES 
  (
            'Angouleme-6_BD_ALLENDE-0.1671;45.6472',
            'Angouleme-6_BD_ALLENDE-0.1671;45.6473'
        ),
        (
            'Angouleme-bd_bury-0.1654;45.6476',
            'Angouleme-bd_bury-0.1654;45.6477'
        ),
        (
            'Angouleme-bd_liédot-0.1700;45.6464',
            'Angouleme-bd_liédot-0.1700;45.6465'
        ),
        (
            'Angouleme-av_gambetta-0.1622;45.6513',
            'Angouleme-av_gambetta-0.1622;45.6514'
        ),
        (
            'LimMet-avenue emile zola-1.3394;45.9199',
            'LimMet-avenue emile zola-1.3394;45.9200'
        ),
        (
            'LimMet-avenue maryse bastie-1.3294;45.8762',
            'LimMet-avenue maryse bastie-1.3294;45.8763'
        ),
        (
            'LimMet-charmeaux-1.3624;45.866',
            'LimMet-charmeaux-1.3624;45.867'
        ),
        (
            'LimMet-rue daniel gelin-1.2688;45.8878',
            'LimMet-rue daniel gelin-1.2688;45.8879'
        ),
        (
            'LimMet-rue de beaupuy-1.2506;45.8379',
            'LimMet-rue de beaupuy-1.2506;45.8380'
        ),
        (
            'LimMet-rue martin nadaud-1.3193;45.9013',
            'LimMet-rue martin nadaud-1.3193;45.9014'
        )
        ) v(
            id_cpteur_asso,
            id_cpteur_asso_modif
        )
),
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
),
compteur2020 AS (
    SELECT
        ce.*
    FROM
        comptage_assoc.compteur ce
    JOIN comptage2020 ca
            USING(id_cpteur_asso)
),
compteur2020_map AS (
    SELECT
        m.id_cpteur_asso_modif id_comptag,
        c.geom,
        c.route,
        c.pr,
        c.abs,
        c.type_poste,
        c.techno,
        c.src_geo,
        c.obs_geo,
        c.obs_supl,
        c.src_cpt,
        c.convention,
        c.sens_cpt,
        c.id_cpt,
        c.id_sect
    FROM
        compteur2020 c
    JOIN mappage m
            USING(id_cpteur_asso)
),
compteur2020_notmap AS (
    SELECT
        c.id_cpteur_asso id_comptag,
        c.geom,
        c.route,
        c.pr,
        c.abs,
        c.type_poste,
        c.techno,
        c.src_geo,
        c.obs_geo,
        c.obs_supl,
        c.src_cpt,
        c.convention,
        c.sens_cpt,
        c.id_cpt,
        c.id_sect
    FROM
        compteur2020 c
    LEFT JOIN mappage m
            USING(id_cpteur_asso)
    WHERE
        m.id_cpteur_asso IS NULL
)
SELECT
    *
FROM
    compteur2020_notmap
UNION
SELECT
    *
FROM
    compteur2020_map ;
-- On applique ces modification d'id_cpteur_asso dans les données de comptage_assoc.comptage également
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
),
mappage AS (
    SELECT
        *
    FROM
        (
        VALUES 
  (
            'Angouleme-6_BD_ALLENDE-0.1671;45.6472',
            'Angouleme-6_BD_ALLENDE-0.1671;45.6473'
        ),
        (
            'Angouleme-bd_bury-0.1654;45.6476',
            'Angouleme-bd_bury-0.1654;45.6477'
        ),
        (
            'Angouleme-bd_liédot-0.1700;45.6464',
            'Angouleme-bd_liédot-0.1700;45.6465'
        ),
        (
            'Angouleme-av_gambetta-0.1622;45.6513',
            'Angouleme-av_gambetta-0.1622;45.6514'
        ),
        (
            'LimMet-avenue emile zola-1.3394;45.9199',
            'LimMet-avenue emile zola-1.3394;45.9200'
        ),
        (
            'LimMet-avenue maryse bastie-1.3294;45.8762',
            'LimMet-avenue maryse bastie-1.3294;45.8763'
        ),
        (
            'LimMet-charmeaux-1.3624;45.866',
            'LimMet-charmeaux-1.3624;45.867'
        ),
        (
            'LimMet-rue daniel gelin-1.2688;45.8878',
            'LimMet-rue daniel gelin-1.2688;45.8879'
        ),
        (
            'LimMet-rue de beaupuy-1.2506;45.8379',
            'LimMet-rue de beaupuy-1.2506;45.8380'
        ),
        (
            'LimMet-rue martin nadaud-1.3193;45.9013',
            'LimMet-rue martin nadaud-1.3193;45.9014'
        )
        ) v(
            id_cpteur_asso,
            id_cpteur_asso_modif
        )
),
comptage2020_map AS (
    SELECT
        m.id_cpteur_asso_modif id_comptag,
        c.periode,
        c.type_veh,
        c.src,
        c.obs,
        c.annee,
        c.suspect,
        c.id + 1000000000
    FROM
        comptage2020 c
    JOIN mappage m
            USING(id_cpteur_asso)
),
comptage2020_notmap AS (
    SELECT
        c.id_cpteur_asso id_comptag,
        c.periode,
        c.type_veh,
        c.src,
        c.obs,
        c.annee,
        c.suspect,
        c.id + 1000000000
    FROM
        comptage2020 c
    LEFT JOIN mappage m
            USING(id_cpteur_asso)
    WHERE
        m.id_cpteur_asso IS NULL
)
SELECT
    *
FROM
    comptage2020_notmap
UNION 
SELECT
    *
FROM
    comptage2020_map ;
-- et on va modifier les identififant des indicateurs concernés pour garder les relations
-- indic_agrege : 
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
)
SELECT
    ia.id + 1000000000 id,
    ia.id_comptag_uniq + 1000000000 id_comptag_uniq,
    ia.indicateur,
    ia.valeur,
    ia.fichier
FROM
    comptage_assoc.indic_agrege ia
JOIN comptage2020 c ON
    ia.id_comptag_uniq = c.id ;
-- indic_mensuel : 
 WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
)
SELECT
    im.id + 1000000000 id,
    im.id_comptag_uniq + 1000000000 id_comptag_uniq,
    im.indicateur,
    im.mois,
    im.valeur,
    im.fichier
FROM
    comptage_assoc.indic_mensuel im
JOIN comptage2020 c ON
    im.id_comptag_uniq = c.id ;
-- indic_horaire : 
 WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
)
SELECT
    ih.id + 1000000000 id,
    ih.jour,
    ih.id_comptag_uniq + 1000000000 id_comptag_uniq,
    ih.indicateur, 
       ih.h0_1,
    ih.h1_2,
    ih.h2_3,
    ih.h3_4,
    ih.h4_5,
    ih.h5_6,
    ih.h6_7,
    ih.h7_8,
    ih.h8_9,
    ih.h9_10,
    ih.h10_11,
       ih.h11_12,
    ih.h12_13,
    ih.h13_14,
    ih.h14_15,
    ih.h15_16,
    ih.h16_17,
    ih.h17_18,
    ih.h18_19,
    ih.h19_20,
       ih.h20_21,
    ih.h21_22,
    ih.h22_23,
    ih.h23_24,
    ih.fichier
FROM
    comptage_assoc.indic_horaire ih
JOIN comptage2020 c ON
    ih.id_comptag_uniq = c.id ;

/*
 * Schéma comptage
 */;
-- trouver les comptages qui n'ont que des valeurs pour 2020
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage.comptage
    WHERE
        annee = '2020'
),
tt_comptag_2020_autre AS (
    SELECT
        c.*,
        max(c.annee) OVER(
            PARTITION BY c.id_comptag
        ) annee_cptag_max,
        min(c.annee) OVER(
            PARTITION BY c.id_comptag
        ) annee_cptag_min
    FROM
        comptage.comptage c
    JOIN comptage2020 c2
            USING(id_comptag)
    ORDER BY
        c.id_comptag
)
SELECT
    *
FROM
    tt_comptag_2020_autre
WHERE
    annee_cptag_max = annee_cptag_min;

/* -----------------------------------
 * Création de la structure de données
 *------------------------------------*/;
-- Schéma
CREATE SCHEMA covid ;
--Table compteur
CREATE TABLE covid.compteur(
    id_comptag CHARACTER VARYING(254) NOT NULL,
    geom public.geometry(
        point,
        2154
    ) NULL,
    dep CHARACTER(2) ,
    route CHARACTER VARYING(254),
    pr SMALLINT,
    abs SMALLINT,
    reseau CHARACTER VARYING(20) ,
    gestionnai CHARACTER VARYING(254) ,
    concession boolean ,
    type_poste CHARACTER VARYING(20) ,
    techno CHARACTER VARYING(50),
    src_geo CHARACTER VARYING(80),
    obs_geo TEXT,
    x_l93 NUMERIC,
    y_l93 NUMERIC,
    obs_supl CHARACTER VARYING(254),
    id_cpt CHARACTER VARYING(254),
    last_ann_cpt CHARACTER(4),
    id_sect CHARACTER VARYING(80),
    last_ann_sect CHARACTER(4),
    fictif boolean ,
    src_cpt CHARACTER VARYING(50) ,
    convention boolean ,
    sens_cpt CHARACTER VARYING(11) ,
    en_service boolean DEFAULT TRUE,
    id_cpteur_ref CHARACTER VARYING(254),
    PRIMARY KEY(id_comptag)
) ;
--Table comptage
CREATE TABLE covid.comptage (
    id int4 NOT NULL,
    id_comptag comptage.id_comptage NOT NULL,
    annee comptage.annee ,
    periode comptage.periode NULL,
    src TEXT NULL,
    obs TEXT NULL,
    type_veh varchar(50) ,
    suspect bool ,
    CONSTRAINT comptage_pkey PRIMARY KEY (id)
) ;
--Table indic_agrege
CREATE TABLE covid.indic_agrege (
    id int4 NOT NULL,
    id_comptag_uniq int8 NOT NULL,
    indicateur varchar(20) NOT NULL,
    valeur comptage.numeric_positif NOT NULL,
    fichier TEXT NULL,
    CONSTRAINT indic_agrege_pkey PRIMARY KEY (id)
) ;
--Table indic_mensuel
CREATE TABLE covid.indic_mensuel (
    id int4 NOT NULL,
    id_comptag_uniq int8 NOT NULL,
    indicateur varchar(20) NOT NULL,
    mois varchar(4) NOT NULL,
    valeur comptage.numeric_positif NOT NULL,
    fichier TEXT NULL,
    CONSTRAINT indic_mensuel_pkey PRIMARY KEY (id)
) ;
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
    fichier TEXT NULL,
    CONSTRAINT indic_horaire_pkey PRIMARY KEY (id)
) ;

/* ------------------------
 * Transfert des données
 *-------------------------*/;

/*
 * Schéma comptage_assoc
 */
-- table compteur
WITH
mappage AS (
    SELECT
        *
    FROM
        (
        VALUES 
  (
            'Angouleme-6_BD_ALLENDE-0.1671;45.6472',
            'Angouleme-6_BD_ALLENDE-0.1671;45.6473'
        ),
        (
            'Angouleme-bd_bury-0.1654;45.6476',
            'Angouleme-bd_bury-0.1654;45.6477'
        ),
        (
            'Angouleme-bd_liédot-0.1700;45.6464',
            'Angouleme-bd_liédot-0.1700;45.6465'
        ),
        (
            'Angouleme-av_gambetta-0.1622;45.6513',
            'Angouleme-av_gambetta-0.1622;45.6514'
        ),
        (
            'LimMet-avenue emile zola-1.3394;45.9199',
            'LimMet-avenue emile zola-1.3394;45.9200'
        ),
        (
            'LimMet-avenue maryse bastie-1.3294;45.8762',
            'LimMet-avenue maryse bastie-1.3294;45.8763'
        ),
        (
            'LimMet-charmeaux-1.3624;45.866',
            'LimMet-charmeaux-1.3624;45.867'
        ),
        (
            'LimMet-rue daniel gelin-1.2688;45.8878',
            'LimMet-rue daniel gelin-1.2688;45.8879'
        ),
        (
            'LimMet-rue de beaupuy-1.2506;45.8379',
            'LimMet-rue de beaupuy-1.2506;45.8380'
        ),
        (
            'LimMet-rue martin nadaud-1.3193;45.9013',
            'LimMet-rue martin nadaud-1.3193;45.9014'
        )
        ) v(
            id_cpteur_asso,
            id_cpteur_asso_modif
        )
),
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
),
compteur2020 AS (
    SELECT
        ce.*
    FROM
        comptage_assoc.compteur ce
    JOIN comptage2020 ca
            USING(id_cpteur_asso)
),
compteur2020_map AS (
    SELECT
        m.id_cpteur_asso_modif id_comptag,
        c.geom,
        c.route,
        c.pr,
        c.abs,
        c.type_poste,
        c.techno,
        c.src_geo,
        c.obs_geo,
        c.obs_supl,
        c.src_cpt,
        c.convention,
        c.sens_cpt,
        c.id_cpt,
        c.id_sect,
        c.id_cpteur_ref
    FROM
        compteur2020 c
    JOIN mappage m
            USING(id_cpteur_asso)
),
compteur2020_notmap AS (
    SELECT
        c.id_cpteur_asso id_comptag,
        c.geom,
        c.route,
        c.pr,
        c.abs,
        c.type_poste,
        c.techno,
        c.src_geo,
        c.obs_geo,
        c.obs_supl,
        c.src_cpt,
        c.convention,
        c.sens_cpt,
        c.id_cpt,
        c.id_sect,
        c.id_cpteur_ref
    FROM
        compteur2020 c
    LEFT JOIN mappage m
            USING(id_cpteur_asso)
    WHERE
        m.id_cpteur_asso IS NULL
),
compteur AS (
    SELECT
        *
    FROM
        compteur2020_notmap
UNION
    SELECT
        *
    FROM
        compteur2020_map
)
INSERT
    INTO
    covid.compteur (
        id_comptag,
        geom,
        route,
        pr,
        abs,
        type_poste,
        techno,
        src_geo,
        obs_geo,
        obs_supl, 
                            src_cpt,
        convention,
        sens_cpt,
        id_cpt,
        id_sect,
        id_cpteur_ref
    )
 SELECT
    id_comptag,
    geom,
    route,
    pr,
    abs,
    type_poste,
    techno,
    src_geo,
    obs_geo,
    obs_supl, 
                            src_cpt,
    convention,
    sens_cpt,
    id_cpt,
    id_sect,
    id_cpteur_ref
FROM
    compteur ;
-- table comptage
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
),
mappage AS (
    SELECT
        *
    FROM
        (
        VALUES 
  (
            'Angouleme-6_BD_ALLENDE-0.1671;45.6472',
            'Angouleme-6_BD_ALLENDE-0.1671;45.6473'
        ),
        (
            'Angouleme-bd_bury-0.1654;45.6476',
            'Angouleme-bd_bury-0.1654;45.6477'
        ),
        (
            'Angouleme-bd_liédot-0.1700;45.6464',
            'Angouleme-bd_liédot-0.1700;45.6465'
        ),
        (
            'Angouleme-av_gambetta-0.1622;45.6513',
            'Angouleme-av_gambetta-0.1622;45.6514'
        ),
        (
            'LimMet-avenue emile zola-1.3394;45.9199',
            'LimMet-avenue emile zola-1.3394;45.9200'
        ),
        (
            'LimMet-avenue maryse bastie-1.3294;45.8762',
            'LimMet-avenue maryse bastie-1.3294;45.8763'
        ),
        (
            'LimMet-charmeaux-1.3624;45.866',
            'LimMet-charmeaux-1.3624;45.867'
        ),
        (
            'LimMet-rue daniel gelin-1.2688;45.8878',
            'LimMet-rue daniel gelin-1.2688;45.8879'
        ),
        (
            'LimMet-rue de beaupuy-1.2506;45.8379',
            'LimMet-rue de beaupuy-1.2506;45.8380'
        ),
        (
            'LimMet-rue martin nadaud-1.3193;45.9013',
            'LimMet-rue martin nadaud-1.3193;45.9014'
        )
        ) v(
            id_cpteur_asso,
            id_cpteur_asso_modif
        )
),
comptage2020_map AS (
    SELECT
        m.id_cpteur_asso_modif id_comptag,
        c.periode,
        c.type_veh,
        c.src,
        c.obs,
        c.annee,
        c.suspect,
        c.id + 1000000000 id
    FROM
        comptage2020 c
    JOIN mappage m
            USING(id_cpteur_asso)
),
comptage2020_notmap AS (
    SELECT
        c.id_cpteur_asso id_comptag,
        c.periode,
        c.type_veh,
        c.src,
        c.obs,
        c.annee,
        c.suspect,
        c.id + 1000000000 id
    FROM
        comptage2020 c
    LEFT JOIN mappage m
            USING(id_cpteur_asso)
    WHERE
        m.id_cpteur_asso IS NULL
),
comptage AS (
    SELECT
        *
    FROM
        comptage2020_notmap
UNION
    SELECT
        *
    FROM
        comptage2020_map
)
INSERT
    INTO
    covid.comptage(
        id_comptag,
        periode,
        type_veh,
        src,
        obs,
        annee,
        suspect,
        id
    )
 SELECT
    id_comptag,
    periode,
    type_veh,
    src,
    obs,
    annee,
    suspect,
    id
FROM
    comptage ;
-- table indic_agrege : 
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
),
indic_agrege AS (
    SELECT
        ia.id + 1000000000 id,
        ia.id_comptag_uniq + 1000000000 id_comptag_uniq,
        ia.indicateur,
        ia.valeur,
        ia.fichier
    FROM
        comptage_assoc.indic_agrege ia
    JOIN comptage2020 c ON
        ia.id_comptag_uniq = c.id
)
INSERT
    INTO
    covid.indic_agrege (
        id,
        id_comptag_uniq,
        indicateur,
        valeur,
        fichier
    ) 
 SELECT
    id,
    id_comptag_uniq,
    indicateur,
    valeur,
    fichier
FROM
    indic_agrege;
-- table indic_mensuel : 
 WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
),
indic_mensuel AS (
    SELECT
        im.id + 1000000000 id,
        im.id_comptag_uniq + 1000000000 id_comptag_uniq,
        im.indicateur,
        im.mois,
        im.valeur,
        im.fichier
    FROM
        comptage_assoc.indic_mensuel im
    JOIN comptage2020 c ON
        im.id_comptag_uniq = c.id
)
INSERT
    INTO
    covid.indic_mensuel (
        id,
        id_comptag_uniq,
        indicateur,
        mois,
        valeur,
        fichier
    ) 
 SELECT
    id,
    id_comptag_uniq,
    indicateur,
    mois,
    valeur,
    fichier
FROM
    indic_mensuel;
-- table indic_horaire : 
 WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage_assoc.comptage
    WHERE
        annee = '2020'
),
indic_horaire AS (
    SELECT
        ih.id + 1000000000 id,
        ih.jour,
        ih.id_comptag_uniq + 1000000000 id_comptag_uniq,
        ih.indicateur,
        ih.h0_1,
        ih.h1_2,
        ih.h2_3,
        ih.h3_4,
        ih.h4_5,
        ih.h5_6,
        ih.h6_7,
        ih.h7_8,
        ih.h8_9,
        ih.h9_10,
        ih.h10_11,
        ih.h11_12,
        ih.h12_13,
        ih.h13_14,
        ih.h14_15,
        ih.h15_16,
        ih.h16_17,
        ih.h17_18,
        ih.h18_19,
        ih.h19_20,
        ih.h20_21,
        ih.h21_22,
        ih.h22_23,
        ih.h23_24,
        ih.fichier
    FROM
        comptage_assoc.indic_horaire ih
    JOIN comptage2020 c ON
        id_comptag_uniq = c.id
)
INSERT
    INTO
    covid.indic_horaire (
        id,
        jour,
        id_comptag_uniq,
        indicateur,
        h0_1,
        h1_2,
        h2_3,
        h3_4,
        h4_5,
        h5_6,
        h6_7,
        h7_8,
        h8_9,
        h9_10,
        h10_11,
        h11_12,
        h12_13,
        h13_14,
        h14_15,
        h15_16,
        h16_17,
        h17_18,
        h18_19,
        h19_20,
        h20_21,
        h21_22,
        h22_23,
        h23_24,
        fichier
    )
 SELECT
    id,
    jour,
    id_comptag_uniq,
    indicateur, 
       h0_1,
    h1_2,
    h2_3,
    h3_4,
    h4_5,
    h5_6,
    h6_7,
    h7_8,
    h8_9,
    h9_10,
    h10_11,
       h11_12,
    h12_13,
    h13_14,
    h14_15,
    h15_16,
    h16_17,
    h17_18,
    h18_19,
    h19_20,
       h20_21,
    h21_22,
    h22_23,
    h23_24,
    fichier
FROM
    indic_horaire
  ;

/*
 * Schéma comptage
 */;
-- trouver les compteurs qui n'ont que des valeurs pour 2020
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage.comptage
    WHERE
        annee = '2020'
),
tt_comptag_2020_autre AS (
    SELECT
        c.*,
        max(c.annee) OVER(
            PARTITION BY c.id_comptag
        ) annee_cptag_max,
        min(c.annee) OVER(
            PARTITION BY c.id_comptag
        ) annee_cptag_min
    FROM
        comptage.comptage c
    JOIN comptage2020 c2
            USING(id_comptag)
    ORDER BY
        c.id_comptag
),
compteur AS (
    SELECT
        DISTINCT c.*
    FROM
        tt_comptag_2020_autre t
    JOIN comptage.compteur c
            USING (id_comptag)
    WHERE
        t.annee_cptag_max = t.annee_cptag_min
)
INSERT
    INTO
    covid.compteur (
        id_comptag, geom,
        dep,
        route,
        pr,
        abs,
        reseau,
        gestionnai,
        concession,
        type_poste,
        techno,
        src_geo,
        obs_geo,
        x_l93,
        y_l93,
        obs_supl,
        id_cpt,
        last_ann_cpt,
        id_sect,
        last_ann_sect,
        fictif,
        src_cpt,
        convention,
        sens_cpt,
        en_service
    )
    SELECT id_comptag, geom,
        dep,
        route,
        pr,
        abs,
        reseau,
        gestionnai,
        concession,
        type_poste,
        techno,
        src_geo,
        obs_geo,
        x_l93,
        y_l93,
        obs_supl,
        id_cpt,
        last_ann_cpt,
        id_sect,
        last_ann_sect,
        fictif,
        src_cpt,
        convention,
        sens_cpt,
        en_service
    FROM compteur ;


-- table comptage
INSERT INTO covid.comptage (id, id_comptag, "annee", "periode", src, obs, type_veh, suspect)
SELECT
        id, id_comptag, "annee", "periode", src, obs, type_veh, suspect
    FROM
        comptage.comptage
    WHERE
        annee = '2020' ;
    
-- table indic_agrege
INSERT INTO covid.indic_agrege (id, id_comptag_uniq, indicateur, valeur, fichier)
SELECT
    ia.*
FROM
    comptage.comptage c JOIN comptage.indic_agrege ia ON c.id = ia.id_comptag_uniq
WHERE
    c.annee = '2020' ;

-- table indic_mensuel
INSERT INTO covid.indic_mensuel (id, id_comptag_uniq, indicateur, mois, valeur, fichier)
SELECT
    im.id, im.id_comptag_uniq, im.indicateur, im.mois, im.valeur, im.fichier
FROM
    comptage.comptage c JOIN comptage.indic_mensuel im ON c.id = im.id_comptag_uniq
WHERE
    c.annee = '2020' ;

-- table indic_horaire
INSERT INTO covid.indic_horaire (id, jour, id_comptag_uniq, indicateur, h0_1, h1_2, h2_3, h3_4, h4_5, h5_6, h6_7, h7_8, h8_9, h9_10,
                                 h10_11, h11_12, h12_13, h13_14, h14_15, h15_16, h16_17, h17_18, h18_19, h19_20, h20_21, h21_22,
                                 h22_23, h23_24, fichier)
SELECT
    ih.id, ih.jour, ih.id_comptag_uniq, ih.indicateur, ih.h0_1, ih.h1_2, ih.h2_3, ih.h3_4, ih.h4_5, ih.h5_6, ih.h6_7, ih.h7_8, ih.h8_9, ih.h9_10,
    ih.h10_11, ih.h11_12, ih.h12_13, ih.h13_14, ih.h14_15, ih.h15_16, ih.h16_17, ih.h17_18, ih.h18_19, ih.h19_20, ih.h20_21, ih.h21_22,
    ih.h22_23, ih.h23_24, ih.fichier
FROM
    comptage.comptage c JOIN comptage.indic_horaire ih ON c.id = ih.id_comptag_uniq
WHERE
    c.annee = '2020' ;
    

/* ------------------------
 * Suppression des données
 *-------------------------*/

/*
 * Schéma comptage_assoc
 */

-- tables comptage et indic (FK on delete cascade)
DELETE FROM comptage_assoc.comptage WHERE annee = '2020' ;
-- table compteur
DELETE FROM comptage_assoc.compteur WHERE id_cpteur_asso NOT IN (SELECT id_cpteur_asso FROM comptage_assoc.comptage) ;


/*
 * Schéma comptage toute table (FK on delete cascade)
 */
-- vérification que les compteurs à supprimer ne sont pas référencer dans les comptages assoc
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage.comptage
    WHERE
        annee = '2020'
),
tt_comptag_2020_autre AS (
    SELECT
        c.*,
        max(c.annee) OVER(
            PARTITION BY c.id_comptag
        ) annee_cptag_max,
        min(c.annee) OVER(
            PARTITION BY c.id_comptag
        ) annee_cptag_min
    FROM
        comptage.comptage c
    JOIN comptage2020 c2
            USING(id_comptag)
    ORDER BY
        c.id_comptag
)
SELECT DISTINCT (id_comptag) 
 FROM tt_comptag_2020_autre
 WHERE annee_cptag_max = annee_cptag_min AND id_comptag IN (SELECT id_cpteur_ref FROM comptage_assoc.compteur) ;
-- c'est le cas pour le compteur 33-D115-55+330 que l'on va rappatrier au préalable dans le schéma comptage.
--table compteur
INSERT INTO comptage.compteur (id_comptag, geom, dep, route, pr, abs, reseau, gestionnai, concession, type_poste, techno, src_geo, obs_geo, x_l93, y_l93, obs_supl, id_cpt, last_ann_cpt, 
       id_sect, last_ann_sect, fictif, src_cpt, convention, sens_cpt, en_service)
SELECT id_cpteur_asso id_comptag, geom, '33'::TEXT dep, route, pr, abs, 'RD'::TEXT reseau, 'CD33'::TEXT gestionnai, 
       FALSE::bool concession, type_poste, techno, src_geo, obs_geo, round(st_x(geom)::NUMERIC, 3) x_l93, round(st_y(geom)::NUMERIC, 3) y_l93, obs_supl, id_cpt, NULL::text last_ann_cpt, 
       NULL::text id_sect, NULL::text last_ann_sect, FALSE::bool fictif, src_cpt, convention, sens_cpt, TRUE::bool en_service
  FROM comptage_assoc.compteur
  WHERE id_cpteur_ref = '33-D115-55+330' ;
-- table comptage
INSERT INTO comptage.comptage (id_comptag, "annee", "periode", src, obs, type_veh, suspect)
SELECT ca.id_cpteur_asso id_comptag, ca."annee", ca."periode", ca.src, ca.obs, ca.type_veh, ca.suspect
  FROM comptage_assoc.compteur ce JOIN comptage_assoc.comptage ca ON ce.id_cpteur_asso = ca.id_cpteur_asso
  WHERE ce.id_cpteur_ref = '33-D115-55+330' ;
-- table indic_agrege
INSERT INTO comptage.indic_agrege (id_comptag_uniq, indicateur, valeur, fichier)
SELECT ca2.id id_comptag_uniq, ia.indicateur, ia.valeur, ia.fichier
  FROM comptage_assoc.compteur ce JOIN comptage_assoc.comptage ca ON ce.id_cpteur_asso = ca.id_cpteur_asso
                                  JOIN comptage_assoc.indic_agrege ia ON ca.id = ia.id_comptag_uniq
                                  JOIN comptage.comptage ca2 ON ca.id_cpteur_asso = ca2.id_comptag
  WHERE ce.id_cpteur_ref = '33-D115-55+330' ;

-- suppression de ce compteur et de ses valeurs dans les comptages assoc
delete FROM comptage_assoc.indic_agrege ia2 WHERE id IN (
SELECT ia.id
 FROM comptage_assoc.indic_agrege ia JOIN comptage_assoc.comptage ca ON ca.id = ia.id_comptag_uniq
                                  JOIN comptage_assoc.compteur ce ON ce.id_cpteur_asso = ca.id_cpteur_asso
                                  JOIN comptage.comptage ca2 ON ca.id_cpteur_asso = ca2.id_comptag 
 WHERE ce.id_cpteur_ref = '33-D115-55+330');
delete FROM comptage_assoc.comptage ca2 WHERE ca2.id IN (
SELECT ca.id
  FROM comptage_assoc.compteur ce JOIN comptage_assoc.comptage ca ON ce.id_cpteur_asso = ca.id_cpteur_asso
  WHERE ce.id_cpteur_ref = '33-D115-55+330');
DELETE FROM comptage_assoc.compteur WHERE id_cpteur_ref = '33-D115-55+330' ;

--suppression des compteurs dont seul un comptage sur 2020 existe : 
WITH 
comptage2020 AS (
    SELECT
        *
    FROM
        comptage.comptage
    WHERE
        annee = '2020'
),
tt_comptag_2020_autre AS (
    SELECT
        c.*,
        max(c.annee) OVER(
            PARTITION BY c.id_comptag
        ) annee_cptag_max,
        min(c.annee) OVER(
            PARTITION BY c.id_comptag
        ) annee_cptag_min
    FROM
        comptage.comptage c
    JOIN comptage2020 c2
            USING(id_comptag)
    ORDER BY
        c.id_comptag
)
DELETE FROM comptage.compteur WHERE id_comptag IN (SELECT id_comptag FROM tt_comptag_2020_autre WHERE annee_cptag_max = annee_cptag_min) ;

-- supprssion des comptages 2020 dans le schéma comptage
DELETE FROM comptage.comptage WHERE annee = '2020' ;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*
 * FIN DU SCRIPT
 */*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/