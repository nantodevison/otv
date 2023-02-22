# -*- coding: utf-8 -*-
"""
Created on 26 janv. 2022

@author: martin.schoreisz
Module pour integrer les nouveaux comptages fournis par les gestionnaires
"""

import warnings, re
import datetime as dt
from Outils import Outils as O
import pandas as pd
from Connexions import Connexion_Transfert as ct
from collections import Counter
from Params.Bdd_OTV import (
    nomConnBddOtv,
    schemaComptage,
    schemaComptageAssoc,
    tableComptage,
    tableEnumTypeVeh,
    tableCompteur,
    tableCorrespIdComptag,
    attrCompteurAssoc,
    attBddCompteur,
    attrComptageMano,
    attrCompteurValeurMano,
    attrComptageAssoc,
    enumTypePoste,
    vueLastAnnKnow,
    attrIndicHoraireAssoc,
    attrIndicHoraire,
    dicoTypeAttributs,
    attBddCompteurNonNull,
)
from Import_export_comptage import (
    recupererIdUniqComptage,
    recupererIdUniqComptageAssoc,
    compteur_existant_bdd,
    recupererLastAnnKnow,
)
from Params.Mensuel import dico_mois
import geopandas as gp
from shapely.geometry import Point


def corresp_nom_id_comptag(df):
    """
    pour les id_comptag dont on sait que les noms gti et gestionnaire different  mais que ce sont les memes (cf table comptage.corresp_id_comptag),
    on remplace le nom gest par le nom_gti, pour pouvoir faire des jointure ensuite
    in :
        df : dataframe des comptage du gest. attention doit contenir l'attribut 'id_comptag', ene general prendre df_attr
    """
    rqt_corresp_comptg = f"select * from {schemaComptage}.{tableCorrespIdComptag}"
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        corresp_comptg = pd.read_sql(rqt_corresp_comptg, c.sqlAlchemyConn)
    dfMerge = corresp_comptg.merge(
        df, left_on="id_gest", right_on="id_comptag", how="right"
    )
    if not dfMerge.empty:
        dfMerge["id_comptag"] = dfMerge.apply(
            lambda x: x.id_gti if not pd.isnull(x.id_gest) else x.id_comptag, axis=1
        )
        dfMerge.drop(["id", "id_gest", "id_gti"], axis=1, errors="ignore", inplace=True)
    else:
        return pd.DataFrame([])
    return dfMerge


def scinderComptagExistant(dfATester, annee):
    """
    utiliser la fonction recupererIdUniqComptage pour comparer une df avec les donnees de comptage dans la base
    in:
        dfTest : df avec un champs id_comptag
        annee : string 4 : des donnees de comptag
    out :
        dfIdsConnus : dataframe testee dont les id_comptag sont dabs la bdd
        dfIdsInconnus : dataframe testee dont les id_comptag ne sont pas dabs la bdd
    """
    # verifier que la colonne id_comptag est presente
    dfTest = dfATester.copy()
    O.checkAttributsinDf(dfTest, "id_comptag")
    dfTest["annee"] = annee
    dfTest = corresp_nom_id_comptag(dfTest)
    dfIdCptUniqs = recupererIdUniqComptage(dfTest)
    dfIdsConnus = dfIdCptUniqs.loc[dfIdCptUniqs.id_comptag_uniq.notna()].copy()
    dfIdsInconnus = dfIdCptUniqs.loc[dfIdCptUniqs.id_comptag_uniq.isna()].copy()
    return dfIdsConnus, dfIdsInconnus


def classer_compteur_update_insert(dfAClasser, departement=False, gest=False):
    """
    vérifier les comptages existants, et les correspondances, et séparer une df entre les compteurs déjà présents dans la base
    et ceux qui doivent être créés
    in :
        dfAClasser : df des données de comptage mise en forme
        deprtement : string du departement sur deux caracteres
    out :
        df_attr_update : extraction de la données source : identifiant de comptages deja presents dans la base
        df_attr_insert : extraction de la données source : identifiant de comptages non presents dans la base
    """
    O.checkAttributsinDf(dfAClasser, "id_comptag")
    dfAClasser = corresp_nom_id_comptag(dfAClasser)
    existant = compteur_existant_bdd(dep=departement, gest=gest)
    df_attr_update = (
        dfAClasser.loc[dfAClasser.id_comptag.isin(existant.id_comptag.tolist())]
        .copy()
        .drop_duplicates()
    )
    df_attr_insert = (
        dfAClasser.loc[~dfAClasser.id_comptag.isin(existant.id_comptag.tolist())]
        .copy()
        .drop_duplicates()
    )
    # ajout d'une vérif sur les longueurs resectives de donnees
    if len(df_attr_update) + len(df_attr_insert) != len(dfAClasser):
        raise ValueError(
            "la separation enetre comptage a mettre a jour et comptage a inserer ne correspond pas a la taille des donnees sources"
        )
    return df_attr_update, df_attr_insert


def localiser_comptage_a_inserer(df, schema_temp, nom_table_temp, table_ref, table_pr):
    """
    récupérer la geometrie de pt de comptage à inserer dans une df sans les inserer dans la Bdd
    in :
        df : les données de comptage nouvelles, normalement c'est self.df_attr_insert (cf Comptage_Cd17 ou Compatge cd47
        schema_temp : string : nom du schema en bdd opur calcul geom, cf localiser_comptage_a_inserer
        nom_table_temp : string : nom de latable temporaire en bdd opur calcul geom, cf localiser_comptage_a_inserer
        table_ref : string : schema qualifyed nom de la table de reference du lineaire
        table_pr : string : schema qualifyed nom de la table de reference des pr
    """
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        # passer les données dans Bdd
        c.sqlAlchemyConn.execute(f"drop table if exists {schema_temp}.{nom_table_temp}")
        df.to_sql(nom_table_temp, c.sqlAlchemyConn, schema_temp)
        # ajouter une colonne geometrie
        rqt_ajout_geom = f"""ALTER TABLE {schema_temp}.{nom_table_temp} ADD COLUMN geom geometry('POINT',2154)"""
        c.sqlAlchemyConn.execute(rqt_ajout_geom)
        # mettre a jour la geometrie. attention, il faut un fichier de référentiel qui va bein, cf fonction geoloc_pt_comptag dans la Bdd
        rqt_maj_geom = f"""update {schema_temp}.{nom_table_temp}
                         set geom=(select geom_out  from comptage.geoloc_pt_comptag('{table_ref}','{table_pr}',id_comptag))
                         where geom is null"""
        c.sqlAlchemyConn.execute(rqt_maj_geom)
        points_a_inserer = gp.GeoDataFrame.from_postgis(
            f"select * from {schema_temp}.{nom_table_temp}",
            c.sqlAlchemyConn,
            geom_col="geom",
            crs="epsg:2154",
        )
    return points_a_inserer


def ventilerParSectionHomogene(
    tableCptNew, tableSectionHomo, codeDept, distancePlusProcheVoisin=10
):
    """
    Pour les points supposes a creer, les separer selon les cas (cf Out)
    in :
        tableCptNew: string : schemaqualified table des pointsgeolocalise. creer lors de localiser_comptage_a_inserer().
        tableSectionHomo: string : schemaqualified table des sections homogene dans la bdd
        codeDept: string: code departement sur 2 caractères
        distancePlusProcheVoisin : integer : distance max de recherche du plus proche voisin
    out :
        cptSansGeom: point sans geometrie
        ppvHorsSectHomo: point avec geom mais eloigne du referentiel
        cptSimpleSectHomo: point avec geom, proche d'une section homogene de trafic, avec 1 seul point sur la section
        cptMultiSectHomo: point avec geom, proche d'une section homogene de trafic, avec plusieurs point sur la section
    """
    # recupérer les données
    # params : nom de table  ou df des points geolocalises, nom de table de linauto, nom de Bdd
    if not O.check_colonne_in_table_bdd(
        nomConnBddOtv,
        tableSectionHomo.split(".")[0],
        tableSectionHomo.split(".")[1],
        "gid",
    )[0]:
        raise AttributeError("la colonne gid n'est pas dans la tabl de bdd")
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        pt = gp.read_postgis(f"select * from {tableCptNew}", c.sqlAlchemyConn)
        ptGeom = gp.read_postgis(
            f"select * from {tableCptNew} where geom is not null", c.sqlAlchemyConn
        )
        cptSansGeom = gp.read_postgis(
            f"select * from {tableCptNew} where geom is null", c.sqlAlchemyConn
        )
        linauto = gp.read_postgis(
            f"""
                                     select DISTINCT ON (t.gid) c.id_comptag id_comptag_bdd,c.type_poste type_poste_bdd, t.gid, t.geom, st_distance(c.geom, t.geom)
                                      from (SELECT * FROM {tableSectionHomo} where list_dept like '%%{codeDept}%%') t LEFT JOIN {schemaComptage}.{vueLastAnnKnow} c ON st_dwithin(c.geom, t.geom, 30)
                                      ORDER BY t.gid, CASE WHEN c.type_poste = 'permanent' THEN 1 
                                                           WHEN c.type_poste = 'tournant' THEN 2
                                                           WHEN c.type_poste = 'ponctuel' THEN 3 
                                                           ELSE 4 
                                                           end, CASE WHEN c.vacances_zone_b IS NULL OR NOT c.vacances_zone_b THEN 1 ELSE 2 END, 
                                                                         c.tmja DESC
                                     """,
            c.sqlAlchemyConn,
        )
    # on en déduit le plus proche voisin entre les points et la linauto
    ppV = O.plus_proche_voisin(
        ptGeom, linauto, distancePlusProcheVoisin, "id_comptag", "gid"
    )
    ppVSectHomo = ppV.loc[~ppV.gid.isna()].copy()
    ppvHorsSectHomo = ptGeom.loc[
        ~ptGeom.id_comptag.isin(ppV.id_comptag.tolist())
    ].copy()  # PENSER A CREER LES COMPTEURS POUR CEUX LA !!!!!
    # et on récupère l'id_comptag_existant et l'id de section homogene
    ppVTot = ptGeom.merge(ppVSectHomo, how="left", on="id_comptag").merge(
        linauto, how="left", on="gid"
    )
    # trouver le nombre de comptage par section homogène (gid)
    nbCptSectHomo = ppVTot.gid.value_counts()
    # separer les differents cas a traiter
    cptSimpleSectHomo = ppVTot.loc[
        ppVTot.gid.isin(nbCptSectHomo.loc[nbCptSectHomo == 1].index.tolist())
    ].copy()
    cptMultiSectHomo = ppVTot.loc[
        ppVTot.gid.isin(nbCptSectHomo.loc[nbCptSectHomo > 1].index.tolist())
    ].copy()
    # verif
    if len(cptSansGeom) + len(cptSimpleSectHomo) + len(cptMultiSectHomo) + len(
        ppvHorsSectHomo
    ) > len(pt):
        warnings.warn(
            """la somme des éléments présents dans 'cptSansGeom', 'ppvHorsSectHomo', 'cptSimpleSectHomo', 'cptMultiSectHomo' est supérieure au nombre d'éléments initiaux. vérifier les doublons dans les résultats, vérifier la linauto et les données sources"""
        )
    elif len(cptSansGeom) + len(cptSimpleSectHomo) + len(cptMultiSectHomo) + len(
        ppvHorsSectHomo
    ) < len(pt):
        warnings.warn(
            f"""la somme des éléments présents dans 'cptSansGeom', 'ppvHorsSectHomo', 'cptSimpleSectHomo', 'cptMultiSectHomo' est inférieure au nombre d'éléments initiaux. 
        vérifier le id_comptag = {~pt.loc[pt.id_comptag.isin(cptSansGeom.id_comptag.tolist()+ppvHorsSectHomo.id_comptag.tolist()+cptSimpleSectHomo.id_comptag.tolist()+
        cptMultiSectHomo.id_comptag.tolist())].gid}"""
        )
    return cptSansGeom, ppvHorsSectHomo, cptSimpleSectHomo, cptMultiSectHomo


def geomFromIdComptagCommunal(id_comptag, epsgSrc="4326", epsgDest="2154"):
    """
    trouver la géométrie depuis un identifiant de comptage communal format BDD (i.e avec les coordonnées WGS84 à la fin)
    in :
        id_comptag : string format Bdd communale (exemple 'Niort-place_de_la_breche--0.4585;46.3220')
    out :
        shapely geometrie de type point en EPSG:2154
    """
    coords = (
        [
            float(e)
            for e in re.findall(
                "(-[0-9]\.[0-9]{3,4});([0-9]{2}\.[0-9]{3,4})", id_comptag
            )[0]
        ]
        if Counter(id_comptag)["-"] >= 3
        else [
            float(e)
            for e in re.findall(
                "([0-9]\.[0-9]{3,4});([0-9]{2}\.[0-9]{3,4})", id_comptag
            )[0]
        ]
    )
    return O.reprojeter_shapely(Point(coords[0], coords[1]), epsgSrc, epsgDest)[1]


def creerCompteur(
    cptRef,
    attrGeom,
    dep,
    reseau,
    gestionnai,
    concession,
    type_poste,
    src_geo,
    periode,
    pr,
    absc,
    route,
    src_cpt,
    convention,
    sens_cpt,
    techno=None,
    obs_geo=None,
    obs_supl=None,
    id_cpt=None,
    id_sect=None,
    fictif=False,
    en_service=True,
):
    """
    creation d'une df prete a etre integree dans la table des compteurs
    les cahmps sont soit des valeurs, soit des series, soit des listes avec la bonne longueur
    in :
        cptRef : df ou geodataframe avec un attribut de géométrie en SRID = 2154
        attrGeom : string : nom de l'attribut supportant la géométrie
        dep : departement sur 2 caractère ou pd.Series
        reseau : valeur depuis liste enumeree (cf bdd) ou pd.Series de ces valeurs
        gestionnai : valeur depuis liste enumeree (cf bdd) ou pd.Series de ces valeurs
        concession : boolean ou pd.Series de booleean
    """
    df = cptRef.copy()
    df = df.assign(
        dep=dep,
        reseau=reseau,
        gestionnai=gestionnai,
        concession=concession,
        techno=techno,
        obs_geo=obs_geo,
        obs_supl=obs_supl,
        id_cpt=id_cpt,
        id_sect=id_sect,
        fictif=fictif,
        en_service=en_service,
        type_poste=type_poste,
        src_geo=src_geo,
        periode=periode,
        pr=pr,
        absc=absc,
        route=route,
        src_cpt=src_cpt,
        convention=convention,
        sens_cpt=sens_cpt,
    )
    df.loc[df[attrGeom].notna(), "x_l93"] = df.loc[df[attrGeom].notna()][attrGeom].apply(
        lambda x: round(x.x, 3))
    df.loc[df[attrGeom].isna(), "src_geo"] = None
    df.loc[df[attrGeom].isna(), "x_l93"] = None
    df.loc[df[attrGeom].notna(), "y_l93"] = df.loc[df[attrGeom].notna()][attrGeom].apply(
        lambda x: round(x.y, 3))
    O.checkAttributsinDf(df, [attrGeom] + attrCompteurValeurMano)
    O.checkAttributNonNull(df, attBddCompteurNonNull)
    df.drop(
        [c for c in df.columns if c not in (attBddCompteur + [attrGeom])],
        axis=1,
        inplace=True,
    )
    gdfCpt = gp.GeoDataFrame(df, geometry=attrGeom, crs=2154)
    gdfCpt = O.gp_changer_nom_geom(gdfCpt.drop('geom', axis=1, errors='ignore'), "geom")
    return gdfCpt


def creer_comptage(listComptage, annee, src, type_veh, obs=None, periode=None):
    """
    creer une df a inserer dans la table des comptage
    in :
        listComptage : liste des id_comptag concernes
        annee : text 4 caracteres : annee des comptage
        periode : text de forme YYYY/MM/DD-YYYY/MM/DD separe par ' ; ' si plusieurs periodes ou list de texte
        src : texte ou list : source des donnnes
        obs : txt ou list : observation
        type_veh : txt parmis les valeurs de enum_type_veh
    """
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        enumTypeVeh = pd.read_sql(
            f"select code from {schemaComptage}.{tableEnumTypeVeh}", c.sqlAlchemyConn
        ).code.tolist()
        listIdcomptagBdd = pd.read_sql(
            f"select id_comptag from {schemaComptage}.{tableCompteur}", c.sqlAlchemyConn
        ).id_comptag.tolist()
    if any([e not in listIdcomptagBdd for e in listComptage]):
        raise ValueError(
            f"les comptages {[e for e in listComptage if e not in listIdcomptagBdd]} ne sont pas dans la Bdd. Vérifier les correspondance de comptage ou creer les compteur en premier"
        )
    if type_veh not in enumTypeVeh:
        raise ValueError(f"type_veh doit etre parmi {enumTypeVeh}")
    if (
        not (int(annee) <= dt.datetime.now().year and int(annee) > 2000)
        or annee == "1900"
    ):
        raise ValueError(
            f"annee doit etre compris entre 2000 et {dt.datetime.now().year} ou egale a 1900"
        )
    # periode
    if isinstance(periode, str):
        if periode and not re.search(
            "(20[0-9]{2}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1])-20[0-9]{2}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1]))+( ; )*",
            periode,
        ):
            raise ValueError(
                f"la periode doit etre de la forme YYYY/MM/DD-YYYY/MM/DD separe par ' ; ' si plusieurs periodes"
            )
    if isinstance(periode, list):
        # verif que ça colle avec les id_comptag
        if len(periode) != len(listComptage):
            raise ValueError(
                "les liste de comptage et de periode doievnt avoir le mm nombre d elements"
            )
        for p in periode:
            if p and not re.search(
                "(20[0-9]{2}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1])-20[0-9]{2}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1]))+( ; )*",
                p,
            ):
                raise ValueError(
                    f"la periode doit etre de la forme YYYY/MM/DD-YYYY/MM/DD separe par ' ; ' si plusieurs periodes"
                )
    return pd.DataFrame(
        {
            "id_comptag": listComptage,
            "annee": annee,
            "periode": periode,
            "src": src,
            "obs": obs,
            "type_veh": type_veh,
        }
    )


def structureBddOld2NewForm(dfAConvertir, listAttrFixe, listAttrIndics, typeIndic):
    """
    convertir les données creer par les classes et issus de la structure de bdd 2010-2019 (wide-form) vers une structure 2020 (long-form)
    en ajoutant au passage les ids_comptag_uniq
    in :
        annee : texte : annee sur 4 caractere
        typeIndic : texte parmi 'agrege', 'mensuel', 'horaire'
        dfAConvertir : dataframe a transformer
        listAttrFixe : liste des attributs qui ne vont pas deveir des indicateurs
        listAttrIndics : liste des attributs qui vont devenir des indicateurs
    """
    if typeIndic not in ("agrege", "mensuel", "horaire"):
        raise ValueError(
            "le type d'indicateur doit etre parmi 'agrege', 'mensuel', 'horaire'"
        )
    if any([e not in listAttrFixe for e in ["id_comptag", "annee"]]):
        raise AttributeError(
            "les attributs id_comptag et annee sont obligatoire dans listAttrFixe"
        )
    if dfAConvertir.annee.isna().any() or not "annee" in dfAConvertir.columns:
        raise ValueError(
            "il manque l'attribut 'annee' ou certaines lignes de la df a convertir ont une valeur d'annee nulle, ce qui peut fausser les jointures. Corriger"
        )
    if typeIndic == "agrege":
        dfIndic = pd.melt(
            dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)),
            id_vars=listAttrFixe,
            value_vars=listAttrIndics,
            var_name="indicateur",
            value_name="valeur",
        )
        columns = [
            c
            for c in ["id_comptag", "indicateur", "valeur", "fichier", "obs", "annee"]
            if c in dfIndic.columns
        ]
        dfIndic = dfIndic[columns].rename(columns={"id": "id_comptag_uniq"})
    elif typeIndic == "mensuel":
        dfIndic = pd.melt(
            dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)),
            id_vars=listAttrFixe,
            value_vars=listAttrIndics,
            var_name="mois",
            value_name="valeur",
        )
        columns = [
            c
            for c in [
                "id_comptag",
                "donnees_type",
                "valeur",
                "mois",
                "fichier",
                "annee",
                "indicateur",
            ]
            if c in dfIndic.columns
        ]
        dfIndic = dfIndic[columns].rename(columns={"donnees_type": "indicateur"})
    elif typeIndic == "horaire":
        dfIndic = dfAConvertir.rename(columns={"type_veh": "indicateur"})
    dfIndic = recupererIdUniqComptage(dfIndic).drop(
        ["id_comptag", "annee"], axis=1, errors="ignore"
    )
    # si valeur vide on vire la ligne
    if typeIndic in ("agrege", "mensuel"):
        dfIndic.dropna(subset=["valeur"], inplace=True)
    # mise en forme des donnees horaires
    if typeIndic == "horaire":
        dfIndic.drop(
            [c for c in dfIndic.columns if c not in attrIndicHoraire],
            axis=1,
            errors="ignore",
            inplace=True,
        )
    if not dfIndic.loc[dfIndic.id_comptag_uniq.isna()].empty:
        print(dfIndic.columns)
        raise ValueError(
            f"certains comptages ne peuvent etre associes a la table des comptages de la Bdd {dfIndic.loc[dfIndic.id_comptag_uniq.isna()].id_comptag_uniq.tolist()}"
        )
    return dfIndic


def structureBddOld2NewFormAssoc(
    dfAConvertir, annee, listAttrFixe, listAttrIndics, typeIndic
):
    """
    convertir les données creer par les classes et issus de la structure de bdd 2010-2019 (wide-form) vers une structure 2020 (long-form)
    en ajoutant au passage les ids_comptag_uniq
    in :
        annee : texte : annee sur 4 caractere
        typeIndic : texte parmi 'agrege', 'mensuel', 'horaire'
        dfAConvertir : dataframe a transformer
        listAttrFixe : liste des attributs qui ne vont pas deveir des indicateurs
        listAttrIndics : liste des attributs qui vont devenir des indicateurs
    """
    if typeIndic not in ("agrege", "mensuel", "horaire"):
        raise ValueError(
            "le type d'indicateur doit etre parmi 'agrege', 'mensuel', 'horaire'"
        )
    if any(
        [e not in listAttrFixe for e in ["id_cptag_ref", "annee", "id_cpteur_asso"]]
    ):
        raise AttributeError(
            "les attributs id_comptag_ref, id_cpteur_asso et annee sont obligatoire dans listAttrFixe"
        )
    if typeIndic == "agrege":
        dfIndic = pd.melt(
            dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)),
            id_vars=listAttrFixe,
            value_vars=listAttrIndics,
            var_name="indicateur",
            value_name="valeur",
        )
        columns = [
            c
            for c in [
                "id_cptag_ref",
                "indicateur",
                "valeur",
                "fichier",
                "obs",
                "annee",
                "id_cpteur_asso",
            ]
            if c in dfIndic.columns
        ]
        dfIndic = dfIndic[columns].rename(
            columns={"id": "id_comptag_uniq"}
        )  # [attrIndicAgregeAssoc]
    # elif typeIndic == 'mensuel':
    #    dfIndic = pd.melt(dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)), id_vars=listAttrFixe, value_vars=listAttrIndics,
    #                         var_name='mois', value_name='valeur')
    #    columns = [c for c in ['id_comptag', 'donnees_type', 'valeur', 'mois', 'fichier', 'annee'] if c in dfIndic.columns]
    #    dfIndic = dfIndic[columns].rename(columns={'donnees_type':'indicateur'})
    elif typeIndic == "horaire":
        dfIndic = dfAConvertir.rename(columns={"type_veh": "indicateur"})
        dfIndic = dfIndic.drop(
            ["id_comptag"]
            + [
                c
                for c in dfIndic.columns
                if c
                not in attrIndicHoraireAssoc
                + ["id_cptag_ref", "annee", "id_cpteur_asso"]
            ],
            axis=1,
            errors="ignore",
        )
    dfIndic = dfIndic.merge(
        recupererIdUniqComptageAssoc(dfIndic.id_cptag_ref.tolist()),
        on=["id_cptag_ref", "id_cpteur_asso"],
    ).drop(["id_cptag_ref", "annee", "id_cpteur_asso"], axis=1, errors="ignore")
    # si valeur vide on vire la ligne
    if typeIndic in ("agrege", "mensuel"):
        dfIndic.dropna(subset=["valeur"], inplace=True)
    if not dfIndic.loc[dfIndic.id_comptag_uniq.isna()].empty:
        print(dfIndic.columns)
        raise ValueError(
            f"certains comptages ne peuvent etre associes a la table des comptages de la Bdd {dfIndic.loc[dfIndic.id_comptag_uniq.isna()].id_comptag_uniq.tolist()}"
        )
    return dfIndic


def rangBddComptageAssoc(id_comptag_ref_Bdd):
    """
    Récupérer dans la bdd, le rang max associé à un comptage de réference dans la table comptage_assoc.comptage
    in :
        id_comptag_ref_Bdd : identifiant de comptage de reference dans la bdd
    """
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        rqt = f"""SELECT COALESCE(max(cac.rang),0) rang 
                   FROM {schemaComptageAssoc}.{tableComptage} cac JOIN {schemaComptage}.{tableComptage} c ON c.id = cac.id_cptag_ref 
                   WHERE c.id_comptag='{id_comptag_ref_Bdd}'"""
        rang = pd.read_sql(rqt, c.sqlAlchemyConn).iloc[0]
    return rang


def creerComptageAssoc(
    df,
    id_comptag_ref_nom,
    annee,
    id_compteur_asso_nom,
    src=None,
    listIdCptExclu=None,
    dicoIdCpteurRef=None,
):
    """
    a partir d'une df, creer la df a injecter dans les tables compteur et comptage du schema comptage_assoc de la bdd
    in :
        df : la df de base, doit contenir obligatoirement les attributs decrivant l'id_comptag_ref, le rang, le type_veh. si possible la periode, src, obs.
             si le comptage_assoc comprend une dimension géométrique, les attributs decrivant l'id_compteur_asso, le type_poste, la src_geo, la src_cpt, convention,
             et sens_cpt sont obligatoire. si possible ajouter route, pr, abs, techno, obs_geo, obs_supl
        id_comptag_ref_nom : string : nom de l'attribut supportant l'id_comptag du point de référence
        id_compteur_asso_nom : string : nom de l'attribut supportant l'id_comptag du point associé si composante geometrique (i.e l'id_comptag issu des donnees gestionnaire)
        annee : string annee sur 4 caractères
        listIdCptExclu : liste d'identifiant de comptage a ne pas conserver dans les resultats,
        dicoIdCpteurRef : cf comptagesAssocDefinirIdCompteurRef()
    out :
        dfIds : df contenant tous les points de la df en entree, avec les attributs issus de la Bdd en plus
        tableComptage : df au format de la table des comptage Associes. ne contient que les points
    """

    def remplirObsSelonVacances(obsExistant, vacances):
        """
        si unn champs obs existe deja, on ajoute un phrase sur le fait que le point a été réalisé pendant les vacances, sinon on
        ne met ue la menstion relatives aux vacances
        in :
            obsexistant : strin ou valeur nulle
            vacances : boolean
        out :
            string
        """
        if not pd.isnull(obsExistant):
            if vacances:
                return f"{obsExistant} ; une partie des mesures sont réalisées pendant les vacances scolaires"
        else:
            if vacances:
                return "une partie des mesures sont réalisées pendant les vacances scolaires"
        return None

    # mise en forme
    dfSource = df.copy()
    O.checkAttributsinDf(dfSource, ["type_veh", "periode"])
    if listIdCptExclu:
        dfSource = dfSource.loc[
            ~dfSource[id_compteur_asso_nom].isin(listIdCptExclu)
        ].copy()
    dfSource.rename(columns={id_compteur_asso_nom: "id_cpteur_asso"}, inplace=True)
    if dicoIdCpteurRef:
        dfSource[id_comptag_ref_nom] = dfSource.apply(
            lambda x: comptagesAssocDefinirIdCompteurRef(x, dicoIdCpteurRef), axis=1
        )
    # correspondance d'identifiants de comptage si besoin
    dfSource[id_comptag_ref_nom] = corresp_nom_id_comptag(
        dfSource.drop("id_comptag", axis=1, errors="ignore").rename(
            columns={id_comptag_ref_nom: "id_comptag"}
        )
    ).id_comptag.tolist()
    corresIdComptagInterne = recupererIdUniqComptage(
        dfSource[[id_comptag_ref_nom]].rename(
            columns={id_comptag_ref_nom: "id_comptag"}
        ),
        True,
    )
    dfIds = (
        dfSource.merge(
            corresIdComptagInterne,
            left_on=id_comptag_ref_nom,
            right_on="id_comptag",
            how="left",
        )
        .rename(columns={"id_comptag_uniq": "id_cptag_ref"})
        .assign(annee=annee)
    )
    dfIds["rang_bdd"] = dfIds[id_comptag_ref_nom].apply(
        lambda x: rangBddComptageAssoc(x)
    )
    dfIds["rang_df"] = dfIds.groupby(id_comptag_ref_nom).cumcount() + 1
    dfIds["rang"] = dfIds["rang_bdd"] + dfIds["rang_df"]
    tableComptage = dfIds[[c for c in dfIds.columns if c in attrComptageAssoc]].copy()
    # ajouter ou modifier le champs observation pour les ponctuels ou tournant dont une partie de la periode est en vacances scolaire
    if "obs" in tableComptage.columns:
        tableComptage["obs"] = tableComptage.apply(
            lambda x: remplirObsSelonVacances(x.obs, O.verifVacanceRange(x.periode)),
            axis=1,
        )
    else:
        tableComptage["obs"] = tableComptage.periode.apply(
            lambda x: remplirObsSelonVacances(None, O.verifVacanceRange(x))
        )
    return dfIds, tableComptage


def creerCompteurAssoc(
    df,
    nomAttrIdCpteurAsso,
    nomAttrGeom=None,
    dicoIdCpteurRef=None,
    listIdCptExclu=None,
    NomAttrFinal=None,
):
    """
    a partir d'une df, creer la table des compteurdu schema comptage_assoc de la bdd. la table source doit contenir les attributs d'ientifiant
    de comptage, type_poste, src_geo, src_cpt, convention, sens_cpt.
    pour plus de precision elle peut contenir geom, route, pr, abs, techno, obs_geo, obs_supl, id_cpt, id_sect, id_cpteur_ref.
    Cf Bdd pour plus de détail
    in :
        df : dataframe des donnees sources
        nomAttrIdCpteurAsso : string : nom de l'attribut contenant les id_comptag mis en forme a partir du gestionnaire
        nomAttrIdCpteurRef : string : nom de l'attribut contenant les id_comptag de référence de la table compteur du schema
                                      comptapge de la bdd
        nomAttrGeom : string : nom de l'attribut qui supporte la géométrie
        dicoIdCpteurRef : cf comptagesAssocDefinirIdCompteurRef()
    out :
        dataframe au format bdd comptage_assoc.compteur
    """
    O.checkAttributsinDf(
        df,
        [
            nomAttrIdCpteurAsso,
            "type_poste",
            "src_geo",
            "src_cpt",
            "convention",
            "sens_cpt",
        ],
    )
    dfSource = df.copy()
    if listIdCptExclu:
        dfSource = dfSource.loc[
            ~dfSource[nomAttrIdCpteurAsso].isin(listIdCptExclu)
        ].copy()
    dfSource.rename(columns={nomAttrIdCpteurAsso: "id_cpteur_asso"}, inplace=True)
    if dicoIdCpteurRef and NomAttrFinal:
        dfSource[NomAttrFinal] = dfSource.apply(
            lambda x: comptagesAssocDefinirIdCompteurRef(x, dicoIdCpteurRef), axis=1
        )
    if nomAttrGeom:
        dfSource = O.gp_changer_nom_geom(
            dfSource.drop("geom", axis=1, errors="ignore"), "geom"
        )
    dfSource = dfSource.drop("id_cpteur_ref", axis=1, errors="ignore").rename(
        columns={NomAttrFinal: "id_cpteur_ref"}
    )
    tableCompteur = dfSource[
        [c for c in dfSource.columns if c in attrCompteurAssoc]
    ].copy()
    for k, v in dicoTypeAttributs.items():
        if tableCompteur[k].dtype != v:
            tableCompteur.loc[tableCompteur[k].notna(), k] = tableCompteur.loc[
                tableCompteur[k].notna()
            ][k].astype(v)
    # correspondance d'identifiants de comptage si besoin
    tableCompteur["id_cpteur_ref"] = corresp_nom_id_comptag(
        tableCompteur.drop("id_comptag", axis=1, errors="ignore").rename(
            columns={"id_cpteur_ref": "id_comptag"}
        )
    ).id_comptag.tolist()
    return tableCompteur


def comptagesAssocDefinirIdCompteurRef(
    df,
    dicoIdCpteurRef={
        1: "id_comptag_bdd_tronc_homo_topo",
        2: "id_comptag_tronc_homo_traf",
    },
):
    """
    si il y a plusieurs attributs d'identifiant de comptage de référence pour un compteur associé, affecter le bon.
    se base sur un dico de type entier auto-incrémentée: attribut a oprendre en compte. Plus l'entier est petit, plus l'attribut est prioritaire
    in :
        df : la ligne de la df à modifier
        dicoIdCpteurRef : dico de type entier auto-incrémentée: attribut a prendre en compte. Plus l'entier est petit, plus l'attribut est prioritaire
        attrFinal : string : nom final de l'attribut du compteur de référence
    """
    sorted_dicoIdCpteurRef = {
        key: dicoIdCpteurRef[key] for key in sorted(dicoIdCpteurRef.keys())
    }
    for v in sorted_dicoIdCpteurRef.values():
        if not pd.isnull(df[v]):
            return df[v]


def creerListTransfertComptage2ComptageAssoc(
    dfCreationCompteurExistDevientAssoc, annee, dicoIdCpteurRef
):
    """
    fonction de creation d'uine liste de paramètre pour le transfert de compteur et comptage
    depuis le schema comptage vers le schema comoptage_assoc
    de la Bdd
    in :
        dfCreationCompteurExistDevientAssoc : df des compteurs à transféré, issu du process de caractérisation des nouveaux
                                              points gestionnaires.
        annee  : string sur 4 caractères
        dicoIdCpteurRef : nécéssaire à la fonction comptagesAssocDefinirIdCompteurRef ; dico de type entier auto-incrémentée:
                          attribut a prendre en compte.
                          Plus l'entier est petit, plus l'attribut est prioritaire
    """
    O.checkAttributsinDf(
        dfCreationCompteurExistDevientAssoc,
        ["id_comptag"] + list(dicoIdCpteurRef.values()),
    )
    dfCreationCompteurExistDevientAssoc = dfCreationCompteurExistDevientAssoc.copy()
    dfCreationCompteurExistDevientAssoc[
        "id_comptag_bdd"
    ] = dfCreationCompteurExistDevientAssoc.apply(
        lambda x: comptagesAssocDefinirIdCompteurRef(x, dicoIdCpteurRef), axis=1
    )
    dfLastAnKnow = recupererLastAnnKnow(
        dfCreationCompteurExistDevientAssoc.id_comptag_bdd.tolist()
    )
    dfMerge = dfCreationCompteurExistDevientAssoc[
        ["id_comptag", "id_comptag_bdd"]
    ].merge(
        dfLastAnKnow.rename(columns={"id_comptag": "id_comptag_bdd"}),
        on="id_comptag_bdd",
    )
    listParamsFonctionPostgres = list(
        dfMerge[["id_comptag", "id_comptag_bdd", "annee_tmja"]]
        .assign(annee=annee)
        .itertuples(index=False, name=None)
    )
    return listParamsFonctionPostgres


def appelFonctionTransfertComptage2ComptageAssoc(
    listParamsFonctionPostgres, recup_compteur=False, sup_compteur=False
):
    """
    fonction de creation d'une string permettant l'appel de la fonction comptage.transfert_comptage_assoc() dans postgres puyis
    d'appel de cette fonction
    in :
        listParamsFonctionPostgres : liste de tuples de tsring, issue de creerListTransfertComptage2ComptageAssoc().
                                     attention !!! : dans les tuples, l'ordre doit etre :
                                         1 : id_comptage_ref : id_comptag qui va rester dans le schema comptage de la bdd
                                         2 : id_comptage_asso : id_comptag qui va etre basculé dans le schema comptage_assoc de la bdd
                                         3 : annee_asso : annee du comptage qui va etre basculé dans le sxchmé comptage assoc
                                         4 : annee_ref : annee du comptage qui va rester dans le schema comptage de la bdd
    """
    txtAppelFonction = " ; ".join(
        [
            f"""select comptage.transfert_comptage_assoc('{e[0]}', '{e[1]}', '{e[3]}', '{e[2]}', {recup_compteur}, {sup_compteur})"""
            for e in listParamsFonctionPostgres
        ]
    )
    with ct.ConnexionBdd() as c:
        # si le compteur qui devient asso est référencé dans la table des cpteur assoc, il faut modifier ce référencement
        listIdCpteurRefDevientAssoc = [e[1] for e in listParamsFonctionPostgres]
        rqtCpteurRef = f"select * from {schemaComptageAssoc}.{tableCompteur} where id_cpteur_ref = ANY(array{listIdCpteurRefDevientAssoc})"
        idCpteurRefDansAssoc = pd.read_sql(rqtCpteurRef, c.sqlAlchemyConn)
        for e in filter(
            lambda x: x[1] in idCpteurRefDansAssoc.id_cpteur_ref.tolist(),
            listParamsFonctionPostgres,
        ):
            c.curs.execute(
                f"update {schemaComptageAssoc}.{tableCompteur} set id_cpteur_ref = '{e[0]}' where id_cpteur_ref = '{e[1]}'",
                c.sqlAlchemyConn,
            )
            c.connexionPsy.commit()
        c.curs.execute(txtAppelFonction, c.sqlAlchemyConn)
        c.connexionPsy.commit()
    return txtAppelFonction


def creerCorrespComptag(df, nomAttrIdComptagGest, nomAttrIdComptagGti, listIdCptExclu):
    """
    creer une df a integrer dans la bdd a partir d'une df comprenant : un attribut id_comptag de la bdd et un attribut id_comptag issu des données gestionnaire.
    si la df commprend des type poste gestionnaire et bdd on peut ajouter le nom de ces attributs pour faire un test
    in :
        df : df de base doit contenir un attribut id_comptag de la bdd et un attribut id_comptag issu des données gestionnaire
        nomAttrIdComptagGest : string : nom de l'attribut décrivant l'id_comptag issu des données gestionnaire
        nomAttrIdComptagGti : string : nom de l'attribut décrivant l'id_comptag de la bdd
        listIdCptExclu : list ou tuple : list des id_comptage à ne pas prendre en compte
    """
    return (
        df.loc[~df[nomAttrIdComptagGest].isin(listIdCptExclu)][
            [nomAttrIdComptagGest, nomAttrIdComptagGti]
        ]
        .copy()
        .rename(
            columns={nomAttrIdComptagGest: "id_gest", nomAttrIdComptagGti: "id_gti"}
        )
    )


def hierarchisationCompteur(typePoste, periode, tmja, pc_pl):
    """
    Pour les compteurs dont plusieurs sont présent sur la même section homogène de trafic
    obtention d"'une note globale de hierarhisation, a partir des 3 fonction incluse'
    """

    def hierarchisationTypePoste(typePoste):
        """
        attribuer une valeur de 3*10**21, 2*10**21 ou 10**21 selon le type de poste
        in :
            typePoste : string parmi 'permanent', 'tournant', 'ponctuel'
        """
        O.checkParamValues(typePoste, enumTypePoste)
        if typePoste == "permanent":
            return 3 * 10**15
        elif typePoste == "tournant":
            return 2 * 10**15
        elif typePoste == "ponctuel":
            return 10**15
        else:
            raise ValueError("type de poste non affecte a une note")

    def hierarchisationVacance(periode):
        """
        attribuer une valeur de 2*10**18 ou 10**18 selon qu'une partie de la mesure a été faie pendant les vacances scolaire
        in :
            periode : string sou forme YYYY/MM/DD-YYYY/MM/DD éventuellement séparé par des ' ; '
        """
        testVacances = O.verifVacanceRange(periode)
        if not periode or testVacances:
            return 10**12
        else:
            return 2 * 10**12

    def hierarchisationTrafic(tmja, pc_pl):
        """
        fournir une valeur qui concatene le trafic et le pc_pl, en favorisant le trafic
        """
        if not tmja or tmja <= 0:
            return ValueError("le tmja doit etre une superieur a 0  non nulle")
        elif not pc_pl:
            return tmja * 1000
        else:
            return (tmja * 1000) + pc_pl

    return (
        hierarchisationTypePoste(typePoste)
        + hierarchisationVacance(periode)
        + hierarchisationTrafic(tmja, pc_pl)
    )


def ventilerDoublons(df):
    """
    dans une df, vérifier qu'i ln'y a pas de doublons en id_comptag, que ce soit de façon naticve ou suite à une modification
    de correspondance
    si c'est le cas, ventiler les doublons entre comptage de reference et comptage associe
    in :
        df : dataframe des donnees a tester
    out :
        ref : dataframe des id_comptag de reference
        assoc : dataframe des id_comptag associe, avec champs id_comptag_ref
    """
    doublonsNatifs = (
        df.loc[df.duplicated("id_comptag", keep=False)]
        .copy()
        .drop("gid", axis=1, errors="ignore")
    )
    notDoublonsNatifs = df.loc[~df.duplicated("id_comptag", keep=False)].copy()
    if (
        not doublonsNatifs.empty
    ):  # si c'est le cas, il faut néttoyer la donnees et creer des comptages associés (a reprendre en natif dans les fonctions)
        ref, assoc = ventilerCompteurRefAssoc(
            doublonsNatifs.assign(id_comptag2=doublonsNatifs.id_comptag).rename(
                columns={"id_comptag2": "gid"}
            )
        )
        ref = pd.concat([notDoublonsNatifs, ref])
        if len(ref) + len(assoc) != len(df):
            raise ValueError("pb de répartition des doublons, vérifier manuellement")
    else:
        ref = df.copy()
        assoc = None
    return ref, assoc


def ventilerCompteurRefAssoc(cptMultiSectHomo):
    """
    a partir d'une df des points a inserer multiple pour une section homogene (cf ventilerParSectionHomogene())
    classer les points en comptages de reference ou comptage associe, pour ne conserver qu'un seul point de reference
    par section
    in :
        cptMultiSectHomo : df des points qui partage une section homogene avec un autre bnouveau point
                           (doit contenir l'identifiantde section homogene 'gid', et 'id_comptag', 'type_poste',
                           'periode', 'pc_pl', 'tmja')
    out :
        cptRefMultiSectHomo : dfdes comptages de references
        cptAssocMultiSectHomo : df des comptages associes
    """
    if cptMultiSectHomo.empty:
        return pd.DataFrame([]), pd.DataFrame([])
    O.checkAttributsinDf(
        cptMultiSectHomo, ["gid", "type_poste", "periode", "pc_pl", "tmja"]
    )
    cptMultiSectHomo["note_hierarchise"] = cptMultiSectHomo.apply(
        lambda x: hierarchisationCompteur(x.type_poste, x.periode, x.tmja, x.pc_pl),
        axis=1,
    )
    cptRefMultiSectHomo = (
        cptMultiSectHomo.loc[
            cptMultiSectHomo.groupby("gid").note_hierarchise.transform("max")
            == cptMultiSectHomo.note_hierarchise
        ]
        .sort_values("gid")
        .copy()
    )
    cptAssocMultiSectHomo = (
        cptMultiSectHomo.loc[
            cptMultiSectHomo.groupby("gid").note_hierarchise.transform("max")
            != cptMultiSectHomo.note_hierarchise
        ]
        .sort_values("gid")
        .copy()
    )
    cptAssocMultiSectHomo = cptAssocMultiSectHomo.merge(
        cptRefMultiSectHomo[["gid", "id_comptag"]], on="gid", suffixes=(None, "_ref")
    )
    # gestion du cas ou un comptage est associe a2 comptage de references differents (i.e il sont du mmm type, sur periode equivalente, avec le mm TMJA)
    # dans ce cas on prend au hasard
    cptAssocMultiSectHomo.drop_duplicates(["id_comptag"], inplace=True)
    # verif que tous les gid ont un cpt ref
    if not cptAssocMultiSectHomo.loc[
        ~cptAssocMultiSectHomo.gid.isin(cptRefMultiSectHomo.gid.unique())
    ].empty:
        raise ValueError("un des comptage associe n'a pas de comptage de reference")
    if not len(cptRefMultiSectHomo) + len(cptAssocMultiSectHomo) == len(
        cptMultiSectHomo
    ):
        raise ValueError(
            "un ou plusieurs comptage n'ont pas ete affecte comme reference ou associe, ou sont en doublons"
        )
    return cptRefMultiSectHomo, cptAssocMultiSectHomo


def ventilerCompteurIdComptagExistant(cptSimpleSectHomo, cptRefMultiSectHomo):
    """
    pour les comptages de référence, catégoriser en fonction de la présence d'un id_comptag ou non sur la section homogène
    in :
        cptSimpleSectHomo : df des comptage situe seul sur une section homogne (cf ventilerParSectionHomogene())
        cptRefMultiSectHomo : df des comptage de references issus des points situes sur des sections homogenes a point multiple (cf ventilerCompteurRefAssoc())
    out :
        cptRefSectHomoNew : dataframe des comptages de reference sur section homogene sans id_comptag
        cptRefSectHomoOld : dataframe des comptages de reference sur section homogene avec id_comptag
    """
    cptRefMultiSectHomoNew = (
        cptRefMultiSectHomo.loc[cptRefMultiSectHomo.id_comptag_bdd.isna()].copy()
        if not cptRefMultiSectHomo.empty
        else pd.DataFrame([])
    )
    cptRefMultiSectHomoOld = (
        cptRefMultiSectHomo.loc[~cptRefMultiSectHomo.id_comptag_bdd.isna()].copy()
        if not cptRefMultiSectHomo.empty
        else pd.DataFrame([])
    )
    cptSimpleSectHomoNew = (
        cptSimpleSectHomo.loc[cptSimpleSectHomo.id_comptag_bdd.isna()].copy()
        if not cptSimpleSectHomo.empty
        else pd.DataFrame([])
    )
    cptSimpleSectHomoOld = (
        cptSimpleSectHomo.loc[~cptSimpleSectHomo.id_comptag_bdd.isna()].copy()
        if not cptSimpleSectHomo.empty
        else pd.DataFrame([])
    )
    cptRefSectHomoNew = pd.concat([cptRefMultiSectHomoNew, cptSimpleSectHomoNew])
    cptRefSectHomoOld = pd.concat([cptRefMultiSectHomoOld, cptSimpleSectHomoOld])
    return cptRefSectHomoNew, cptRefSectHomoOld


def ventilerNouveauComptageRef(
    df, dep, tableTroncHomo, tableRefLineaire, distancePlusProcheVoisin=20
):
    """
    depuis une df des comptage de référence situé sur des tronçons avec un id_comptag existant dans la base,
    séparer en 4 groupe selon le type de poste dans la bdd et le type de poste du hestionnaire.
    in :
        df : dataframe a classifier
        tableTronconHomogeneTrafic : table ou vue dans bdd OTV qui contientid_ign, id_groupe de troncon homogene de trafic et compteur associe si il existe
        dep :string sur 2 caractères
        tableTroncHomo : table du schema linauto qui contient le regroupement en tronçons homogène des trafic
        tableRefLineaire : table qui contient le lineaire epuree, avec un id integer
        distancePlusProcheVoisin : distance au plus proche voisin pour recherche de corresp_id_comptag
    out :
        dfCorrespIdComptag : dataframe des points qui vont simplelnet faire l'objet d'une correspondance d'id_comptage
        dfCreationComptageAssocie : dataframe de spoint qui vont de suite devenir des comptages associes
        dfModifTypePoste : dataframe des points existant dont on va modifier le type de poste
        dfCreationCompteur : dataframe des points qui vont faire l'objet d'un  nouveau compteur
    """
    rqtTronconHomogene = f"""
    WITH ppv_cpt_ref AS (
    SELECT DISTINCT ON (t.id) t.id, t.geom, c.id_comptag, c.type_poste, c.annee_tmja, c.tmja, c.vacances_zone_b 
     FROM comptage.{vueLastAnnKnow} c JOIN {tableRefLineaire} t ON st_dwithin(c.geom, t.geom, 30)
     WHERE t.dept = '{dep}' AND c.dep = '{dep}'
     ORDER BY t.id, st_distance(c.geom, t.geom)),
    pt_par_gid AS (
    SELECT DISTINCT ON (l.gid) t.id_comptag, t.type_poste, t.annee_tmja, t.tmja, t.vacances_zone_b, l.gid
     FROM ppv_cpt_ref t LEFT JOIN (SELECT UNNEST(list_id) id, gid FROM {tableTroncHomo}) l using(id)
     ORDER BY l.gid, CASE WHEN t.type_poste = 'permanent' THEN 1 
                               WHEN t.type_poste = 'tournant' THEN 2
                               WHEN t.type_poste = 'ponctuel' THEN 3 
                               ELSE 4 
                               end, CASE WHEN t.vacances_zone_b IS NULL OR NOT t.vacances_zone_b THEN 1
                                         ELSE 2 END, t.tmja DESC),
    gid_et_ligne as( 
    SELECT t.id, t.geom, l.gid
     FROM {tableRefLineaire} t LEFT JOIN (SELECT UNNEST(list_id) id, gid FROM {tableTroncHomo}) l USING(id)
     WHERE t.dept = '{dep}')                                     
    SELECT t.id, t.geom,  p.id_comptag, p.type_poste type_poste_bdd, p.annee_tmja annee_tmja_bdd, p.tmja tmja_bdd, p.vacances_zone_b, t.gid
     FROM pt_par_gid p RIGHT JOIN gid_et_ligne t USING (gid) ; """
    # récuperer depuis la bdd la listedes tronçons homogenes de trafic, avec le compteur le plus proche
    O.checkAttributsinDf(df, ["id_comptag", "type_poste", "periode", "annee"])
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        tronconsHomogeneTrafic = gp.read_postgis(
            rqtTronconHomogene, c.sqlAlchemyConn
        ).rename(columns={"gid": "id_tronc_homo"})
        # associer les comptages que l'on chreche a qualifier aux tronçons qui supportent (ou non) un compteur, au sein d'un tronçon homogene
    ppvtronconsHomogeneTrafic = O.plus_proche_voisin(
        gp.GeoDataFrame(df, geometry="geom_x", crs="epsg:2154").rename(
            columns={"id_comptag": "id_comptag_gest"}
        ),
        tronconsHomogeneTrafic,
        distancePlusProcheVoisin,
        "id_comptag_gest",
        "id_tronc_homo",
    )
    # ventiler les compteurs selon leur rattachement a un troncon homogene topologique ou un tronçon homogene de trafic
    dfMergeCptGestTroncHomo = (
        ppvtronconsHomogeneTrafic.merge(
            tronconsHomogeneTrafic.drop_duplicates(
                [
                    "id_comptag",
                    "type_poste_bdd",
                    "annee_tmja_bdd",
                    "tmja_bdd",
                    "vacances_zone_b",
                    "id_tronc_homo",
                ]
            ),
            on="id_tronc_homo",
        )
        .rename(
            columns={
                "id_comptag_gest": "id_comptag",
                "id_comptag": "id_comptag_bdd_tronc_homo_topo",
                "type_poste_bdd": "type_poste_bdd_tronc_homo_topo",
            }
        )
        .merge(
            df.rename(
                columns={
                    "id_comptag_bdd": "id_comptag_tronc_homo_traf",
                    "type_poste_bdd": "type_poste_bdd_tronc_homo_traf",
                }
            ),
            on="id_comptag",
        )
    )
    dfCptGestTroncHomoTopo = dfMergeCptGestTroncHomo[
        (dfMergeCptGestTroncHomo["id_comptag_bdd_tronc_homo_topo"].notna())
    ]
    dfCptGestTroncHomoTraf = dfMergeCptGestTroncHomo[
        (dfMergeCptGestTroncHomo["id_comptag_bdd_tronc_homo_topo"].isna())
    ]
    # ventiler les compteurs sur les tronçons homogene de trafic en fonction de la presence ou non
    # d'un identifiant de comptage dans la bdd sur ce tronçon homo de trafic
    dfCptGestTroncHomoTrafAvecIdComptag = dfCptGestTroncHomoTraf.loc[
        dfCptGestTroncHomoTraf.id_comptag_tronc_homo_traf.notna()
    ]
    dfCptGestTroncHomoTrafSansIdComptag = dfCptGestTroncHomoTraf.loc[
        dfCptGestTroncHomoTraf.id_comptag_tronc_homo_traf.isna()
    ]
    # ventiler les compteur sur un troncon homogne topologique avec un comptage de reference
    dfCorrespIdComptagTroncHomoTopo = dfCptGestTroncHomoTopo.loc[
        dfCptGestTroncHomoTopo.type_poste_bdd_tronc_homo_topo
        == dfCptGestTroncHomoTopo.type_poste
    ]
    dfCreationComptagAssocTroncHomoTopo = dfCptGestTroncHomoTopo.loc[
        (
            (dfCptGestTroncHomoTopo.type_poste == "ponctuel")
            & (
                dfCptGestTroncHomoTopo.type_poste_bdd_tronc_homo_topo.isin(
                    ("permanent", "tournant")
                )
            )
        )
        | (
            (dfCptGestTroncHomoTopo.type_poste == "tournant")
            & (dfCptGestTroncHomoTopo.type_poste_bdd_tronc_homo_topo == "permanent")
        )
    ]
    dfCreationCompteurExistDevientAssocTroncHomoTopo = dfCptGestTroncHomoTopo.loc[
        (
            (dfCptGestTroncHomoTopo.type_poste_bdd_tronc_homo_topo == "ponctuel")
            & (dfCptGestTroncHomoTopo.type_poste.isin(("permanent", "tournant")))
        )
        | (
            (dfCptGestTroncHomoTopo.type_poste == "permanent")
            & (dfCptGestTroncHomoTopo.type_poste_bdd_tronc_homo_topo == "tournant")
        )
    ]
    # vérif
    if sum(
        [
            len(e)
            for e in (
                dfCorrespIdComptagTroncHomoTopo,
                dfCreationComptagAssocTroncHomoTopo,
                dfCreationCompteurExistDevientAssocTroncHomoTopo,
            )
        ]
    ) != len(dfCptGestTroncHomoTopo):
        warnings.warn(
            """la somme des éléments présents dans la ventilation des compteurs présents sur un troncon homogene
                         topologique est différentes du nombre d'éléments initiaux. vérifier le code et les données"""
        )
    # ventiler les compteur sur un troncon homogene de trafic avec un comptage de reference
    dfCreationComptagAssocTroncHomoTraf = dfCptGestTroncHomoTrafAvecIdComptag.loc[
        (
            (
                dfCptGestTroncHomoTrafAvecIdComptag.periode.apply(
                    lambda x: O.verifVacanceRange(x)
                )
            )
            & (
                dfCptGestTroncHomoTrafAvecIdComptag.type_poste_bdd_tronc_homo_traf
                == "ponctuel"
            )
            & (dfCptGestTroncHomoTrafAvecIdComptag.type_poste == "ponctuel")
        )
        | (
            (
                dfCptGestTroncHomoTrafAvecIdComptag.type_poste_bdd_tronc_homo_traf.isin(
                    ("permanent", "tournant")
                )
            )
            & (dfCptGestTroncHomoTrafAvecIdComptag.type_poste == "ponctuel")
        )
        | (
            (
                dfCptGestTroncHomoTrafAvecIdComptag.type_poste_bdd_tronc_homo_traf
                == "permanent"
            )
            & (dfCptGestTroncHomoTrafAvecIdComptag.type_poste == "tournant")
        )
    ]
    dfCreationCompteurExistDevientAssocTroncHomoTraf = dfCptGestTroncHomoTrafAvecIdComptag.loc[
        (
            (
                ~dfCptGestTroncHomoTrafAvecIdComptag.periode.apply(
                    lambda x: O.verifVacanceRange(x)
                )
            )
            & (
                dfCptGestTroncHomoTrafAvecIdComptag.type_poste_bdd_tronc_homo_traf
                == "ponctuel"
            )
            & (dfCptGestTroncHomoTrafAvecIdComptag.type_poste == "ponctuel")
        )
        | (
            ~(
                dfCptGestTroncHomoTrafAvecIdComptag.annee_tmja_bdd
                == dfCptGestTroncHomoTrafAvecIdComptag.annee
            )
            & (
                (
                    dfCptGestTroncHomoTrafAvecIdComptag.type_poste
                    == dfCptGestTroncHomoTrafAvecIdComptag.type_poste_bdd_tronc_homo_traf
                )
                & (dfCptGestTroncHomoTrafAvecIdComptag.type_poste != "ponctuel")
            )
        )
        | (
            (
                dfCptGestTroncHomoTrafAvecIdComptag.type_poste_bdd_tronc_homo_traf
                == "ponctuel"
            )
            & (
                dfCptGestTroncHomoTrafAvecIdComptag.type_poste.isin(
                    ("permanent", "tournant")
                )
            )
        )
        | (
            (
                dfCptGestTroncHomoTrafAvecIdComptag.type_poste_bdd_tronc_homo_traf
                == "tournant"
            )
            & (dfCptGestTroncHomoTrafAvecIdComptag.type_poste == "permanent")
        )
    ]
    dfCreationCompteurTroncHomoTraf = dfCptGestTroncHomoTrafAvecIdComptag.loc[
        (
            (
                dfCptGestTroncHomoTrafAvecIdComptag.annee_tmja_bdd
                == dfCptGestTroncHomoTrafAvecIdComptag.annee
            )
            & (
                (
                    dfCptGestTroncHomoTrafAvecIdComptag.type_poste
                    == dfCptGestTroncHomoTrafAvecIdComptag.type_poste_bdd_tronc_homo_traf
                )
                & (dfCptGestTroncHomoTrafAvecIdComptag.type_poste != "ponctuel")
            )
        )
    ]
    # vérif
    if sum(
        [
            len(e)
            for e in (
                dfCreationComptagAssocTroncHomoTraf,
                dfCreationCompteurExistDevientAssocTroncHomoTraf,
                dfCreationCompteurTroncHomoTraf,
            )
        ]
    ) != len(dfCptGestTroncHomoTrafAvecIdComptag):
        warnings.warn(
            """la somme des éléments présents dans la ventilation des compteurs présents sur un troncon homogene
                         de trafic est différentes du nombre d'éléments initiaux. vérifier le code et les données"""
        )
    # regrouper les compteurs par categorie
    dfCreationComptagAssoc = pd.concat(
        [dfCreationComptagAssocTroncHomoTopo, dfCreationComptagAssocTroncHomoTraf]
    )
    dfCorrespIdComptag = dfCorrespIdComptagTroncHomoTopo
    dfCreationCompteurExistDevientAssoc = pd.concat(
        [
            dfCreationCompteurExistDevientAssocTroncHomoTopo,
            dfCreationCompteurExistDevientAssocTroncHomoTraf,
        ]
    )
    dfCreationCompteur = pd.concat(
        [dfCptGestTroncHomoTrafSansIdComptag, dfCreationCompteurTroncHomoTraf]
    )
    # vérif
    if sum(
        [
            len(e)
            for e in (
                dfCreationComptagAssoc,
                dfCorrespIdComptag,
                dfCreationCompteurExistDevientAssoc,
                dfCreationCompteur,
            )
        ]
    ) != len(df):
        warnings.warn(
            """la somme des en sortie de ventilation des nouveaux compteurs supposés est différentes du nombre d'éléments initiaux.
        vérifier le code et les données"""
        )
    return (
        dfCreationComptagAssoc,
        dfCorrespIdComptag,
        dfCreationCompteurExistDevientAssoc,
        dfCreationCompteur,
    )


def modifierVentilation(
    correspIdComptag,
    creationCompteur,
    comptageAssocie,
    CreationCompteurExistDevientAssoc,
    gest,
    dicoAssocies2Corresp,
    dicoCorresp2Associes,
    dicoNewCompteur2Assoc,
    dicoCreationCompteurExistDevientAssoc2Assoc,
    dicoNewCompteur2CreationCompteurExistDevientAssoc,
    dicoAssoc2CreationCompteurExistDevientAssoc,
    dicoCorresp2CreationCompteurExistDevientAssoc,
    dicoNewCompteur2Corresp,
    dicoCreationCompteurExistDevientAssoc2Corresp,
    listCompteurAForcer,
    listCompteurASupprimer,
):
    """
    à partir des elements crees par ventilerCompteurIdComptagExistant(), ventilerNouveauComptageRef(), ventilerCompteurIdComptagExistant()
    et de liste ou de dico de transfert de d'un resultats vers un autre, redefinir les dataframes des comptages associes
    in :
        dicoAssocies2Corresp : dico dico de transfert de données depuis les compteurs associes vers les corespondace
                                  d'identifant de comptage. clé = compteur qui était un nouveau et qui devient corresp,
                                                            value = ref du compteur qui devient corresp
        correspIdComptag : dataframe isse de ventilerNouveauComptageRef()
        dicoNewCompteur2Associes : dico de transfert de données depuis creationCompteur (ventilerCompteurIdComptagExistant())
                                            vers les comptages associes. clé = comptage qui va devenir comptage associé,
                                            value = compteur ref du comptage qui va devenir associe
        dicoNewCompteur2Corresp : dico dico de transfert de données depuis les nouveaux compteurs vers les corespondace
                                  d'identifant de comptage. clé = compteur qui était un nouveau et qui devient corresp,
                                                            value = ref du compteur qui devient corresp
        creationCompteur : dataframe de comptage qui necesittent creation de compteur
        comptageAssocie : dataframe des compatge associes
        listeAssocies2Corresp : liste des comptages a transferer depuis les comptages associes vers les correspondance de comptage
        CreationCompteurExistDevientAssoc : dataframe de comptage qui necesittent creation de compteur et le passage de celui
                                            en bdd vers le schema comptage_assoc
        gest : gestionnaire
        dicoCreationCompteurExistDevientAssoc2Corresp : dico des comptage que l'on voulait creer , mais qui finalement vont etre des coresp.
                                                        key : id_comptag des nouveaux points
                                                        value : id_comptag e la bdd existante
    out :
        comptageAssocie_MaJMano : dataframe des comptages associes, avec les corresp transferees dedans et
                                            si besoin les comptages vers corrsp sortis. Si pas concerne, renvoi none
        correspIdComptag_MajMano : dataframe des corrsp, avec les corresp transferees dedans et si besoin les comptages
                                     vers comptages associes sortis. Si pas concerne, renvoi none
    """
    # creation des df de Correspondance sur la base des dico
    dfCreationCompteurExistDevientAssoc2Assoc = pd.DataFrame(
        {
            "id_comptag": [
                k for k in dicoCreationCompteurExistDevientAssoc2Assoc.keys()
            ],
            "id_comptag_ref": [
                v for v in dicoCreationCompteurExistDevientAssoc2Assoc.values()
            ],
        },
        dtype=str,
    )
    dfCorresp2Associes = pd.DataFrame(
        {
            "id_comptag": [k for k in dicoCorresp2Associes.keys()],
            "id_comptag_ref": [v for v in dicoCorresp2Associes.values()],
        },
        dtype=str,
    )
    dfNewCompteur2Assoc = pd.DataFrame(
        {
            "id_comptag": [k for k in dicoNewCompteur2Assoc.keys()],
            "id_comptag_ref": [v for v in dicoNewCompteur2Assoc.values()],
        },
        dtype=str,
    )
    dfNewCompteur2CreationCompteurExistDevientAssoc = pd.DataFrame(
        {
            "id_comptag": [
                k for k in dicoNewCompteur2CreationCompteurExistDevientAssoc.keys()
            ],
            "id_comptag_ref": [
                v for v in dicoNewCompteur2CreationCompteurExistDevientAssoc.values()
            ],
        },
        dtype=str,
    )
    dfCorresp2CreationCompteurExistDevientAssoc = pd.DataFrame(
        {
            "id_comptag": [
                k for k in dicoCorresp2CreationCompteurExistDevientAssoc.keys()
            ],
            "id_comptag_ref": [
                v for v in dicoCorresp2CreationCompteurExistDevientAssoc.values()
            ],
        },
        dtype=str,
    )
    dfAssoc2CreationCompteurExistDevientAssoc = pd.DataFrame(
        {
            "id_comptag": [
                k for k in dicoAssoc2CreationCompteurExistDevientAssoc.keys()
            ],
            "id_comptag_ref": [
                v for v in dicoAssoc2CreationCompteurExistDevientAssoc.values()
            ],
        },
        dtype=str,
    )
    dfCreationCompteurExistDevientAssoc2Corresp = pd.DataFrame(
        {
            "id_comptag": [
                k for k in dicoCreationCompteurExistDevientAssoc2Corresp.keys()
            ],
            "id_comptag_bdd": [
                v for v in dicoCreationCompteurExistDevientAssoc2Corresp.values()
            ],
        },
        dtype=str,
    )
    dfNewCompteur2Corresp = pd.DataFrame(
        {
            "id_comptag": [k for k in dicoNewCompteur2Corresp.keys()],
            "id_comptag_bdd": [v for v in dicoNewCompteur2Corresp.values()],
        },
        dtype=str,
    )
    dfAssocies2Corresp = pd.DataFrame(
        {
            "id_comptag": [k for k in dicoAssocies2Corresp.keys()],
            "id_comptag_bdd": [v for v in dicoAssocies2Corresp.values()],
        },
        dtype=str,
    )

    # FUSION DES DONNEES
    # gestion des transfert vers corresp
    correspIdComptag_MajMano = pd.concat(
        [
            creerCorrespComptag(
                correspIdComptag,
                "id_comptag",
                "id_comptag_bdd_tronc_homo_topo",
                listCompteurAForcer,
            ),
            creerCorrespComptag(
                dfCreationCompteurExistDevientAssoc2Corresp,
                "id_comptag",
                "id_comptag_bdd",
                listCompteurAForcer,
            ),
            creerCorrespComptag(
                dfNewCompteur2Corresp,
                "id_comptag",
                "id_comptag_bdd",
                listCompteurAForcer,
            ),
            creerCorrespComptag(
                dfAssocies2Corresp, "id_comptag", "id_comptag_bdd", listCompteurAForcer
            ),
        ]
    ).drop_duplicates()
    # gestion des transferts vers assoc
    comptageAssocie_MaJMano = pd.concat(
        [
            comptageAssocie,
            creationCompteur.loc[
                creationCompteur.id_comptag.isin(dicoNewCompteur2Assoc.keys())
            ].merge(dfNewCompteur2Assoc, on="id_comptag"),
            correspIdComptag.loc[
                correspIdComptag.id_comptag.isin(dicoCorresp2Associes.keys())
            ].merge(dfCorresp2Associes, on="id_comptag"),
            CreationCompteurExistDevientAssoc.loc[
                CreationCompteurExistDevientAssoc.id_comptag.isin(
                    dicoCreationCompteurExistDevientAssoc2Assoc.keys()
                )
            ].merge(dfCreationCompteurExistDevientAssoc2Assoc, on="id_comptag"),
        ]
    )
    comptageAssocie_MaJMano.drop(
        comptageAssocie_MaJMano.loc[
            comptageAssocie_MaJMano.id_comptag.isin(listCompteurASupprimer)
        ].index,
        inplace=True,
    )
    # gestion des transferts vers CreationCompteurExistDevientAssoc
    CreationCompteurExistDevientAssoc_MajMano = pd.concat(
        [
            CreationCompteurExistDevientAssoc,
            creationCompteur.loc[
                creationCompteur.id_comptag.isin(
                    dicoNewCompteur2CreationCompteurExistDevientAssoc.keys()
                )
            ].merge(dfNewCompteur2CreationCompteurExistDevientAssoc, on="id_comptag"),
            correspIdComptag.loc[
                correspIdComptag.id_comptag.isin(
                    dicoCorresp2CreationCompteurExistDevientAssoc.keys()
                )
            ].merge(dfCorresp2CreationCompteurExistDevientAssoc, on="id_comptag"),
            comptageAssocie.loc[
                comptageAssocie.id_comptag.isin(
                    dicoAssoc2CreationCompteurExistDevientAssoc.keys()
                )
            ].merge(dfAssoc2CreationCompteurExistDevientAssoc, on="id_comptag"),
        ]
    )
    CreationCompteurExistDevientAssoc_MajMano.drop(
        CreationCompteurExistDevientAssoc_MajMano.loc[
            CreationCompteurExistDevientAssoc_MajMano.id_comptag.isin(
                listCompteurASupprimer
            )
        ].index,
        inplace=True,
    )
    # gestion des transfert vers creationCompteur
    creationCompteur_MajMano = pd.concat(
        [
            creationCompteur,
            CreationCompteurExistDevientAssoc.loc[
                CreationCompteurExistDevientAssoc.id_comptag.isin(listCompteurAForcer)
            ],
            comptageAssocie.loc[comptageAssocie.id_comptag.isin(listCompteurAForcer)],
            correspIdComptag.loc[correspIdComptag.id_comptag.isin(listCompteurAForcer)],
        ]
    )
    creationCompteur_MajMano.drop(
        creationCompteur_MajMano.loc[
            creationCompteur_MajMano.id_comptag.isin(listCompteurASupprimer)
        ].index,
        inplace=True,
    )

    # EVIDER LES DONNEES EN TROP
    # gestion des transfert vers corresp
    if dicoAssocies2Corresp:
        comptageAssocie_MaJMano = comptageAssocie_MaJMano.loc[
            ~comptageAssocie_MaJMano.id_comptag.isin(
                correspIdComptag_MajMano.id_gest.tolist()
            )
        ].copy()
    if dicoCreationCompteurExistDevientAssoc2Corresp:
        CreationCompteurExistDevientAssoc_MajMano = (
            CreationCompteurExistDevientAssoc_MajMano.loc[
                ~CreationCompteurExistDevientAssoc_MajMano.id_comptag.isin(
                    correspIdComptag_MajMano.id_gest.tolist()
                )
            ].copy()
        )
    if dicoNewCompteur2Corresp:
        creationCompteur_MajMano = creationCompteur_MajMano.loc[
            ~creationCompteur_MajMano.id_comptag.isin(
                correspIdComptag_MajMano.id_gest.tolist()
            )
        ].copy()
    # gestion des transfert vers Assoc
    if dicoCorresp2Associes:
        correspIdComptag_MajMano = correspIdComptag_MajMano.loc[
            ~correspIdComptag_MajMano.id_gest.isin(
                comptageAssocie_MaJMano.id_comptag.tolist()
            )
        ].copy()
    if dicoCreationCompteurExistDevientAssoc2Assoc:
        CreationCompteurExistDevientAssoc_MajMano = (
            CreationCompteurExistDevientAssoc_MajMano.loc[
                ~CreationCompteurExistDevientAssoc_MajMano.id_comptag.isin(
                    comptageAssocie_MaJMano.id_comptag.tolist()
                )
            ].copy()
        )
    if dicoNewCompteur2Assoc:
        creationCompteur_MajMano = creationCompteur_MajMano.loc[
            ~creationCompteur_MajMano.id_comptag.isin(
                comptageAssocie_MaJMano.id_comptag.tolist()
            )
        ].copy()
    # gestion des transferts vers CreationCompteurExistDevientAssoc
    if dicoCorresp2CreationCompteurExistDevientAssoc:
        correspIdComptag_MajMano = correspIdComptag_MajMano.loc[
            ~correspIdComptag_MajMano.id_gest.isin(
                CreationCompteurExistDevientAssoc_MajMano.id_comptag.tolist()
            )
        ].copy()
    if dicoAssoc2CreationCompteurExistDevientAssoc:
        comptageAssocie_MaJMano = comptageAssocie_MaJMano.loc[
            ~comptageAssocie_MaJMano.id_comptag.isin(
                CreationCompteurExistDevientAssoc_MajMano.id_comptag.tolist()
            )
        ].copy()
    if dicoNewCompteur2CreationCompteurExistDevientAssoc:
        creationCompteur_MajMano = creationCompteur_MajMano.loc[
            ~creationCompteur_MajMano.id_comptag.isin(
                CreationCompteurExistDevientAssoc_MajMano.id_comptag.tolist()
            )
        ].copy()
    # gestion des transfert vers creationCompteur
    if listCompteurAForcer:
        comptageAssocie_MaJMano = comptageAssocie_MaJMano.loc[
            ~comptageAssocie_MaJMano.id_comptag.isin(
                creationCompteur_MajMano.id_comptag.tolist()
            )
        ].copy()
        CreationCompteurExistDevientAssoc_MajMano = (
            CreationCompteurExistDevientAssoc_MajMano.loc[
                ~CreationCompteurExistDevientAssoc_MajMano.id_comptag.isin(
                    creationCompteur_MajMano.id_comptag.tolist()
                )
            ].copy()
        )
        correspIdComptag_MajMano = correspIdComptag_MajMano.loc[
            ~correspIdComptag_MajMano.id_gest.isin(
                creationCompteur_MajMano.id_comptag.tolist()
            )
        ].copy()
    # gestion des compteurs à supprimer

    tupleNbElemPostTraites = tuple(
        len(e)
        for e in (
            comptageAssocie_MaJMano,
            CreationCompteurExistDevientAssoc_MajMano,
            creationCompteur_MajMano,
            correspIdComptag_MajMano,
        )
    )
    tupleNbElemInitiaux = tuple(
        len(e)
        for e in (
            comptageAssocie,
            CreationCompteurExistDevientAssoc,
            creationCompteur,
            correspIdComptag,
        )
    )
    if sum(tupleNbElemPostTraites) != (
        sum(tupleNbElemInitiaux) - len(listCompteurASupprimer)
    ):
        raise ValueError(
            f"""le nombre de ligne est différents en entrée {tupleNbElemInitiaux} et en sortie 
    {tupleNbElemPostTraites} (attention aux nombre de compteur a suppr : {len(listCompteurASupprimer)}).
    verifier les doublons dans les réusltats"""
        )
    # remplir les références des comptages associés
    if not comptageAssocie_MaJMano.empty:
        comptageAssocie_MaJMano["id_comptag_ref"] = comptageAssocie_MaJMano.apply(
            lambda x: comptagesAssocDefinirIdCompteurRef(
                x,
                {
                    1: "id_comptag_ref",
                    2: "id_comptag_bdd_tronc_homo_topo",
                    3: "id_comptag_tronc_homo_traf",
                },
            ),
            axis=1,
        )
        # verif
        if comptageAssocie_MaJMano.id_comptag_ref.isna().any():
            raise ValueError(
                f"des références de comptages associés sont nulles. a corriger"
            )
    # assurer l'existance des references des comptages associés
    # défnir les liens existants (depuis Bdd ou suite a ventilation)
    existant = compteur_existant_bdd(gest=gest)
    listIdComptagRefPossible = (
        CreationCompteurExistDevientAssoc_MajMano.id_comptag.tolist()
        + creationCompteur_MajMano.id_comptag.tolist()
        + existant.id_comptag.tolist()
    )
    dfIdComptagModif = pd.concat(
        [
            dfCorresp2Associes,
            dfNewCompteur2Assoc,
            dfCreationCompteurExistDevientAssoc2Assoc,
            dfAssocies2Corresp,
            dfNewCompteur2Corresp,
            dfCreationCompteurExistDevientAssoc2Corresp,
        ]
    )
    dfIdComptagModif.id_comptag_bdd.fillna(
        dfIdComptagModif.id_comptag_ref, inplace=True
    )
    dfChagementRef = (
        pd.concat(
            [
                comptageAssocie_MaJMano[["id_comptag", "id_comptag_ref"]].rename(
                    columns={"id_comptag_ref": "id_comptag_bdd"}
                ),
                dfIdComptagModif,
                correspIdComptag_MajMano[["id_gest", "id_gti"]].rename(
                    columns={"id_gest": "id_comptag", "id_gti": "id_comptag_bdd"}
                ),
            ]
        )
        .drop("id_comptag_ref", axis=1)
        .drop_duplicates()
    )
    # renseigner les references non valides
    comptageAssocie_MaJMano.loc[
        ~comptageAssocie_MaJMano.id_comptag_ref.isin(listIdComptagRefPossible),
        "id_comptag_ref",
    ] = (
        comptageAssocie_MaJMano.loc[
            ~comptageAssocie_MaJMano.id_comptag_ref.isin(listIdComptagRefPossible)
        ][["id_comptag", "id_comptag_ref"]]
        .merge(
            dfChagementRef, left_on="id_comptag_ref", right_on="id_comptag", how="left"
        )
        .id_comptag_bdd.tolist()
    )
    # verif
    if not comptageAssocie_MaJMano.loc[
        ~comptageAssocie_MaJMano.id_comptag_ref.isin(listIdComptagRefPossible)
    ].empty:
        raise ValueError(
            "des comptages associés n'ont pas une référence valide. à chercher / corriger"
        )
    # traiter les geom nulles
    for e in (creationCompteur_MajMano, CreationCompteurExistDevientAssoc_MajMano):
        if e.geom_x.isna().any():
            e.loc[creationCompteur_MajMano.geom_x.isna(), "geom_x"] = e.loc[
                e.geom_x.isna(), "geom"
            ]

    return (
        comptageAssocie_MaJMano,
        correspIdComptag_MajMano,
        creationCompteur_MajMano,
        CreationCompteurExistDevientAssoc_MajMano,
    )


def rassemblerNewCompteur(
    dep,
    tuplesDfGeom,
    reseau=None,
    gestionnai=None,
    concession=False,
    srcGeo=None,
    sensCpt=None,
    techno=None,
    id_sect=None,
):
    """
    regrouper les dataframes issues des fonctions de ventilation dans une seule destinée a etre intégrée das la bdd
    in :
        srcGeo : la source de la géométrie, cf enum_src_geo dans la bdd
        sensCpt : le nombre de sens de circulation des comptages, cf enum_sens_cpt dans la bdd
        dep : string 2 caractère : le département
        reseau : string : cf enum_reseau dans bdd
        gestionnai : string : cf enum_gestionnai dans bdd
        concession : boolean
        tuplesDfGeom : autant de tuple de type (df, NomDeLaGeometrie) que necessaire.
    """
    reseauDf, gestionnaiDf, concessionDf = None, None, None
    listCpteurNew = []
    for e in [a for a in tuplesDfGeom]:
        df = e[0].copy()
        if not df.empty:
            if reseau:
                df["reseau"] = reseau
            reseauDf = df["reseau"].tolist()
            if gestionnai:
                df["gestionnai"] = gestionnai
            gestionnaiDf = df["gestionnai"].tolist()
            if srcGeo:
                df["src_geo"] = srcGeo
            if sensCpt:
                df["sens_cpt"] = sensCpt
            if concession:
                df["concession"] = concession
            concessionDf = df["concession"].tolist()
            if techno:
                df["techno"] = techno
            technoDf = df["techno"].tolist()
            if id_sect:
                df["id_sect"] = id_sect
            id_sectDf = df["id_sect"].tolist()
            df["src_cpt"] = df.type_poste.apply(
                lambda x: "convention gestionnaire" if x == "permanent" else "gestionnaire"
            )
            df["convention"] = df.type_poste.apply(
                lambda x: True if x == "permanent" else False
            )
            # RUSTINE A REPRENDRE :
            if "id_cpt" in df.columns and "obs_supl" in df.columns:
                listCpteurNew.append(
                    creerCompteur(
                        df,
                        e[1],
                        dep,
                        reseauDf,
                        gestionnaiDf,
                        concessionDf,
                        id_cpt=df.id_cpt.tolist(),
                        obs_supl=df.obs_supl.tolist(),
                        techno=technoDf,
                        id_sect=id_sectDf,
                    )
                )
            elif "id_cpt" in df.columns and any(df.id_cpt.notna()):
                listCpteurNew.append(
                    creerCompteur(
                        df,
                        e[1],
                        dep,
                        reseauDf,
                        gestionnaiDf,
                        concessionDf,
                        id_cpt=df.id_cpt.tolist(),
                    )
                )
                listCpteurNew.append(
                    creerCompteur(
                        df,
                        e[1],
                        dep,
                        reseauDf,
                        gestionnaiDf,
                        concessionDf,
                        obs_supl=df.obs_supl.tolist(),
                    )
                )
            else:
                listCpteurNew.append(
                    creerCompteur(df, e[1], dep, reseauDf, gestionnaiDf, concessionDf, 
                                  type_poste=df.type_poste.tolist(), src_geo=df.src_geo.tolist(), periode=df.periode.tolist(),
                                  pr=df.pr.tolist(), absc=df['abs'].tolist(), route=df.route.tolist(), src_cpt=df.src_cpt.tolist(),
                                  convention=df.convention.tolist(), sens_cpt=df.sens_cpt.tolist())
                )
    dfNewCompteur = pd.concat(listCpteurNew)
    if not dfNewCompteur.loc[dfNewCompteur.duplicated("id_comptag")].empty:
        raise ValueError(
            "des identifiants de comptages sont en doublons, a verifier avant insertion. utilisation possible de ventilerCompteurRefAssoc()"
        )
    # vérifier les types des objets
    for k, v in dicoTypeAttributs.items():
        if dfNewCompteur[k].dtype != v:
            dfNewCompteur[k] = dfNewCompteur[k].astype(v)
    return dfNewCompteur


def rassemblerNewComptage(
    annee, type_veh, dfComptageCompteurConnu, *dfComptageCompteurNew
):
    """
    regrouper les dataframes des comptages, selon qu'elle proviennentde compteurs deja connus ou de compteurs que l'on vient d'insérer
    grace a rassemblerNewCompteur. en sortie on obtiens les comptages et les comptages associes issus
    in :
        annee : caractères 4 string l'annee des comptages
        type_veh : tring, cf enum_type_veh dans la bdd
        dfComptageCompteurConnu : df des comptages relatifs a des compteurs deja dans la base, en général issu de df_attr_update
        dfComptageCompteurNew : toutes les df relatives au comptages issues de la ventilation
    """
    # verifs
    O.checkAttributsinDf(dfComptageCompteurConnu, attrComptageMano)
    if dfComptageCompteurNew:
        for d in dfComptageCompteurNew:
            if not d.empty:
                O.checkAttributsinDf(d, attrComptageMano)
        # on va fusionner les sources de données
        concatSources = pd.concat(
            [pd.concat([df for df in dfComptageCompteurNew if not df.empty]), dfComptageCompteurConnu]
        )
    else:
        concatSources = dfComptageCompteurConnu.copy()
    # puis on vérifie les doublons
    ref, assoc = ventilerDoublons(concatSources)
    # association avec la partie des compteurs deja connus
    dfComptageNewTot = creer_comptage(
        ref.id_comptag.tolist(), annee, ref.src, type_veh, periode=ref.periode
    )
    return dfComptageNewTot, assoc, ref


def rassemblerIndics(
    annee,
    dfComptageNewTot,
    dfTraficAgrege,
    dfTraficMensuel=None,
    dfTraficHoraire=None,
    indicAgregeValues=["tmja", "pc_pl"],
):
    """
    regrouper et mettre en forme les dataframes des indicateurs agreges, mensuel et horaires, correspondants aux id_comptages
    des comptages cree par rassemblerNewComptage()
    in :
        annee : string 4 caractères
        dfComptageNewTot : df des nouveaux comptages créées par  rassemblerNewComptage()
        dfTraficAgrege : dataframe des données de trafic agrege. generalement df_attr
        dfTraficMensuel : dataframe des données de trafic mensuelle. generalement df_attr_mens
        dfTraficHoraire : dataframe des données de trafic haorire. generalement df_attr_horaire
        indicAgregeValues : liste des indicateurs agreges à conserver
    """
    # récupérer les données
    listIdComptagIndicNew = dfComptageNewTot.id_comptag.tolist()
    # agrege
    dfAttrIndicAgregeNew = dfTraficAgrege.loc[
        dfTraficAgrege.id_comptag.isin(listIdComptagIndicNew)
    ]
    dfIndicAgregeNew = structureBddOld2NewForm(
        dfAttrIndicAgregeNew.assign(annee=annee),
        ["id_comptag", "annee", "fichier"],
        indicAgregeValues,
        "agrege",
    ).drop_duplicates()
    if not dfIndicAgregeNew.loc[
        dfIndicAgregeNew.duplicated(["id_comptag_uniq", "indicateur"])
    ].empty:
        raise ValueError("des comptages agreges sont en doublons, corrigez")
    # mensuel
    if isinstance(dfTraficMensuel, pd.DataFrame) and not dfTraficMensuel.empty:
        dfAttrIndicMensNew = dfTraficMensuel.loc[
            dfTraficMensuel.id_comptag.isin(listIdComptagIndicNew)
        ]
        dfIndicMensNew = structureBddOld2NewForm(
            dfAttrIndicMensNew.assign(annee=annee),
            ["id_comptag", "annee", "fichier", "donnees_type"],
            list(dico_mois.keys()),
            "mensuel",
        )
        if not dfIndicMensNew.loc[
            dfIndicMensNew.duplicated(["id_comptag_uniq", "indicateur", "mois"])
        ].empty:
            raise ValueError("des comptages mensuels sont en doublons, corrigez")
    else:
        dfIndicMensNew = None
    # horaire
    if isinstance(dfTraficHoraire, pd.DataFrame) and not dfTraficHoraire.empty:
        dfAttrIndicHoraireNew = dfTraficHoraire.loc[
            dfTraficHoraire.id_comptag.isin(listIdComptagIndicNew)
        ]
        dfIndicHoraireNew = structureBddOld2NewForm(
            dfAttrIndicHoraireNew.assign(annee=annee),
            ["id_comptag", "annee"],
            ["tata"],
            "horaire",
        )
        if (
            isinstance(dfIndicHoraireNew, pd.DataFrame)
            and not dfIndicHoraireNew.loc[
                dfIndicHoraireNew.duplicated(["id_comptag_uniq", "indicateur", "jour"])
            ].empty
        ):
            raise ValueError("des comptages hioraires sont en doublons, corrigez")
    else:
        dfIndicHoraireNew = None
    return dfIndicAgregeNew, dfIndicMensNew, dfIndicHoraireNew
