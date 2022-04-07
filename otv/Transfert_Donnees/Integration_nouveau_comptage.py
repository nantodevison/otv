# -*- coding: utf-8 -*-
'''
Created on 26 janv. 2022

@author: martin.schoreisz
Module pour integrer les nouveaux comptages fournis par les gestionnaires
'''

import warnings, re
import datetime as dt
import Outils as O
import pandas as pd
import Connexion_Transfert as ct
from Params.Bdd_OTV import (nomConnBddOtv, schemaComptage, schemaComptageAssoc, tableComptage, tableEnumTypeVeh, 
                            tableCompteur, tableCorrespIdComptag,attrCompteurAssoc, attBddCompteur, attrComptageMano,
                            attrCompteurValeurMano, attrComptageAssoc, enumTypePoste, vueLastAnnKnow)
from Import_export_comptage import (recupererIdUniqComptage, recupererIdUniqComptageAssoc, comptag_existant_bdd)
from Params.Mensuel import dico_mois
import geopandas as gp


def corresp_nom_id_comptag(df):
    """
    pour les id_comptag dont on sait que les noms gti et gestionnaire different  mais que ce sont les memes (cf table comptage.corresp_id_comptag), 
    on remplace le nom gest par le nom_gti, pour pouvoir faire des jointure ensuite
    in : 
        df : dataframe des comptage du gest. attention doit contenir l'attribut 'id_comptag', ene general prendre df_attr
    """
    rqt_corresp_comptg = f'select * from {schemaComptage}.{tableCorrespIdComptag}'
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        corresp_comptg = pd.read_sql(rqt_corresp_comptg, c.sqlAlchemyConn)
    df['id_comptag'] = df.apply(lambda x : corresp_comptg.loc[corresp_comptg['id_gest']==x['id_comptag']].id_gti.values[0] 
                                                    if x['id_comptag'] in corresp_comptg.id_gest.tolist() else x['id_comptag'], axis=1)
    return 

def classer_comptage_update_insert(dfAClasser, departement):
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
    corresp_nom_id_comptag(dfAClasser)
    existant = comptag_existant_bdd(dep=departement)
    df_attr_update = dfAClasser.loc[dfAClasser.id_comptag.isin(existant.id_comptag.tolist())].copy()
    df_attr_insert = dfAClasser.loc[~dfAClasser.id_comptag.isin(existant.id_comptag.tolist())].copy()
    # ajout d'une vérif sur les longueurs resectives de donnees
    if len(df_attr_update) + len(df_attr_insert) != len(dfAClasser):
        raise ValueError('la separation enetre comptage a mettre a jour et comptage a inserer ne correspond pas a la taille des donnees sources')
    return df_attr_update, df_attr_insert


def localiser_comptage_a_inserer(df, schema_temp,nom_table_temp, table_ref, table_pr):
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
        #passer les données dans Bdd
        c.sqlAlchemyConn.execute(f'drop table if exists {schema_temp}.{nom_table_temp}')
        df.to_sql(nom_table_temp,c.sqlAlchemyConn,schema_temp)   
        #ajouter une colonne geometrie
        rqt_ajout_geom=f"""ALTER TABLE {schema_temp}.{nom_table_temp} ADD COLUMN geom geometry('POINT',2154)""" 
        c.sqlAlchemyConn.execute(rqt_ajout_geom)
        #mettre a jour la geometrie. attention, il faut un fichier de référentiel qui va bein, cf fonction geoloc_pt_comptag dans la Bdd
        rqt_maj_geom=f"""update {schema_temp}.{nom_table_temp}
                         set geom=(select geom_out  from comptage.geoloc_pt_comptag('{table_ref}','{table_pr}',id_comptag))
                         where geom is null"""
        c.sqlAlchemyConn.execute(rqt_maj_geom)
        points_a_inserer=gp.GeoDataFrame.from_postgis(f'select * from {schema_temp}.{nom_table_temp}', c.sqlAlchemyConn, geom_col='geom',crs='epsg:2154')
    return points_a_inserer
    
    
def ventilerParSectionHomogene(tableCptNew, tableSectionHomo, codeDept, distancePlusProcheVoisin=10):
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
    if not O.check_colonne_in_table_bdd(nomConnBddOtv, tableSectionHomo.split('.')[0], tableSectionHomo.split('.')[1], 'gid')[0]:
        raise AttributeError("la colonne gid n'est pas dans la tabl de bdd")
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        pt = gp.read_postgis(f'select * from {tableCptNew}', c.sqlAlchemyConn)
        ptGeom = gp.read_postgis(f'select * from {tableCptNew} where geom is not null', c.sqlAlchemyConn)
        cptSansGeom = gp.read_postgis(f'select * from {tableCptNew} where geom is null', c.sqlAlchemyConn)
        linauto = gp.read_postgis(f"""
                                     select DISTINCT ON (t.gid) c.id_comptag id_comptag_bdd,c.type_poste type_poste_bdd, t.gid, t.geom, st_distance(c.geom, t.geom)
                                      from (SELECT * FROM {tableSectionHomo} where list_dept like '%%{codeDept}%%') t LEFT JOIN {schemaComptage}.{vueLastAnnKnow} c ON st_dwithin(c.geom, t.geom, 30)
                                      ORDER BY t.gid, CASE WHEN c.type_poste = 'permanent' THEN 1 
                                                           WHEN c.type_poste = 'tournant' THEN 2
                                                           WHEN c.type_poste = 'ponctuel' THEN 3 
                                                           ELSE 4 
                                                           end, CASE WHEN c.id_comptag IS NULL THEN 1
                                                                     WHEN comptage.verif_periode_vacance(c.id_comptag, c.annee_tmja) THEN 2
                                                                     ELSE 1 END, 
                                                                         c.tmja DESC
                                     """, c.sqlAlchemyConn)
    # on en déduit le plus proche voisin entre les points et la linauto
    ppV = O.plus_proche_voisin(ptGeom, linauto, distancePlusProcheVoisin, 'id_comptag', 'gid')
    ppVSectHomo = ppV.loc[~ppV.gid.isna()].copy()
    ppvHorsSectHomo = pt.loc[~pt.id_comptag.isin(ppV.id_comptag.tolist())].copy()  # PENSER A CREER LES COMPTEURS POUR CEUX LA !!!!!
    # et on récupère l'id_comptag_existant et l'id de section homogene
    ppVTot = ptGeom.merge(ppVSectHomo, how='left', on='id_comptag').merge(linauto, how='left', on='gid')
    # trouver le nombre de comptage par section homogène (gid)
    nbCptSectHomo = ppVTot.gid.value_counts()
    # separer les differents cas a traiter
    cptSimpleSectHomo = ppVTot.loc[ppVTot.gid.isin(nbCptSectHomo.loc[nbCptSectHomo == 1].index.tolist())].copy()
    cptMultiSectHomo = ppVTot.loc[ppVTot.gid.isin(nbCptSectHomo.loc[nbCptSectHomo > 1].index.tolist())].copy()
    #verif
    if len(cptSansGeom)+len(cptSimpleSectHomo)+len(cptMultiSectHomo)+len(ppvHorsSectHomo) > len(pt):
        warnings.warn("""la somme des éléments présents dans 'cptSansGeom', 'ppvHorsSectHomo', 'cptSimpleSectHomo', 'cptMultiSectHomo' est supérieure au nombre d'éléments initiaux. vérifier les doublons dans les résultats, vérifier la linauto et les données sources""")
    elif len(cptSansGeom)+len(cptSimpleSectHomo)+len(cptMultiSectHomo)+len(ppvHorsSectHomo) < len(pt):
        warnings.warn(f"""la somme des éléments présents dans 'cptSansGeom', 'ppvHorsSectHomo', 'cptSimpleSectHomo', 'cptMultiSectHomo' est inférieure au nombre d'éléments initiaux. 
        vérifier le id_comptag = {~pt.loc[pt.id_comptag.isin(cptSansGeom.id_comptag.tolist()+ppvHorsSectHomo.id_comptag.tolist()+cptSimpleSectHomo.id_comptag.tolist()+
        cptMultiSectHomo.id_comptag.tolist())].gid}""")
    return cptSansGeom, ppvHorsSectHomo, cptSimpleSectHomo, cptMultiSectHomo


def creerCompteur(cptRef, attrGeom, dep, reseau, gestionnai, concession, techno=None, obs_geo=None, obs_supl=None, id_cpt=None,
                  id_sect=None, fictif=False, en_service=True):
    """
    creation d'une df prete a etre integree dans la table des compteurs
    presque tout les chmaps 
    in : 
        cptRef : df ou geodataframe avec un attribut de géométrie en SRID = 2154
        attrGeom : string : nom de l'attribut supportant la géométrie
        dep : departement sur 2 caractère ou pd.Series
        reseau : valeur depuis liste enumeree (cf bdd) ou pd.Series de ces valeurs
        gestionnai : valeur depuis liste enumeree (cf bdd) ou pd.Series de ces valeurs
        concession : boolean ou pd.Series de booleean
    """
    df = cptRef.copy()
    O.checkAttributsinDf(df, [attrGeom] + attrCompteurValeurMano)
    df = df[[attrGeom]  + attrCompteurValeurMano].assign(
        dep=dep, reseau=reseau, gestionnai=gestionnai, concession=concession, techno=techno, obs_geo=obs_geo,
        obs_supl=obs_supl, id_cpt=id_cpt, id_sect=id_sect, fictif=fictif, en_service=en_service)
    df['x_l93'] = df[attrGeom].apply(lambda x: round(x.x,3) if not x.is_empty else None)
    df['y_l93'] = df[attrGeom].apply(lambda x: round(x.y,3) if not x.is_empty else None)
    df.drop(
        [c for c in df.columns if c not in (attBddCompteur + [attrGeom])], axis=1, inplace=True)
    gdfCpt = gp.GeoDataFrame(df, geometry=attrGeom, crs=2154)
    gdfCpt = O.gp_changer_nom_geom(gdfCpt, 'geom')
    return gdfCpt


def creer_comptage(listComptage, annee, src, type_veh,obs=None, periode=None) : 
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
    with ct.ConnexionBdd(nomConnBddOtv) as c  :
        enumTypeVeh=pd.read_sql(f'select code from {schemaComptage}.{tableEnumTypeVeh}', c.sqlAlchemyConn).code.tolist()
        listIdcomptagBdd=pd.read_sql(f'select id_comptag from {schemaComptage}.{tableCompteur}', c.sqlAlchemyConn).id_comptag.tolist()
    if any([e not in listIdcomptagBdd for e in listComptage]):
        raise ValueError(f'les comptages {[e for e in listComptage if e not in listIdcomptagBdd]} ne sont pas dans la Bdd. Vérifier les correspondance de comptage ou creer les compteur en premier')
    if type_veh not in enumTypeVeh : 
        raise ValueError(f'type_veh doit etre parmi {enumTypeVeh}')
    if not (int(annee)<=dt.datetime.now().year and int(annee)>2000) or annee=='1900' :
        raise ValueError(f'annee doit etre compris entre 2000 et {dt.datetime.now().year} ou egale a 1900')
    #periode        
    if isinstance(periode, str) :
        if periode and not re.search('(20[0-9]{2}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1])-20[0-9]{2}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1]))+( ; )*', periode) :
            raise ValueError(f'la periode doit etre de la forme YYYY/MM/DD-YYYY/MM/DD separe par \' ; \' si plusieurs periodes')
    if isinstance(periode, list) :
        #verif que ça colle avec les id_comptag
        if len(periode)!=len(listComptage) :
            raise ValueError('les liste de comptage et de periode doievnt avoir le mm nombre d elements')
        for p in periode : 
            if p and not re.search('(20[0-9]{2}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1])-20[0-9]{2}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1]))+( ; )*', p) :
                raise ValueError(f'la periode doit etre de la forme YYYY/MM/DD-YYYY/MM/DD separe par \' ; \' si plusieurs periodes')
    return pd.DataFrame({'id_comptag':listComptage, 'annee':annee, 'periode':periode,'src':src, 'obs':obs, 'type_veh' : type_veh})


def structureBddOld2NewForm(dfAConvertir, annee, listAttrFixe,listAttrIndics,typeIndic):
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
    if typeIndic not in ('agrege', 'mensuel', 'horaire'):
        raise ValueError ("le type d'indicateur doit etre parmi 'agrege', 'mensuel', 'horaire'" )
    if any([e not in listAttrFixe for e in ['id_comptag', 'annee']]):
        raise AttributeError('les attributs id_comptag et annee sont obligatoire dans listAttrFixe')
    if typeIndic == 'agrege':
        dfIndic = pd.melt(dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)), id_vars=listAttrFixe, value_vars=listAttrIndics, 
                              var_name='indicateur', value_name='valeur')
        columns = [c for c in ['id_comptag', 'indicateur', 'valeur', 'fichier', 'obs', 'annee'] if c in dfIndic.columns]
        dfIndic = dfIndic[columns].rename(columns={'id':'id_comptag_uniq'})
    elif typeIndic == 'mensuel':
        dfIndic = pd.melt(dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)), id_vars=listAttrFixe, value_vars=listAttrIndics, 
                              var_name='mois', value_name='valeur')
        columns = [c for c in ['id_comptag', 'donnees_type', 'valeur', 'mois', 'fichier', 'annee', 'indicateur'] if c in dfIndic.columns]
        dfIndic = dfIndic[columns].rename(columns={'donnees_type':'indicateur'})
    elif typeIndic == 'horaire': 
        dfIndic = dfAConvertir.rename(columns={'type_veh':'indicateur'})
    dfIndic = recupererIdUniqComptage(dfIndic).drop(['id_comptag', 'annee'], axis=1)
    #si valeur vide on vire la ligne
    if typeIndic in ('agrege','mensuel'):
        dfIndic.dropna(subset=['valeur'], inplace=True)
    if not dfIndic.loc[dfIndic.id_comptag_uniq.isna()].empty : 
        print(dfIndic.columns)
        raise ValueError(f"certains comptages ne peuvent etre associes a la table des comptages de la Bdd {dfIndic.loc[dfIndic.id_comptag_uniq.isna()].id_comptag_uniq.tolist()}")
    return dfIndic


def structureBddOld2NewFormAssoc(dfAConvertir, annee, listAttrFixe,listAttrIndics,typeIndic):
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
    if typeIndic not in ('agrege', 'mensuel', 'horaire'):
        raise ValueError ("le type d'indicateur doit etre parmi 'agrege', 'mensuel', 'horaire'" )
    if any([e not in listAttrFixe for e in ['id_cptag_ref', 'annee', 'id_cpteur_asso']]):
        raise AttributeError('les attributs id_comptag, id_cpteur_asso et annee sont obligatoire dans listAttrFixe')
    if typeIndic == 'agrege':
        dfIndic = pd.melt(dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)), id_vars=listAttrFixe, value_vars=listAttrIndics, 
                              var_name='indicateur', value_name='valeur')
        columns = [c for c in ['id_cptag_ref', 'indicateur', 'valeur', 'fichier', 'obs', 'annee', 'id_cpteur_asso'] if c in dfIndic.columns]
        dfIndic = dfIndic[columns].rename(columns={'id':'id_comptag_uniq'})
    #elif typeIndic == 'mensuel':
    #    dfIndic = pd.melt(dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)), id_vars=listAttrFixe, value_vars=listAttrIndics, 
    #                         var_name='mois', value_name='valeur')
    #    columns = [c for c in ['id_comptag', 'donnees_type', 'valeur', 'mois', 'fichier', 'annee'] if c in dfIndic.columns]
    #    dfIndic = dfIndic[columns].rename(columns={'donnees_type':'indicateur'})
    elif typeIndic == 'horaire': 
        dfIndic = dfAConvertir.rename(columns={'type_veh':'indicateur'})
        dfIndic = dfIndic.drop(['id_comptag', 'id_cpteur_asso'], axis=1, errors='ignore')
    dfIndic = dfIndic.merge(recupererIdUniqComptageAssoc(dfIndic.id_cptag_ref.tolist()), on=['id_cptag_ref', 'id_cpteur_asso']
                            ).drop(['id_cptag_ref', 'annee'], axis=1, errors='ignore')
    #si valeur vide on vire la ligne
    if typeIndic in ('agrege','mensuel'):
        dfIndic.dropna(subset=['valeur'], inplace=True)
    if not dfIndic.loc[dfIndic.id_comptag_uniq.isna()].empty : 
        print(dfIndic.columns)
        raise ValueError(f"certains comptages ne peuvent etre associes a la table des comptages de la Bdd {dfIndic.loc[dfIndic.id_comptag_uniq.isna()].id_comptag_uniq.tolist()}")
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
    
    
def creerComptageAssoc(df, id_comptag_ref_nom, annee, id_compteur_asso_nom, src=None, listIdCptExclu=None):
    """
    a partir d'une df, creer la df a injecter dans les tables compteur et comptage du schema comptage_assoc de la bdd
    in : 
        df : la df de base, doit contenir obligatoirement les attributs decrivant l'id_comptag_ref, le rang, le type_veh. si possible la periode, src, obs.
             si le comptage_assoc comprend une dimension géométrique, les attributs decrivant l'id_compteur_asso, le type_poste, la src_geo, la src_cpt, convention,
             et sens_cpt sont obligatoire. si possible ajouter route, pr, abs, techno, obs_geo, obs_supl
        id_comptag_ref_nom : string : nom de l'attribut supportant l'id_comptag du point de référence
        id_compteur_asso_nom : string : nom de l'attribut supportant l'id_comptag du point associé si composante geometrique (i.e l'id_comptag issu des donnees gestionnaire)
        annee : string annee sur 4 caractères
        listIdCptExclu : liste d'identifiant de comptage a ne pas conserver dans les resultats
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
                return f'{obsExistant} ; une partie des mesures sont réalisées pendant les vacances scolaires'
        else:
            if vacances:
                return 'une partie des mesures sont réalisées pendant les vacances scolaires'
        return None
    
    # mise en forme
    dfSource = df.copy()
    O.checkAttributsinDf(dfSource, ['type_veh', 'periode'])
    if listIdCptExclu:
        dfSource = dfSource.loc[~dfSource[id_compteur_asso_nom].isin(listIdCptExclu)].copy()
    dfSource.rename(columns={id_compteur_asso_nom: 'id_cpteur_asso'}, inplace=True)
    corresIdComptagInterne = recupererIdUniqComptage(dfSource[[id_comptag_ref_nom]].rename(columns={id_comptag_ref_nom: 'id_comptag'}), True)
    dfIds = dfSource.merge(corresIdComptagInterne, left_on=id_comptag_ref_nom, right_on='id_comptag', how='left').rename(columns={'id_comptag_uniq': 'id_cptag_ref'}).assign(annee=annee)
    dfIds['rang_bdd'] = dfIds[id_comptag_ref_nom].apply(lambda x: rangBddComptageAssoc(x))
    dfIds['rang_df'] = dfIds.groupby(id_comptag_ref_nom).cumcount()+1
    dfIds['rang'] = dfIds['rang_bdd']+dfIds['rang_df']
    print(dfIds.columns)
    tableComptage = dfIds[[c for c in dfIds.columns if c in attrComptageAssoc]].copy()
    # ajouter ou modifier le champs observation pour les ponctuels ou tournant dont une partie de la periode est en vacances scolaire
    if 'obs' in tableComptage.columns:
        tableComptage['obs'] = tableComptage.apply(lambda x: remplirObsSelonVacances(x.obs, O.verifVacanceRange(x.periode)), axis=1)
    else:
        tableComptage['obs'] = tableComptage.periode.apply(lambda x: remplirObsSelonVacances(None, O.verifVacanceRange(x)))
    return dfIds, tableComptage    


def creerCompteurAssoc(df, nomAttrIdCpteurAsso, nomAttrGeom=None, nomAttrIdCpteurRef=None, listIdCptExclu=None):
    """
    a partir d'une df, creer la table des compteurdu schema comptage_assoc de la bdd. la table source doit contenir les attributs d'ientifiantde comptage,
    type_poste, src_geo, src_cpt, convention, sens_cpt. 
    pour plus de precision elle peut contenir geom, route, pr, abs, techno, obs_geo, obs_supl, id_cpt, id_sect, id_cpteur_ref. Cf Bdd pour plus de détail
    in : 
        df : dataframe des donnees sources
        nomAttrIdCpteurAsso : string : nom de l'attribut contenant les id_comptag mis en forme a partir du gestionnaire
        nomAttrIdCpteurRef : string : nom de l'attribut contenant les id_comptag de référence de la table compteur du schema comptapge de la bdd
        nomAttrGeom : string : nom de l'attribut qui supporte la géométrie
    out :
        dataframe au format bdd comptage_assoc.compteur
    """
    O.checkAttributsinDf(df, [nomAttrIdCpteurAsso, 'type_poste', 'src_geo', 'src_cpt', 'convention', 'sens_cpt'])
    dfSource = df.copy()
    if listIdCptExclu:
        dfSource = dfSource.loc[~dfSource[nomAttrIdCpteurAsso].isin(listIdCptExclu)].copy()
    dfSource.rename(columns={nomAttrIdCpteurAsso: 'id_cpteur_asso'}, inplace=True)
    if nomAttrIdCpteurRef:
        dfSource.rename(columns={nomAttrIdCpteurRef: 'id_cpteur_ref'}, inplace=True)
    if nomAttrGeom:
        dfSource = O.gp_changer_nom_geom(dfSource, 'geom')
    tableCompteur = dfSource[[c for c in dfSource.columns if c in attrCompteurAssoc]]
    return tableCompteur


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
    return df.loc[~df[nomAttrIdComptagGest].isin(listIdCptExclu)][[nomAttrIdComptagGest, nomAttrIdComptagGti]].copy().rename(columns={nomAttrIdComptagGest: 'id_gest', nomAttrIdComptagGti: 'id_gti'})
    
    
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
        if typePoste == 'permanent':
            return 3*10**15
        elif typePoste == 'tournant':
            return 2*10**15
        elif typePoste == 'ponctuel':
            return 10**15
        else : 
            raise ValueError('type de poste non affecte a une note')

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
            return 2*10**12

    def hierarchisationTrafic(tmja, pc_pl):
        """
        fournir une valeur qui concatene le trafic et le pc_pl, en favorisant le trafic
        """
        if not tmja or tmja <= 0: 
            return ValueError('le tmja doit etre une superieur a 0  non nulle')
        elif not pc_pl:
            return tmja * 1000
        else:
            return (tmja * 1000) + pc_pl
        
    return hierarchisationTypePoste(typePoste) + hierarchisationVacance(periode) + hierarchisationTrafic(tmja, pc_pl)

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
    doublonsNatifs = df.loc[df.duplicated('id_comptag', keep=False)]
    if not doublonsNatifs.empty:  # si c'est le cas, il faut néttoyer la donnees et creer des comptages associés (a reprendre en natif dans les fonctions)
        ref, assoc = ventilerCompteurRefAssoc(df.loc[df.duplicated('id_comptag', keep=False)].assign(
            id_comptag2=df.loc[df.duplicated('id_comptag', keep=False)].id_comptag).rename(columns={'id_comptag2': 'gid'}))
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
    O.checkAttributsinDf(cptMultiSectHomo, ['gid', 'type_poste', 'periode', 'pc_pl', 'tmja'])
    cptMultiSectHomo['note_hierarchise'] = cptMultiSectHomo.apply(lambda x: hierarchisationCompteur(x.type_poste, x.periode, x.tmja, x.pc_pl), axis=1)
    cptRefMultiSectHomo = cptMultiSectHomo.loc[cptMultiSectHomo.groupby('gid').note_hierarchise.transform('max') == cptMultiSectHomo.note_hierarchise].sort_values('gid').copy()
    cptAssocMultiSectHomo = cptMultiSectHomo.loc[cptMultiSectHomo.groupby('gid').note_hierarchise.transform('max') != cptMultiSectHomo.note_hierarchise].sort_values('gid').copy()
    cptAssocMultiSectHomo = cptAssocMultiSectHomo.merge(cptRefMultiSectHomo[['gid', 'id_comptag']], on='gid', suffixes=(None, '_ref'))
    # gestion du cas ou un comptage est associe a2 comptage de references differents (i.e il sont du mmm type, sur periode equivalente, avec le mm TMJA)
    # dans ce cas on prend au hasard
    cptAssocMultiSectHomo.drop_duplicates(['id_comptag'], inplace=True)
    # verif que tous les gid ont un cpt ref
    if not cptAssocMultiSectHomo.loc[~cptAssocMultiSectHomo.gid.isin(cptRefMultiSectHomo.gid.unique())].empty:
        raise ValueError('un des comptage associe n\'a pas de comptage de reference')
    if not len(cptRefMultiSectHomo)+len(cptAssocMultiSectHomo) == len(cptMultiSectHomo):
        raise ValueError('un ou plusieurs comptage n\'ont pas ete affecte comme reference ou associe, ou sont en doublons')
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
    cptRefMultiSectHomoNew = cptRefMultiSectHomo.loc[cptRefMultiSectHomo.id_comptag_bdd.isna()].copy()
    cptRefMultiSectHomoOld = cptRefMultiSectHomo.loc[~cptRefMultiSectHomo.id_comptag_bdd.isna()].copy()
    cptSimpleSectHomoNew = cptSimpleSectHomo.loc[cptSimpleSectHomo.id_comptag_bdd.isna()].copy()
    cptSimpleSectHomoOld = cptSimpleSectHomo.loc[~cptSimpleSectHomo.id_comptag_bdd.isna()].copy()
    cptRefSectHomoNew = pd.concat([cptRefMultiSectHomoNew, cptSimpleSectHomoNew])
    cptRefSectHomoOld = pd.concat([cptRefMultiSectHomoOld, cptSimpleSectHomoOld])
    return cptRefSectHomoNew, cptRefSectHomoOld


def ventilerNouveauComptageRef(df, nomAttrtypePosteGest, nomAttrtypePosteBdd, nomAttrperiode):
    """
    depuis une df des comptage de référence situé sur des tronçons avec un id_comptag existant dans la base, 
    séparer en 4 groupe selon le type de poste dans la bdd et le type de poste du hestionnaire.
    in : 
        df : dataframe a classifier
        nomAttrtypePosteGest : string : nom de l'attribut supportant le type de poste fourni par le gest 
        nomAttrtypePosteBdd : string : nom de l'attribut supportant le type de poste dans la bdd
        nomAttrperiode : string : nom de l'attribut décrivant la période de mesure
    out : 
        dfCorrespIdComptag : dataframe des points qui vont simplelnet faire l'objet d'une correspondance d'id_comptage
        dfCreationComptageAssocie : dataframe de spoint qui vont de suite devenir des comptages associes
        dfModifTypePoste : dataframe des points existant dont on va modifier le type de poste
        dfCreationCompteur : dataframe des points qui vont faire l'objet d'un  nouveau compteur
    """
    dfCorrespIdComptag = df.loc[((df[nomAttrtypePosteGest] == df[nomAttrtypePosteBdd]) &
                                 (df[nomAttrtypePosteGest] != 'ponctuel')) |
                                ((df[nomAttrtypePosteGest] == df[nomAttrtypePosteBdd]) &
                                 (df[nomAttrtypePosteGest] == 'ponctuel') &
                                 (~df[nomAttrperiode].apply(lambda x: O.verifVacanceRange(x))))].copy()
    dfCreationComptageAssocie = df.loc[((df[nomAttrtypePosteGest] == 'ponctuel') &
                                        (df[nomAttrtypePosteBdd].isin(('permanent', 'tournant')))) |
                                       ((df[nomAttrtypePosteGest] == df[nomAttrtypePosteBdd]) &
                                        (df[nomAttrtypePosteGest] == 'ponctuel') &
                                        (df[nomAttrperiode].apply(lambda x: O.verifVacanceRange(x))))].copy()
    dfModifTypePoste = df.loc[((df[nomAttrtypePosteGest] == 'permanent') & (df[nomAttrtypePosteBdd] == 'tournant'))].copy()
    dfCreationCompteur = df.loc[((df[nomAttrtypePosteGest].isin(('permanent', 'tournant'))) & (df[nomAttrtypePosteBdd] == 'ponctuel'))].copy()
    if len(dfModifTypePoste) + len(dfCorrespIdComptag) + len(dfCreationComptageAssocie) + len(dfCreationCompteur) != len(df):
        warnings.warn("la somme des éléments présents dans 'dfCorrespIdComptag', 'dfCreationComptageAssocie', 'dfModifTypePoste' est différente du nombre d'éléments initiaux. Chercher les id_comptag no présents ou en doublons, creer des id_orresp_compatg en amont et relancer le process si besoin")
    return dfCorrespIdComptag, dfCreationComptageAssocie, dfModifTypePoste, dfCreationCompteur


def modifierVentilation(dfCorrespIdComptag, cptRefSectHomoNew, dfCreationComptageAssocie, cptAssocMultiSectHomo,
                                        listeDepuisAssociesVersCorresp=None,
                                        listeDepuisCorrespVersAssocies=None, 
                                        dicoDepuisNewCompteurVersAssocies=None):
    """
    à partir des elements crees par ventilerCompteurIdComptagExistant(), ventilerNouveauComptageRef(), ventilerCompteurIdComptagExistant()
    et de liste ou de dico de transfert de d'un resultats vers un autre, redefinir les dataframes des comptages associes
    in : 
        listeDepuisCorrespVersAssocies : liste des ids comptages devant etre transferes depuis les correspondances d'id_comptage vers les comptages associes
        dfCorrespIdComptag : dataframe isse de ventilerNouveauComptageRef()
        dicoDepuisNewCompteurVersAssocies : dico de transfert de données depuis cptRefSectHomoNew (ventilerCompteurIdComptagExistant()) vers les comptages associes. clé = comptage qui va devenir comptage associé, value = compteur ref du comptage qui va devenir associe
        cptRefSectHomoNew : dataframe de comptage qui necesittent creation de compteur, cf ventilerCompteurIdComptagExistant()
        dfCreationComptageAssocie : dataframe des compatge associes. issu de ventilerNouveauComptageRef()
        listeDepuisAssociesVersCorresp : liste des comptages a transferer depuis les comptages associes vers les correspondance de comptage
        cptAssocMultiSectHomo : dataframe des comptages associes issue de ventilerCompteurRefAssoc
    out : 
        dfCreationComptageAssocie_MaJMano : dataframe des comptages associes, avec les corresp transferees dedans et si besoin les comptages vers corrsp sortis. Si pas concerne, renvoi none
        dfCorrespIdComptag_MajMano : dataframe des corrsp, avec les corresp transferees dedans et si besoin les comptages vers comptages associes sortis. Si pas concerne, renvoi none
        cptAssocMultiSectHomo_MajMano : dataframe des comptages associes, avec les corresp transferees dedans et si besoin les comptages vers corrsp sortis. Si pas concerne, renvoi none
    """
    # initialisation des variables finales   
    dfCreationComptageAssocie_MaJMano = None
    dfCorrespIdComptag_MajMano = None
    cptAssocMultiSectHomo_MajMano = None
    cptRefSectHomoNew_MajMano = None
    
    # FUSION DES DONNEES
    # transfert des comptages depuis la correspondance d'id_comptage vers les comptages associés (dfCreationComptageAssocie)
    # verif que tious les comptages a transfere ont bien une valeur de id_comptag_bdd
    if listeDepuisCorrespVersAssocies:
        dfCptCorrespVersAssocie = dfCorrespIdComptag.loc[dfCorrespIdComptag.id_comptag.isin(listeDepuisCorrespVersAssocies)]
        if not dfCptCorrespVersAssocie.loc[dfCptCorrespVersAssocie.id_comptag_bdd.isna()].empty:
            raise AttributeError("un des objets n'a pas de valeuyr pour id_comptag_bdd ; vérifier puis corriger")
    # transfert depuis les nouveaux compteurs vers les les comptages associés (dfCreationComptageAssocie)
    # association du comptag_bdd
    if dicoDepuisNewCompteurVersAssocies:
        dfNewCompteurVersAssocie = cptRefSectHomoNew.loc[cptRefSectHomoNew.id_comptag.isin(list(dicoDepuisNewCompteurVersAssocies.keys()))].copy()
        dfNewCompteurVersAssocie['id_comptag_bdd'] = dfNewCompteurVersAssocie.apply(lambda x: [v for k, v in dicoDepuisNewCompteurVersAssocies.items()                                                                                       if x.id_comptag == k][0], axis=1 )
    # verifs
    if listeDepuisCorrespVersAssocies:
        if len(dfCptCorrespVersAssocie) != len(listeDepuisCorrespVersAssocies):
            raise ValueError("le nombre d'id_comptag identifie dans listeDepuisCorrespVersAssocies ne correspond pas au nombre d'objets trouves dans la(es) df visées")
    if dicoDepuisNewCompteurVersAssocies:
        if len(dfNewCompteurVersAssocie) != len(dicoDepuisNewCompteurVersAssocies):
            raise ValueError("le nombre d'id_comptag identifie dans dicoDepuisNewCompteurVersAssocies ne correspond pas au nombre d'objets trouves dans la(es) df visées")
    # fusion avec les comptages associes
    if listeDepuisCorrespVersAssocies and dicoDepuisNewCompteurVersAssocies:
        dfCreationComptageAssocie_MaJMano = pd.concat([dfCreationComptageAssocie, dfCptCorrespVersAssocie, dfNewCompteurVersAssocie])
    elif listeDepuisCorrespVersAssocies:
        dfCreationComptageAssocie_MaJMano = pd.concat([dfCreationComptageAssocie, dfCptCorrespVersAssocie])
    elif dicoDepuisNewCompteurVersAssocies:
        dfCreationComptageAssocie_MaJMano = pd.concat([dfCreationComptageAssocie, dfNewCompteurVersAssocie])
    # si besoin retrait des comptages associes qui vont etre envoyes dans les corres_id_comptag
    if listeDepuisAssociesVersCorresp:
        dfCreationComptageAssocie_MaJMano = dfCreationComptageAssocie_MaJMano.drop(dfCreationComptageAssocie_MaJMano.loc[
            dfCreationComptageAssocie_MaJMano.id_comptag.isin(listeDepuisAssociesVersCorresp)].index)    
    # transfert des données depuis les comptages associes vers les correspondances d'id_comptag
    if listeDepuisAssociesVersCorresp:
        depuisDfCreationComptageAssocieVersCorresp = dfCreationComptageAssocie.loc[dfCreationComptageAssocie.id_comptag.isin(listeDepuisAssociesVersCorresp)]
        depuisCptAssocMultiSectHomoVersCorresp = cptAssocMultiSectHomo.loc[cptAssocMultiSectHomo.id_comptag.isin(listeDepuisAssociesVersCorresp)]
        dfDepuisAssocVersCorresp = pd.concat([depuisDfCreationComptageAssocieVersCorresp, depuisCptAssocMultiSectHomoVersCorresp])
        # verifs
        if len(dfDepuisAssocVersCorresp) != len(listeDepuisAssociesVersCorresp):
            raise ValueError("le nombre d'id_comptag identifie dans listeDepuisAssociesVersCorresp ne correspond pas au nombre d'objets trouves dans la(es) df visées")
        # fusion avec les corresp
        dfCorrespIdComptag_MajMano = pd.concat([dfCorrespIdComptag, dfDepuisAssocVersCorresp])

    # EVIDER LES DONNEES EN TROP
    # retrait des comptages associes qui vont etre envoyes dans les corres_id_comptag
    if listeDepuisCorrespVersAssocies:
        dfCreationComptageAssocie_MaJMano = dfCreationComptageAssocie_MaJMano.loc[
            ~dfCreationComptageAssocie_MaJMano.id_comptag.isin(listeDepuisAssociesVersCorresp)].copy()
        cptAssocMultiSectHomo_MajMano = cptAssocMultiSectHomo.loc[
            ~cptAssocMultiSectHomo.id_comptag.isin(listeDepuisAssociesVersCorresp)].copy()
    if dicoDepuisNewCompteurVersAssocies:
        # retrait des nouveaux compteurs qui vont etre envoyes dans associes
        cptRefSectHomoNew_MajMano = cptRefSectHomoNew.loc[~cptRefSectHomoNew.id_comptag.isin(list(dicoDepuisNewCompteurVersAssocies.keys()))].copy()
    if listeDepuisCorrespVersAssocies:
        # retrait des corres_id_comptag qui vont etre envoyes dans les comptages associes
        dfCorrespIdComptag_MajMano = dfCorrespIdComptag_MajMano.loc[~dfCorrespIdComptag_MajMano.id_comptag.isin(listeDepuisCorrespVersAssocies)].copy()
        
    # verifs finales de coherence
    listeSortie = [dfCreationComptageAssocie_MaJMano, dfCorrespIdComptag_MajMano, cptAssocMultiSectHomo_MajMano, cptRefSectHomoNew_MajMano]
    listeEntree = [dfCreationComptageAssocie, dfCorrespIdComptag, cptAssocMultiSectHomo, cptRefSectHomoNew]
    if sum([len(a) for a in listeEntree]) != sum([len(b) for b in listeSortie]):
        raise ValueError('la somme des élémenst en entrée est différente de la somem des éléments en sortie. vérifier les transferts')
    
    return (dfCreationComptageAssocie_MaJMano, dfCorrespIdComptag_MajMano, cptAssocMultiSectHomo_MajMano, cptRefSectHomoNew_MajMano)      


def rassemblerNewCompteur(dep, reseau, gestionnai, concession, srcGeo, sensCpt, *tupleDfGeom):
    """
    regrouper les dataframes issues des fonctions de ventilation dans une seule destinée a etre intégrée das la bdd
    in : 
        srcGeo : la source de la géométrie, cf enum_src_geo dans la bdd
        sensCpt : le nombre de sens de circulation des comptages, cf enum_sens_cpt dans la bdd
        dep : string 2 caractère : le département
        reseau : string : cf enum_reseau dans bdd
        gestionnai : string : cf enum_gestionnai dans bdd
        concession : boolean
        tupleDfGeom : autant de tuple de type (df, NomDeLaGeometrie) que necessaire.
    """
    listCpteurNew = []
    for c in tupleDfGeom:
        df = c[0].copy()
        if srcGeo:
            df['src_geo'] = srcGeo
        if sensCpt:
            df['sens_cpt'] = sensCpt
        df['src_cpt'] = df.type_poste.apply(lambda x: 'convention gestionnaire' if x == 'permanent' else 'gestionnaire')
        df['convention'] = df.type_poste.apply(lambda x: True if x == 'permanent' else False)
        # RUSTINE A REPRENDRE : 
        if 'id_cpt' in df.columns:
            listCpteurNew.append(creerCompteur(df, c[1], dep, reseau, gestionnai, concession, id_cpt=df.id_cpt.tolist()))
        else: 
            listCpteurNew.append(creerCompteur(df, c[1], dep, reseau, gestionnai, concession, id_cpt=df.id_cpt.tolist()))
    return pd.concat(listCpteurNew)


def rassemblerNewComptage(annee, type_veh, dfComptageCompteurConnu, *dfComptageCompteurNew):
    """
    regrouper les dataframes des comptages, selon qu'elle proviennentde compteurs deja connus ou de compteurs que l'on vient d'insérer
    grace a rassemblerNewCompteur
    in :
        annee : caractères 4 string l'annee des comptages
        type_veh : tring, cf enum_type_veh dans la bdd
        dfComptageCompteurConnu : df des comptages relatifs a des compteurs deja dans la base, en général issu de df_attr_update
        dfComptageCompteurNew : toutes les df relatives au comptages issues de la ventilation
    """
    # verifs
    O.checkAttributsinDf(dfComptageCompteurConnu, attrComptageMano)
    for d in dfComptageCompteurNew:
        O.checkAttributsinDf(d, attrComptageMano)
    # pour la partie des nouveaux compteurs
    dfComptageNew = pd.concat(dfComptageCompteurNew)[attrComptageMano].assign(annee=annee)
    # association avec la partie des compteurs deja connus
    dfComptageNewTot = pd.concat([creer_comptage(dfComptageNew.id_comptag.tolist(), annee, dfComptageNew.src, type_veh, periode=dfComptageNew.periode),
                             creer_comptage(dfComptageCompteurConnu.id_comptag.tolist(), annee, dfComptageCompteurConnu.src.tolist(), 
                                            type_veh, periode=dfComptageCompteurConnu.periode.tolist())])
    return dfComptageNewTot
    
def rassemblerIndics(annee, dfComptageNewTot, dfTraficAgrege, dfTraficMensuel=None, dfTraficHoraire=None):
    """
    regrouper et mettre en forme les dataframes des indicateurs agreges, mensuel et horaires, correspondants aux id_comptages
    des comptages cree par rassemblerNewComptage()
    in : 
        annee : string 4 caractères
        dfComptageNewTot : df des nouveaux comptages créées par  rassemblerNewComptage()
        dfTraficAgrege : dataframe des données de trafic agrege. generalement df_attr
        dfTraficMensuel : dataframe des données de trafic mensuelle. generalement df_attr_mens
        dfTraficHoraire : dataframe des données de trafic haorire. generalement df_attr_horaire
    """   
    # récupérer les données
    listIdComptagIndicNew = dfComptageNewTot.id_comptag.tolist()
    dfAttrIndicAgregeNew = dfTraficAgrege.loc[dfTraficAgrege.id_comptag.isin(listIdComptagIndicNew)]
    dfIndicAgregeNew = structureBddOld2NewForm(dfAttrIndicAgregeNew.assign(annee=annee), annee, ['id_comptag', 'annee', 'fichier'], ['tmja', 'pc_pl'], 'agrege')
    if isinstance(dfTraficMensuel, pd.DataFrame) and not dfTraficMensuel.empty:
        dfAttrIndicMensNew = dfTraficMensuel.loc[dfTraficMensuel.id_comptag.isin(listIdComptagIndicNew)]
        dfIndicMensNew = structureBddOld2NewForm(dfAttrIndicMensNew.assign(annee=annee), annee, ['id_comptag', 'annee', 'fichier', 'donnees_type'], 
                                              list(dico_mois.keys()), 'mensuel')
    else :
        dfIndicMensNew = None
    if isinstance(dfTraficHoraire, pd.DataFrame) and not dfTraficHoraire.empty:
        dfAttrIndicHoraireNew = dfTraficHoraire.loc[dfTraficHoraire.id_comptag.isin(listIdComptagIndicNew)]    
        dfIndicHoraireNew = structureBddOld2NewForm(dfAttrIndicHoraireNew.assign(annee=annee), '2020', ['id_comptag', 'annee'], ['tata'], 'horaire')
    else:
        dfIndicHoraireNew = None
    return dfIndicAgregeNew, dfIndicMensNew, dfIndicHoraireNew
    
    
    
    
    
    
    