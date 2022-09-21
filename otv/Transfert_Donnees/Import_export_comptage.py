# -*- coding: utf-8 -*-
'''
Created on 27 janv. 2022

@author: martin.schoreisz
regroupe les fonctions d'import export des donnees de comptages
'''

import Outils as O
import pandas as pd
import warnings
import Connexion_Transfert as ct
from Params.Bdd_OTV import (nomConnBddOtv, schemaComptage, tableIndicAgrege, tableComptage, 
                            tableCompteur, tableIndicHoraire, schemaComptageAssoc,
                            tableIndicMensuel, tableCorrespIdComptag, vueLastAnnKnow)
import geopandas as gp


def compteur_existant_bdd(table=tableCompteur, schema=schemaComptage,dep=False, type_poste=False, gest=False):
    """
    recuperer les comptages existants dans une df
    en entree : 
        table : string : nom de la table
        schema : string : nom du schema
        dep : string, code du deprtament sur 2 lettres
        type_poste : string ou list de string: type de poste dans 'permanent', 'tournant', 'ponctuel'
    en sortie : 
        existant : df selon la structure de la table interrog�e
    """
    if not dep and not type_poste and not gest:
        rqt = f"select * from {schema}.{table}"
    elif not dep and not type_poste and gest:
        rqt = f"select * from {schema}.{table} where gestionnai='{gest}'"
    elif dep and not type_poste and not gest:
        rqt = f"select * from {schema}.{table} where dep='{dep}'"
    elif dep and not type_poste and gest:
        rqt = f"select * from {schema}.{table} where dep='{dep}' and gestionnai='{gest}'"
    elif not dep and isinstance(type_poste, str) and gest:
        rqt = f"select * from {schema}.{table} where gestionnai='{gest}' and type_poste='{type_poste}'"
    elif dep and isinstance(type_poste, str) and not gest:
        rqt = f"select * from {schema}.{table} where dep='{dep}' and type_poste='{type_poste}'"
    elif dep and isinstance(type_poste, str) and gest :
        rqt = f"select * from {schema}.{table} where dep='{dep}' and type_poste='{type_poste}' and gestionnai='{gest}' "
    elif dep and isinstance(type_poste, list) and not gest : 
        list_type_poste = '\',\''.join(type_poste)
        rqt = f"""select * from {schema}.{table} where dep='{dep}' and type_poste in ('{list_type_poste}')"""
    elif dep and isinstance(type_poste, list) and gest : 
        list_type_poste = '\',\''.join(type_poste)
        rqt = f"""select * from {schema}.{table} where dep='{dep}' and type_poste in ('{list_type_poste}') and gestionnai='{gest}'"""
    with ct.ConnexionBdd(nomConnBddOtv) as c:
        if table == tableCompteur :
            existant = gp.GeoDataFrame.from_postgis(rqt, c.sqlAlchemyConn, geom_col='geom',crs='epsg:2154')
        else :
            existant = pd.read_sql(rqt, c.sqlAlchemyConn)
    return existant


def recupererIdUniqComptage(dfSource, derniereAnnee=False):
        """
        a partir d'une liste d'id_comptag et d'une annee, recuperer les identifiant unique associes à chaque couple dans l'OTV, ou simplement
        l'dientifiant unique de la dernière année comptée
        in : 
            dfSource : df contenat un champs id_comptag (et annee si derniereAnnee = True ; qui soit etre unique) 
                        que l'on va joindre pour obtenir l'id_comptag_uniq
            derniereAnnee : booleen : si True, alors on recupere l'idcomptag_uniq de la derniere annee connue
        out : 
            dfIdCptUniqs dfSource avec id_comptag_uniq, id_comptag, annee
        """
        if not derniereAnnee:
            O.checkAttributsinDf(dfSource, ['id_comptag', 'annee'])
            dfidComptagSource = dfSource.drop_duplicates(['id_comptag', 'annee'])[['id_comptag', 'annee']]
        else:
            O.checkAttributsinDf(dfSource, ['id_comptag'])
            dfidComptagSource = dfSource.drop_duplicates(['id_comptag'])[['id_comptag']]
        with ct.ConnexionBdd(nomConnBddOtv) as c:
            if derniereAnnee:
                txt = '\'),(\''.join(dfSource.id_comptag.tolist())
                listIdCpt = f"('{txt}')"
                rqt = f"""select DISTINCT ON (ca.id_comptag) ca.id id_comptag_uniq, ca.id_comptag, ca.annee
                             from {schemaComptage}.{tableComptage} ca JOIN (SELECT * from (VALUES  
                             {listIdCpt}) 
                             AS t (id_cpt)) t ON t.id_cpt=ca.id_comptag
                             ORDER BY ca.id_comptag, ca.annee desc"""
            else:
                rqt = f"""select distinct on (ca.id_comptag) ca.id id_comptag_uniq, ca.id_comptag, ca.annee
                                         from {schemaComptage}.{tableComptage} ca JOIN (SELECT * from (VALUES  
                                         {','.join([f'{a, b}' for a, b in zip(dfidComptagSource.id_comptag.tolist(),
                                          dfidComptagSource.annee.tolist())])}) 
                                         AS t (id_cpt, ann)) t ON t.id_cpt=ca.id_comptag AND t.ann=ca.annee
                                         order by ca.id_comptag, ca.annee DESC"""
            dfIdCptUniqs = pd.read_sql(rqt, c.sqlAlchemyConn)
        dfSourceIds = dfSource.merge(dfIdCptUniqs, on=['id_comptag', 'annee'], how='left').drop_duplicates(
            ) if not derniereAnnee else dfSource.merge(dfIdCptUniqs, on='id_comptag', how='left').drop_duplicates(
            )
        #verif que tout le monde a un id_comptag_uniq
        if dfSourceIds.id_comptag_uniq.isna().any():
            dfSourceIdsNanList = dfSourceIds.loc[dfSourceIds.id_comptag_uniq.isna()].id_comptag.unique()
            warnings.warn(f"les id_comptag {dfSourceIdsNanList} n'ont pas d'id_comptag_uniq. Creer un comptage avant ")
        return  dfSourceIds 
  
    
def recupererIdUniqComptageAssoc(listIdComptag):
    """
    a partir d'une liste d'id_comptag ede reference, recuperer les identifiant unique dans la table comptage du schema comptage_assoc
    in : 
        listIdComptag : list des id_comptage a chercher
    out : 
        dfIdCptUniqs df avec id_comptag_uniq, id_comptag, annee
    """
    with ct.ConnexionBdd(nomConnBddOtv) as c  :
        dfIdCptUniqsAssoc=pd.read_sql(f'select id id_comptag_uniq, id_cptag_ref, id_cpteur_asso from {schemaComptageAssoc}.{tableComptage} where id_cptag_ref=ANY(ARRAY{listIdComptag})', c.sqlAlchemyConn)
    return  dfIdCptUniqsAssoc


def recupererComptageSansTrafic(listIdComptag, annee):
    """
    a partir d'une liste d'id_comptag et d'une annee, recuperer les id_comptag n'ayant pas de données de TMJA associees
    in : 
        listIdComptag : list des id_comptage a chercher
        annee : texte : annee sur 4 caractere
    """
    with ct.ConnexionBdd(nomConnBddOtv) as c  :
        dfCptSansTmja=pd.read_sql(f'select id, id_comptag, annee from comptage.vue_comptage_sans_tmja where id_comptag=ANY(ARRAY{listIdComptag}) and annee=\'{annee}\'', c.sqlAlchemyConn)
    return  dfCptSansTmja


def recupererLastAnnKnow(listIdComptag):
    """
    a partir d'une liste d'id_comptag, recuperer les données contenues dans la vue de la derniere annee connue
    """
    with ct.ConnexionBdd() as c:
        dfLastAnKnow = pd.read_sql(f"select * from {schemaComptage}.{vueLastAnnKnow} where id_comptag = any(array{listIdComptag})",
                                   c.sqlAlchemyConn)
    return dfLastAnKnow
    
    
    
def insert_bdd(schema, table, df, if_exists='append',geomType='POINT'):
    """
    insérer les données dans la bdd et mettre à jour la geometrie
    en entree : 
        schema : string nom du schema de la table
        table : string : nom de la table
    """
    if isinstance(df, gp.GeoDataFrame) :
        nomGeom = df.geometry.name
        dfAvecGeom = df.loc[(~df[nomGeom].isna()) & (~df[nomGeom].is_empty)]
        dfSansGeom = df.loc[(df[nomGeom].isna()) | (df[nomGeom].is_empty)]
        with ct.ConnexionBdd(nomConnBddOtv) as c:
            if not dfAvecGeom.empty:
                dfAvecGeom.to_postgis(table,c.sqlAlchemyConn,schema=schema,if_exists=if_exists, index=False)
            if not dfSansGeom.empty:
                dfSansGeom.to_sql(table,c.sqlAlchemyConn,schema=schema,if_exists=if_exists, index=False )
    elif isinstance(df, pd.DataFrame) : 
        with ct.ConnexionBdd(nomConnBddOtv) as c:
            df.to_sql(table,c.sqlAlchemyConn,schema=schema,if_exists=if_exists, index=False )
    return
           
def insererSchemaComptage(df, typeData):
    """
    spécialisation de la fonction insert_bdd pour le cas des compteurs
    in : 
        df : dataframe a inserer. doit respecter le formet de la bdd
        typeData : string parmi 'comptage', compteur, indicAgrege, indicMensuel, indicHoraire
    """
    O.checkParamValues(typeData, ['comptage', 'compteur', 'indicAgrege', 'indicMensuel', 'indicHoraire', 'corresp_id_comptag'])
    if typeData == 'comptage': 
        nomTable = tableComptage
    elif typeData == 'compteur': 
        nomTable = tableCompteur
    elif typeData == 'indicAgrege':
        nomTable = tableIndicAgrege
    elif typeData == 'indicMensuel':
        nomTable = tableIndicMensuel 
    elif typeData == 'indicHoraire':
        nomTable = tableIndicHoraire
    elif typeData == 'corresp_id_comptag':
        nomTable = tableCorrespIdComptag
    insert_bdd(schemaComptage, nomTable,
                    df, 'append', 'POINT')
    return

def insererSchemaComptageAssoc(df, typeData):
    """
    spécialisation de la fonction insert_bdd pour le cas des compteurs
    in : 
        df : dataframe a inserer. doit respecter le formet de la bdd
        typeData : string parmi 'comptage', compteur, indicAgrege, indicMensuel, indicHoraire
    """
    O.checkParamValues(typeData, ['comptage', 'compteur', 'indicAgrege', 'indicMensuel', 'indicHoraire'])
    if typeData == 'comptage': 
        nomTable = tableComptage
    elif typeData == 'compteur': 
        nomTable = tableCompteur
    elif typeData == 'indicAgrege':
        nomTable = tableIndicAgrege
    elif typeData == 'indicMensuel':
        nomTable = tableIndicMensuel 
    elif typeData == 'indicHoraire':
        nomTable = tableIndicHoraire
    insert_bdd(schemaComptageAssoc, nomTable,
                    df, 'append', 'POINT')
    return