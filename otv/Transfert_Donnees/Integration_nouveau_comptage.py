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
                            tableCompteur, tableCorrespIdComptag,attrCompteurAssoc, attBddCompteur,
                            attrCompteurValeurMano, attrComptageAssoc, enumTypePoste, attrIndicHoraireAssoc)
from Import_export_comptage import (recupererIdUniqComptage, recupererIdUniqComptageAssoc)
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
    
def ventilerParSectionHomogene(tableCptNew, tableSectionHomo, codeDept):
    """
    Pour les points supposes a creer, les separer selon les cas (cf Out)
    in : 
        tableCptNew: string : schemaqualified table des pointsgeolocalise. creer lors de localiser_comptage_a_inserer().
        tableSectionHomo: string : schemaqualified table des sections homogene dans la bdd
        codeDept: string: code departement sur 2 caractères
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
                                      from (SELECT * FROM {tableSectionHomo} where list_dept like '%%{codeDept}%%') t LEFT JOIN {schemaComptage}.{tableCompteur} c ON st_dwithin(c.geom, t.geom, 30)
                                      ORDER BY t.gid, CASE WHEN c.type_poste = 'permanent' THEN 1 
                                                           WHEN c.type_poste = 'tournant' THEN 2
                                                           WHEN c.type_poste = 'ponctuel' THEN 3 
                                                           ELSE 4 
                                                           end
                                     """, c.sqlAlchemyConn)
    # on en déduit le plus proche voisin entre les points et la linauto
    ppV = O.plus_proche_voisin(ptGeom, linauto, 10, 'id_comptag', 'gid')
    ppVSectHomo = ppV.loc[~ppV.gid.isna()].copy()
    ppvHorsSectHomo = pt.loc[pt.id_comptag.isin(ppV.loc[ppV.gid.isna()].id_comptag.tolist())].copy()  # PENSER A CREER LES COMPTEURS POUR CEUX LA !!!!!
    # et on récupère l'id_comptag_existant et l'id de section homogene
    ppVTot = ptGeom.merge(ppVSectHomo, how='left', on='id_comptag').merge(linauto, how='left', on='gid')
    # trouver le nombre de comptage par section homogène (gid)
    nbCptSectHomo = ppVTot.gid.value_counts()
    # separer les differents cas a traiter
    cptSimpleSectHomo = ppVTot.loc[ppVTot.gid.isin(nbCptSectHomo.loc[nbCptSectHomo == 1].index.tolist())].copy()
    cptMultiSectHomo = ppVTot.loc[ppVTot.gid.isin(nbCptSectHomo.loc[nbCptSectHomo > 1].index.tolist())].copy()
    #verif
    if len(cptSansGeom)+len(cptSimpleSectHomo)+len(cptMultiSectHomo)+len(ppvHorsSectHomo) != len(pt):
        warnings.warn("""la somme des éléments présents dans 'cptSansGeom', 'ppvHorsSectHomo', 'cptSimpleSectHomo', 'cptMultiSectHomo' est supérieure au nombre d'éléments initiaux. vérifier les doublons dans les résultats, vérifier la linauto et les données sources""")
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
        columns = [c for c in ['id_comptag', 'donnees_type', 'valeur', 'mois', 'fichier', 'annee'] if c in dfIndic.columns]
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
    if any([e not in listAttrFixe for e in ['id_cptag_ref', 'annee']]):
        raise AttributeError('les attributs id_comptag et annee sont obligatoire dans listAttrFixe')
    if typeIndic == 'agrege':
        dfIndic = pd.melt(dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)), id_vars=listAttrFixe, value_vars=listAttrIndics, 
                              var_name='indicateur', value_name='valeur')
        columns = [c for c in ['id_cptag_ref', 'indicateur', 'valeur', 'fichier', 'obs', 'annee'] if c in dfIndic.columns]
        dfIndic = dfIndic[columns].rename(columns={'id':'id_comptag_uniq'})
    #elif typeIndic == 'mensuel':
    #    dfIndic = pd.melt(dfAConvertir.assign(annee=dfAConvertir.annee.astype(str)), id_vars=listAttrFixe, value_vars=listAttrIndics, 
    #                         var_name='mois', value_name='valeur')
    #    columns = [c for c in ['id_comptag', 'donnees_type', 'valeur', 'mois', 'fichier', 'annee'] if c in dfIndic.columns]
    #    dfIndic = dfIndic[columns].rename(columns={'donnees_type':'indicateur'})
    elif typeIndic == 'horaire': 
        dfIndic = dfAConvertir.rename(columns={'type_veh':'indicateur'})
        dfIndic = dfIndic.drop(['id_comptag', 'id_cpteur_asso'], axis=1, errors='ignore')
    dfIndic = dfIndic.merge(recupererIdUniqComptageAssoc(dfIndic.id_cptag_ref.tolist()), on='id_cptag_ref'
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
    # verif que tous les gid ont un cpt ref
    if not cptAssocMultiSectHomo.loc[~cptAssocMultiSectHomo.gid.isin(cptRefMultiSectHomo.gid.unique())].empty:
        raise ValueError('un des comptage associe n\'a pas de comptage de reference')
    if not len(cptRefMultiSectHomo)+len(cptAssocMultiSectHomo) == len(cptMultiSectHomo):
        raise ValueError('un ou plusieurs comptage n\'ont pas ete affecte comme reference ou associe')
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
    depuis une df des comptage de référence situé sur des tronçons avec un id_comptag existantdans la base, 
    séparer en 3 groupe selon le type de poste dans la bdd et le type de poste du hestionnaire.
    in : 
        df : dataframe a classifier
        nomAttrtypePosteGest : string : nom de l'attribut supportant le type de poste fourni par le gest 
        nomAttrtypePosteBdd : string : nom de l'attribut supportant le type de poste dans la bdd
        nomAttrperiode : string : nom de l'attribut décrivant la période de mesure
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
    dfModifTypePoste = df.loc[((df[nomAttrtypePosteGest].isin(('permanent', 'tournant'))) & (df[nomAttrtypePosteBdd] == 'ponctuel')) |
                              ((df[nomAttrtypePosteGest] == 'permanent') & (df[nomAttrtypePosteBdd] == 'tournant'))].copy()
    return dfCorrespIdComptag, dfCreationComptageAssocie, dfModifTypePoste
    
    