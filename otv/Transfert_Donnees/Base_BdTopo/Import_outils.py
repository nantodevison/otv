# -*- coding: utf-8 -*-
'''
Created on 29 janv. 2019
@author: martin.schoreisz

Module de creation d'importataion des donnees de base seravnt a l'aregation et des outils utiles

'''
import geopandas as gp
import pandas as pd
from collections import Counter
import Connexion_Transfert as ct
from shapely.wkt import loads
from shapely.ops import linemerge, unary_union
import Outils

#liste des colonnes necessaires pour aire le traotement
list_colonnes_necessaires=['codevoie_d','id_ign','source','target','numero','id','sens','geom','nature']

def import_donnes_base(bdd, schema, table_graph,table_vertex, localisation='boulot' ):
    """
    OUvrir une connexion vers le servuer de reference et recuperer les donn�es
    en entree : 
       bdd : string de reference de la connexion, selon le midule Outils , fichier Id_connexions, et module Connexion_transferts
       schema : schema contenant les tables de graph et de vertex
       table_graph : table contennat le referentiel (cf pgr_createtopology)
       table_vertex : table contenant la ecsription des vertex (cf pgr_analyzegraph)
       localisation : juste un moyen d'appeler le bon fichier d'identifiants de connexions en fonction de l'ordi sur lequel je bosse
    en sortie : 
        df : dataframe telle que telcharg�es depuis la bdd
    """
    #avant tout verf que toute les colonnes necessaires au traitements sont presents
    flag_col, col_manquante=Outils.check_colonne_in_table_bdd(bdd, schema, table_graph,*list_colonnes_necessaires)
    if not flag_col : 
        raise ManqueColonneError(col_manquante)
        
    
    with ct.ConnexionBdd(bdd,localisation) as c : 
        requete1=f"""with jointure as (
            select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom 
             from {schema}.{table_graph} t 
            left join {schema}.{table_vertex} v1 on t.source=v1.id 
            left join {schema}.{table_vertex} v2  on t.target=v2.id
            )
            select j.* from jointure j, zone_test_agreg z
            where st_intersects(z.geom, j.geom)"""
        requete2=f"""select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom 
             from {schema}.{table_graph} t 
            left join {schema}.{table_vertex} v1 on t.source=v1.id 
            left join {schema}.{table_vertex} v2  on t.target=v2.id"""
        df = gp.read_postgis(requete2, c.connexionPsy)
        return df

def fusion_ligne_calc_lg(gdf): 
    """
    créer une ligne ou une multi si pb à partir de plusieurs lignes d'une gdf
    en entre : 
        gdf : une geodataframe faite de ligne
    en sortie 
        lgn_agrege : une shpely geometrie
        longueur_base : un float decrivant la longuer
    """
    try : #union des geometries, si possible en linestring sinon en multi
        lgn_agrege=linemerge(gdf.unary_union) #union des geometries
    except ValueError :
        lgn_agrege=gdf.unary_union
    longueur_base=lgn_agrege.length
    return lgn_agrege, longueur_base

def angle_3_lignes(ligne_depart, lignes_adj, noeud,geom_noeud_central, type_noeud, df_lignes) : 
    """
    angles 3 lignes qui se separent
    en entree : 
        ligne_depart : sdtring : id_ign de la igne qui va separer
        lignes_adj : geodf des lignes qui se separentde la ligne de depart
        noeud : integer : numero du noeud central
        geom_noeud_central : wkt du point central de separation
        type_noeud : type du noeud centrale par rapport à la ligne de depart : 'source' ou 'target'
        df_lignes : df de l'ensemble des lignes du transparent
    en sortie : 
        lgn_cote_1 : df de la ligne qui se separe de la ligne etudie
        lgn_cote_2 : df de l'autre ligne qui se separe
        angle_1 : float : angle entre la ligne de depart et la 1er des lignes_adj
        angle_2 : float : angle entre la ligne de depart et la 2eme des lignes_adj
        angle_3 : l'ecart entre les 2 lignes qui se separent
    """
    #noeud partage = noeud, dc coord_noeud_partage=
    coord_noeud_partage=list(loads(geom_noeud_central).coords)[0] 
    lgn_cote_1=lignes_adj.iloc[0]
    lgn_cote_2=lignes_adj.iloc[1]
    #coord noeud non partage lignes de depart : c'est le 2eme ou avant dernier point dans la liste des points, selon le type_noeud
    coord_lgn_dep_pt1=([coord for coord in df_lignes.loc[ligne_depart].geom[0].coords][1] if type_noeud=='source' 
                       else [coord for coord in df_lignes.loc[ligne_depart].geom[0].coords][-2])
    # trouver type noeud pour ligne fin et en deduire la coord du noeud suivant sur la ligne:
    type_noeud_comp_lgn1='source' if lgn_cote_1['source']==noeud else 'target'
    coord_lgn1_fin_pt2=([coord for coord in lgn_cote_1.geom[0].coords][1] if type_noeud_comp_lgn1=='source' 
                       else [coord for coord in lgn_cote_1.geom[0].coords][-2])
    type_noeud_comp_lgn2='source' if lgn_cote_2['source']==noeud else 'target'
    coord_lgn2_fin_pt2=([coord for coord in lgn_cote_2.geom[0].coords][1] if type_noeud_comp_lgn2=='source' 
                       else [coord for coord in lgn_cote_2.geom[0].coords][-2])
    #angle entre les 2 lignes : 
    angle_1=Outils.angle_entre_2_ligne(coord_noeud_partage, coord_lgn_dep_pt1, coord_lgn1_fin_pt2)
    angle_2=Outils.angle_entre_2_ligne(coord_noeud_partage, coord_lgn_dep_pt1, coord_lgn2_fin_pt2)
    angle_3=360-(angle_1+angle_2)

    return lgn_cote_1, lgn_cote_2, angle_1,angle_2,angle_3 

def tronc_tch(ids, df_lignes) : 
    """
    obtenir une df des troncon touches par une ligne avec l'angle entre les lignes, le numero, le codevoie_d
    attention, la geom doit etre de type multilinestring
    en entree : 
        ids : tuple de string de id_ign
        df_lignes : données issues de identifier_rd_pt() avec id_ign en index
    en sortie : 
        df_tronc_tch : df avec les attributs precites
    """
    if len(ids)==1 : #une seule lignes dans le troncon
        df_ids=df_lignes.loc[ids].copy()
        noeuds = [df_ids.source,df_ids.target] 
        list_ids_tch= df_lignes.loc[((df_lignes.target.isin(noeuds)) | (df_lignes.source.isin(noeuds))) & 
                (~df_lignes.index.isin(ids))].index.tolist()
        list_carac_tch=[(a,b) for a,b in zip(list_ids_tch,[('source',df_lignes.loc[a].source) if df_lignes.loc[a].source in noeuds else 
                      ('target',df_lignes.loc[a].target) for a in list_ids_tch])]
        list_carac_fin=[(a[0][0],a[0][1][0],a[0][1][1], a[1]) for a in zip(list_carac_tch,['source' if b[1]==df_lignes.loc[ids].source else 'target' 
                                       for a,b in list_carac_tch])]
        df_tronc_tch=pd.DataFrame.from_records(list_carac_fin, columns=['id_ign', 'type_noeud_lgn', 'id_noeud_lgn', 'type_noeud_src'])
        #calcul des coordonnées des points 
        df_tronc_tch['coord_lgn_base']=df_tronc_tch.apply(lambda x : [df_ids.geom[0].coords[i] for i in range(len(df_ids.geom[0].coords))][1] if 
            x['type_noeud_src']=='source' else [df_ids.geom[0].coords[i] for i in range(len(df_ids.geom[0].coords))][-2],axis=1)
        df_tronc_tch['coord_lgn_comp']=df_tronc_tch.apply(lambda x : [df_lignes.loc[x['id_ign']].geom[0].coords[i] for i in range(len(df_lignes.loc[x['id_ign']].geom[0].coords))][1] if 
            x['type_noeud_lgn']=='source' else [df_lignes.loc[x['id_ign']].geom[0].coords[i] for i in range(len(df_lignes.loc[x['id_ign']].geom[0].coords))][-2],axis=1)
        df_tronc_tch['coord_noued_centr']=df_tronc_tch.apply(lambda x : [df_lignes.loc[x['id_ign']].geom[0].coords[i] for i in range(len(df_lignes.loc[x['id_ign']].geom[0].coords))][0] if x['type_noeud_lgn']=='source' 
            else [df_lignes.loc[x['id_ign']].geom[0].coords[i] for i in range(len(df_lignes.loc[x['id_ign']].geom[0].coords))][-1],axis=1)
        #angle
        df_tronc_tch['angle']=df_tronc_tch.apply(lambda x : Outils.angle_entre_2_ligne(x['coord_noued_centr'],x['coord_lgn_comp'], x['coord_lgn_base']),axis=1)
        df_tronc_tch=df_tronc_tch.merge(df_lignes[['numero','codevoie_d',df_lignes.geometry.name]], left_on='id_ign', right_index=True)
        df_tronc_tch['longueur']=df_tronc_tch[df_lignes.geometry.name].apply(lambda x : x.length)
    else : 
        df_ids=df_lignes.loc[list(ids)].copy()
        noeuds=[k for k,v in Counter(df_ids.source.tolist()+df_ids.target.tolist()).items() if v==1] #liste des noeuds uniques
        geom_noeuds=[loads(k).coords[0] for k,v in Counter(df_ids.src_geom.tolist()+df_ids.tgt_geom.tolist()).items() if v==1] #liste des geoms uniques
        geom_ligne=linemerge(unary_union(df_ids.geom.tolist())) #agregation des lignes
        pt_source=tuple([round(a,8) for a in geom_ligne.coords[0]]) #calculdes coordonées deb et fin de la lignes agregee
        pt_target=tuple([round(a,8) for a in geom_ligne.coords[-1]])
        df_noeud=pd.DataFrame(zip(noeuds,geom_noeuds), columns=['id_noeud', 'geom_noeud'])
        df_noeud['type_noeud']=df_noeud.apply(lambda x : 'target' if x['geom_noeud']==pt_target else 'source',axis=1) #affectation du type de noeud
        df_noeud.set_index('id_noeud',inplace=True)
        list_ids_tch= df_lignes.loc[((df_lignes.target.isin(noeuds)) | (df_lignes.source.isin(noeuds))) & 
                        (~df_lignes.index.isin(ids))].index.tolist()
        list_carac_tch=[(a,b) for a,b in zip(list_ids_tch,[('source',df_lignes.loc[a].source) if df_lignes.loc[a].source in noeuds else 
                      ('target',df_lignes.loc[a].target) for a in list_ids_tch])]
        list_carac_fin=[(a[0][0],a[0][1][0],a[0][1][1], a[1]) for a in zip(list_carac_tch,['source' if df_noeud.loc[b[1]].type_noeud=='source' else 'target' 
                                       for a,b in list_carac_tch])]
        df_tronc_tch=pd.DataFrame.from_records(list_carac_fin, columns=['id_ign', 'type_noeud_lgn', 'id_noeud_lgn', 'type_noeud_src'])
        #calcul des coordonnées des points 
        df_tronc_tch['coord_lgn_base']=df_tronc_tch.apply(lambda x : [geom_ligne.coords[i] for i in len(range(geom_ligne.coords))][1] if 
                    x['type_noeud_src']=='source' else [coord for coord in geom_ligne.coords][-2],axis=1)
        df_tronc_tch['coord_lgn_comp']=df_tronc_tch.apply(lambda x : [df_lignes.loc[x['id_ign']].geom[0].coords[i] for i in range(len(df_lignes.loc[x['id_ign']].geom[0].coords))][1] if 
                    x['type_noeud_lgn']=='source' else [df_lignes.loc[x['id_ign']].geom[0].coords[i] for i in range(len(df_lignes.loc[x['id_ign']].geom[0].coords))][-2],axis=1)
        df_tronc_tch['coord_noued_centr']=df_tronc_tch.apply(lambda x : [df_lignes.loc[x['id_ign']].geom[0].coords[i] for i in range(len(df_lignes.loc[x['id_ign']].geom[0].coords))][0] if x['type_noeud_lgn']=='source' 
            else [df_lignes.loc[x['id_ign']].geom[0].coords[i] for i in range(len(df_lignes.loc[x['id_ign']].geom[0].coords))][-1],axis=1)
        #angle
        df_tronc_tch['angle']=df_tronc_tch.apply(lambda x : Outils.angle_entre_2_ligne(x['coord_noued_centr'],x['coord_lgn_comp'], x['coord_lgn_base']),axis=1)
        #df_tronc_tch=df_tronc_tch.merge(df_lignes[['numero','codevoie_d']], left_on='id_ign', right_index=True)
        #ce qui suit n'est pas verifie, si besoin reprendre la ligne commentaire ci-dessus, mais qui ne permet pas d'avoir la geom et longueur
        df_tronc_tch=df_tronc_tch.merge(df_lignes[['numero','codevoie_d',df_lignes.geometry.name]], left_on='id_ign', right_index=True)
        df_tronc_tch['longueur']=df_tronc_tch[df_lignes.geometry.name].apply(lambda x : x.length)
    return df_tronc_tch

class ManqueColonneError(Exception):  
        """
        Exception levee si il manque une des colonnes necessaire
        """     
        def __init__(self, listeColonneManquante):
            Exception.__init__(self,f'il manque les colonnes {listeColonneManquante} dans la table de départ')