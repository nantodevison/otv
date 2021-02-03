# -*- coding: utf-8 -*-

'''
Created on 27 oct. 2019

@author: martin.schoreisz

gestion des ronds points lors de l'agregation
'''

import geopandas as gp
import pandas as pd
import numpy as np
from Base_BdTopo import Import_outils as io
from shapely.ops import polygonize





def df_rond_point(df, taille_buffer=0):
    """
    creer une dataframe de polygone aux rond point a partir d'une df issue des routes de la Bdtopo
    en entree : 
        df : geodataframe issues des route des la Bdtopo
        taille buffer : integer : taille du buffer pour créer le polygone (par defaut 0)
    en sortie : 
        gdf_rd_point : geodataframe des rond points avec id_rdpt(integer), rapport_aire(float), geometry(polygon, 2154)
    """
    #créer une liste de rd points selon le rapport longueur eu carré sur aire et créer la geodtaframe qui va bien
    dfRondsPoints=df.loc[df.nature=='Rond-point'].copy()
    dico_rd_pt=[[i, ((t.length**2)/t.area),t.buffer(taille_buffer), t.area] for i,t in enumerate(polygonize(dfRondsPoints.geometry))]
    if not dico_rd_pt : 
        raise PasDeRondPointError(df)
    gdf_rd_point=gp.GeoDataFrame([(a[0],a[1], a[3]) for a in dico_rd_pt], geometry=[a[2] for a in dico_rd_pt],crs="EPSG:2154", columns=['id_rdpt', 'rapport_aire', 'aire'])
    return gdf_rd_point

def lignes_rd_pts(df) :
    """
    obtenir les lignes qui forment les rd points
    en entree : 
       df : geodataframe issues des route des la Bdtopo
    en sortie : 
        lignes_rd_pt : geodataframe des lignes composant les rd points, contenat les attributs id_rdtpt(integer) et rapport_aire(float)
    """
    gdf_rd_point_ext=df_rond_point(df,0.1) #polygone extereiur des rond points
    gdf_rd_point_int=df_rond_point(df,-0.1) #polygone interieur des ronds points
    #trouver les lignes Bdtopo contenues dans les rd points ext et non contenues ds les rds points int
    try  : 
        lignes_ds_rd_pt_ext=gp.sjoin(df.drop('id_tronc_elem',axis=1),gdf_rd_point_ext,op='within') #contenues ds rd_points ext (le drop est pour éviter un runtimeWarning du aux NaN)
        lignes_ds_rd_pt_int=gp.sjoin(df.drop('id_tronc_elem',axis=1),gdf_rd_point_int,op='within') #contenues ds rd_points int
    except KeyError : 
        lignes_ds_rd_pt_ext=gp.sjoin(df,gdf_rd_point_ext,op='within') #contenues ds rd_points ext (le drop est pour éviter un runtimeWarning du aux NaN)
        lignes_ds_rd_pt_int=gp.sjoin(df,gdf_rd_point_int,op='within')
    lignes_ds_rd_pt_ext_ss_doublon=lignes_ds_rd_pt_ext.drop_duplicates('id_ign') #nettoyage doublons
    lignes_rd_pt=lignes_ds_rd_pt_ext_ss_doublon.loc[~lignes_ds_rd_pt_ext_ss_doublon.index.isin(lignes_ds_rd_pt_int.index.tolist())].drop('index_right',axis=1)
    return lignes_rd_pt

def lign_entrant_rd_pt(df, df_lgn_rd_pt):
    """
    trouver les lignes qui arrivent sur un rond point sans en faire partie
    en entree : 
        df : dataframe bdtopo
        df_lgn_rd_pt : dataframe des lignes qui constituent les rd-points
    en sortie : 
        ligne_entrant_rd_pt : dataframe des lignes qui arrivent sur un rd point
    """
    #trouver les lignes entrantes sur les rd points : celle qui intersectent le poly ext mais pas le poly int d'un rd pt et qui ne sont pas ds ligne rd pt
    poly_ext=df_rond_point(df,0.1)
    poly_int=df_rond_point(df,-0.1)
    try :
        ligne_inter_poly_ext=gp.sjoin(df.drop('id_tronc_elem',axis=1),poly_ext,op='intersects')
        ligne_inter_poly_int=gp.sjoin(df.drop('id_tronc_elem',axis=1),poly_int,op='intersects')
    except KeyError :
        ligne_inter_poly_ext=gp.sjoin(df,poly_ext,op='intersects')
        ligne_inter_poly_int=gp.sjoin(df,poly_int,op='intersects')
    ligne_entrant_rd_pt=ligne_inter_poly_ext.loc[~ligne_inter_poly_ext.id_ign.isin(ligne_inter_poly_int.id_ign.tolist()+df_lgn_rd_pt.id_ign.tolist())].copy()
    
    #cas des ignes qui touchent plusieurs rdpoint : si trop de lignes touchent 2 rdpt on en perds des references àcertains rdpt si on fait 
    #un simple drop_duplicated (notamment les rdpt uniquemnet touches par des lignes qui touchent d'autre drpt.
    #dc : 
    ligne_entrant_rd_ptss_dbl=ligne_entrant_rd_pt.drop_duplicates('id_ign') #caracterisation du drop duplicated
    list_rdpt_ss_dbl=ligne_entrant_rd_ptss_dbl.id_rdpt.unique() #list des rdpt après drop
    list_rdpt_tot=df_lgn_rd_pt.id_rdpt.unique() #compraaison avec la liste des rdpt totaux
    if any([a not in list_rdpt_ss_dbl for a in list_rdpt_tot]) : #s'il manque des rdpt ça peut venir de trop de rdpt connectés par des lignes 
        double=ligne_entrant_rd_pt.loc[ligne_entrant_rd_pt.duplicated('id_ign', keep=False)].sort_values('id_ign').copy() #liste des doublons avec la ref pr chaq rdpt
        ligne_entrant_rd_filtree=ligne_entrant_rd_ptss_dbl.drop(double.loc[double.id_rdpt.isin([a for a in list_rdpt_tot if a not in list_rdpt_ss_dbl])].index.tolist()) #on suppirme de la base initiale les lignes qui font références aux rd points qui manque
        double_a_ajout=double.loc[double.id_rdpt.isin([a for a in list_rdpt_tot if a not in list_rdpt_ss_dbl])].copy() #la df des rd points qui manque 
        ligne_entrant_rd_pt=pd.concat([ligne_entrant_rd_filtree,double_a_ajout], sort=False, axis=0) #fusion
    
    return ligne_entrant_rd_pt

def carac_rond_point(df_lign_entrant_rdpt) : 
    """
    caractériser les rd points en fonction du nombre de noms de voies entrant, du nom et codevoie_d de ces voies
    en entree : 
        df_lign_entrant_rdpt : dataframe des lignes qui arrivent sur les rond points. issus de lign_entrant_rd_pt
    en sortie : 
        carac_rd_pt : dataframe des ronds points avec comme index leur id issu de df_rond_point et comme attribut nb_rte_rdpt(integer), 
                    nom_rte_rdpt(tuple de string), codevoie_rdpt(tuple de string)
    """
    def nb_routes_entree_rdpt(nb_obj_sig_entrant, valeur_sens, nom_rte_rdpt, codevoie_rdpt, nb_rte_rdpt) : 
        """
        nombre de voie différentes entrant sur un rond point pour le cas particulier du rd point avec 1 seul nom de voie entrant
        en entree : 
            nb_obj_sig_entrant : integer : nb de ligne toucahnat le rd point
            valeur_sens : tuple de set de l'attribut sens des lignes touchant le rd point
            nom_rte_rdpt : tuple de set de l'attribut numero des lignes touchant le rd point
            codevoie_rdpt : tuple de set de l'attribut codevoie_d des lignes touchant le rd point
            nb_rte_rdpt : nombre de voie entrante au rd point à l'origine
        en sortie : 
            nb_rte_rdpt_corr : integer : nb de voie differentes arrivant sur le rd point
        """
        if nom_rte_rdpt != ('NC',) :
            return nb_rte_rdpt
        elif codevoie_rdpt !=('NR',) :
            return len(codevoie_rdpt)
        elif nb_obj_sig_entrant==1 or (nb_obj_sig_entrant==2  and all([a !='Double' for a in valeur_sens])):
            return 1
        elif all([a =='Double' for a in valeur_sens]) : 
            if nb_obj_sig_entrant>=2 : 
                return nb_obj_sig_entrant
            elif nb_obj_sig_entrant>=2 : 
                return (nb_obj_sig_entrant//2)+1
        elif all([a!='Double' for a in valeur_sens]) : 
            if nb_obj_sig_entrant<=4 :
                return 1 
            if nb_obj_sig_entrant>2 and nb_obj_sig_entrant%2==0 : 
                return (nb_obj_sig_entrant//2)
            elif nb_obj_sig_entrant>2 and nb_obj_sig_entrant%2!=0 :
                return (nb_obj_sig_entrant//2)+1
   
            
    #rgrouper par id
    carac_rd_pt=(pd.concat([df_lign_entrant_rdpt.groupby('id_rdpt').numero.nunique(),#compter les nom de voi uniques
                df_lign_entrant_rdpt.groupby('id_rdpt').agg({'numero': lambda x: tuple((set(x))), #agereges les noms de voie et codevoie_d dans des tuples
                                                            'nom_1_d' : lambda x: tuple((set(x))),
                                                            'id':'count',
                                                            'sens': lambda x: tuple((set(x))),
                                                            'id_ign':lambda x: tuple((set(x)))})], axis=1))
    carac_rd_pt.columns=['nb_rte_rdpt', 'nom_rte_rdpt_entrant','codevoie_rdpt_entrant','nb_obj_sig_entrant','valeur_sens','id_ign_entrant']#noms d'attriuts explicite
    #pour les rd point avec 1 seul nom de voie entrant on vérifie bien qu'il n'y ai pas plusieurs voies différentes mais non connues par l'IGN
    carac_rd_pt.loc[carac_rd_pt['nb_rte_rdpt']==1,'nb_rte_rdpt']=(carac_rd_pt.loc[carac_rd_pt['nb_rte_rdpt']==1].apply(
        lambda x : nb_routes_entree_rdpt(x['nb_obj_sig_entrant'], x['valeur_sens'], x['nom_rte_rdpt_entrant'], x['codevoie_rdpt_entrant'], x['nb_rte_rdpt']),axis=1))
    return carac_rd_pt

def identifier_rd_pt(df):
    """
    ajouter les identifiant de rd pt aux lignes bdtopo et creer une df des rd pt
    en entree : 
        df : datafrmae isssu des routes de la bdttopo
    en sortie 
        df_avec_rd_pt : dataframe initiale + attributs rd pt
        carac_rd_pt : df des rd points avec nb voies entrantes, nom voies entrantes et num voie du rdpt
    """
    #recuperer les lignes qui constituent les rd points
    try :
        lgn_rd_pt=lignes_rd_pts(df)
    except PasDeRondPointError : 
        df_avec_rd_pt=df.copy()
        df_avec_rd_pt['id_rdpt']=np.NaN
        carac_rd_pt, ligne_entrant_rdpt=pd.DataFrame(np.array([])),pd.DataFrame(np.array([]))
        return df_avec_rd_pt, carac_rd_pt, ligne_entrant_rdpt
    #trouver les lignes entrantes sur les rd points : celle qui intersectent le poly ext mais pas le poly int d'un rd pt et qui ne sont pas ds ligne rd pt
    ligne_entrant_rdpt=lign_entrant_rd_pt(df,lgn_rd_pt)#caractériser les rd points
    carac_rd_pt=carac_rond_point(ligne_entrant_rdpt)
    #ajouter l'identifiant et attibuts du rd point aux données BdTopo
    df_avec_rd_pt=df.merge(lgn_rd_pt[['id_ign', 'id_rdpt']], how='left', on='id_ign').merge(carac_rd_pt.reset_index(), how='left', on='id_rdpt')
    #grouper les données bdtopo par dr point por recup la liste des lignes et des noms de voies par rd_pt
    ligne_et_num_rdpt=df_avec_rd_pt.groupby('id_rdpt').agg({'numero': lambda x: tuple((set(x))),
                                                            'id_ign': lambda x: tuple((set(x))),
                                                            'nom_1_d': lambda x: tuple((set(x)))}).reset_index()
    ligne_et_num_rdpt.columns=['id_rdpt','numero_rdpt','id_ign_rdpt', 'codevoie_rdpt']                                                        
    carac_rd_pt=carac_rd_pt.reset_index().merge(ligne_et_num_rdpt, on='id_rdpt' ).set_index('id_rdpt')
    return df_avec_rd_pt, carac_rd_pt, ligne_entrant_rdpt


def verif_touche_rdpt(lignes_adj):
    """
    vérifier si une ligne touche un rd point
    en entree : 
        lignes_adj : lignes issues de df_lignes : df des lignes avec rd points
    en sortie 
        True ou False ; si la ligne touche un rd point ou non
        num_rdpt : float ou np.NaN
    """
    num_rdpt=lignes_adj.id_rdpt.unique()
    #print(f'num rdpt : {num_rdpt}, lignes adj : {lignes_adj.index.tolist()}')
    if not np.isnan(num_rdpt).all() :
        return True, num_rdpt[~np.isnan(num_rdpt)]
    else : 
        return False, np.NaN

def recup_lignes_rdpt(carac_rd_pt,num_rdpt,list_troncon,num_voie,codevoie):
    """
    obtenir les lignes qui composent un rd pt et les lignes suivantes s'il n'est pas une fin de troncon
    en entree : 
        carac_rd_pt : df des caractereistiques des rdpt, issu de identifier_rd_pt
        num_rdpt : float, issu de verif_touche_rdpt
        list_troncon : list des lignes du troncon arrivant sur le rd pt, issu de liste_complete_tronc_base
        num_voie : string : numero de la voie entrante (ex : D113)
        codevoie : string codevoie_d de la ligne de depart
    en sortie : 
        ligne_rdpt : liste des id_ign qui composent le rd point, ou liste vide si le rd pt n'est pas a ssocier a cette voie
        lignes_sortantes : liste des id_ign qui continue apres le rd pt si le troncon elementaire n'est pas delimite par celui-ci, liste vide sinon
    """
    num_rdpt=int(num_rdpt[0])
    df_rdpt=carac_rd_pt.loc[num_rdpt]
    nb_voie_rdpt=df_rdpt.nb_rte_rdpt
    ligne_rdpt=list(df_rdpt.id_ign_rdpt)
    #dans le cas ou 1 routes arrivent sur le rd point on doit aussi prevoir de continuer à chercher des lignes qui continue, car le troncon reste le même, dc on recupère
    # les lignes sortantes 
    if  nb_voie_rdpt==1 :
        lignes_sortantes=[a for a in filter(lambda x : x not in list_troncon,df_rdpt.id_ign_entrant)]
        return ligne_rdpt, lignes_sortantes  
    #sinon, si le numero de la voie de la ligne qui touchent le rd pt est le même et different de NC, on retourne les lignes du rd pt
    # mais pas le suivantes car plus de 2 voies entrent sur le rd pont
    else :
        lignes_sortantes=[]
        if num_voie != 'NC' :
            if num_voie in df_rdpt.numero_rdpt : 
                return ligne_rdpt, lignes_sortantes
            elif (not [a in df_rdpt.nom_rte_rdpt_entrant for a  in df_rdpt.numero_rdpt]) and  codevoie in df_rdpt.codevoie_rdpt : 
                return ligne_rdpt, lignes_sortantes 
            else : return [], lignes_sortantes
        else : 
            if not any([x for x in df_rdpt.codevoie_rdpt if x in df_rdpt.codevoie_rdpt_entrant]): #si aucun des code_voie ne correspond au rd point
                return ligne_rdpt, lignes_sortantes #on affecte arbitrairement
            elif codevoie !='NR' : 
                if codevoie in df_rdpt.codevoie_rdpt : 
                    return ligne_rdpt, lignes_sortantes
                else : return [], lignes_sortantes
            else : 
                if len(set(df_rdpt.codevoie_rdpt))==1 and 'NR' in df_rdpt.codevoie_rdpt :
                    return ligne_rdpt, lignes_sortantes
                elif codevoie=='NR' and 'NR' in df_rdpt.codevoie_rdpt : 
                    return ligne_rdpt, lignes_sortantes
                else : 
                    return [], lignes_sortantes
                

class PasDeRondPointError(Exception):  
    """
    Exception levee si il n'y a pas de rd point dans le jeud de donnees
    """     
    def __init__(self, df_lignes):
        Exception.__init__(self,f'pas de rond points dans la df {df_lignes}')
        self.erreur_type='PasDeRondPointError'