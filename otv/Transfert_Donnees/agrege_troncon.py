# -*- coding: utf-8 -*-
'''
Created on 29 janv. 2019
@author: martin.schoreisz

Module de creation de troncon homogene

'''
import matplotlib
import geopandas as gp
import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
import Connexion_Transfert as ct
from shapely.wkt import loads
from shapely.ops import polygonize, linemerge
import Outils
#from psycopg2 import extras


def import_donnes_base(ref_connexion):
    """
    OUvrir une connexion vers le servuer de reference et recuperer les données
    en entree : 
       ref_connexion : string de reference de la connexion, selon le midule Outils , fichier Id_connexions, et module Connexion_transferts
    en sortie : 
        df : dataframe telle que telchargées depuis la bdd
    """
    with ct.ConnexionBdd('local_otv') as c : 
        requete1="""with jointure as (
            select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom 
             from public.traf2015_bdt17_ed15_l t 
            left join public.traf2015_bdt17_ed15_l_vertices_pgr v1 on t.source=v1.id 
            left join public.traf2015_bdt17_ed15_l_vertices_pgr v2  on t.target=v2.id
            )
            select j.* from jointure j, zone_test_agreg z
            where st_intersects(z.geom, j.geom)"""
        requete2="""select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom 
             from public.traf2015_bdt17_ed15_l t 
            left join public.traf2015_bdt17_ed15_l_vertices_pgr v1 on t.source=v1.id 
            left join public.traf2015_bdt17_ed15_l_vertices_pgr v2  on t.target=v2.id"""
        df = gp.read_postgis(requete1, c.connexionPsy)
        return df

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
    dico_rd_pt=[[i, ((t.length**2)/t.area),t.buffer(taille_buffer), t.area] for i,t in enumerate(polygonize(df.geometry)) 
            if 12<=((t.length**2)/t.area)<=14 or (14<=((t.length**2)/t.area)<20 and 700<t.area<1000) ] # car un cercle à un rappor de ce type entre 12 et 13
    gdf_rd_point=gp.GeoDataFrame([(a[0],a[1], a[3]) for a in dico_rd_pt], geometry=[a[2] for a in dico_rd_pt], columns=['id_rdpt', 'rapport_aire', 'aire'])
    gdf_rd_point.crs={'init':'epsg:2154'} #mise à jour du systeme de projection
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
    lignes_ds_rd_pt_ext=gp.sjoin(df.drop('id_tronc_elem',axis=1),gdf_rd_point_ext,op='within') #contenues ds rd_points ext (le drop est pour éviter un runtimeWarning du aux NaN)
    lignes_ds_rd_pt_int=gp.sjoin(df.drop('id_tronc_elem',axis=1),gdf_rd_point_int,op='within') #contenues ds rd_points int
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
    ligne_inter_poly_ext=gp.sjoin(df.drop('id_tronc_elem',axis=1),poly_ext,op='intersects')
    ligne_inter_poly_int=gp.sjoin(df.drop('id_tronc_elem',axis=1),poly_int,op='intersects')
    ligne_entrant_rd_pt=ligne_inter_poly_ext.loc[~ligne_inter_poly_ext.id_ign.isin(ligne_inter_poly_int.id_ign.tolist()+df_lgn_rd_pt.id_ign.tolist())].copy()
    ligne_entrant_rd_pt.drop_duplicates('id_ign', inplace=True) #cas des ignes qui touchent plusieurs rdpoint
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
        elif all([a =='Double' for a in valeur_sens]) and nb_obj_sig_entrant>=2 : 
            return nb_obj_sig_entrant
        elif any([a=='Double' for a in valeur_sens]) and nb_obj_sig_entrant>=2 : 
            return (nb_obj_sig_entrant//2)+1
        elif all([a!='Double' for a in valeur_sens]) and nb_obj_sig_entrant>2 and nb_obj_sig_entrant%2==0 :    
            return (nb_obj_sig_entrant//2)
        elif all([a!='Double' for a in valeur_sens]) and nb_obj_sig_entrant>2 and nb_obj_sig_entrant%2!=0 :    
            return (nb_obj_sig_entrant//2)+1
    #rgrouper par id
    carac_rd_pt=(pd.concat([df_lign_entrant_rdpt.groupby('id_rdpt').numero.nunique(),#compter les nom de voi uniques
                df_lign_entrant_rdpt.groupby('id_rdpt').agg({'numero': lambda x: tuple((set(x))), #agereges les noms de voie et codevoie_d dans des tuples
                                                            'codevoie_d' : lambda x: tuple((set(x))),
                                                            'id':'count',
                                                            'sens': lambda x: tuple((set(x))),
                                                            'id_ign':lambda x: tuple((set(x)))})], axis=1))
    carac_rd_pt.columns=['nb_rte_rdpt', 'nom_rte_rdpt','codevoie_rdpt','nb_obj_sig_entrant','valeur_sens','id_ign_entrant']#noms d'attriuts explicite
    #pour les rd point avec 1 seul nom de voie entrant on vérifie bien qu'il n'y ai pas plusieurs voies différentes mais non connues par l'IGN
    carac_rd_pt.loc[carac_rd_pt['nb_rte_rdpt']==1,'nb_rte_rdpt']=(carac_rd_pt.loc[carac_rd_pt['nb_rte_rdpt']==1].apply(
        lambda x : nb_routes_entree_rdpt(x['nb_obj_sig_entrant'], x['valeur_sens'], x['nom_rte_rdpt'], x['codevoie_rdpt'], x['nb_rte_rdpt'] ),axis=1))
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
    lgn_rd_pt=lignes_rd_pts(df)
    #trouver les lignes entrantes sur les rd points : celle qui intersectent le poly ext mais pas le poly int d'un rd pt et qui ne sont pas ds ligne rd pt
    ligne_entrant_rd_pt=lign_entrant_rd_pt(df,lgn_rd_pt)#caractériser les rd points
    carac_rd_pt=carac_rond_point(ligne_entrant_rd_pt)
    #ajouter l'identifiant et attibuts du rd point aux données BdTopo
    df_avec_rd_pt=df.merge(lgn_rd_pt[['id_ign', 'id_rdpt']], how='left', on='id_ign').merge(carac_rd_pt.reset_index(), how='left', on='id_rdpt')
    #grouper les données bdtopo par dr point por recup la liste des lignes et des noms de voies par rd_pt
    ligne_et_num_rdpt=df_avec_rd_pt.groupby('id_rdpt').agg({'numero': lambda x: tuple((set(x))),
                                                            'id_ign': lambda x: tuple((set(x)))}).reset_index()
    ligne_et_num_rdpt.columns=['id_rdpt','numero_rdpt','id_ign_rdpt']                                                        
    carac_rd_pt=carac_rd_pt.reset_index().merge(ligne_et_num_rdpt, on='id_rdpt' ).set_index('id_rdpt')
    
    return df_avec_rd_pt, carac_rd_pt

def liste_troncon_base(id_ligne,df_lignes,ligne_traite_troncon=[]):
    """
    recupérer les troncons qui se suivent sans point de jonction à  + de 2 lignes
    en entree : 
        id_ligne : string : id_ign de la ligne a etudier
        df_lignes : df des lignes avec rd points
        ligne_traite_troncon : liste des ligne traitees dans le cadre de ce troncon elementaire -> liste de str
    en sortie
        fonction génératrice
    """
    ligne=df_lignes.loc[id_ligne]#df pour la ligne
    ligne_traitee=list(set(ligne_traite_troncon+[ligne.name]))#on met le num de base de la ligne dans la liste 
    liste_ligne_suivantes=[]  
    for key, value in {'nb_intrsct_src':['source', 'src_geom'],'nb_intrsct_tgt':['target', 'tgt_geom']}.items() : 
            # cas simple de la ligne qui en touche qu'uen seule autre du cote source
            if ligne.loc[key] == 2 : 
                # recuperer le troncon qui ouche le point d'origine et qui n'est pas deja traite
                df_touches_source = df_lignes.loc[(~df_lignes.index.isin(ligne_traitee)) & 
                                                  ((df_lignes['source'] == ligne[value[0]]) | 
                                                   (df_lignes['target'] == ligne[value[0]]))]  
                if len(df_touches_source) > 0:  # car la seule voie touchee peut déjà etre dans les lignes traitees
                    id_ign_suivant = df_touches_source.index.tolist()[0]
                    ligne_traitee.append(id_ign_suivant) #liste des lignes deja traitees
                    liste_ligne_suivantes.append(id_ign_suivant)
                    yield id_ign_suivant
    for ligne_a_traiter in liste_ligne_suivantes :
        yield from liste_troncon_base(ligne_a_traiter, df_lignes, ligne_traitee)
        
def liste_complete_tronc_base(id_ligne,df_lignes,ligne_traite_troncon):
    """
    simplement ajouter l'id de laligne de depart au générateur de la fonction liste_troncon_base
    en entree : 
        id_ligne : string : id_ign de la ligne a etudier
        df_lignes : df des lignes avec rd points
        ligne_traite_troncon : list des trocnons deja traites. vide si debut
    en sortie : 
        list_troncon : liste des ligne traitees dans le cadre de ce troncon elementaire -> liste de str
    """
    return [id_ligne]+[a for a in liste_troncon_base(id_ligne,df_lignes,ligne_traite_troncon)]

def deb_fin_liste_tronc_base(df_lignes, list_troncon):
    """
    dico des lignes d debut et de fin des troncon de base
    le dico contient l'id_ign, si le noeud de fin est source ou target, le numero de noeud, le nom de la voie, le codevoie
    en entree : 
        df_lignes :  df des lignes avec rd points et id_ign en index
        list_troncon : list des troncons creant le troncon de base, issue de liste_complete_tronc_base
    en sortie : 
        dico_deb_fin : dico avec comme key l'id_ign, puis en item un noveau dico avec les key 'type', 'num_node', 'voie', 'codevoie'
    """
    lignes_troncon=df_lignes.loc[list_troncon]
    tronc_deb_fin=lignes_troncon.loc[(lignes_troncon['nb_intrsct_src']>2)|(lignes_troncon['nb_intrsct_tgt']>2)]
    if len(tronc_deb_fin)==0 : # cas des autoroutes : au bout elle ne croise personnes dc tronc_deb_fin=rien
        noeud_ss_suite=Counter(lignes_troncon.source.tolist()+lignes_troncon.target.tolist())
        noeuds_fin=[k for k,v in noeud_ss_suite.items() if v==1]
        tronc_deb_fin=lignes_troncon.loc[lignes_troncon['source'].isin(noeuds_fin) | lignes_troncon['target'].isin(noeuds_fin)]
    dico_deb_fin={}
    if len(tronc_deb_fin)>1 :
        for i, e in enumerate(tronc_deb_fin.itertuples()) :
            dico_deb_fin[i]={'id':e[0],'type':'source','num_node':e[54],'geom_node':e[61],'voie':e[4],'codevoie':e[58]} if e[60]>=3 else {'id':e[0],
                'type':'target','num_node':e[55],'geom_node':e[63],'voie':e[4],'codevoie':e[58]}
    else  : #pour tester les 2 cotés de la ligne
        dico_deb_fin[0]={'id':tronc_deb_fin.index.values[0],'type':'source','num_node':tronc_deb_fin['source'].values[0],'geom_node':tronc_deb_fin['src_geom'].values[0]
                         ,'voie':tronc_deb_fin['numero'].values[0],'codevoie':tronc_deb_fin['codevoie_d'].values[0]}
        dico_deb_fin[1]={'id':tronc_deb_fin.index.values[0],'type':'target','num_node':tronc_deb_fin['target'].values[0],'geom_node':tronc_deb_fin['tgt_geom'].values[0]
                         ,'voie':tronc_deb_fin['numero'].values[0],'codevoie':tronc_deb_fin['codevoie_d'].values[0]}
    return dico_deb_fin

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
    if not np.isnan(num_rdpt).any() :
        return True, num_rdpt
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
    nb_voie_rdpt=carac_rd_pt.loc[num_rdpt].nb_rte_rdpt
    ligne_rdpt=list(carac_rd_pt.loc[num_rdpt].id_ign_rdpt)
    #dans le cas ou 1 ou 2 routes arrivent sur le rd point on doit aussi prevoir de continuer à chercher des lignes qui continue, car le troncon reste le même, dc on recupère
    # les lignes sortantes 
    if  nb_voie_rdpt==1 :
        lignes_sortantes=[a for a in filter(lambda x : x not in list_troncon,carac_rd_pt.loc[num_rdpt].id_ign_entrant)]
        return ligne_rdpt, lignes_sortantes   
    #sinon, si le numero de la voie de la ligne qui touchent le rd pt est le même et different de NC, on retourne les lignes du rd pt
    # mais pas le suivantes car plus de 2 voies entrent sur le rd pont
    else :
        lignes_sortantes=[]
        if num_voie != 'NC' :
            if num_voie in carac_rd_pt.loc[num_rdpt].numero_rdpt : 
                return ligne_rdpt, lignes_sortantes
            else : return [], lignes_sortantes
        else : 
            if codevoie !='NR' : 
                if codevoie in carac_rd_pt.loc[num_rdpt].codevoie_rdpt : 
                    return ligne_rdpt, lignes_sortantes
                else : return [], lignes_sortantes
            else : 
                if len(set(carac_rd_pt.loc[num_rdpt].codevoie_rdpt))==1 and 'NR' in carac_rd_pt.loc[num_rdpt].codevoie_rdpt :
                    return ligne_rdpt, lignes_sortantes
                else : 
                    return [], lignes_sortantes
                

def recup_route_split(ligne_depart,voie,codevoie, lignes_adj,noeud,geom_noeud, type_noeud, df_lignes):
    """
    récupérer les id_ign des voies adjacentes qui sont de la mm voie que la ligne de depart
    en entree : 
        ligne_depart : string : id_ign de la ligne sui se separe
        voie : nom de la voie de la ligne de depart
        codevoie : codevoie_d de la ligne de depart
        lignes_adj : df des lignes qui touchent, issues de identifier_rd_pt avec id_ign en index
        noeud : integer : numero du noeud central
        geom_noeud_central : wkt du point central de separation
        type_noeud : type du noeud centrale par rapport à la ligne de depart : 'source' ou 'target'
        df_lignes : df de l'ensemble des lignes du transparent
    en sortie : 
        ligne_mm_voie : list d'id_ign ou liste vide
    """
    if len(lignes_adj)!=2 : 
        return []
    if len(set(lignes_adj.source.tolist()+lignes_adj.target.tolist()))==2 : #cas d'une ligne qui separe pour se reconnecter ensuite
       return lignes_adj.index.tolist()
    if voie!='NC' : 
        if (voie==lignes_adj.numero).all() : 
            ligne_mm_voie=lignes_adj.index.tolist()
            return ligne_mm_voie
    elif voie=='NC' and codevoie!='NR' :
        if (codevoie==lignes_adj.codevoie_d).all(): 
            ligne_mm_voie=lignes_adj.index.tolist()
            return ligne_mm_voie
        else : return []
    else : 
        lignes_adj_tot=lignes_adj.copy()
        lignes_adj_tot['tronc_supp']=lignes_adj_tot.apply(lambda x : liste_complete_tronc_base(x.name,df_lignes,[ligne_depart]), axis=1)
        geom_l1, lg_l1=fusion_ligne_calc_lg(df_lignes.loc[lignes_adj_tot.iloc[0].tronc_supp])
        geom_l2, lg_l2=fusion_ligne_calc_lg(df_lignes.loc[lignes_adj_tot.iloc[1].tronc_supp])
        tt_tronc=[x for y in lignes_adj_tot.tronc_supp.tolist() for x in y ]
        tt_noeud=Counter(df_lignes.loc[tt_tronc].source.tolist()+df_lignes.loc[tt_tronc].target.tolist())
        if (min(lg_l1, lg_l2) / max(lg_l1, lg_l2)) > 0.66 : 
            noeuds_fin=[k for k,v in tt_noeud.items() if v==1]
            lignes_jointure=df_lignes.loc[(df_lignes.source.isin(noeuds_fin)) & (df_lignes.target.isin(noeuds_fin))]
            if not lignes_jointure.empty : 
                angle_3=angle_3_lignes(ligne_depart, lignes_adj, noeud,geom_noeud, type_noeud, df_lignes)[4]
                if angle_3<60 :
                    return tt_tronc
                else :
                    return []
            else : 
                    return []
        else : return []    
    
def recup_triangle(ligne_depart,voie,codevoie, lignes_adj, noeud,geom_noeud,type_noeud, df_lignes):
    """
    récuperer un bout de voe coincé entre 2 lignes de carrefourd'une voie perpendiculaire
    en entree : 
        ligne_depart : string : id_ign de la ligne sui se separe
        voie : nom de la voie de la ligne de depart
        code_voie : string : codevoie_d de la BdTopo
        lignes_adj : df des lignes qui touchent, issues de identifier_rd_pt avec id_ign en index
        noeud : integer noeud de depart du triangle
        geom_noeud : geometrie du noeud central pour angle_3_lignes
        type_noeud : type du noeud centrale par rapport à la ligne de depart : 'source' ou 'target' (pour angle_3_lignes)
        df_lignes : df globale des lignes
    en sortie :
        id_rattache : list id_ign de bout a recuperer ou liste vide
    """
    if df_lignes.loc[ligne_depart].nature in ['Autoroute', 'Quasi-autoroute'] : 
        return []
    if len(lignes_adj)!=2 : 
        return []
                
    if voie != 'NC' : 
        lgn_cote_1=lignes_adj.loc[(lignes_adj['numero']!=voie)]  
        if lgn_cote_1.empty : 
            return []
        noeud_centre = lgn_cote_1['source'].values[0] if lgn_cote_1['source'].values[0] != noeud else lgn_cote_1['target'].values[0]
        lgn_suiv=lignes_adj.loc[~lignes_adj.index.isin(lgn_cote_1.index.tolist())]
        if lgn_suiv.empty : 
            return []
        noeud_suiv=lgn_suiv['source'].values[0] if lgn_suiv['source'].values[0] != noeud else lgn_suiv['target'].values[0]
        
       #dans le cas ou plusieurs ligne a la suite forme le cote du triangle que l'on veut récupérer on veut connaitre le nb de ligne qui touchent le noeud_suiv
        if lgn_suiv['source'].values[0] != noeud : 
            noeud_suiv=lgn_suiv['source'].values[0]
            nb_intersect_noeud_suiv=df_lignes.loc[lgn_suiv.index.values[0]].nb_intrsct_src
        else : 
            noeud_suiv=lgn_suiv['target'].values[0]
            nb_intersect_noeud_suiv=df_lignes.loc[lgn_suiv.index.values[0]].nb_intrsct_tgt
    
        if  nb_intersect_noeud_suiv==2 : 
            lgn_suiv=df_lignes.loc[liste_complete_tronc_base(lgn_suiv.index.values[0],df_lignes,[])] 
            noeud_ss_suite=Counter(lgn_suiv.source.tolist()+lgn_suiv.target.tolist())
            noeud_suiv=[k for k,v in noeud_ss_suite.items() if v==1 and v!=noeud][0]

        id_rattache=lgn_suiv.index.tolist()
        if noeud_centre==noeud_suiv : #ça veut dire une seule et mm lign qui fait 2 cote du triangle, dc on peu de suite garder la ligne suiv car 1 seule route non coupe l'intersec
            return id_rattache
        else : # on cherche si une ligne touche ces 2 noeuds, a le mme nom de voie que la ligne de cote  et un sens direct ou inverse
            ligne_a_rattacher=df_lignes.loc[((df_lignes['source']==noeud_centre) | (df_lignes['target']==noeud_centre)) & 
                      (df_lignes['numero']==lgn_cote_1['numero'].values[0]) & (~df_lignes.index.isin(lgn_cote_1.index.tolist())) & 
                      ((df_lignes['source']==noeud_suiv) | (df_lignes['target']==noeud_suiv))]
            if not ligne_a_rattacher.empty : 
                return id_rattache
            else :
                return []
    elif voie=='NC' and codevoie!='NR' :
        lgn_cote_1=lignes_adj.loc[(lignes_adj['codevoie_d']!=codevoie)]
        if lgn_cote_1.empty : 
            return []
        noeud_centre = lgn_cote_1['source'].values[0] if lgn_cote_1['source'].values[0] != noeud else lgn_cote_1['target'].values[0]
        lgn_suiv=lignes_adj.loc[~lignes_adj.index.isin(lgn_cote_1.index.tolist())]
        if lgn_suiv.empty : 
            return []
        noeud_suiv=lgn_suiv['source'].values[0] if lgn_suiv['source'].values[0] != noeud else lgn_suiv['target'].values[0]
        id_rattache=lgn_suiv.index.tolist()
        if noeud_centre==noeud_suiv : #ça veut dire une seule et mm lign qui fait 2 cote du triangle, dc on peu de suite garder la ligne suiv car 1 seule route non coupe l'intersec
            return id_rattache
        else : # on cherche si une ligne touche ces 2 noeuds, a le mme nom de voie que la ligne de cote  et un sens direct ou inverse
            ligne_a_rattacher=df_lignes.loc[((df_lignes['source']==noeud_centre) | (df_lignes['target']==noeud_centre)) & 
                      (df_lignes['codevoie_d']==lgn_cote_1['codevoie_d'].values[0]) & (~df_lignes.index.isin(lgn_cote_1.index.tolist())) & 
                      ((df_lignes['source']==noeud_suiv) | (df_lignes['target']==noeud_suiv))]
            if not ligne_a_rattacher.empty : 
                return id_rattache
            else :
                return []
    else : #normalement un seul cas : voie=='NC' et codevoie=='NR'
        lgn_cote_1, lgn_cote2,angle_1,angle_2=angle_3_lignes(ligne_depart, lignes_adj, noeud,geom_noeud, type_noeud, df_lignes)[:4]
        # la ligne la dont l'écart avec 180 est le plus eleve est la ligne qui part
        if 170<angle_1<190 : 
            lgn_suiv, lgn_cote=lgn_cote_1, lgn_cote2
        elif 170<angle_2<190 :
            lgn_suiv, lgn_cote=lgn_cote2,lgn_cote_1
        else : 
            return []   
        
        try :
            noeud_centre = lgn_cote['source'] if lgn_cote['source'] != noeud else lgn_cote['target']
        except IndexError :
            return []
        noeud_suiv=lgn_suiv['source'] if lgn_suiv['source'] != noeud else lgn_suiv['target']
        id_rattache=[lgn_suiv.name]
        if noeud_centre==noeud_suiv : #ça veut dire une seule et mm lign qui fait 2 cote du triangle, dc on peu de suite garder la ligne suiv car 1 seule route non coupe l'intersec
            return id_rattache
        else : # on cherche si une ligne touche ces 2 noeuds
            ligne_a_rattacher=df_lignes.loc[((df_lignes['source']==noeud_centre) | (df_lignes['target']==noeud_centre)) & 
                               (~df_lignes.index.isin(lgn_cote.index.tolist())) & ((df_lignes['source']==noeud_suiv) | (df_lignes['target']==noeud_suiv))]
            if not ligne_a_rattacher.empty : 
                return id_rattache
            else :
                return []
    
       
def trouver_chaussees_separee(list_troncon, df_avec_rd_pt):
    """
    Trouver la ligne parrallele de la voie représentée par 2 chaussées
    en entree : 
       list_troncon : list des troncon elementaire de l'idligne recherché, issu de  liste_complete_tronc_base
       df_avec_rd_pt  :df des lignes vace rd point, issu de identifier_rd_pt
    en sortie : 
        ligne_proche : string : id_ign de la ligne la plus proche
        ligne_filtres : df des lignes proches avec distance au repere
        longueur_base : longueur du troncon elementaire servant de base
    """    
    lgn_tron_e=df_avec_rd_pt.loc[df_avec_rd_pt['id_ign'].isin(list_troncon)]
    list_noeud=lgn_tron_e.source.tolist()+lgn_tron_e.target.tolist()
    lgn_agrege, longueur_base=fusion_ligne_calc_lg(lgn_tron_e)
    xmin,ymin,xmax,ymax=lgn_agrege.interpolate(0.5, normalized=True).buffer(50).bounds #limtes du carre englobant du buffer 50 m du centroid de la ligne
    #gdf_global=gp.GeoDataFrame(df, geometry='geom')#donnees de base
    lignes_possibles=df_avec_rd_pt.cx[xmin:xmax,ymin:ymax]#recherche des lignes proches du centroid
    
    # GESTION DES VOIES COMMUNALESS AVEC NUMERO = 'NC' et CODEVOIE DIFFRENT DE NR
    voie=max(set(lgn_tron_e.numero.tolist()), key=lgn_tron_e.numero.tolist().count)
    code_voie=max(set(lgn_tron_e.codevoie_d.tolist()), key=lgn_tron_e.codevoie_d.tolist().count)
    
    if voie !='NC' : 
        ligne_filtres=lignes_possibles.loc[(~lignes_possibles.id_ign.isin(list_troncon)) & (lignes_possibles.loc[:,'numero']==lgn_tron_e.iloc[0]['numero'])].copy()
    elif voie =='NC' and code_voie != 'NR' : 
        ligne_filtres=lignes_possibles.loc[(~lignes_possibles.id_ign.isin(list_troncon)) & (lignes_possibles.loc[:,'codevoie_d']==lgn_tron_e.iloc[0]['codevoie_d'])].copy()
    else : #cas tordu d'une2*2 de nom inconnu 
        ligne_filtres=lignes_possibles.loc[(~lignes_possibles.id_ign.isin(list_troncon)) & (~lignes_possibles.source.isin(list_noeud)) & 
                                           (~lignes_possibles.target.isin(list_noeud))].copy()
    
    #obtenir les distances au centroid
    ligne_filtres['distance']=ligne_filtres.geom.apply(lambda x : lgn_agrege.centroid.distance(x))
    if ligne_filtres.empty : 
        raise ParralleleError(list_troncon)
    #garder uniquement la valeur la plus proche du centroid
    ligne_proche=ligne_filtres.loc[ligne_filtres['distance']==ligne_filtres['distance'].min()].id_ign.tolist()[0]
    return ligne_proche, ligne_filtres, longueur_base

def chercher_chaussee_proche(ligne, ligne_proche, df_avec_rd_pt):
    """
    Pour trouver la partie de route proche du troncon le plus proche du trocnon d'une voie a 2*2 voie dont la rechreche de troncon parrallele a foiree
    en entree : 
        ligne : string id_ign de la ligne d'analyse de depart
        ligne_proche : id de a ligne proche de la voie de base
        df_avec_rd_pt  :df des lignes vace rd point, issu de identifier_rd_pt
    """
    df_lignes=df_avec_rd_pt.set_index('id_ign')
    voie=df_lignes.loc[ligne_proche].numero
    #tronc elem de la ligne proche
    te_ligne_proche=liste_complete_tronc_base(ligne_proche,df_lignes,[])
    #debut et fin
    te_dico_deb_fin=deb_fin_liste_tronc_base(df_lignes, te_ligne_proche)
    #trouver le troncon plus proche de la ligne de départ
    tronc_deb_fin=[v['id'] for v in te_dico_deb_fin.values()]
    if len(tronc_deb_fin)>1 : 
        tronc_proche=tronc_deb_fin[0] if (df_lignes.loc[tronc_deb_fin[0]]['geom'].distance(df_lignes.loc[ligne]['geom']) < 
                                          df_lignes.loc[tronc_deb_fin[1]]['geom'].distance(df_lignes.loc[ligne]['geom'])) else tronc_deb_fin[1]
    else : 
        tronc_proche=tronc_deb_fin[0]
    #trouver les troncon qui touchent et qui ont le mm nom
    noeuds_tch=[df_lignes.loc[tronc_proche]['source'],df_lignes.loc[tronc_proche]['target']]
    lignes_adj=df_lignes.loc[((df_lignes['source'].isin(noeuds_tch))|(df_lignes['target'].isin(noeuds_tch)))&(~df_lignes.index.isin(te_ligne_proche))&
                            (df_lignes['numero']==voie)]
    if lignes_adj.empty:
        raise ParralleleError(ligne)
    # tronc eleme de cette ligne
    tronc_final=liste_complete_tronc_base(lignes_adj.index[0],df_lignes,[])
    #regroupement et longeuer
    tronc_final_agrg, longueur_final_agreg=fusion_ligne_calc_lg(df_lignes.loc[tronc_final])
    return tronc_final, longueur_final_agreg
    

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
    
def gestion_voie_2_chaussee(list_troncon, df_avec_rd_pt, ligne): 
    """
    fonction de dteremination des tronc elem de voie à chaussees separees
    en entree : 
       list_troncon : list des troncon elementaire de l'idligne recherché, issu de  liste_complete_tronc_base
       df_avec_rd_pt  :df des lignes vace rd point, issu de identifier_rd_pt
       ligne : ligne d'analyse de depart
    en sortie : 
        list_troncon_comp : liste des id_ign complementaire si le lien est ok, sinon liste vide 
        ligne_proche : string : id_ign de la liste la plus proche
        ligne_filtres : df des lignes proches avec distance au repere
        longueur_base : float longueur du troncon elementaire servant de base
        long_comp : float longueur du troncon elementaire compare
    """  
    ligne_proche, ligne_filtres, longueur_base=trouver_chaussees_separee(list_troncon, df_avec_rd_pt)
    df_lignes=df_avec_rd_pt.set_index('id_ign')#mettre l'id_ign en index
    #recherche des troncon du mm tronc elem
    list_troncon_comp=liste_complete_tronc_base(ligne_proche,df_lignes,[])
    #cacul de la longueur
    long_comp=fusion_ligne_calc_lg(df_avec_rd_pt.loc[df_avec_rd_pt['id_ign'].isin(list_troncon_comp)])[1]
    #verif que les longueurs coincident
    if min(long_comp,longueur_base)*100/max(long_comp,longueur_base) >50 : #si c'est le cas il gfaut transférer list_troncon_comp dans la liste des troncon du tronc elem 
        return list_troncon_comp, ligne_proche, ligne_filtres, longueur_base, long_comp
    else : 
        lignes_suivante, long_suivante=chercher_chaussee_proche(ligne, ligne_proche, df_avec_rd_pt)
        if min(long_suivante,longueur_base)*100/max(long_suivante,longueur_base) >50 :
            return lignes_suivante,ligne_proche, [], longueur_base, long_suivante
        else :
            return [],ligne_proche, ligne_filtres, longueur_base, long_comp
    
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
    
class ParralleleError(Exception):  
    """
    Exception levee si la recherched'une parrallele ne donne rien
    """     
    def __init__(self, id_ign):
        Exception.__init__(self,f'pas de parrallele trouvee pour les troncons {id_ign}')
        
        
        
        


def recup_troncon_elementaire (id_ign_ligne,df, ligne_traite_troncon=[]):
    """
    Fonction generatrice
    Recuperer les lignes d'un troncon a partir d'une ligne source
    gere les cas simple (ligne qui se suivent), les voies à 2 lignes, les 3 lignes qui se touchent
    en entree : id d'une ligne -> str
                liste des ligne traitees dans le cadre de ce troncon elementaire -> liste de str
    en sortie : id de la ligne suivante -> str
    """
    
    #donnees de la ligne
    df_ligne = df.loc[id_ign_ligne]
    #print(df_ligne)
    nature=df_ligne.loc['nature']
    #print('ligne_traite_troncon : ',ligne_traite_troncon)
    ligne_traite_troncon.append(id_ign_ligne)
    #yield id_ign_ligne 
    liste_ligne_suivantes=[]   
    
    for key, value in {'nb_intrsct_src':['source', 'src_geom'],'nb_intrsct_tgt':['target', 'tgt_geom']}.items() : 
        #print(id_ign_ligne,key, value)
        # cas simple de la ligne qui en touche qu'uen seule autre du cote source
        if df_ligne.loc[key] == 2 : 
            #print (f' cas = 2 ; src : avant test isin : {datetime.now()}, ligne : {id_ign_ligne} ')
            #print(id_ign_ligne,key, value)
            df_touches_source = df.loc[(~df.index.isin(ligne_traite_troncon)) & ((df['source'] == df_ligne[value[0]]) | (df['target'] == df_ligne[value[0]]))]  # recuperer le troncon qui ouche le point d'origine et qui n'est pas deja traite
            #print (f' cas = 2 ; src : apres test isin : {datetime.now()}')
            if len(df_touches_source) > 0:  # car la seule voie touchee peut déjà etre dans les lignes traitees
                id_ign_suivant = df_touches_source.index.tolist()[0]
                liste_ligne_suivantes.append(id_ign_suivant)
                #print (f'fin traitement cas = 2 ; src : apres test isin : {datetime.now()}')
                ligne_traite_troncon.append(id_ign_suivant) #liste des lignes deja traitees
                #yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon) #on boucle pour chercher la suivante
                yield id_ign_suivant 
        elif df_ligne.loc[key] >= 3 :  # cas plus complexe d'une ligne a un carrefour. soit c'est la meme voie qui se divise, soit ce sont d'autre voie qui touche
            #print (f' cas = 3 ; src : avant test isin : {datetime.now()}')
            df_touches_source = df.loc[(~df.index.isin(ligne_traite_troncon)) & ((df['source'] == df_ligne[value[0]]) | (df['target'] == df_ligne[value[0]]))]  # recuperer le troncon qui ouche le point d'origine
            liste_ligne_touchees=df_touches_source.index.tolist()
            #print (f' cas = 3 ; src : apres test isin : {datetime.now()}')
            
            #gestion des bretelles et des rond points  : si la ligne qui est traitée est dessus et que plusieurs voies en partent : on ne traite pas la ligne, elle sera traitee avec les voies qui arrivent sur le rd point
            if nature == 'Bretelle' or df_touches_source['nature'].all()=='Bretelle' : #comme ça les bretelles sont séparrées des sections courantes, si on dispose de données de ccomptage dessus (type dira)
                continue
            elif nature=='Rd_pt' and df_ligne['nb_rte_rdpt']>1 : 
                ligne_traite_troncon,liste_ligne_suivantes=[],[]
                break
            
            if len(liste_ligne_touchees) > 0:  # si les voies touchees n'on pas ete traiees
                if ((df_ligne.loc['numero'] == df_touches_source['numero']).all() == 1 and (df_ligne.loc['numero']!='NC')): # pour les voies hors voies communales si elles ont le mm nom on prend toutes les lignes, pour les voies communales dont les nom_voie_g sont equivalent c'est pareil
                    #gestion des rd points
                    if df_touches_source['nature'].any()=='Rd_pt' : #si une des lignes touchées est un rd point on prend toutes les autres du m^me rd point
                        id_rdpt=df_touches_source.iloc[0]['id_rdpt'] #recuperer l'id du rd pt
                        nb_rte_rdpt=df_touches_source.iloc[0]['nb_rte_rdpt'] #recuperer le nb de route qui croise le rd point
                        liste_ligne_touchees+=df.loc[(df['id_rdpt']==id_rdpt) & (~df.index.isin(liste_ligne_touchees))].index.tolist() #recuperer les lignes de cet id_drpt non deja recuperee
                        #print(f'ligne{id_ign_ligne} rd point {liste_ligne_touchees}, nb rroute rd pt {nb_rte_rdpt}')
                        if nb_rte_rdpt > 1 : #si le rd point concentre plusieurs routes différentes, on stocke kes voies du rond point mais on ne traite pas les autres voies du mm nom qui en sortent
                            for id_ign_suivant in liste_ligne_touchees :
                                ligne_traite_troncon.append(id_ign_suivant)
                                yield id_ign_suivant
                        else : # a l'inverse, si le rond point ne traite qu'une seule route, on associe le rd pt  + les voies qui en sortent au mm troncon
                            for id_ign_suivant in liste_ligne_touchees :
                                liste_ligne_suivantes.append(id_ign_suivant)
                                ligne_traite_troncon.append(id_ign_suivant)
                                yield id_ign_suivant
                    else :
                        for id_ign_suivant in liste_ligne_touchees:
                            #print (f'fin traitement cas = 3 mm numero ; src : apres test isin : {datetime.now()}') 
                            liste_ligne_suivantes.append(id_ign_suivant)
                            ligne_traite_troncon.append(id_ign_suivant)
                            #yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon) 
                            yield id_ign_suivant  
                elif ((df_ligne.loc['codevoie_d'] == df_touches_source['codevoie_d']).all() == 1 and (df_ligne.loc['numero']=='NC')
                      and df_touches_source['numero'].all()=='NC') : #si les voies qui se croisent sont les memes
                    if df_touches_source['nature'].any()=='Rd_pt' : #si une des lignes touchées est un rd point on prend toutes les autres du m^me rd point
                            id_rdpt=df_touches_source.iloc[0]['id_rdpt'] #recuperer l'id du rd pt
                            liste_ligne_touchees+=df.loc[(df['id_rdpt']==id_rdpt) & (~df.index.isin(liste_ligne_touchees))].index.tolist() #recuperer les lignes de cet id_drpt non deja recuperee
                            #print(f'ligne{id_ign_ligne} rd point {liste_ligne_touchees}, nb rroute rd pt {nb_rte_rdpt}')
                            for id_ign_suivant in liste_ligne_touchees :
                                liste_ligne_suivantes.append(id_ign_suivant)
                                ligne_traite_troncon.append(id_ign_suivant)
                                yield id_ign_suivant
                    else :
                        for id_ign_suivant in liste_ligne_touchees:
                            #print (f'fin traitement cas = 3 mm numero ; src : apres test isin : {datetime.now()}') 
                            liste_ligne_suivantes.append(id_ign_suivant)
                            ligne_traite_troncon.append(id_ign_suivant)
                            #yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon) 
                            yield id_ign_suivant  
                elif (df_ligne.loc['numero']=='NC' and len(set(df_touches_source['codevoie_d'].values.tolist()))==2
                       and 'NR' in df_touches_source['codevoie_d'].values.tolist() and (df_touches_source['numero']=='NC').all() and
                       df_ligne.loc['codevoie_d'] in df_touches_source['codevoie_d'].values.tolist() ) :   #si les voies croisés ont un nom pour ue d'entre elle et l'autre non  
                    df_touches_source = df_touches_source.loc[df_touches_source['codevoie_d']==df_ligne['codevoie_d']] #on limite le df touche sources aux voies qui ont le même nom
                    liste_ligne_touchees=df_touches_source.index.tolist()
                    if df_touches_source['nature'].any()=='Rd_pt' : #si une des lignes touchées est un rd point on prend toutes les autres du m^me rd point
                            id_rdpt=df_touches_source.iloc[0]['id_rdpt'] #recuperer l'id du rd pt
                            liste_ligne_touchees+=df.loc[(df['id_rdpt']==id_rdpt) & (~df.index.isin(liste_ligne_touchees))].index.tolist() #recuperer les lignes de cet id_drpt non deja recuperee
                            #print(f'ligne{id_ign_ligne} rd point {liste_ligne_touchees}, nb rroute rd pt {nb_rte_rdpt}')
                            for id_ign_suivant in liste_ligne_touchees :
                                #liste_ligne_suivantes.append(id_ign_suivant)
                                ligne_traite_troncon.append(id_ign_suivant)
                                yield id_ign_suivant
                    else :
                        for id_ign_suivant in liste_ligne_touchees:
                            #print (f'fin traitement cas = 3 mm numero ; src : apres test isin : {datetime.now()}') 
                            liste_ligne_suivantes.append(id_ign_suivant)
                            ligne_traite_troncon.append(id_ign_suivant)
                            #yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon) 
                            yield id_ign_suivant
                elif (df_ligne.loc['numero']=='NC' and (df_touches_source['nature']=='Rd_pt').all()
                      and df_touches_source['assigne_rdpt'].all()==False) : #si on touche un rond point dont on ne peut pas affecter le nom, on va lui affecter un id arbitraire, mais pas au lignes suivantes
                    id_rdpt=df_touches_source.iloc[0]['id_rdpt']
                    liste_ligne_touchees+=df.loc[(df['id_rdpt']==id_rdpt) & (~df.index.isin(liste_ligne_touchees))].index.tolist()
                    for id_ign_suivant in liste_ligne_touchees :
                                #liste_ligne_suivantes.append(id_ign_suivant)
                                ligne_traite_troncon.append(id_ign_suivant)
                                yield id_ign_suivant
                    
                else: #si toute les voies n'ont pas le même nom
                    if nature in ['Autoroute', 'Quasi-autoroute'] :
                        df_ligne_autre=df_touches_source.loc[df_touches_source['numero']!=df_ligne['numero']]
                        if len(df_touches_source)-len(df_ligne_autre)>0 : #sinon ce veut dire que le troncon suivant est deja traite
                            df_ligne_pt_avant_src=df_ligne['geom'][0].coords[1] #point avant le point  target
                            coord_source_arrondi=[round(i,1) for i in list(loads(df_ligne[value[1]]).coords)[0]] #coordonnees du point target
                            #trouver le point de df_ligne autre juste aprs le point source
                            coord_ligne_arrondi=[[round(coord[0],1),round(coord[1],1)] for coord in list((df_ligne_autre.geometry.iloc[0])[0].coords)] #(df_ligne_autre.geometry.iloc[0]) la geometryde la 1ere ligne
                            pt_suivant_src_ligne_autre=coord_ligne_arrondi[-2] if coord_ligne_arrondi[-1]==coord_source_arrondi else coord_ligne_arrondi[1]
                            #recuperer l'angle
                            angle=Outils.angle_entre_2_ligne(coord_source_arrondi, pt_suivant_src_ligne_autre, df_ligne_pt_avant_src)
                            #si l'angle estdans les 90°, on ignor et on continue sur la l'autre ligne
                            if 55 < angle < 135 :
                                id_ign_suivant=df_touches_source.loc[~df.id.isin(df_ligne_autre.id)].index.values.tolist()[0]
                                liste_ligne_suivantes.append(id_ign_suivant)  
                                #print (f'fin traitement cas = 3 ; src : apres test isin : {datetime.now()}')
                                ligne_traite_troncon.append(id_ign_suivant)
                                yield id_ign_suivant
                                #yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon)  
                
    #print(f'ligne : {id_ign_ligne}, liste a traiter : {liste_ligne_suivantes}' ) 
    for ligne_a_traiter in liste_ligne_suivantes :
        yield from recup_troncon_elementaire(ligne_a_traiter, df, ligne_traite_troncon)
    #yield ligne_traite_troncon

def recup_troncon_parallele(id_ign_ligne,ligne_traitee_global,df,df2_chaussees):  
    """
    Obtenir la ligne parrallele pour les voies decrites par 2 lignes
    en entree : id de le ligne -> str 
    en sortie : id de la parrallele -> str
    """ 

    #les variables liées à la ligne
    df_ligne = df.loc[id_ign_ligne]
    geom_ligne=df_ligne['geom'][0]
    
    buffer_parralleles=geom_ligne.parallel_offset(df_ligne['largeur']+3, 'left').buffer(5).union(geom_ligne.parallel_offset(df_ligne['largeur']+3, 'right').buffer(5)) #on fat un buffer des deux cote
    buff_xmin, buff_ymin, buff_xmax, buff_ymax=buffer_parralleles.bounds # les coordonnees x et y in et max du rectangle englobant
    lignes_possibles=df2_chaussees.cx[buff_xmin:buff_xmax, buff_ymin:buff_ymax] # le filtre des donnéess selon le rectangle englobant
    #print(lignes_possibles)
    
    #on cherche d'abord a voir si une ligne est dans le buffer, non traitees et avec le mm nom de voie, si c'est le cas on la retourne
    ligne_dans_buffer=lignes_possibles.loc[lignes_possibles.loc[:, 'geom'].within(buffer_parralleles)]
    ligne_dans_buffer=ligne_dans_buffer.loc[(~ligne_dans_buffer.index.isin(ligne_traitee_global)) & (ligne_dans_buffer.loc[:,'numero']==df_ligne['numero'])]
    if len(ligne_dans_buffer)>0 : #si une ligne repond aux critere
        return ligne_dans_buffer.index.tolist()[0]
    else : #sinon on prend la premiere des lignes qui intersctent (non traitees avec le mm nom d evoie
        lignes_intersect_buffer=lignes_possibles.loc[lignes_possibles.loc[:, 'geom'].intersects(buffer_parralleles)]
        lignes_intersect_buffer=lignes_intersect_buffer.loc[(~lignes_intersect_buffer.index.isin(ligne_traitee_global)) & (lignes_intersect_buffer.loc[:,'numero']==df_ligne['numero'])]
        if len(lignes_intersect_buffer)>0 : #si une ligne repond aux critere
            return lignes_intersect_buffer.index.tolist()[0]
        else : 
            pass

def recup_troncon_parallele_v2(df,liste_troncon):
    
    #on prend la liste des troncon, on en déduit le df
    #on agrege les lignes
    #on recupere le centre de la ligne
    gdf_lignes=gp.GeoDataFrame(df.loc[liste_troncon], geometry='geom') #conversion en geodf
    try : #union des geometries, si possible en linestring sinon en multi
        gdf_lignes2=linemerge(gdf_lignes.unary_union) #union des geometries
    except ValueError :
        gdf_lignes2=gdf_lignes.unary_union
    xmin,ymin,xmax,ymax=gdf_lignes2.interpolate(0.5, normalized=True).buffer(50).bounds #centroid de la ligne
    gdf_global=gp.GeoDataFrame(df, geometry='geom')#donnees de base
    lignes_possibles=gdf_global.cx[xmin:xmax,ymin:ymax]#recherche des lignes proches du centroid
    #uniquement les lignes non présentes dans la liste de troncons avec le même nom de voie
    ligne_filtres=lignes_possibles.loc[(~lignes_possibles.index.isin(liste_troncon)) & (lignes_possibles.loc[:,'numero']==gdf_lignes.iloc[0]['numero'])]
    #obtenir les distances au centroid
    ligne_filtres['distance']=ligne_filtres.geom.apply(lambda x : gdf_lignes2.centroid.distance(x))
    #garder uniquement la valeur la plus proche du centroid
    ligne_proche=ligne_filtres.loc[ligne_filtres['distance']==ligne_filtres['distance'].min()].index.tolist()[0]
    #print(f'ligne parrallele proche : {ligne_proche}')
    return ligne_proche

def affecter_troncon(df,df2_chaussees):
    """
    Grouper les troncon par numero arbitraire
    baser sur recup_troncon_elementaire et recup_troncon_parallele
    en entree : liste d'id de ligne -> lite de str
    """
    #appel du dico de resultat
    dico_tronc_elem={}
    #global ligne_traitee_global
    #ligne_traitee_global = np.empty(1, dtype='<U24')
    ligne_traitee_global=set([]) #pour avoir une liste de valuer unique
    
    #liste des des lignes
    liste_ligne=df.index.tolist()
    
    #pour chaque ligne on va creer un id dans le dico, avec les tronon associes
    for indice, ligne in enumerate(liste_ligne) :
        #if indice >285 : break
        if len(ligne_traitee_global)==len(liste_ligne) : break
        if indice % 5000 == 0 :
            print (f"{indice}eme occurence : {ligne} à {datetime.now().strftime('%H:%M:%S')} nb ligne traite : {len(ligne_traitee_global)}, nb ligne differente={len(set(ligne_traitee_global))}")
        #print (f"{indice}eme occurence : {ligne} à {datetime.now().strftime('%H:%M:%S')} nb ligne traite : {len(ligne_traitee_global)}")
        if ligne in ligne_traitee_global :
            continue 
        else:
            """if indice>=10 : 
                break
            #recuperation ds troncons connexes en cas simple"""
            liste_troncon=list(recup_troncon_elementaire(ligne,df,[]))
            liste_troncon.append(ligne)
            ligne_traitee_global.update(liste_troncon)
            for troncon in liste_troncon:
                #ligne_traitee_global=np.append(ligne_traitee_global,liste_troncon)
                #print('lignes : ', liste_troncon,ligne_traitee_global )
                #dico_tronc_elem[indice[0]]=liste_troncon
                dico_tronc_elem[troncon]=indice
            #print(f'ligne : {ligne} , liste : {liste_troncon} ')

            #recuperation des toncons connexes si 2 lignes pour une voie
            if ligne in df2_chaussees.index  :
                #print(f'ligne : {ligne} , liste : {liste_troncon.append(ligne)} ')
                try : 
                    ligne_parrallele=recup_troncon_parallele_v2(df,liste_troncon)
                    if ligne_parrallele==None: #cas où pas de ligne parrallele trouvee
                        continue
                    dico_tronc_elem[ligne_parrallele]=indice
                    liste_troncon_para=list(recup_troncon_elementaire(ligne_parrallele,df,[]))
                    liste_troncon_para.append(ligne_parrallele)
                    ligne_traitee_global.update(liste_troncon_para)
                    for troncon_para in liste_troncon_para :
                        #print('lignes : ', liste_troncon)
                        dico_tronc_elem[troncon_para]=indice
                except IndexError :
                    print ('indexError ligne ',ligne)
                    pass
                    #print(f"erreur index a ligne ligne : {ligne}")
                #print('parrallele ',ligne_parrallele)
                
            
    return dico_tronc_elem

def affecter_troncon_ligne(ligne,df,df2_chaussees):
    """
    Grouper les troncon par numero arbitraire
    baser sur recup_troncon_elementaire et recup_troncon_parallele
    en entree : liste d'id de ligne -> lite de str
    """
    #appel du dico de resultat
    dico_tronc_elem={}
    #global ligne_traitee_global
    #ligne_traitee_global = np.empty(1, dtype='<U24')
    ligne_traitee_global=set([])
    
    #liste des des lignes
    liste_ligne=[ligne]
    #liste_ligne=np.array(df.index.tolist())
    #pour chaque ligne on va creer un id dans le dico, avec les tronon associes
    for indice, ligne in enumerate(liste_ligne) :
        if indice % 1000 == 0 :
            print (f"{indice}eme occurence : {ligne} à {datetime.now().strftime('%H:%M:%S')} nb ligne traite : {len(ligne_traitee_global)}, nb ligne differente={len(set(ligne_traitee_global))}")
        #print (f"{indice}eme occurence : {ligne} à {datetime.now().strftime('%H:%M:%S')} nb ligne traite : {len(ligne_traitee_global)}")
        if ligne in ligne_traitee_global :
            continue 
        else:
            """if indice>=10 : 
                break
            #recuperation ds troncons connexes en cas simple"""
            liste_troncon=list(recup_troncon_elementaire(ligne,df,[]))
            liste_troncon.append(ligne)
            ligne_traitee_global.update(liste_troncon)
            for troncon in liste_troncon:
                #ligne_traitee_global=np.append(ligne_traitee_global,liste_troncon)
                #print('lignes : ', liste_troncon,ligne_traitee_global )
                #dico_tronc_elem[indice[0]]=liste_troncon
                dico_tronc_elem[troncon]=indice
            #print(f'ligne : {ligne} , liste : {liste_troncon} ')

            #recuperation des toncons connexes si 2 lignes pour une voie
            if ligne in df2_chaussees.index  :
                print(f'ligne : {ligne} 2 chaussees ')
                try : 
                    ligne_parrallele=recup_troncon_parallele_v2(df,liste_troncon)
                    if ligne_parrallele==None: #cas où pas de ligne parrallele trouvee
                        continue
                    dico_tronc_elem[ligne_parrallele]=indice
                    liste_troncon_para=list(recup_troncon_elementaire(ligne_parrallele,df,[]))
                    ligne_traitee_global.update(liste_troncon_para)
                    for troncon_para in liste_troncon_para :
                        #print('lignes : ', liste_troncon)
                        dico_tronc_elem[troncon_para]=indice
                except IndexError :
                    pass
                    print(f"erreur index a ligne ligne : {ligne}")
                #print('parrallele ',ligne_parrallele)
    
            
    return dico_tronc_elem
                
def inserer_dico(conn, dico):
    
    conn.curs.executemany("""INSERT INTO referentiel.tronc_elem_bdt17_ed15_l (id_ign,id_tronc_elem) VALUES (%s, %s)""", dico.items())
    conn.connexionPsy.commit()
"""
if __name__ == '__main__' : 
    #affecter_troncon(['TRONROUT0000000202559719'])
    print ('debut : ',datetime.now().strftime('%H:%M:%S'))
    df_rd_pt=identifier_rd_pt(df)
    dico=affecter_troncon(df_rd_pt)
    with ct.ConnexionBdd('local_otv') as c :
        inserer_dico(c, dico)
    print ('fin : ',datetime.now().strftime('%H:%M:%S'), dico)
"""
    
    
    
    
    

