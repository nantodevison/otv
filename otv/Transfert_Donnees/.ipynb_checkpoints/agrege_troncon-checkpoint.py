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
import Connexion_Transfert as ct
from shapely.wkt import loads
from shapely.ops import polygonize, linemerge
import Outils
#from psycopg2 import extras


# ouvrir connexion, recuperer donnees
with ct.ConnexionBdd('local_otv') as c : 
    df = gp.read_postgis("select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom from public.traf2015_bdt17_ed15_l t left join public.traf2015_bdt17_ed15_l_vertices_pgr v1 on t.\"source\"=v1.id left join public.traf2015_bdt17_ed15_l_vertices_pgr v2  on t.target=v2.id ", c.connexionPsy)

#variables generales
nature_2_chaussees=['Route à 2 chaussées','Quasi-autoroute','Autoroute']
#donnees generale du jdd
df.set_index('id_ign', inplace=True)  # passer l'id_ign commme index ; necessaire sinon pb avec geometrie vu comme texte
df.crs={'init':'epsg:2154'}
df2_chaussees=df.loc[df['nature'].isin(nature_2_chaussees)] #isoler les troncon de voies decrits par 2 lignes

def import_donnes_base(ref_connexion):
    """
    OUvrir une connexion vers le servuer de reference et recuperer les données
    en entree : 
       ref_connexion : string de reference de la connexion, selon le midule Outils , fichier Id_connexions, et module Connexion_transferts
    en sortie : 
        df : dataframe telle que telchargées depuis la bdd
    """
    with ct.ConnexionBdd('local_otv') as c : 
        requete="""with jointure as (
            select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom 
             from public.traf2015_bdt17_ed15_l t 
            left join public.traf2015_bdt17_ed15_l_vertices_pgr v1 on t.source=v1.id 
            left join public.traf2015_bdt17_ed15_l_vertices_pgr v2  on t.target=v2.id
            )
            select j.* from jointure j, zone_test_agreg z
            where st_intersects(z.geom, j.geom)"""
        df = gp.read_postgis(requete, c.connexionPsy)
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
    dico_rd_pt=[[i, ((t.length**2)/t.area),t.buffer(taille_buffer)] for i,t in enumerate(polygonize(df.geometry)) if 12<=((t.length**2)/t.area)<=14] # car un cercle à un rappor de ce type entre 12 et 13
    gdf_rd_point=gp.GeoDataFrame([(a[0],a[1]) for a in dico_rd_pt], geometry=[a[2] for a in dico_rd_pt], columns=['id_rdpt', 'rapport_aire'])
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
        df_lignes :  df des lignes avec rd points
        list_troncon : list des troncons creant le troncon de base, issue de liste_complete_tronc_base
    en sortie : 
        dico_deb_fin : dico avec comme key l'id_ign, puis en item un noveau dico avec les key 'type', 'num_node', 'voie', 'codevoie'
    """
    lignes_troncon=df_lignes.loc[list_troncon]
    tronc_deb_fin=lignes_troncon.loc[(lignes_troncon['nb_intrsct_src']>2)|(lignes_troncon['nb_intrsct_tgt']>2)]
    dico_deb_fin={}
    for e in tronc_deb_fin.itertuples() :
        dico_deb_fin[e[0]]={'type':'source','num_node':e[54],'voie':e[4],'codevoie':e[58]} if e[61]==3 else {
            'type':'target','num_node':e[55],'voie':e[4],'codevoie':e[58]}
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
    if not np.isnan(num_rdpt) :
        return True, num_rdpt
    else : 
        return False, np.NaN

def recup_lignes_rdpt(carac_rd_pt,num_rdpt,list_troncon,num_voie ):
    """
    obtenir les lignes qui composent un rd pt et les lignes suivantes s'il n'est pas une fin de troncon
    en entree : 
        carac_rd_pt : df des caractereistiques des rdpt, issu de identifier_rd_pt
        num_rdpt : float, issu de verif_touche_rdpt
        list_troncon : list des lignes du troncon arrivant sur le rd pt, issu de liste_complete_tronc_base
        num_voie : string : numero de la voie entrante (ex : D113)
    en sortie : 
        ligne_rdpt : liste des id_ign qui composent le rd point, ou liste vide si le rd pt n'est pas a ssocier a cette voie
        lignes_sortantes : liste des id_ign qui continue apres le rd pt si le troncon elementaire n'est pas delimite par celui-ci, liste vide sinon
    """
    num_rdpt=int(num_rdpt[0])
    nb_voie_rdpt=carac_rd_pt.loc[num_rdpt].nb_rte_rdpt
    ligne_rdpt=list(carac_rd_pt.loc[num_rdpt].id_ign_rdpt)
    #dans le cas ou 1 ou 2 routes arrivent sur le rd point on doit aussi prevoir de continuer à chercher des lignes qui continue, car le troncon reste le même, dc on recupère
    # les lignes sortantes 
    if  nb_voie_rdpt<=2 :
        lignes_sortantes=[a for a in filter(lambda x : x not in list_troncon,carac_rd_pt.loc[num_rdpt].id_ign_entrant)]
        return ligne_rdpt, lignes_sortantes   
    #sinon, si le numero de la voie de la ligne qui touchent le rd pt est le même et different de NC, on retourne les lignes du rd pt
    # mais pas le suivantes car plus de 2 voies entrent sur le rd pont
    else :
        lignes_sortantes=[]
        if num_voie in carac_rd_pt.loc[num_rdpt].numero_rdpt : 
            return ligne_rdpt, lignes_sortantes
        else : 
            
            return [], lignes_sortantes

def recup_route_split(voie, lignes_adj):
    """
    récupérer les id_ign des voies adjacentes qui sont de la mm voie que la ligne de depart
    en entree : 
        voie : nom de la voie de la ligne de depart
        lignes_adj : df des lignes qui touchent, issues de identifier_rd_pt avec id_ign en index
    """
    if voie==lignes_adj.numero.all() : 
        ligne_mm_voie=lignes_adj.index.tolist()
        return ligne_mm_voie
    else : 
        return []
        
    



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

def recup_troncon_parallele(id_ign_ligne,ligne_traitee_global):  
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

if __name__ == '__main__' : 
    #affecter_troncon(['TRONROUT0000000202559719'])
    print ('debut : ',datetime.now().strftime('%H:%M:%S'))
    df_rd_pt=identifier_rd_pt(df)
    dico=affecter_troncon(df_rd_pt)
    with ct.ConnexionBdd('local_otv') as c :
        inserer_dico(c, dico)
    print ('fin : ',datetime.now().strftime('%H:%M:%S'), dico)
