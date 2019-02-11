# -*- coding: utf-8 -*-
'''
Created on 29 janv. 2019
@author: martin.schoreisz

Module de creation de troncon homogene

'''
import matplotlib
import geopandas as gp
import numpy as np
from Martin_Perso import Connexion_Transfert as ct

# ouvrir connexion, recuperer donnees
with ct.ConnexionBdd('local_otv') as c : 
    df = gp.read_postgis("select t.*, v1.cnt nb_intrsct_src, v2.cnt as nb_intrsct_tgt from referentiel.troncon_route_bdt17_ed15_l t left join referentiel.troncon_route_bdt17_ed15_l_vertices_pgr v1 on t.\"source\"=v1.id left join referentiel.troncon_route_bdt17_ed15_l_vertices_pgr v2  on t.target=v2.id", c.connexionPsy)

#donnees generale du jdd
df.set_index('id_ign', inplace=True)  # passer l'id_ign commme index ; necessaire sinon pb avec geometrie vu comme texte
df2_chaussees=df.loc[df.loc[:,'nature'].isin(['Route à 2 chaussées','Quasi-autoroute','Autoroute'])] #isoler les troncon de voies decrits par 2 lignes

# stockge des données lignes traitees dans une numpy array (uen focntion géératrice issue de yield marcherait bien aussi
ligne_traitee_global = np.empty(1, dtype='<U24')

#resultat
dico_tronc_elem={}

"""#pour visu test
schema_polygon = {'geometry': 'MultiPolygon','properties': {'id': 'str'}}
fichier_visu_polygon=f.open(r'D:\temp\otv\test_auto\test_polygon.shp', 'w', 'ESRI Shapefile', schema_polygon)
schema_ligne = {'geometry': 'LineString','properties': {'id': 'str'}}
fichier_visu_ligne=f.open(r'D:\temp\otv\test_auto\test_lignes.shp', 'w', 'ESRI Shapefile', schema_ligne)"""


def recup_troncon_elementaire (id_ign_ligne):
    """
    Fonction generatrice
    Recuperer les lignes d'un troncon a partir d'une ligne source
    gere les cas simple (ligne qui se suivent), les voies à 2 lignes, les 3 lignes qui se touchent
    en entree : id d'une ligne -> str
    en sortie : id de la ligne suivante -> str
    """
    #preparation des donnees
    global ligne_traitee_global, df2_chaussees #recuperer la liste des troncons traites et celle des voies representees par 2 lignes
    ligne_traitee_global = np.insert(ligne_traitee_global, 1, id_ign_ligne)
    
    #donnees de la ligne
    df_ligne = df.loc[id_ign_ligne]
    
    # cas simple de la ligne qui en touche qu'uen seule autre du cote source
    if df_ligne.loc['nb_intrsct_src'] == 2 : 
        df_touches_source = df.loc[(~df.index.isin(ligne_traitee_global)) & ((df.loc[:, 'source'] == df_ligne.loc['source']) | (df.loc[:, 'target'] == df_ligne.loc['source']))]  # recuperer le troncon qui ouche le point d'origine et qui n'est pas deja traite
        if len(df_touches_source) > 0:  # car la seule voie touchee peut déjà etre dans les lignes traitees
            id_ign_suivant = df_touches_source.index.tolist()[0]
            yield from recup_troncon_elementaire(id_ign_suivant) #on boucle pour chercher la suivante 
    elif df_ligne.loc['nb_intrsct_src'] == 3 :  # cas plus complexe d'une ligne a un carrefour. soit c'est la meme voie qui se divise, soit ce sont d'autre voie qui touche
        df_touches_source = df.loc[(~df.index.isin(ligne_traitee_global)) & ((df.loc[:, 'source'] == df_ligne.loc['source']) | (df.loc[:, 'target'] == df_ligne.loc['source']))]  # recuperer le troncon qui ouche le point d'origine
        if len(df_touches_source) > 0:  # si les voies touchees n'on pas ete traiees
            if ((df_ligne.loc['numero'] == df_touches_source['numero']).all() == 1 and 
                 ((df_ligne.loc['nature'] == 'Route à 1 chaussée' and ('Route à 2 chaussées' == df_touches_source['nature']).all())
                   or(df_ligne.loc['nature'] == 'Route à 2 chaussée' and 
                      df_touches_source['nature'].isin(['Route à 1 chaussée', 'Route à 2 chaussées'])))):  # !! on ne compare que à numero, dc pb en urbain si les 2 lignes qui touchent ont le mm numero, et que la ligne de voie etait decrite par 1 ligne puis par 2
                for id_ign_suivant in df_touches_source.index.tolist():
                    yield from recup_troncon_elementaire(id_ign_suivant)  
   
    #cas simple de la ligne qui en touche qu'uen seule autre du cote target
    if df_ligne.loc['nb_intrsct_tgt'] == 2 :   
        df_touches_target = df.loc[(~df.index.isin(ligne_traitee_global)) & ((df.loc[:, 'source'] == df_ligne.loc['target']) | (df.loc[:, 'target'] == df_ligne.loc['target']))]  # recuperer le troncon qui ouche le point d'origine  et qui n'est pas deja traite
        if len(df_touches_target) > 0:  # car la seule voie touchee peut déjà etre dans les lignes traitees
            id_ign_suivant = df_touches_target.index.tolist()[0]
            yield from recup_troncon_elementaire(id_ign_suivant)        
    elif df_ligne.loc['nb_intrsct_tgt'] == 3 :  # cas plus complexe d'une ligne a un carrefour. soit c'est la meme voie qui se divise, soit ce sont d'autre voie qui touche
        df_touches_target = df.loc[(~df.index.isin(ligne_traitee_global)) & ((df.loc[:, 'source'] == df_ligne.loc['target']) | (df.loc[:, 'target'] == df_ligne.loc['target']))]  # recuperer le troncon qui ouche le point d'origine
        if len(df_touches_target) > 0:  # si les voies touchees n'on pas ete traiees
            if ((df_ligne.loc['numero'] == df_touches_target['numero']).all() == 1 and 
                 ((df_ligne.loc['nature'] == 'Route à 1 chaussée' and ('Route à 2 chaussées' == df_touches_target['nature']).all())
                   or(df_ligne.loc['nature'] == 'Route à 2 chaussée' and 
                      df_touches_target['nature'].isin(['Route à 1 chaussée', 'Route à 2 chaussées'])))):  # !! on ne compare que à numero, dc pb en urbain si les 2 lignes qui touchent ont le mm numero, et que la ligne de voie etait decrite par 1 ligne puis par 2
                for id_ign_suivant in df_touches_target.index.tolist():
                    yield from recup_troncon_elementaire(id_ign_suivant)
    yield id_ign_ligne

def recup_troncon_parallele(id_ign_ligne):  
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


def affecter_troncon(id_ign_ligne):
    """
    Grouper les troncon par numero arbitraire
    baser sur recup_troncon_elementaire et recup_troncon_parallele
    en entree : id d'une ligne -> str
    """
    
    #les variables liées à la ligne
    df_ligne = df.loc[id_ign_ligne[0]]
    
    #appel du dico de resultat
    global dico_tronc_elem
    
    #pour chaque ligne on va creer un id dans le dico, avec les tronon associes
    for indice, ligne in enumerate(id_ign_ligne) :
        print(ligne)
        #recuperation ds troncons connexes en cas simple
        for troncon in recup_troncon_elementaire(ligne):
            if indice in dico_tronc_elem.keys():
                dico_tronc_elem[indice].append(troncon)
            else :
                dico_tronc_elem[indice]=[troncon]
        
        #recuperation des toncons connexes si 2 lignes pour une voie
        if df_ligne.loc['nature'] in ['Route à 2 chaussées','Quasi-autoroute','Autoroute']  :
            ligne_parrallele=recup_troncon_parallele(ligne)
            print(ligne_parrallele)
            for troncon in recup_troncon_elementaire(ligne_parrallele):
                if indice in dico_tronc_elem.keys():
                    dico_tronc_elem[indice].append(troncon)
                else :
                    dico_tronc_elem[indice]=[troncon] 
            


if __name__ == '__main__' : 
    affecter_troncon (['TRONROUT0000000202559704'])
    print (dico_tronc_elem)
