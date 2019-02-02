# -*- coding: utf-8 -*-
'''
Created on 29 janv. 2019
@author: martin.schoreisz
'''
import matplotlib
import geopandas as gp
import numpy as np
from Martin_Perso import Connexion_Transfert as ct
from shapely.geometry import Point

#ouvrir connexion, recuperer donnees
with ct.ConnexionBdd('local_otv') as c : 
    df=gp.read_postgis("select t.*, v1.cnt nb_intrsct_src, v2.cnt as nb_intrsct_tgt from referentiel.troncon_route_bdt17_ed15_l t left join referentiel.troncon_route_bdt17_ed15_l_vertices_pgr v1 on t.\"source\"=v1.id left join referentiel.troncon_route_bdt17_ed15_l_vertices_pgr v2  on t.target=v2.id", c.connexionPsy)

df.set_index('id_ign',inplace=True) #passer l'id_ign commme index ; necessaire sinon pb avec geometrie vu comme texte


#stockge des données lignes traitees dans une numpy array (uen focntion géératrice issue de yield marcherait bien aussi
ligne_traitee_global=np.empty(1,dtype='<U24')

def recup_troncon_elementaire (id_ign_ligne):
    global ligne_traitee_global
    ligne_traitee_global=np.insert(ligne_traitee_global,1,id_ign_ligne)
    df_ligne=df.loc[id_ign_ligne]

    if df_ligne.loc['nb_intrsct_src']==2 : #cas simple de la ligne qui en touche qu'uen seule autre du dote source
        df_touches_source=df.loc[(~df.index.isin(ligne_traitee_global)) & ((df.loc[:,'source']==df_ligne.loc['source']) | (df.loc[:,'target']==df_ligne.loc['source']))]#recuperer le troncon qui ouche le point d'origine
        if len(df_touches_source>0): #car la seule voie touchee peut déjà etre dans les lignes traitees
            id_ign_suivant=df_touches_source.index.tolist()[0]
            print (f'cas source nb lign = 2 ; liste totale traite {ligne_traitee_global}, id en cours : {id_ign_suivant}')#il faut ajouter une condition de sortie de la boucle pour qu'iil ne tourne pas en rond sur les 2 même lignes
            yield from recup_troncon_elementaire(id_ign_suivant)
    
    if df_ligne.loc['nb_intrsct_tgt']==2 : #cas simple de la ligne qui en touche qu'uen seule autre du cote target
        df_touches_target=df.loc[(~df.index.isin(ligne_traitee_global)) & ((df.loc[:,'source']==df_ligne.loc['target']) | (df.loc[:,'target']==df_ligne.loc['target']))]#recuperer le troncon qui ouche le point d'origine
        if len(df_touches_target>0): #car la seule voie touchee peut déjà etre dans les lignes traitees
            id_ign_suivant=df_touches_target.index.tolist()[0]
            print (f'cas target nb lign = 2 ; liste totale traite {ligne_traitee_global}, id en cours : {id_ign_suivant}')#il faut ajouter une condition de sortie de la boucle pour qu'iil ne tourne pas en rond sur les 2 même lignes
            yield from recup_troncon_elementaire(id_ign_suivant)        
    elif df_ligne.loc['nb_intrsct_tgt']==3 : #cas plus complexe d'une ligne a un carrefour. soit c'est la meme voie qui se divise, soit ce sont d'autre voie qui touche
        df_touches_target=df.loc[(~df.index.isin(ligne_traitee_global)) & ((df.loc[:,'source']==df_ligne.loc['target']) | (df.loc[:,'target']==df_ligne.loc['target']))]#recuperer le troncon qui ouche le point d'origine
        if len(df_touches_target>0): #si les voies touchees n'on pas ete traiees
            if ((df_ligne.loc['numero']==df_touches_target['numero']).all()==1 and 
                 ((df_ligne.loc['nature']=='Route à 1 chaussée' and ('Route à 2 chaussées'==df_touches_target['nature']).all())
                   or(df_ligne.loc['nature']=='Route à 2 chaussée' and 
                      df_touches_target['nature'].isin(['Route à 1 chaussée','Route à 2 chaussées'])))):# !! on ne compare que à numero, dc pb en urbain si les 2 lignes qui touchent ont le mm numero, et que la ligne de voie etait decrite par 1 ligne puis par 2
                for id_ign_suivant in df_touches_target.index.tolist():
                    print (f'cas target nb lign = 3 ; liste totale traite {ligne_traitee_global}, id en cours : {id_ign_suivant}')#il faut ajouter une condition de sortie de la boucle pour qu'iil ne tourne pas en rond sur les 2 même lignes
                    yield from recup_troncon_elementaire(id_ign_suivant)
    
    yield id_ign_ligne

def recuperer_troncon(id_ign_ligne,ligne_troncon=[]):
    
    #ligne_troncon=[]#liste de récupereation des id des troncon
    ligne_troncon.append(id_ign_ligne)
    
    for ligne in df.loc[id_ign_ligne,'geom'] : #recuperer le startpoit et endpoint ; oblige si lignes considerees comme multi
        startpoint, endpoint = Point(ligne.coords[0]), Point(ligne.coords[-1])
    
    #récupérer les lignes qui touches le startpoint et le endpoint et qui ne sont la ligne de depart
    df_touche_st=df.loc[(df.loc[:,'geom'].touches(Point(startpoint)))& (df.index!=id_ign_ligne)] #la il faudrait le test non pas sur df.loc{:] mais sur df.loc[: sans les valeurs de la liste]
    df_touche_en=df.loc[(df.loc[:,'geom'].touches(Point(endpoint)))& (df.index!=id_ign_ligne)]
    
    ign_stpt=df_touche_st.index.values
    
    
    if len(df_touche_st.index)==1 and ign_stpt not in ligne_troncon : #si le nb de ligne qui touche le starpoint = 1 et que la lignene fait pas partie de celles deja testes
        """print ('dans le if', df_touche_st.index.values[0])
        
        print('sors du if')"""
        yield from recuperer_troncon(df_touche_st.index.values[0])#on recupere l'ig_ign de la ligne qui touche
    yield(id_ign_ligne)

if __name__=='__main__' : 
    for i in recup_troncon_elementaire ('TRONROUT0000000033007539', i=0) :
        print (i)
