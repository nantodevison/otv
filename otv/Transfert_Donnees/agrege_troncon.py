# -*- coding: utf-8 -*-
'''
Created on 29 janv. 2019
@author: martin.schoreisz

Module de creation de troncon homogene

'''
import matplotlib
import geopandas as gp
import numpy as np
from datetime import datetime
import Connexion_Transfert as ct
from shapely.wkt import loads
import Outils
#from psycopg2 import extras

# ouvrir connexion, recuperer donnees
with ct.ConnexionBdd('local_otv') as c : 
    df = gp.read_postgis("select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom from public.test_agreg_lineaire t left join public.test_agreg_lineaire_vertices_pgr v1 on t.\"source\"=v1.id left join public.test_agreg_lineaire_vertices_pgr v2  on t.target=v2.id ", c.connexionPsy)

#variables generales
nature_2_chaussees=['Route à 2 chaussées','Quasi-autoroute','Autoroute']
#donnees generale du jdd
df.set_index('id_ign', inplace=True)  # passer l'id_ign commme index ; necessaire sinon pb avec geometrie vu comme texte
df2_chaussees=df.loc[df.loc[:,'nature'].isin(nature_2_chaussees)] #isoler les troncon de voies decrits par 2 lignes



def recup_troncon_elementaire (id_ign_ligne, ligne_traite_troncon=[]):
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
    nature=df_ligne.loc['nature']
    #print('ligne_traite_troncon : ',ligne_traite_troncon)
    ligne_traite_troncon.append(id_ign_ligne)
    
    # cas simple de la ligne qui en touche qu'uen seule autre du cote source
    if df_ligne.loc['nb_intrsct_src'] == 2 : 
        #print (f' cas = 2 ; src : avant test isin : {datetime.now()}, ligne : {id_ign_ligne} ')
        df_touches_source = df.loc[(~df.index.isin(ligne_traite_troncon)) & ((df.loc[:, 'source'] == df_ligne.loc['source']) | (df.loc[:, 'target'] == df_ligne.loc['source']))]  # recuperer le troncon qui ouche le point d'origine et qui n'est pas deja traite
        #print (f' cas = 2 ; src : apres test isin : {datetime.now()}')
        if len(df_touches_source) > 0:  # car la seule voie touchee peut déjà etre dans les lignes traitees
            id_ign_suivant = df_touches_source.index.tolist()[0]
            #print (f'fin traitement cas = 2 ; src : apres test isin : {datetime.now()}')
            #ligne_traite_troncon+=[id_ign_ligne,id_ign_suivant] #liste des lignes deja traitees
            yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon) #on boucle pour chercher la suivante 
    elif df_ligne.loc['nb_intrsct_src'] == 3 :  # cas plus complexe d'une ligne a un carrefour. soit c'est la meme voie qui se divise, soit ce sont d'autre voie qui touche
        #print (f' cas = 3 ; src : avant test isin : {datetime.now()}')
        df_touches_source = df.loc[(~df.index.isin(ligne_traite_troncon)) & ((df.loc[:, 'source'] == df_ligne.loc['source']) | (df.loc[:, 'target'] == df_ligne.loc['source']))]  # recuperer le troncon qui ouche le point d'origine
        #print (f' cas = 3 ; src : apres test isin : {datetime.now()}')
        if len(df_touches_source) > 0:  # si les voies touchees n'on pas ete traiees
            if (df_ligne.loc['numero'] == df_touches_source['numero']).all() == 1 :
                if ((nature == 'Route à 1 chaussée' and ('Route à 2 chaussées' == df_touches_source['nature']).all()) #si c'est une route qui se divise en 2 
                   or(nature == 'Route à 2 chaussée' and df_touches_source['nature'].isin(['Route à 1 chaussée', 'Route à 2 chaussées']))):  # !!  ou une route qui était en deux et qui passe à 1
                    for id_ign_suivant in df_touches_source.index.tolist():
                        #print (f'fin traitement cas = 3 mm numero ; src : apres test isin : {datetime.now()}')
                        #ligne_traite_troncon+=[id_ign_ligne,id_ign_suivant]
                        yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon)  
            else: #si toute les voies n'ont pas le même nom
                if nature in ['Autoroute', 'Quasi-autoroute'] :
                    df_ligne_autre=df_touches_source.loc[df_touches_source.loc[:,'numero']!=df_ligne.loc['numero']]
                    if len(df_touches_source)-len(df_ligne_autre)>0 : #sinon ce veut dire que le troncon suivant est deja traite
                        df_ligne_pt_avant_src=df_ligne['geom'][0].coords[1] #point avant le point  target
                        coord_source_arrondi=[round(i,1) for i in list(loads(df_ligne['src_geom']).coords)[0]] #coordonnees du point target
                        #trouver le point de df_ligne autre juste aprs le point source
                        coord_ligne_arrondi=[[round(coord[0],1),round(coord[1],1)] for coord in list((df_ligne_autre.geometry.iloc[0])[0].coords)] #(df_ligne_autre.geometry.iloc[0]) la geometryde la 1ere ligne
                        pt_suivant_src_ligne_autre=coord_ligne_arrondi[-2] if coord_ligne_arrondi[-1]==coord_source_arrondi else coord_ligne_arrondi[1]
                        #recuperer l'angle
                        angle=Outils.angle_entre_2_ligne(coord_source_arrondi, pt_suivant_src_ligne_autre, df_ligne_pt_avant_src)
                        #si l'angle estdans les 90°, on ignor et on continue sur la l'autre ligne
                        if 55 < angle < 135 :
                            id_ign_suivant=df_touches_source.loc[~df.id.isin(df_ligne_autre.id)].index.values.tolist()[0]
                            #print (f'fin traitement cas = 3 ; src : apres test isin : {datetime.now()}')
                            #ligne_traite_troncon+=[id_ign_ligne,id_ign_suivant]
                            yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon)    

    #cas simple de la ligne qui en touche qu'uen seule autre du cote target
    if df_ligne.loc['nb_intrsct_tgt'] == 2 :  
        #print (f' cas = 2 ; tgt : avant test isin : {datetime.now()}') 
        df_touches_target = df.loc[(~df.index.isin(ligne_traite_troncon)) & ((df.loc[:, 'source'] == df_ligne.loc['target']) | (df.loc[:, 'target'] == df_ligne.loc['target']))]  # recuperer le troncon qui ouche le point d'origine  et qui n'est pas deja traite
        #print (f' cas = 2 ; tgt : apres test isin : {datetime.now()}')
        if len(df_touches_target) > 0:  # car la seule voie touchee peut déjà etre dans les lignes traitees
            id_ign_suivant = df_touches_target.index.tolist()[0]
            #print (f'fin traitement cas = 2 ; tgt : apres test isin : {datetime.now()}')
            #ligne_traite_troncon+=[id_ign_ligne,id_ign_suivant]
            yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon)        
    # cas plus complexe d'une ligne a un carrefour de 3 lignes
    elif df_ligne.loc['nb_intrsct_tgt'] == 3 :
        #print (f' cas = 3 ; tgt : avant test isin : {datetime.now()}')  
        df_touches_target = df.loc[(~df.index.isin(ligne_traite_troncon)) & ((df.loc[:, 'source'] == df_ligne.loc['target']) | (df.loc[:, 'target'] == df_ligne.loc['target']))]  # recuperer le troncon qui ouche le point d'origine
        #print (f' cas = 3 ; tgt : apres test isin : {datetime.now()}')
        if len(df_touches_target) > 0:  # si les voies touchees n'on pas ete traiees
            if (df_ligne.loc['numero'] == df_touches_target['numero']).all() ==  1: #si les voies ont le mm noms
                if ((nature == 'Route à 1 chaussée' and ('Route à 2 chaussées' == df_touches_target['nature']).all()) #si c'est une route qui se divise en 2 
                   or(nature == 'Route à 2 chaussée' and df_touches_target['nature'].isin(['Route à 1 chaussée', 'Route à 2 chaussées']))):  # !!  ou une route qui était en deux et qui passe à 1
                    for id_ign_suivant in df_touches_target.index.tolist():
                        #print (f'fin traitement cas = 3 mm numero ; tgt : apres test isin : {datetime.now()}')
                        #ligne_traite_troncon+=[id_ign_ligne,id_ign_suivant]
                        yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon)
            else: #si toute les voies n'ont pas le même nom
                if nature in ['Autoroute', 'Quasi-autoroute'] :
                    df_ligne_autre=df_touches_target.loc[df_touches_target.loc[:,'numero']!=df_ligne.loc['numero']]
                    if len(df_touches_target)-len(df_ligne_autre)>0 : #sinon ce veut dire que le troncon suivant est deja traite
                        df_ligne_pt_avant_tgt=df_ligne['geom'][0].coords[-2] #point avant le point  target
                        coord_target_arrondi=[round(i,1) for i in list(loads(df_ligne['tgt_geom']).coords)[0]] #coordonnees du point target
                        #trouver le point de df_ligne autre juste aprs le point target
                        coord_ligne_arrondi=[[round(coord[0],1),round(coord[1],1)] for coord in list((df_ligne_autre.geometry.iloc[0])[0].coords)] #(df_ligne_autre.geometry.iloc[0]) la geometryde la 1ere ligne
                        pt_suivant_tgt_ligne_autre=coord_ligne_arrondi[-2] if coord_ligne_arrondi[-1]==coord_target_arrondi else coord_ligne_arrondi[1]
                        #recuperer l'angle
                        angle=Outils.angle_entre_2_ligne(coord_target_arrondi, pt_suivant_tgt_ligne_autre, df_ligne_pt_avant_tgt)
                        #si l'angle estdans les 90°, on ignor et on continue sur la l'autre ligne
                        if 55 < angle < 135 :
                            id_ign_suivant=df_touches_target.loc[~df.id.isin(df_ligne_autre.id)].index.tolist()[0]
                            #print (f'fin traitement cas = 3 ; tgt : apres test isin : {datetime.now()}')
                            #ligne_traite_troncon+=[id_ign_ligne,id_ign_suivant]
                            yield from recup_troncon_elementaire(id_ign_suivant, ligne_traite_troncon)
    yield ligne_traite_troncon

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
    gdf_lignes2=gdf_lignes.unary_union #union des geometries
    xmin,ymin,xmax,ymax=gdf_lignes2.centroid.buffer(200).bounds #centroid de la ligne
    gdf_global=gp.GeoDataFrame(df, geometry='geom')#donnees de base
    lignes_possibles=gdf_global.cx[xmin:xmax,ymin:ymax]#recherche des lignes proches du centroid
    #uniquement les lignes non présentes dans la liste de troncons avec le même nom de voie
    ligne_filtres=lignes_possibles.loc[(~lignes_possibles.index.isin(liste_troncon)) & (lignes_possibles.loc[:,'numero']==gdf_lignes.iloc[0]['numero'])]
    #obtenir les distances au centroid
    ligne_filtres['distance']=ligne_filtres.apply(lambda x : gdf_lignes2.centroid.distance(x.iloc['geom']), axis=1)
    #garder uniquement la valeur la plus proche du centroid
    ligne_proche=ligne_filtres.loc[ligne_filtres['distance']==ligne_filtres['distance'].min()].index[0]
    return ligne_proche

def affecter_troncon(df):
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
    liste_ligne=df.index.tolist()
    
    #pour chaque ligne on va creer un id dans le dico, avec les tronon associes
    for indice, ligne in enumerate(liste_ligne) :
        if indice % 300 == 0 :
            print (f"{indice}eme occurence : {ligne} à {datetime.now().strftime('%H:%M:%S')} nb ligne traite : {len(ligne_traitee_global)}, nb ligne differente={len(set(ligne_traitee_global))}")
        #print (f"{indice}eme occurence : {ligne} à {datetime.now().strftime('%H:%M:%S')} nb ligne traite : {len(ligne_traitee_global)}")
        if ligne in ligne_traitee_global :
            continue 
        else:
            """if indice>=10 : 
                break
            #recuperation ds troncons connexes en cas simple"""
            for liste_troncon in recup_troncon_elementaire(ligne,[]):
                #ligne_traitee_global=np.append(ligne_traitee_global,liste_troncon)
                ligne_traitee_global.update(liste_troncon)
                #print('lignes : ', liste_troncon,ligne_traitee_global )
                #dico_tronc_elem[indice[0]]=liste_troncon
                for troncon in liste_troncon : 
                    dico_tronc_elem[troncon]=indice
                break
                    
            #recuperation des toncons connexes si 2 lignes pour une voie
            if ligne in df2_chaussees.index  :
                ligne_parrallele=recup_troncon_parallele_v2(df,liste_troncon)
                #print('parrallele ',ligne_parrallele)
                if ligne_parrallele==None: #cas où pas de ligne parrallele trouvee
                    continue
                
                for liste_troncon_para in recup_troncon_elementaire(ligne_parrallele, []):
                    ligne_traitee_global.update(liste_troncon)
                    #print('lignes : ', liste_troncon)
                    for troncon_para in liste_troncon_para : 
                        dico_tronc_elem[troncon_para]=indice
                    break
    return dico_tronc_elem

def affecter_troncon_ligne(ligne):
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
        if ligne in ligne_traitee_global :
            continue 
        else:
            """if indice>=10 : 
                break
            #recuperation ds troncons connexes en cas simple"""
            for liste_troncon in recup_troncon_elementaire(ligne,[]):
                #ligne_traitee_global=np.append(ligne_traitee_global,liste_troncon)
                ligne_traitee_global.update(liste_troncon)
                #print('lignes : ', liste_troncon,ligne_traitee_global )
                #dico_tronc_elem[indice[0]]=liste_troncon
                for troncon in liste_troncon : 
                    dico_tronc_elem[troncon]=indice
                break
                    
            #recuperation des toncons connexes si 2 lignes pour une voie
            if ligne in df2_chaussees.index  :
                ligne_parrallele=recup_troncon_parallele(ligne,ligne_traitee_global)
                #print('parrallele ',ligne_parrallele)
                if ligne_parrallele==None: #cas où pas de ligne parrallele trouvee
                    continue
                
                for liste_troncon_para in recup_troncon_elementaire(ligne_parrallele, []):
                    ligne_traitee_global.update(liste_troncon)
                    #print('lignes : ', liste_troncon)
                    for troncon_para in liste_troncon_para : 
                        dico_tronc_elem[troncon_para]=indice
                    break
    return dico_tronc_elem
                
def inserer_dico(conn, dico):
    
    conn.curs.executemany("""INSERT INTO referentiel.tronc_elem_bdt17_ed15_l (id_ign,id_tronc_elem) VALUES (%s, %s)""", dico.items())
    conn.connexionPsy.commit()

if __name__ == '__main__' : 
    #affecter_troncon(['TRONROUT0000000202559719'])
    print ('debut : ',datetime.now().strftime('%H:%M:%S'))
    dico=affecter_troncon(df)
    with ct.ConnexionBdd('local_otv') as c :
        inserer_dico(c, dico)
    print ('fin : ',datetime.now().strftime('%H:%M:%S'), dico)
