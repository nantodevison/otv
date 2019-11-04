# -*- coding: utf-8 -*-
'''
Created on 27 oct. 2019

@author: martin.schoreisz

agregation des troncon dans sa forme la plus simple
'''

from collections import Counter



def liste_troncon_base(id_ligne,df_lignes,ligne_traite_troncon=[]):
    """
    recup�rer les troncons qui se suivent sans point de jonction �  + de 2 lignes
    en entree : 
        id_ligne : string : id_ign de la ligne a etudier
        df_lignes : df des lignes avec rd points, doit contenir les attributs 'nb_intrsct_src', 'source', 'src_geom','nb_intrsct_tgt','target','tgt_geom' avec id_ign en index
        ligne_traite_troncon : liste des ligne traitees dans le cadre de ce troncon elementaire -> liste de str
    en sortie
        fonction g�n�ratrice
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
                if len(df_touches_source) > 0:  # car la seule voie touchee peut d�j� etre dans les lignes traitees
                    id_ign_suivant = df_touches_source.index.tolist()[0]
                    ligne_traitee.append(id_ign_suivant) #liste des lignes deja traitees
                    liste_ligne_suivantes.append(id_ign_suivant)
                    yield id_ign_suivant
    for ligne_a_traiter in liste_ligne_suivantes :
        yield from liste_troncon_base(ligne_a_traiter, df_lignes, ligne_traitee)
        
def liste_complete_tronc_base(id_ligne,df_lignes,ligne_traite_troncon=[]):
    """
    simplement ajouter l'id de laligne de depart au g�n�rateur de la fonction liste_troncon_base
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
        tronc_deb_fin=lignes_troncon.loc[(lignes_troncon['source'].isin(noeuds_fin)) | (lignes_troncon['target'].isin(noeuds_fin))]
    dico_deb_fin={}
    if len(tronc_deb_fin)>1 :
        for i, e in enumerate(tronc_deb_fin.itertuples()) :
            #print(e,'\n', list(df_lignes.columns),'\n', e[list(df_lignes.columns).index('nb_intrsct_src')+1], '\n',list(df_lignes.columns).index('nb_intrsct_src')+1)
            dico_deb_fin[i]={'id':e[0],'type':'source','num_node':e[list(df_lignes.columns).index('source')+1],
                             'geom_node':e[list(df_lignes.columns).index('src_geom')+1],'voie':e[list(df_lignes.columns).index('numero')+1],
                             'codevoie':e[list(df_lignes.columns).index('codevoie_d')+1]} if e[list(df_lignes.columns).index('nb_intrsct_src')+1]>=3 else {
                             'id':e[0],'type':'target','num_node':e[list(df_lignes.columns).index('target')+1],
                             'geom_node':e[list(df_lignes.columns).index('tgt_geom')+1],'voie':e[list(df_lignes.columns).index('numero')+1],
                             'codevoie':e[list(df_lignes.columns).index('codevoie_d')+1]}
    else  : #pour tester les 2 cot�s de la ligne
        dico_deb_fin[0]={'id':tronc_deb_fin.index.values[0],'type':'source','num_node':tronc_deb_fin['source'].values[0],'geom_node':tronc_deb_fin['src_geom'].values[0]
                         ,'voie':tronc_deb_fin['numero'].values[0],'codevoie':tronc_deb_fin['codevoie_d'].values[0]}
        dico_deb_fin[1]={'id':tronc_deb_fin.index.values[0],'type':'target','num_node':tronc_deb_fin['target'].values[0],'geom_node':tronc_deb_fin['tgt_geom'].values[0]
                         ,'voie':tronc_deb_fin['numero'].values[0],'codevoie':tronc_deb_fin['codevoie_d'].values[0]}
    return dico_deb_fin
