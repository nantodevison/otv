# -*- coding: utf-8 -*-
'''
Created on 27 oct. 2019

@author: martin.schoreisz

prise en compte des troncon de base et extrapolation sur cas particluiers
'''

import numpy as np
from collections import Counter
from Base_BdTopo.Import_outils import tronc_tch, fusion_ligne_calc_lg,angle_3_lignes
from Base_BdTopo.Troncon_base import liste_complete_tronc_base,deb_fin_liste_tronc_base
from Base_BdTopo.Rond_points import verif_touche_rdpt,recup_lignes_rdpt


def recup_route_split(ligne_depart,list_troncon,voie,codevoie, lignes_adj,noeud,geom_noeud, type_noeud, nature,df_lignes):
    """
    r�cup�rer les id_ign des voies adjacentes qui sont de la mm voie que la ligne de depart
    en entree : 
        ligne_depart : string : id_ign de la ligne sui se separe
        list_troncon : list de string des troncon composant le troncon debase. issu de liste_complete_tronc_base
        voie : nom de la voie de la ligne de depart
        codevoie : codevoie_d de la ligne de depart
        lignes_adj : df des lignes qui touchent, issues de identifier_rd_pt avec id_ign en index
        noeud : integer : numero du noeud central
        geom_noeud_central : wkt du point central de separation
        type_noeud : type du noeud centrale par rapport � la ligne de depart : 'source' ou 'target'
        df_lignes : df de l'ensemble des lignes du transparent
    en sortie : 
        ligne_mm_voie : list d'id_ign ou liste vide
    """    
    #CAS PARTICULIERS
    if len(lignes_adj)!=2 : 
        return []
    if len(set(lignes_adj.source.tolist()+lignes_adj.target.tolist()))==2 : #cas d'une ligne qui separe pour se reconnecter ensuite
        return lignes_adj.index.tolist()
    
    #donnees de base : 
    tronc_tch_lign=tronc_tch((ligne_depart,), df_lignes)
    tronc_tch_lign=tronc_tch_lign.loc[tronc_tch_lign['id_noeud_lgn']==noeud].copy()
    
    #si on est sur une 2*2 voies avec ue ligne au milieu provenant d'une autre voie qui intersecte, on s'arrete (cf filaire voie BdxM)
    if nature in ['Route à 2 chaussées', 'Type autoroutier'] and (((
        tronc_tch_lign.angle>65) & (tronc_tch_lign.angle<125)).any() and ((tronc_tch_lign.angle>150) & (tronc_tch_lign.angle<200)).any() 
        and (tronc_tch_lign.longueur<20).any()) : 
        return []
    
    
    #CAS GENERAL  
    if voie!='NC' : 
        if nature=='Bretelle' and (lignes_adj.nature.isin(['Type autoroutier'])).any()==1 : 
            return []
        if (voie==lignes_adj.numero).all() : #les voies qui se séparent ont le mm numero
            return lignes_adj.index.tolist() 
        #une des voies qui se séparent a le mm numero, les angles 
        elif (voie==lignes_adj.numero).any() and ((tronc_tch_lign.angle>120).all() 
            & (tronc_tch_lign.longueur<50).all() 
            & (abs(tronc_tch_lign.iloc[0].angle-tronc_tch_lign.iloc[1].angle)<90)) :
            return lignes_adj.index.tolist()
    elif voie=='NC' and codevoie!='NR' :
        if (codevoie==lignes_adj.codevoie_d).all(): 
            return lignes_adj.index.tolist()
        else : return []
    else : # cas des nc / nr qui se s�pare : ont r�flechi en angle et longueurs e�quivalente, avec si besoin comparaiosn des lignes qui touvhent
        if (lignes_adj.nature=='Bretelle').any()==1 : #une bretelle qui se separe on garde le mm identifiant
            return lignes_adj.index.tolist()
        if (lignes_adj.id_rdpt>0).any()==1  :#si une des lignes qui se separent fait partie d'un rd point on passe 
            return []
        if lignes_adj.nature.isin(['Type autoroutier', 'Route à 2 chaussées']).any() : #pour ne pas propager une bertelle a uune autoroute
            return []
        tronc_tch_lign['tronc_supp']=tronc_tch_lign.apply(lambda x : liste_complete_tronc_base(x['id_ign'],df_lignes,[ligne_depart]), axis=1)
        tronc_tch_lign['long']=tronc_tch_lign.apply(lambda x : fusion_ligne_calc_lg(df_lignes.loc[x['tronc_supp']])[1],axis=1)
        noeuds_fin=[k for k,v in Counter(df_lignes.loc[tronc_tch_lign.id_ign.tolist()].source.tolist()+df_lignes.loc[tronc_tch_lign.id_ign.tolist()].target.tolist()).items() if v==1]
        #print(360-tronc_tch_lign.angle.sum()) 
        if tronc_tch_lign.long.min() / tronc_tch_lign.long.max() > 0.66 : 
            if not df_lignes.loc[(df_lignes.source.isin(noeuds_fin)) & (df_lignes.target.isin(noeuds_fin))].empty :
                if 360-tronc_tch_lign.angle.sum() < 75 :
                    return [x for y in tronc_tch_lign.tronc_supp.tolist() for x in y ]
                else : return []
            else : return []
        else : 
            return []

def recup_triangle(ligne_depart,voie,codevoie, lignes_adj, noeud,geom_noeud,type_noeud, df_lignes):
    """
    r�cuperer un bout de voe coinc� entre 2 lignes de carrefourd'une voie perpendiculaire
    en entree : 
        ligne_depart : string : id_ign de la ligne sui se separe
        voie : nom de la voie de la ligne de depart
        code_voie : string : codevoie_d de la BdTopo
        lignes_adj : df des lignes qui touchent, issues de identifier_rd_pt avec id_ign en index
        noeud : integer noeud de depart du triangle
        geom_noeud : geometrie du noeud central pour angle_3_lignes
        type_noeud : type du noeud centrale par rapport � la ligne de depart : 'source' ou 'target' (pour angle_3_lignes)
        df_lignes : df globale des lignes
    en sortie :
        id_rattache : list id_ign de bout a recuperer ou liste vide
    """
    if df_lignes.loc[ligne_depart].nature in ['Autoroute', 'Quasi-autoroute'] : 
        return []
    if len(lignes_adj)!=2 : 
        return []
    
    #dans tout les cas, si une voie se separe en 2, et que les 2 parties touchent la mm lignes apres, on recupere les 2 parties
    liste_noeud=[x for x in set(lignes_adj.source.tolist()+lignes_adj.target.tolist()) if x!=noeud]
    if not df_lignes.loc[(df_lignes['source'].isin(liste_noeud)) & (df_lignes['target'].isin(liste_noeud))].empty : 
        tt=tronc_tch((ligne_depart,), df_lignes)
        if (tt.loc[tt['id_noeud_lgn']==noeud].angle>=120).all() : #si toute les lignes qui partent sont sup�120� (�viter casparticulier avec petit bout rourte perpendicualaire)
            return lignes_adj.index.tolist()
    
    #IL FAUDRA FAIRE LE MENAGE DANS LE SUITE DE CETTE FONCTION : CERTAINE CHOSES NE SERVENT PEUT ETRE PLUS
    
               
    if voie != 'NC' : 
        lgn_cote_1=lignes_adj.loc[(lignes_adj['numero']!=voie)]  
        if lgn_cote_1.empty : 
            return []
        noeud_centre = lgn_cote_1['source'].values[0] if lgn_cote_1['source'].values[0] != noeud else lgn_cote_1['target'].values[0]
        lgn_suiv=lignes_adj.loc[~lignes_adj.index.isin(lgn_cote_1.index.tolist())]
        if lgn_suiv.empty : 
            return []
        noeud_suiv=lgn_suiv['source'].values[0] if lgn_suiv['source'].values[0] != noeud else lgn_suiv['target'].values[0]
        #dans le cas ou plusieurs ligne a la suite forme le cote du triangle que l'on veut r�cup�rer on veut connaitre le nb de ligne qui touchent le noeud_suiv
        if lgn_suiv['source'].values[0] != noeud : 
            noeud_suiv=lgn_suiv['source'].values[0]
            nb_intersect_noeud_suiv=df_lignes.loc[lgn_suiv.index.values[0]].nb_intrsct_src
        else : 
            noeud_suiv=lgn_suiv['target'].values[0]
            nb_intersect_noeud_suiv=df_lignes.loc[lgn_suiv.index.values[0]].nb_intrsct_tgt
    
        if  nb_intersect_noeud_suiv==2 : 
            lgn_suiv=df_lignes.loc[liste_complete_tronc_base(lgn_suiv.index.values[0],df_lignes,[])] 
            noeud_ss_suite=Counter(lgn_suiv.source.tolist()+lgn_suiv.target.tolist())
            noeud_suiv=[k for k,v in noeud_ss_suite.items() if v==1 and k!=noeud][0]

        id_rattache=lgn_suiv.index.tolist()
        if noeud_centre==noeud_suiv : #�a veut dire une seule et mm lign qui fait 2 cote du triangle, dc on peu de suite garder la ligne suiv car 1 seule route non coupe l'intersec
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
        if noeud_centre==noeud_suiv : #�a veut dire une seule et mm lign qui fait 2 cote du triangle, dc on peu de suite garder la ligne suiv car 1 seule route non coupe l'intersec
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
        # la ligne la dont l'�cart avec 180 est le plus eleve est la ligne qui part
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
        if noeud_centre==noeud_suiv : #�a veut dire une seule et mm lign qui fait 2 cote du triangle, dc on peu de suite garder la ligne suiv car 1 seule route non coupe l'intersec
            return id_rattache
        else : # on cherche si une ligne touche ces 2 noeuds
            ligne_a_rattacher=df_lignes.loc[((df_lignes['source']==noeud_centre) | (df_lignes['target']==noeud_centre)) & 
                               (~df_lignes.index.isin(lgn_cote.index.tolist())) & ((df_lignes['source']==noeud_suiv) | (df_lignes['target']==noeud_suiv))]
            if not ligne_a_rattacher.empty : 
                return id_rattache
            else :
                return []

def lignes_troncon_elem(df_avec_rd_pt,carac_rd_pt, ligne, lignes_exclues) : 
    """
    trouver les lignes qui appartiennent au m�me troncon elementaire que la ligne de depart
    en entree : 
        df_avec_rd_pt : df des lignes Bdtopo
        carac_rd_pt : df des caract�ristiques des rd points, issus de carac_rond_point
        ligne : string : id_ign de la ligne a tester
        lignes_exclues : list destring : lignes qui arrete la propoagtation du troncon elementaire
    en sortie :
        liste_troncon_finale : set de string des id_ign des lignes r�unies dans le troncon elementaire
    """
    df_lignes2=df_avec_rd_pt.set_index('id_ign')#mettre l'id_ign en index
    lignes_a_tester, liste_troncon_finale, list_troncon2=[ligne],[],[]

    while lignes_a_tester :
        for id_lignes2 in lignes_a_tester :
            #print(id_lignes2)
            list_troncon2=liste_complete_tronc_base(id_lignes2,df_lignes2,lignes_exclues)
            #print(f"list_troncon2 : {list_troncon2}, lignes_exclues : {lignes_exclues}")
            liste_troncon_finale+=list_troncon2
            dico_deb_fin2=deb_fin_liste_tronc_base(df_lignes2, list_troncon2)
            #print(liste_troncon_finale)            
            for k, v in dico_deb_fin2.items() :
                #print(k, v)
                lignes_adj2=df_lignes2.loc[((df_lignes2['source']==v['num_node'])|
                                          (df_lignes2['target']==v['num_node']))&
                                         (df_lignes2.index!=v['id'])&(~df_lignes2.index.isin(liste_troncon_finale))]
                
                if lignes_adj2.empty :
                    #print('vide : ',lignes_adj2.empty) 
                    continue
                if not carac_rd_pt.empty :
                    check_rdpt2, num_rdpt2=verif_touche_rdpt(lignes_adj2)
                else : 
                    check_rdpt2, num_rdpt2=False, np.NaN
                #print('chck rd pt',check_rdpt2,num_rdpt2)
                # on attaque la liste des cas possible
                #1. Rond point
                if check_rdpt2 : 
                    lignes_rdpt2, lignes_sortantes2=recup_lignes_rdpt(carac_rd_pt,num_rdpt2,list_troncon2,v['voie'],v['codevoie'])
                    #print('func troncelem',lignes_rdpt2)
                    liste_troncon_finale+=lignes_rdpt2
                    lignes_a_tester+=[x for x in lignes_sortantes2 if x not in lignes_exclues]
                else : #2. route qui se s�pare
                    liste_rte_separe=recup_route_split(v['id'],list_troncon2,v['voie'],v['codevoie'], lignes_adj2,v['num_node'],
                                                       v['geom_node'],v['type'],v['nature'], df_lignes2)
                    #print(f"{v['id']},liste_rte_separe {liste_rte_separe}, ligne a tester {lignes_a_tester}, final : {liste_troncon_finale}")
                    if liste_rte_separe : 
                        #print('route separees')
                        lignes_a_tester+=[x for x in liste_rte_separe if x not in lignes_exclues]
                        continue
                    liste_triangle=recup_triangle(v['id'],v['voie'],v['codevoie'], lignes_adj2, v['num_node'],v['geom_node'],v['type'], df_lignes2)
                    #print(f"{v['id']},liste_rte_separe {liste_rte_separe}, liste_triangle {liste_triangle}")
                    if liste_triangle :
                        #print('route triangle')
                        liste_troncon_finale+=[x for x in liste_triangle if x not in lignes_exclues]
                lignes_exclues+=liste_troncon_finale
            lignes_a_tester=[x for x in lignes_a_tester if x not in liste_troncon_finale and x not in lignes_exclues]
            #print('lignes_a_teser: ',lignes_a_tester)
    liste_troncon_finale=list(set(liste_troncon_finale))
    lignes_exclues=[]#ça c'est juste que sinon je ne peux pas faire tourner 2 fois la fonction de suite dans le notebook, c'est surement du au generateur
    return liste_troncon_finale
