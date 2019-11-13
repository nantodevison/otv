# -*- coding: utf-8 -*-
'''
Created on 27 oct. 2019

@author: martin.schoreisz

gerer les voies repr�sentee par 2 lignes
'''


from Base_BdTopo.Import_outils import fusion_ligne_calc_lg
from Base_BdTopo.Troncon_base import liste_complete_tronc_base,deb_fin_liste_tronc_base
from Base_BdTopo.Troncon_elementaire import lignes_troncon_elem


def trouver_chaussees_separee(list_troncon, df_avec_rd_pt):
    """
    Trouver la ligne parrallele de la voie repr�sent�e par 2 chauss�es
    en entree : 
       list_troncon : list des troncon elementaire de l'idligne recherch�, issu de  liste_complete_tronc_base
       df_avec_rd_pt  :df des lignes vace rd point, issu de identifier_rd_pt
    en sortie : 
        ligne_proche : string : id_ign de la ligne la plus proche
        ligne_filtres : df des lignes proches avec distance au repere
        longueur_base : longueur du troncon elementaire servant de base
    """    
    lgn_tron_e=df_avec_rd_pt.loc[df_avec_rd_pt['id_ign'].isin(list_troncon)]
    list_noeud=lgn_tron_e.source.tolist()+lgn_tron_e.target.tolist()
    lgn_agrege, longueur_base=fusion_ligne_calc_lg(lgn_tron_e)
    xmin,ymin,xmax,ymax=lgn_agrege.interpolate(0.5, normalized=True).buffer(75).bounds #limtes du carre englobant du buffer 50 m du centroid de la ligne
    #gdf_global=gp.GeoDataFrame(df, geometry='geom')#donnees de base
    lignes_possibles=df_avec_rd_pt.cx[xmin:xmax,ymin:ymax]#recherche des lignes proches du centroid
    
    # GESTION DES VOIES COMMUNALESS AVEC NUMERO = 'NC' et CODEVOIE DIFFRENT DE NR
    voie=max(set(lgn_tron_e.numero.tolist()), key=lgn_tron_e.numero.tolist().count)
    code_voie=max(set(lgn_tron_e.codevoie_d.tolist()), key=lgn_tron_e.codevoie_d.tolist().count)
    importance=max(set(lgn_tron_e.importance.tolist()), key=lgn_tron_e.importance.tolist().count)
    
    if voie !='NC' : 
        ligne_filtres=lignes_possibles.loc[(~lignes_possibles.id_ign.isin(list_troncon)) & (lignes_possibles['numero']==voie) & 
                                           (lignes_possibles['importance']==importance)].copy()
    elif voie =='NC' and code_voie != 'NR' : 
        ligne_filtres=lignes_possibles.loc[(~lignes_possibles.id_ign.isin(list_troncon)) & (lignes_possibles['codevoie_d']==code_voie) &
                                           (lignes_possibles['importance']==importance)].copy()
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
    DEPRECATED : finalement ce n'est pas une bonne idee : les troncons se melangent
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
    #trouver le troncon plus proche de la ligne de d�part
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

def gestion_voie_2_chaussee(list_troncon, df_avec_rd_pt, ligne,carac_rd_pt): 
    """
    fonction de dteremination des tronc elem de voie � chaussees separees
    en entree : 
       list_troncon : list des troncon elementaire de l'idligne recherch�, issu de  liste_complete_tronc_base
       df_avec_rd_pt  :df des lignes vace rd point, issu de identifier_rd_pt
       ligne : ligne d'analyse de depart
       carac_rd_pt : df des caract�ristiques des rd points, issus de carac_rond_point
    en sortie : 
        list_troncon_comp : liste des id_ign complementaire si le lien est ok, sinon liste vide 
        ligne_proche : string : id_ign de la liste la plus proche
        ligne_filtres : df des lignes proches avec distance au repere
        longueur_base : float longueur du troncon elementaire servant de base
        long_comp : float longueur du troncon elementaire compare
    """  
    #filtre des troncon de base pour ne pas prendre les tronc qui font parti d'un rd pt car il fausse le calcul de la longueur
    list_troncon=df_avec_rd_pt.loc[(df_avec_rd_pt['id_ign'].isin(list_troncon)) & (df_avec_rd_pt['id_rdpt'].isna())].id_ign.tolist()
    ligne_proche, ligne_filtres, longueur_base=trouver_chaussees_separee(list_troncon, df_avec_rd_pt)
    #recherche des troncon du mm tronc elem
    list_troncon_comp=lignes_troncon_elem(df_avec_rd_pt,carac_rd_pt, ligne_proche,[])
    list_troncon_comp=df_avec_rd_pt.loc[(df_avec_rd_pt.id_ign.isin(list_troncon_comp)) & (df_avec_rd_pt['id_rdpt'].isna())].id_ign.tolist()
    #cacul de la longueur
    long_comp=fusion_ligne_calc_lg(df_avec_rd_pt.loc[df_avec_rd_pt['id_ign'].isin(list_troncon_comp)])[1]
    #verif que les longueurs coincident
    if min(long_comp,longueur_base)*100/max(long_comp,longueur_base) >50 : #si c'est le cas il gfaut transf�rer list_troncon_comp dans la liste des troncon du tronc elem 
        return list_troncon_comp, ligne_proche, ligne_filtres, longueur_base, long_comp
    else :
        #print(list_troncon_comp, ligne_proche, ligne_filtres, longueur_base, long_comp)
        raise ParralleleError(list_troncon)
        """ 
        ca c'�tait pour associer un troncon suivant si il faisait la bonne longeueur, mais au final mauvaise idee
        lignes_suivante, long_suivante=chercher_chaussee_proche(ligne, ligne_proche, df_avec_rd_pt)
        if min(long_suivante,longueur_base)*100/max(long_suivante,longueur_base) >50 :
            return lignes_suivante,ligne_proche, [], longueur_base, long_suivante
        else :
            return [],ligne_proche, ligne_filtres, longueur_base, long_comp
        """

class ParralleleError(Exception):  
    """
    Exception levee si la recherched'une parrallele ne donne rien
    """     
    def __init__(self, id_ign):
        Exception.__init__(self,f'pas de parrallele trouvee pour les troncons {id_ign}')
        self.id_ign = tuple(id_ign)
        self.erreur_type='ParralleleError'