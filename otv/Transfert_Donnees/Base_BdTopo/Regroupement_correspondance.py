# -*- coding: utf-8 -*-

'''
Created on 27 oct. 2019

@author: martin.schoreisz

regrouper les troncons, apres avoir fait tourner sur toute une base
'''

import pandas as pd
import numpy as np
import Connexion_Transfert as ct
from datetime import datetime
from Base_BdTopo.Import_outils import tronc_tch
from Base_BdTopo.Troncon_elementaire import lignes_troncon_elem
from Base_BdTopo.Gestion_2_chaussee import gestion_voie_2_chaussee, ParralleleError
from geoalchemy2 import Geometry
from sqlalchemy import Table, Column, Integer, MetaData

def regrouper_troncon(list_troncon, df_avec_rd_pt, carac_rd_pt,df2_chaussees,lignes_exclues):
    """
    Premier run de regroupement des id_ign. il reste des erreurs à la fin. version qui peut tournerà part de la caractérisation des rd points.
    on peut faire tourner cette fonction sur un ensemble de lignes pour caractériser tout un fichier ou sur une seule en la mettant seule dans le liste list_troncon
    en entree : 
        list_troncon : list de string des id_ign a regrouper
        df_avec_rd_pt : df des lignes Bdtopo issu de identifier_rd_pt()
        carac_rd_pt : df des caractéristiques des rd points, issus de carac_rond_point
        df2_chaussees : df des lignes ayant une nature = 'Autoroute', Quasi-autoroute ou Route à 2 chaussées
        lignes_exclues : list destring : lignes qui arrete la propoagtation du troncon elementaire
    en sortie : 
        lignes_traitees : np array des id_ign affecte à un troncon elementaire
        lignes_non_traitees : list des id_ign non affectes a un troncon elementaires
        dico_erreur : dico des erreurs survenue pendant le traietement
        df_affectation : df de correspondance id_ign <-> id troncon elementaire
        
    """
    dico_fin={}
    dico_erreur={}
    liste_id_ign_base=np.array(list_troncon)
    lignes_traitees=np.array([],dtype='<U24')
    lignes_exclues=[]
    for i,l in enumerate(liste_id_ign_base) :
        if len(lignes_traitees)>=len(liste_id_ign_base) : break
        if i%500==0 : 
            print(i, datetime.now(), f'nb lignes traitees : {len(lignes_traitees)}')
        if l in lignes_traitees : 
            continue
        #critère d'exclusion : si la ligne est un rdpt ou une des lignes qui arrivent sur un rdpt avec certaines conditions
        ligne=df_avec_rd_pt.set_index('id_ign').loc[l]
        if ~np.isnan(ligne['id_rdpt']):
            continue
        else : 
            #print(l)
            try : 
                liste_ligne=lignes_troncon_elem(df_avec_rd_pt,carac_rd_pt, l, lignes_exclues) 
                #print(f'liste ligne non filtree : {liste_ligne}, num id_tronc : {i}')
                liste_ligne=[x for x in liste_ligne if x not in lignes_traitees] #filtre des lignes deja affectees
                #print(f'liste ligne filtree : {liste_ligne}, num id_tronc : {i}')
                if any([x in df2_chaussees.id_ign.tolist() for x in liste_ligne]) :  
                    try : 
                        liste_ligne+=gestion_voie_2_chaussee(liste_ligne, df_avec_rd_pt, l,carac_rd_pt)[0]
                        liste_ligne=[x for x in liste_ligne if x not in lignes_traitees]
                        #print(f'apres 2 chaussee : {liste_ligne}, num id_tronc : {i}')
                    except ParralleleError as Pe:
                        dico_erreur[Pe.id_ign]=Pe.erreur_type
                lignes_traitees=np.unique(np.append(lignes_traitees,liste_ligne))
            except Exception as e : 
                print(e)
                dico_erreur[l]=e
        for ligne_tronc in liste_ligne : 
            dico_fin[ligne_tronc]=i
    #print('fin : ', datetime.now(), f'nb lignes traitees : {len(lignes_traitees)}')  
    df_affectation=pd.DataFrame.from_dict(dico_fin, orient='index').reset_index()
    if df_affectation.empty :
        raise PasAffectationError(list_troncon)
    df_affectation.columns=['id', 'idtronc']
    lignes_non_traitees=[x for x in df_affectation.id.tolist() if x not in lignes_traitees]
    return df_affectation, dico_erreur, lignes_traitees, lignes_non_traitees    

def corresp_petit_tronc(df_lignes,df_affectation,tronc_elem_synth,long_max=50) : 
    """
    df de correspondance pour les troncon de moins de metres
    en entree : 
        tronc_elem_synth : df des tronc elem, issu de carac_te()
        long_max : integer pour considere un tronc_elem comme petit
        df_lignes : donn�es issues de identifier_rd_pt() avec id_ign en index
        df_affectation : df de correspondance id_ign - tronc elem, issue de regrouper_troncon()
    en sortie : 
        corresp_petit_tronc : dfde correspondance entre l'ancen tronc elem et le nouveau, uniquement pour les lignes concernees
    """
    def corresp_1_tronc(ids,df_lignes,df_affectation,list_petit_tronc):
        """
        Obtention du tronc elem auquel rattache un petit tronon, sinon False
        en entree : 
            ids : tuple de string de id_ign    
        en sortie : 
            tronc_elem_ref : integer>0 si corresp, sinon -99
        """
        try : 
            df_tronc_tch=tronc_tch(ids, df_lignes)
        except NotImplementedError : #si sheply bug c'est un cas trop complexe et on laisse tomber
            return -99
        except ValueError : #erreur cree par un rd point foireux
            return -99
        #filtrer les lignes qui correspondent � des petits tronc_elem
        df_tronc_tch=df_tronc_tch.loc[~df_tronc_tch.id_ign.isin(list_petit_tronc)].copy()
        id_ign_ref=df_tronc_tch.loc[df_tronc_tch['angle'].idxmax()].id_ign if 160<df_tronc_tch.angle.max()<200 else False
        #puis trouver le tronc_elem correspondant
        try : 
            tronc_elem_ref=df_affectation.loc[df_affectation['id']==id_ign_ref].idtronc.values[0] if id_ign_ref else -99
        except IndexError: #si l'id_ign n'a pas �t� associ� � un troncon elementaire idtronc.value[0] renvoie cette erreur
            tronc_elem_ref=-99
            #print(ids, tronc_elem_ref)
        return tronc_elem_ref
    
    petit_tronc=tronc_elem_synth.loc[tronc_elem_synth['long']<=long_max].copy()# 467 troncons
    list_petit_tronc=[b for a in petit_tronc.id.tolist() for b in a]#list des id_ign present dans les petits troncons
    petit_tronc['idtronc_y']=petit_tronc.apply(lambda x : corresp_1_tronc(x['id'],df_lignes,df_affectation,list_petit_tronc), axis=1)
    #creation table de corresp
    corresp_petit_tronc=petit_tronc.loc[petit_tronc['idtronc_y']!=-99][['idtronc_y']]
    return corresp_petit_tronc


def carac_te(df_avec_rd_pt, df_affectation) :
    """
    caracteriser les tronc elementaires selon leur longueur et les id_ign qui les composent
    en entree : 
        df des lignes ign issue de identifier_rd_pt()
        df_affectation : df de correspondance id_ign - id_tronc_elem iisue de regrouper_troncon()
    en sortie : 
        tronc_elem_synth : df des tronc elem vec longuer et ids
    """
    tronc_elem_geom=df_avec_rd_pt.merge(df_affectation, left_on='id_ign',right_on='id')[['geom','idtronc']].dissolve(by='idtronc')
    tronc_elem_geom['long']=tronc_elem_geom.geom.length
    tronc_elem_synth=tronc_elem_geom.merge(df_affectation.groupby('idtronc').agg({'id' : lambda x : tuple(set(x))}), left_index=True, right_index=True)
    return tronc_elem_synth

def creer_MaJ_te(df_affectation):
    """
    creer la df qui va accueillir l'affectaion mise � jour
    en entree :
        df_affectation : df de correspondance id_ign - id_tronc_elem iisue de regrouper_troncon()
    en sortie : 
        df_affectation_upd : la mm df qu'en entree avec idtronc en index et des champs 'corr_te' (booleen) et 'typ_cor_te' (string)
    """
    df_affectation_upd=df_affectation.copy()#copie pour update
    df_affectation_upd['idtronc_y']=df_affectation_upd.idtronc
    df_affectation_upd.set_index('idtronc', inplace=True)
    df_affectation_upd['corr_te']=False
    df_affectation_upd['typ_cor_te']=np.NaN
    return df_affectation_upd

def corresp_te_bretelle(df_lignes, tronc_elem_synth, dist_agreg=50) : 
    """
    regrouper les troncon elementaires des bretelles
    en entree : 
        df_lignes : df des lignes ign issue de identifier_rd_pt avec id_ign en index
        tronc_elem_synth : df des tronc elem, issu de carac_te()
        dist_agreg : integer : distance d'agreg entre les troncons
    en sortie : 
        corresp_bret : dfde correspondance entre l'ancen tronc elem et le nouveau, uniquement pour les lignes concernees
    """
    #select tronc_elem bretelles
    list_id_bretelle=df_lignes.loc[(df_lignes['nature']=='Bretelle') & (df_lignes['numero']=='NC')].index.tolist()
    tronc_bretelle=tronc_elem_synth.loc[tronc_elem_synth.apply(lambda x : any([a for a in x['id'] if a in list_id_bretelle]), axis=1)].copy()
    #geom en wkt pour tranfert dans bdd
    tronc_bretelle_wkt=tronc_bretelle.reset_index()[['idtronc','geom']].copy()
    tronc_bretelle_wkt['geom']=tronc_bretelle_wkt.apply(lambda x : x['geom'].wkt, axis=1)
    #on bascule en sql pour utiliser le cluster de postgres qui est plus puissant car il gere tout type de geom et donc la distance entre les geoms devient plus precise
    #cree la table qui va accueillir les donnees de bretelles
    metadata = MetaData()
    bretel_table=Table(
        'bretelle',metadata,
        Column('idtronc', Integer, primary_key=True),
        Column('geom', Geometry()))
    requete = f"SELECT idtronc, ST_ClusterDBSCAN(geom, eps := {dist_agreg}, minpoints := 2) over () AS cid FROM public.bretelle"

    #creation dans Bdd puis utilisation sql puis retour
    with ct.ConnexionBdd('local_otv') as c :
        try :
            bretel_table.drop(c.engine)
        except  : pass
        bretel_table.create(c.engine)
        c.sqlAlchemyConn.execute(bretel_table.insert(), tronc_bretelle_wkt.to_dict('records'))
        df_cluster=pd.read_sql_query(requete, c.sqlAlchemyConn)

    #creation de la tble de correspondance
    bretlle_a_grp=df_cluster.loc[~df_cluster.cid.isna()].copy()
    corresp_bret=bretlle_a_grp.merge(bretlle_a_grp.groupby('cid')['idtronc'].min().reset_index(), on='cid')
    corresp_bret=corresp_bret.loc[corresp_bret['idtronc_x']!=corresp_bret['idtronc_y']].copy()[['idtronc_x','idtronc_y']].set_index('idtronc_x')
    return corresp_bret

def corresp_te_arrive_rdpt(carac_rd_pt,df_avec_rd_pt,tronc_elem_synth):
    """
    poyr regrouper 2 lignes entrantes de rd point qui sont 2 tronc_elem_differents mais qui se rejoignent (TRONROUT0000000112535635)
    en entree : 
        carac_rd_pt : df des carac de rd point issue de carac_rond_point()
        df_avec_rd_pt : df des lignes Bdtopo iissue de identifier_rd_pt()
        tronc_elem_synth : df des tronc elem, issu de carac_te()
    en sortie : 
        corresp_entree_rdpt : dfde correspondance entre l'ancen tronc elem et le nouveau, uniquement pour les lignes concernees
    """
    #liste des voies entrantes sur rdpt
    list_voie_entre_rdpt=[ (x,b) for a, b in zip(carac_rd_pt.id_ign_entrant.tolist(),carac_rd_pt.index.tolist()) for x in a]
    list_voie_entre_rdpt=pd.DataFrame.from_records(list_voie_entre_rdpt, columns=['id_ign','id_rdpt'])
    #list edes tronc_elem avec 1 seul id_ign
    tronc_elem_1id=tronc_elem_synth.loc[tronc_elem_synth.apply(lambda x : len(x['id'])==1,axis=1)].copy()
    tronc_elem_1id['id']=tronc_elem_1id.apply(lambda x : x['id'][0], axis=1)
    #tronc elem 1id qui touche rdpoint
    tronc_elem_1id_rdpt=tronc_elem_1id.reset_index().merge(list_voie_entre_rdpt,left_on='id', right_on='id_ign' )
    #jointure entre tous les tronc qui tch les mm rd point
    cross_join=tronc_elem_1id_rdpt[['idtronc','id_ign','id_rdpt']].merge(tronc_elem_1id_rdpt[['idtronc','id_ign','id_rdpt']], on='id_rdpt')
    cross_join_filtre=cross_join.loc[cross_join['idtronc_x']!=cross_join['idtronc_y']].copy()
    #trouver les lignes qui se touchnet
    cross_join_tot=cross_join_filtre.merge(df_avec_rd_pt[['id_ign', 'source','target']], left_on='id_ign_x', right_on='id_ign').merge(df_avec_rd_pt[['id_ign', 'source','target']], left_on='id_ign_y', right_on='id_ign')
    cross_join_tot.drop(['id_ign_x','id_ign_y'],axis=1,inplace=True)
    lign_tch=cross_join_tot.loc[cross_join_tot.apply(lambda x : x['source_x'] in [x['source_y'], x['target_y']] or x['target_x'] in [x['source_y'], x['target_y']],axis=1)]
    #supprimer les doublons et formatter la tablede correspondance
    corresp_entree_rdpt=lign_tch.drop_duplicates('id_rdpt')[['idtronc_x','idtronc_y']].set_index('idtronc_x')
    return corresp_entree_rdpt



def MaJ_te(df_affectation_upd, tbl_corresp, type_MaJ) : 
    """
    mettre � jour en place de la coquille cr��e par creer_MaJ_te()
    en entree : 
        df_affectation_upd : issue de creer_MaJ_te()
        tbl_corresp : table de correspondance ancien idtronc - nouveau idtronc
        type_MaJ : string : mise � jour effectuee (actuellement varie entre 'Bretelle','entree_rd_pt','petit_troncon' )
    """
    df_affectation_upd.update(tbl_corresp)
    df_affectation_upd.loc[df_affectation_upd.index.isin(tbl_corresp.index.tolist()),'corr_te']=True
    df_affectation_upd.loc[df_affectation_upd.index.isin(tbl_corresp.index.tolist()),'typ_cor_te']=type_MaJ
    return 

class PasAffectationError(Exception):  
    """
    Exception levee si la variable df_affectation de la fonction regrouper_troncon() est vide (i.e il n'y a aucune affectation de faite)
    """     
    def __init__(self, list_lignes):
        Exception.__init__(self,f'pas d\'affectation pour les lignes {list_lignes}')
        self.erreur_type='PasAffectationError'