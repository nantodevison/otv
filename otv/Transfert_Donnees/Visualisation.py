# -*- coding: utf-8 -*-
'''
Created on 1 sept. 2021

@author: martin.schoreisz
Module de regroupement des fonctions de visualisation
'''

from Donnees_horaires import calculJourneeType 
import pandas as pd
import altair as alt
import Connexion_Transfert as ct

def prepGraphJourneeType(dfTMJA,dfTMJO):
    """
    preparer une df pour realiser un graph avec Altair. le graph comprend les courbes TMJA et TMJO d'unpoint de comptage.
    in : 
        dfTMJA : df issue de calculJourneeType()
        dfTMJO : df issue de calculJourneeType()
    """
    dfTMJAStack=dfTMJA[[f'h{i}_{i+1}' for i in range (24)]].stack().reset_index().assign(type_jour='tmja')
    dfTMJOStack=dfTMJO[[f'h{i}_{i+1}' for i in range (24)]].stack().reset_index().assign(type_jour='tmjo')
    dfTMJAStack.columns, dfTMJOStack.columns=['id_comptag','heure','nb_veh','type_jour'],['id_comptag','heure','nb_veh','type_jour']
    dfVisu=pd.concat([dfTMJAStack,dfTMJOStack], axis=0, sort=False)
    dfVisu.heure=dfVisu.heure.apply(lambda x : pd.to_datetime(f"2019-01-01 {x.split('_')[0][1:]}:00:00"))
    return dfVisu

def GraphJourneeType(df,id_comptag):
    dfTMJA,dfTMJO=calculJourneeType(df)
    dfVisu=prepGraphJourneeType(dfTMJA,dfTMJO)
    return alt.Chart(dfVisu.loc[dfVisu['id_comptag']==id_comptag], title=id_comptag).mark_line().encode(x='heure', y='nb_veh', color='type_jour')
 
def graph2SensParJour(dfComp): 
    """
    a partir de la Df dfComp fournie par la classe d'erreur SensAssymetriqueError ou la fonction comparer2Sens,
    preparer une df et visu sur un graph la somme par jour pour cahque sens
    """
    dfGraph2sens=pd.concat([dfComp[['jour','type_veh','id_comptag','total_x']].rename(columns={'total_x':'nb_veh'}).assign(
        sens='sens1'),dfComp[['jour','type_veh','id_comptag','total_y']].rename(columns={'total_y':'nb_veh'}).assign(
            sens='sens2')], axis=0)
    return alt.Chart(dfGraph2sens).mark_line().encode(x='jour', y='nb_veh', color='sens')

class IdComptage(object):
    """ 
    classe permettant de regrouepr tout les infos liées à un id_comptag dans la bdd version 2021 (millesime 2020)
    """
    def __init__(self, id_comptag, bdd='local_otv_boulot'):
        """
        attributs : 
            id_comptag: text : identifiant du comptage
            bdd : text : valeur de connexion à la bdd, default 'local_otv_boulot'
            dfIdComptagBdd : dataframe recensensant tout les indicateurs agreges de toute les annees
            chartTrafic : cahart Altair contenant soit que le TMJA ou TMJA + Pc_pl si dispo
        """
        self.id_comptag=id_comptag
        self.bdd=bdd
        self.recup_indic_agreges()
        
    def recup_indic_agreges(self):
        """
        fonction de récupérartion des données d'indicateur agrege
        """
        with ct.ConnexionBdd(self.bdd) as c :
            rqt=f"select * from comptage_new.vue_indic_agrege_info_cpt where id_comptag='{self.id_comptag}'"
            self.dfIdComptagBdd=pd.read_sql(rqt,c.sqlAlchemyConn)
        if self.dfIdComptagBdd.empty : 
            raise IdComptagInconnuError(self.id_comptag)
                
    def graphTrafic(self):
        """
        fonction qui retourne un graph avec Pc_pl et TMJA, si pc_pl est présent
        """
        dfIdComptagBddTmja=self.dfIdComptagBdd.loc[self.dfIdComptagBdd['indicateur']=='tmja']
        chartTmja=alt.Chart(dfIdComptagBddTmja, 
                    title=f'{self.id_comptag} : {self.dfIdComptagBdd.type_poste.unique()[0]}',
                    width=dfIdComptagBddTmja.annee.nunique()*50).mark_bar().encode(
                    x='annee:O',
                    y=alt.Y('valeur:Q', axis=alt.Axis(title='TMJA')))
        dfIdComptagBddPcpl=self.dfIdComptagBdd.loc[self.dfIdComptagBdd['indicateur']=='pc_pl']
        if dfIdComptagBddPcpl.empty : 
            self.chartTrafic=chartTmja
        else : 
            self.chartTrafic=(chartTmja+alt.Chart(dfIdComptagBddPcpl, 
                    title=f'{self.id_comptag} : {self.dfIdComptagBdd.type_poste.unique()[0]}',
                    width=dfIdComptagBddPcpl.annee.nunique()*50).mark_line(color='red').encode(
                   x='annee:O',
                   y=alt.Y('valeur:Q', axis=alt.Axis(title='% PL'), 
                           scale=alt.Scale(domain=(0,dfIdComptagBddPcpl.valeur.max()*2 ))))).resolve_scale(y='independent')

class IdComptagInconnuError(Exception):
    """
    Exception levee si un id_comptag n'est pas présent dans la base de données
    """     
    def __init__(self, id_comptag):
        self.id_comptag=id_comptag
        Exception.__init__(self,f'le compteur {self.id_comptag} n\'est pas référencé dans la base')
