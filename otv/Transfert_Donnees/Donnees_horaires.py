# -*- coding: utf-8 -*-
'''
Created on 14 sept 2020

@author: martin.schoreisz

module d'importation des donnees de trafics forunies par les gestionnaires
'''

import pandas as pd
import re


vacances_2019=[j for k in [pd.date_range('2019-01-01','2019-01-06'),pd.date_range('2019-02-16','2019-03-03'),
                            pd.date_range('2019-04-13','2019-04-28'),pd.date_range('2019-05-30','2019-06-02'),
                            pd.date_range('2019-07-06','2019-09-01'),pd.date_range('2019-10-19','2019-11-04'),
                            pd.date_range('2019-12-21','2019-12-31')] for j in k]
ferie_2019=['2019-01-01','2019-04-22','2019-05-01','2019-05-08','2019-05-30','2019-06-10',
                      '2019-07-14','2019-08-15','2019-11-01','2019-11-11','2019-12-25']
dicoFerieVacance={'2019':pd.to_datetime(vacances_2019+[pd.to_datetime(a) for a in ferie_2019 if pd.to_datetime(a) not in vacances_2019])}

def statsHoraires(df_horaire,attributHeure, typeVeh,typeJour='semaine'):
    """
    Calculer qq stats a  partir d'une df des donnees horaires formattee  comme dans la Bdd  
    in : 
        attributHeure : nom de l'heuer a verifier, a coorlere avec les noms de colonne dans la bdd horaires
        typeVeh : type de vehiicules, 'PL', 'TV', 'VL'
        typeJour : string : semaine ou we
    """    
    df=df_horaire.loc[(df_horaire['type_veh']==typeVeh)&(~df_horaire[attributHeure].isna())&
        (df_horaire['jour'].apply(lambda x : x.dayofweek in (0,1,2,3,4)))][attributHeure] if typeJour=='semaine' else df_horaire.loc[(df_horaire['type_veh']==typeVeh)&(~df_horaire[attributHeure].isna())&
        (df_horaire['jour'].apply(lambda x : x.dayofweek in (5,6)))][attributHeure]
    ecartType=df.std()
    moyenne=df.mean()
    median=df.median()
    plageMin=moyenne-2*ecartType
    plageMax=moyenne+2*ecartType
    return ecartType,moyenne,median,plageMin,plageMax

def verifValiditeFichier(dfHoraireFichier):
    """
    supprimer d'une dfHoraire fciher l'ensemble des lignes pourlesquells il y a une valeur NaN, ou qui contiennent plus de 12h à 0
    Pour filtrer à la fois les deux sens dans le cas où un seul est à NaN ou à 0 on fait une liste des jour et id_comptag concernes, puis on filtre tout ces jours / id_comptage sur le fichier final
    """
    #liste des jours et id_comptag où il y a au moins une valeur NaN 
    dfNaN=dfHoraireFichier.loc[dfHoraireFichier.isna().any(axis=1)]
    listJourIdcptNaN=[(j,i) for j,i in zip(dfNaN.jour.tolist(),dfNaN.id_comptag.tolist())]
    #liste des jours et id_comptag où il y a plus de 12h à 0 veh
    df0=dfHoraireFichier.loc[dfHoraireFichier[dfHoraireFichier==0].count(axis=1)>8]        
    listJourIdcpt0=[(j,i) for j,i in zip(df0.jour.tolist(),df0.id_comptag.tolist())]
    #on ne cnserve aucun de ces jours dans la df finale
    listJourIdcptARetirer=listJourIdcptNaN+listJourIdcpt0
    dfJourIdcptARetirer=pd.concat([dfNaN,df0], axis=0)
    dfHoraireFichierFiltre=dfHoraireFichier.loc[~dfHoraireFichier.apply(lambda x : (x['jour'],x['id_comptag']) in listJourIdcptARetirer, axis=1)].copy()
    return dfHoraireFichierFiltre,dfJourIdcptARetirer

def correctionHoraire(df_horaire):
    """
    corriger une df horaire en passant à-99 les valeurs qui semble non correlees avec le reste des valeusr
    """
    #corriger les valuers inferieures  a moyenne-2*ecart_type
    for attributHeure, typeVeh, typeJour in [e+s for e in [(h,t) for h in [f'h{i}_{i+1}' for i in range (24)]
                                                           for t in ('VL','PL')] for s in (('semaine',), ('we',))] :
        plageMinSemaine=statsHoraires(df_horaire,attributHeure, typeVeh, typeJour)[3]
        plageMinWe=statsHoraires(df_horaire,attributHeure, typeVeh, typeJour)[3]
        if typeJour=='semaine' :
            df_horaire.loc[(df_horaire['type_veh']==typeVeh)&
                   (df_horaire['jour'].apply(lambda x : x.dayofweek in (0,1,2,3,4)))&
                   (df_horaire[attributHeure]<plageMinSemaine),attributHeure]=-99
        if typeJour=='we' :
            df_horaire.loc[(df_horaire['type_veh']==typeVeh)&
                   (df_horaire['jour'].apply(lambda x : x.dayofweek in (5,6)))&
                   (df_horaire[attributHeure]<plageMinWe),attributHeure]=-99
                   
def verifNbJoursValidDispo(df,nbJours):
    """
    verifier que pour chaque id_comptag on a bien au moins XX jours de dispo
    in :
        nbJours : integer : nb de jours mini de mesures necessaires
    """
    dfNbJour=df.groupby('id_comptag').jour.nunique().reset_index()
    idCptInvalid=dfNbJour.loc[dfNbJour['jour']<nbJours].id_comptag.tolist()
    dfCptInvalid=df.loc[df.id_comptag.isin(idCptInvalid)].copy()
    dfFinale=df.loc[~df.id_comptag.isin(idCptInvalid)].copy()
    return dfFinale, idCptInvalid,dfCptInvalid
                   
def comparer2Sens(dfHoraireFichierFiltre,facteurComp=3,TauxErreur=10) : 
    """
    Pour une df horaire d'un id_comptag regroupant les sections courantes et entree / sortie, comparer les sections courantes et fournir un indicateur 
    si le TV ou PL d'un des deux sens est superieur à 3* l'autre
    in :
        dfHoraireFichierFiltre : df de format horaire type bdd avec une description des sens en sens 1, sens 2, sens exter, sens inter
        facteurComp : integer : facteur multiplicatif limite entre les deux sens
        TauxErreur : rapport nblignInvalides/NbligneTot tolere par defaut si nblignInvalides>NbligneTot/10 on bloque
    """
    dfSc=dfHoraireFichierFiltre.loc[dfHoraireFichierFiltre.voie.apply(lambda x : re.sub('ç','c',re.sub('(é|è|ê)','e',re.sub('( |_)','',x.lower()))) in ('sens1','sens2','sensexter','sensinter'))].copy()
    senss=dfSc.voie.unique()
    if len(senss)==2 : 
        sens1=dfSc.loc[dfSc['voie']==senss[0]].copy()
        sens2=dfSc.loc[dfSc['voie']==senss[1]].copy()
    else : 
        raise SensAssymetriqueError(dfSc,dfSc)
    sens1['total']=sens1[[f'h{i}_{i+1}' for i in range (24)]].sum(axis=1)#.groupby(['jour','type_veh']).sum()
    sens2['total']=sens2[[f'h{i}_{i+1}' for i in range (24)]].sum(axis=1)
    dfComp=sens1[['jour','type_veh','id_comptag','total']].merge(sens2[['jour','type_veh','id_comptag','total']], 
                                                                 on=['jour','type_veh','id_comptag'])
    dfCompInvalid=dfComp.loc[(dfComp['total_x']>facteurComp*dfComp['total_y']) | (dfComp['total_y']>3*dfComp['total_x'])]
    if len(dfCompInvalid)>len(dfSc)/TauxErreur : 
        raise SensAssymetriqueError(dfCompInvalid,dfComp)
    else : 
        return True, dfComp
    
def concatIndicateurFichierHoraire(dfHoraireFichier):
    """
    creer les données TV et PL à partir dela dfHOraire creee par miseEnFormeFichier()
    il y a un jeu entre les fillna() de la presente et de miseEnFormeFichier() pour garder les valeusr NaN malgé les sommes
    """
    dfTv=dfHoraireFichier[['jour','id_comptag']+
         [f'h{i}_{i+1}' for i in range (24)]].groupby(['jour','id_comptag']).sum().assign(type_veh='TV').reset_index()
    dfPl=dfHoraireFichier.loc[dfHoraireFichier['type_veh']=='PL'][['jour','id_comptag']+[f'h{i}_{i+1}' for i in range (24)]].groupby(['jour','id_comptag']).sum().assign(type_veh='PL').reset_index()
    dfHoraireFinale=pd.concat([dfTv,dfPl], axis=0, sort=False)
    return dfHoraireFinale

def calculJourneeType(dfHoraire):
    """
    à partir d'un fichier horaire type Bdd, calculer une journee type selon que l'on soit en jours Ouvres (en semaine en dehaors des 
    vacances et jours feries) ou en TMJA
    """
    def regrouperHoraire(df):
        """
        regrouper unde df horaire type Bdd par id_comptag en ajoutant le nombre de jour 
        ete calculant la moyenne pour cahque heure
        """
        df2=df.loc[df['type_veh']=='TV'].groupby(['id_comptag']).sum().merge(df.loc[df['type_veh']=='TV'].groupby(
            ['id_comptag']).agg({'jour':'count'}).rename(columns={'jour':'nb_jours'}),left_on='id_comptag', right_index=True)
        for attrheur in [f'h{i}_{i+1}' for i in range (24)]:
            df2[attrheur]=df2.apply(lambda x : int(x[attrheur]/x['nb_jours']), axis=1)
        return df2
        
    dfMjo=dfHoraire.loc[dfHoraire.jour.apply(lambda x : x not in dicoFerieVacance['2019'] and x.dayofweek in range(5))].copy()
    dfMja=dfHoraire.copy()

    dfMja=regrouperHoraire(dfMja)
    dfMjo=regrouperHoraire(dfMjo)
    return dfMja,dfMjo

    
class SensAssymetriqueError(Exception):
    """
    Exception levee si le fichier comport emoins de 7 jours
    """     
    def __init__(self, dfCompInvalid,dfComp):
        self.dfCompInvalid=dfCompInvalid
        self.dfComp=dfComp
        Exception.__init__(self,f'les 2 sens de section courante ont des tafics non correles, ou il n\'y a qu\'un seul sens au lieu de 2')  



