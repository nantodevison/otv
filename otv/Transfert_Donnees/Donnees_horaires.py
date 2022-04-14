# -*- coding: utf-8 -*-
'''
Created on 14 sept 2020

@author: martin.schoreisz

module d'importation des donnees de trafics forunies par les gestionnaires
'''

import pandas as pd
import re
import Outils as O
from Params.Mensuel import dico_mois

# POUR LES VACANCES PENSER A BESCULER SUR LE MODULE VACANCES_SCOLAIRE_FRANCE !!!!!
vacances_2019=[j for k in [pd.date_range('2019-01-01','2019-01-06'),pd.date_range('2019-02-16','2019-03-03'),
                            pd.date_range('2019-04-13','2019-04-28'),pd.date_range('2019-05-30','2019-06-02'),
                            pd.date_range('2019-07-06','2019-09-01'),pd.date_range('2019-10-19','2019-11-04'),
                            pd.date_range('2019-12-21','2019-12-31')] for j in k]
ferie_2019=['2019-01-01','2019-04-22','2019-05-01','2019-05-08','2019-05-30','2019-06-10',
                      '2019-07-14','2019-08-15','2019-11-01','2019-11-11','2019-12-25']
dicoFerieVacance={'2019':pd.to_datetime(vacances_2019+[pd.to_datetime(a) for a in ferie_2019 if pd.to_datetime(a) not in vacances_2019])}
attributsHoraire=[f'h{i}_{i+1}' for i in range (24)]

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

def verifValiditeFichier(dfHoraireFichier, NbHeures0Max=8):
    """
    supprimer d'une dfHoraire fciher l'ensemble des lignes pourlesquells il y a une valeur NaN, ou qui contiennent plus de 12h à 0
    Pour filtrer à la fois les deux sens dans le cas où un seul est à NaN ou à 0 on fait une liste des jour et id_comptag concernes, puis on filtre tout ces jours / id_comptage sur le fichier final
    in : 
        NbHeures0Max : int : nombre d'heure à 0 au dela duquel ion considere la ligne comme fausse
    """
    #liste des jours et id_comptag où il y a au moins une valeur NaN 
    dfNaN=dfHoraireFichier.loc[dfHoraireFichier.isna().any(axis=1)]
    listJourIdcptNaN=[(j,i) for j,i in zip(dfNaN.jour.tolist(),dfNaN.id_comptag.tolist())]
    #liste des jours et id_comptag où il y a plus de Xh à 0 veh
    df0=dfHoraireFichier.loc[dfHoraireFichier[dfHoraireFichier==0].count(axis=1)>NbHeures0Max]        
    listJourIdcpt0=[(j,i) for j,i in zip(df0.jour.tolist(),df0.id_comptag.tolist())]
    #on ne cnserve aucun de ces jours dans la df finale
    listJourIdcptARetirer=listJourIdcptNaN+listJourIdcpt0
    dfJourIdcptARetirer=pd.concat([dfNaN,df0], axis=0)
    dfHoraireFichierFiltre=dfHoraireFichier.loc[~dfHoraireFichier.apply(lambda x : (x['jour'],x['id_comptag']) in listJourIdcptARetirer, axis=1)].copy()
    return dfHoraireFichierFiltre,dfJourIdcptARetirer

def correctionHoraire(df_horaire):
    """
    A AMELIORER
    corriger une df horaire en passant à-99 les valeurs qui semble non correlees avec le reste des valeusr
    """
    #corriger les valuers inferieures  a moyenne-2*ecart_type
    for attributHeure, typeVeh, typeJour in [e+s for e in [(h,t) for h in attributsHoraire
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
                   
def comparer2Sens(dfHoraireFichierFiltre,attributSens='voie', attributIndicateur='indicateur', facteurComp=3,TauxErreur=10) : 
    """
    Pour une df horaire d'un ou plusieurs id_comptag regroupant les sections courantes et entree / sortie, comparer les sections courantes et fournir un indicateur 
    si la somme des TV et PL d'un des deux sens est superieur à 3* l'autre
    in :
        dfHoraireFichierFiltre : df de format horaire type bdd avec une description des sens en sens 1, sens 2, sens exter, sens inter
        facteurComp : integer : facteur multiplicatif limite entre les deux sens
        TauxErreur : rapport nblignInvalides/NbligneTot tolere par defaut si nblignInvalides>NbligneTot/10 on bloque
        attributSens : string : nom de l'attribut qui supporte le sens
        attributIndicateur : nom de l'attribut qui supporte le descriptif du type de vehicule
    """
    dfSc=dfHoraireFichierFiltre.loc[dfHoraireFichierFiltre[attributSens].apply(lambda x : re.sub('ç','c',re.sub('(é|è|ê)','e',re.sub('( |_)','',x.lower()))) in ('sens1','sens2','sensexter','sensinter'))].copy()
    senss=dfSc[attributSens].unique()
    
    idComptages=dfSc.id_comptag.unique()
    listDfComp=[]
    for cpt in idComptages : 
        if len(senss)==2 : 
            sens1=dfSc.loc[(dfSc[attributSens]==senss[0]) & (dfSc.id_comptag==cpt)].copy()
            sens2=dfSc.loc[(dfSc[attributSens]==senss[1]) & (dfSc.id_comptag==cpt)].copy()
        else : 
            raise SensAssymetriqueError(dfSc,dfSc, cpt)
        sens1['total']=sens1[attributsHoraire].sum(axis=1)#.groupby(['jour','type_veh']).sum()
        sens2['total']=sens2[attributsHoraire].sum(axis=1)
        dfComp=sens1[['jour',attributIndicateur,'id_comptag','total']].merge(sens2[['jour',attributIndicateur,'id_comptag','total']], 
                                                                     on=['jour',attributIndicateur,'id_comptag'])
        dfCompInvalid=dfComp.loc[(dfComp['total_x']>facteurComp*dfComp['total_y']) | (dfComp['total_y']>3*dfComp['total_x'])]
        if len(dfCompInvalid)>len(dfSc)/TauxErreur : 
            raise SensAssymetriqueError(dfCompInvalid,dfComp, cpt)
        listDfComp.append(dfComp)
    return True, pd.concat(listDfComp)   
    
def concatIndicateurFichierHoraire(dfHoraireFichier, attributIndicateur='type_veh'):
    """
    creer les données TV et PL à partir d'une dfHOraire format bdd creee
    il y a un jeu entre les fillna() de la presente et de miseEnFormeFichier() pour garder les valeusr NaN malgé les sommes
    in: 
        dfHoraireFichier : dfHoraire au foprmat bdd
        attributIndicateur : nom de l'attribut décrivant le type de vehicule
    """
    O.checkAttributsinDf(dfHoraireFichier, ['jour','id_comptag', 'fichier']+attributsHoraire)
    O.checkAttributValues(dfHoraireFichier, attributIndicateur, 'TV', 'VL', 'PL')
    dicoAgg={'h0_1':'sum', 'h1_2':'sum', 'h2_3':'sum', 'h3_4':'sum', 'h4_5':'sum', 'h5_6':'sum', 'h6_7':'sum', 'h7_8':'sum', 'h8_9':'sum',
                   'h9_10':'sum', 'h10_11':'sum', 'h11_12':'sum', 'h12_13':'sum', 'h13_14':'sum', 'h14_15':'sum', 'h15_16':'sum', 'h16_17':'sum',
                   'h17_18':'sum', 'h18_19':'sum', 'h19_20':'sum', 'h20_21':'sum', 'h21_22':'sum', 'h22_23':'sum', 'h23_24':'sum',
                   'fichier':lambda x :list(x)[0] if len(set(x))==1 else ', '.join(set(x))}
    if all([e in dfHoraireFichier[attributIndicateur].unique() for e in ('VL', 'PL')]) and 'TV' not in dfHoraireFichier[attributIndicateur].unique() :
        dfTv=dfHoraireFichier[['jour','id_comptag', 'fichier']+
             attributsHoraire].groupby(['jour','id_comptag']).agg(dicoAgg).reset_index()
        dfTv[attributIndicateur] = 'TV'
    elif 'TV' in dfHoraireFichier[attributIndicateur].unique() and 'VL' not in dfHoraireFichier[attributIndicateur].unique() : 
        dfTv=dfHoraireFichier.loc[dfHoraireFichier[attributIndicateur]=='TV'].groupby(['jour','id_comptag']
            ).agg(dicoAgg).assign(type_veh='TV').reset_index().rename(columns={'type_veh':attributIndicateur})
    else : 
        raise ValueError('des valeurs TV et VL sont mélangées')
    dfPl=dfHoraireFichier.loc[dfHoraireFichier[attributIndicateur]=='PL'][['jour','id_comptag', 'fichier']+attributsHoraire].groupby(
        ['jour','id_comptag']).agg(dicoAgg).assign(type_veh='PL').reset_index().rename(columns={'type_veh':attributIndicateur})
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
        df2=df.loc[df['indicateur']=='TV'].groupby(['id_comptag']).sum().merge(df.loc[df['indicateur']=='TV'].groupby(
            ['id_comptag']).agg({'jour':'count'}).rename(columns={'jour':'nb_jours'}),left_on='id_comptag', right_index=True)
        for attrheur in attributsHoraire:
            df2[attrheur]=df2.apply(lambda x : int(x[attrheur]/x['nb_jours']), axis=1)
        return df2
        
    dfMjo=dfHoraire.loc[dfHoraire.jour.apply(lambda x : x not in dicoFerieVacance['2019'] and x.weekday() in range(5))].copy()
    dfMja=dfHoraire.copy()

    dfMja=regrouperHoraire(dfMja)
    dfMjo=regrouperHoraire(dfMjo)
    return dfMja,dfMjo

def tmjaDepuisHoraire(dfHoraire):
    """
    fournir une df pour les tables comptage et indic_agrege depuis une df Horaire
    la df comprend les attribut id_comptag, annee, indicateur, valeur
    in :
        dfHoraire : dataframe sous forme indic_horaire, doit contenir les attributs 'id_comptag', 'annee','indicateur', 'jour'
    out : 
        dfTmjaPcpl : dataframe agrege eu format id_comptag, annee, indicaeur, valeur, 
    """
    O.checkAttributsinDf(dfHoraire, ['id_comptag', 'annee','indicateur', 'jour']+attributsHoraire)
    dfMeltInconnus=pd.melt(dfHoraire, value_vars=[c for c in dfHoraire.columns if c[0]=='h'], id_vars=['id_comptag', 'annee','indicateur', 'jour'],
                   value_name='valeur')
    dfTmja=dfMeltInconnus.groupby(['id_comptag','annee','indicateur']).agg({'valeur':'sum', 'jour':'count'}).reset_index()
    dfTmja2=dfTmja.assign(jour=dfTmja.jour/24 )
    dfTmja2['valeur']=(dfTmja2.valeur/dfTmja2.jour).astype(int)
    dfTmjaPcpl=dfTmja2.loc[dfTmja2.indicateur.str.upper()=='TV'].merge(dfTmja2.loc[dfTmja2.indicateur.str.upper()=='PL'][['id_comptag', 'valeur']], on=['id_comptag'])
    dfTmjaPcpl['pc_pl']=round(dfTmjaPcpl.valeur_y/dfTmjaPcpl.valeur_x*100, 2)
    dfTmjaPcpl.rename(columns={'valeur_x':'tmja'}, inplace=True)
    dfTmjaPcpl=pd.melt(dfTmjaPcpl, value_vars=['pc_pl', 'tmja'], id_vars=['id_comptag', 'annee'], value_name='valeur', var_name='indicateur')
    return dfTmjaPcpl

def mensuelDepuisHoraire(dfHoraire):
    """
    fournir une df pour la table indic_mensuel depuis une df Horaire
    in : 
        dfHoraire : dataframe sous forme indic_horaire, doit contenir les attributs 'id_comptag', 'annee','indicateur', 'jour'
    out : 
        dfTmjaPcpl : dataframe agrege eu format id_comptag, annee, indicaeur,mois, valeur
    """
    dfHoraireConcat = dfHoraire.copy()
    dfHoraireConcat['mois'] = dfHoraireConcat.jour.dt.month
    dfMeltInconnus = pd.melt(dfHoraireConcat, value_vars=[c for c in dfHoraireConcat.columns if c[0] == 'h'], id_vars=['id_comptag', 'annee','indicateur', 'mois', 'jour', 'fichier'],
                             value_name='valeur')
    dfTmja = dfMeltInconnus.groupby(['id_comptag', 'annee', 'indicateur', 'mois']).agg({'valeur': 'sum', 
                                                                                        'jour': lambda x: int(len(x)/24),
                                                                                        'fichier': lambda x: ' ; '.join(set(list(x)))}).reset_index()
    dfTmja['valeur'] = (dfTmja.valeur/dfTmja.jour).astype(int)
    dfTmjaVlPl = dfTmja.loc[dfTmja.indicateur.str.upper() == 'VL'].merge(dfTmja.loc[dfTmja.indicateur.str.upper() == 'PL'
                                                                                    ][['id_comptag', 'valeur', 'mois', 'fichier']], on=['id_comptag', 'mois'])
    dfTmjaVlPlForm = dfTmjaVlPl.assign(indicateur='TV', valeur_x=dfTmjaVlPl.valeur_x + dfTmjaVlPl.valeur_y)
    dfTmjaTvPlForm = dfTmja.loc[dfTmja.indicateur.str.upper() == 'TV'].merge(dfTmja.loc[dfTmja.indicateur.str.upper() == 'PL'
                                                                                    ][['id_comptag', 'valeur', 'mois', 'fichier']], on=['id_comptag', 'mois', 'fichier'])
    dfTmjaPcpl = pd.concat([dfTmjaVlPlForm, dfTmjaTvPlForm]) 
    if not dfTmjaPcpl.loc[dfTmjaPcpl.duplicated(['id_comptag', 'annee', 'indicateur', 'mois'], keep=False)].empty:
        raise ValueError(f"des doublons ['id_comptag', 'annee', 'indicateur', 'mois'] sont présents dans la df des comptages mensuels avant calcuil du pc_pl. vérifiez")
    dfTmjaPcpl['pc_pl'] = round(dfTmjaPcpl. valeur_y/dfTmjaPcpl.valeur_x*100, 2)
    dfTmjaPcpl.rename(columns={'valeur_x': 'tmja'}, inplace=True)
    dfTmjaPcpl = pd.melt(dfTmjaPcpl, value_vars=['pc_pl', 'tmja'], id_vars=['id_comptag', 'annee', 'mois'], value_name='valeur', var_name='indicateur')
    dfTmjaPcpl.mois.replace({v[0]: k for k, v in dico_mois.items()}, inplace=True)
    return dfTmjaPcpl
    

def periodeDepuisHoraire(dfHoraire):
    """
    sur la base du travail Dira, a partir de donnees de comptages permanents horaires repartis sur plusieurs fichier, calculer
    les periodes de comptage en utilisant la colonne jour
    in : 
       dfHoraire : dataframe sous forme indic_horaire, doit contenir les attributs 
    out : 
        dfMeltInconnusPeriode : dataframe avec attribut id_comptag, jourmin, jourmax, periode (au format periode bdd)
    """
    O.checkAttributsinDf(dfHoraire, ['id_comptag', 'annee','indicateur', 'jour']+attributsHoraire)
    dfMeltInconnus=pd.melt(dfHoraire, value_vars=[c for c in dfHoraire.columns if c[0]=='h'], id_vars=['id_comptag', 'annee','indicateur', 'jour'],
                   value_name='valeur')
    dfMeltInconnusPeriode=dfMeltInconnus.groupby('id_comptag').agg({'jour':[min, max]}).reset_index()
    dfMeltInconnusPeriode.columns=dfMeltInconnusPeriode.columns.droplevel(0)
    dfMeltInconnusPeriode.columns=['id_comptag', 'jourmin', 'jourmax']
    dfMeltInconnusPeriode['jourmin']=dfMeltInconnusPeriode.jourmin.astype(str).apply(lambda x : x.replace('-', '/'))
    dfMeltInconnusPeriode['jourmax']=dfMeltInconnusPeriode.jourmax.astype(str).apply(lambda x : x.replace('-', '/'))
    dfMeltInconnusPeriode['periode']=dfMeltInconnusPeriode['jourmin']+'-'+dfMeltInconnusPeriode['jourmax']
    return dfMeltInconnusPeriode

    
class SensAssymetriqueError(Exception):
    """
    Exception levee si le fichier comport emoins de 7 jours
    """     
    def __init__(self, dfCompInvalid,dfComp, id_comptag):
        self.dfCompInvalid=dfCompInvalid
        self.dfComp=dfComp
        Exception.__init__(self,f'les 2 sens de section courante {id_comptag} ont des tafics non correles, ou il n\'y a qu\'un seul sens au lieu de 2')  



