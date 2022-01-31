# -*- coding: utf-8 -*-
'''
Created on 12 oct. 2020

@author: martin.schoreisz
Traitementsdes donnes individuelles en mixtra ou vikings et de donnees mdb fournies par les agglos
'''

import pandas as pd
from pandas.io.sql import DatabaseError
import numpy as np

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from shapely.geometry import Point
from shapely.ops import transform
import pyproj

from datetime import datetime, time
from importlib import resources
import os, re

from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split

from Params.DonneesSourcesParams import MHcorbinMaxLength, MHcorbinMaxSpeed, MHCorbinValue0, MHCorbinFailAdviceCode
import Connexion_Transfert as ct
import Outils as O
from Donnees_horaires import attributsHoraire


#tuple avec le type de jours, le nombre de jours associes et les jours (de 0à6)
tupleParams=(('mja',7,range(7)),('mjo',5,range(5)),('lundi',1,[0]), ('mardi',1,[1]), ('mercredi',1,[2]),('jeudi',1,[3]),('vendredi',1,[4]),('samedi',1,[5]),('dimanche',1,[6]))

def verifNbJour(dfVehiculesValides):
    """
    verifier le nombre de jours dans une df de donnees individuelle, et remonte une erreur si pb, sinon True
    in :     
        dfVehiculesValides : données issues de NettoyageDonnees()
    """
    O.checkAttributsinDf(dfVehiculesValides, ['date_heure',])
    nbJours=dfVehiculesValides.date_heure.dt.dayofyear.nunique()
    timstampMin=dfVehiculesValides.date_heure.min()
    timstampMax=dfVehiculesValides.date_heure.max()
    if nbJours==8 : 
        #vérif qu'il y a recouvrement
        if timstampMin.time()>timstampMax.time() : 
            raise PasAssezMesureError(nbJours-2)
    elif nbJours>8 :
        return
    else : 
        nbJours=nbJours if nbJours!=7 else nbJours-1
        raise PasAssezMesureError(nbJours)
        

def NettoyageTemps(dfVehiculesValides):
    """
    Analyser le nb de jours dispo, lever un erreur si inférieur à 7, si 7 verifier que recouvrement sinon erreur
    ensuite si besoin fusion du 1er et dernier jours, sinon suppression jours incomplet
    in  : 
        dfVehiculesValides : données issues de NettoyageDonnees()
    out : 
        dfValide : dataframe modifié en date selon conditions, sinon egale a l'entree
    """
    #analyse du nombre de jours mesurés 
    O.checkAttributsinDf(dfVehiculesValides, ['date_heure'])
    nbJours=dfVehiculesValides.date_heure.dt.dayofyear.nunique()
    timstampMin=dfVehiculesValides.date_heure.min()
    timstampMax=dfVehiculesValides.date_heure.max()
    #si nb jour==8 : (sinon <8 erreur )
    if nbJours==8 : 
        #vérif qu'il y a recouvrement
        if timstampMin.time()<=timstampMax.time() : 
            #on relimite la df : 
            dfValide=dfVehiculesValides.loc[dfVehiculesValides.date_heure>datetime.combine(timstampMin.date(),timstampMax.time())].copy()
            #puis on triche et on modifie la date du dernier jour pour le mettre sur celle du 1er, en conservant l'heure
            dfValide.loc[dfValide.date_heure.dt.dayofyear==timstampMax.dayofyear,'date_heure']=dfValide.loc[dfValide.date_heure.
               dt.dayofyear==timstampMax.dayofyear].apply(lambda x : datetime.combine(timstampMin.date(),x['date_heure'].time()),axis=1) 
        else :
            # on triche et on modifie la date du 1er jour pour le mettre sur celle du dernier, en conservant l'heure
            dfValide=dfVehiculesValides.copy()
            dfValide.loc[dfValide.date_heure.dt.dayofyear==timstampMin.dayofyear,'date_heure']=dfValide.loc[dfValide.date_heure.
               dt.dayofyear==timstampMin.dayofyear].apply(lambda x : datetime.combine(timstampMax.date(),x['date_heure'].time()),axis=1)
    elif nbJours>8 :#si nb jour >8 on enleve les premier ete dernier jours
        dfValide=dfVehiculesValides.loc[~dfVehiculesValides.date_heure.dt.dayofyear.isin((timstampMin.dayofyear,timstampMax.dayofyear))].copy()
    else : # si le nombre de est inférieur à 8
        dfValide=dfVehiculesValides.copy()
        dfValide.loc[dfValide.date_heure.dt.dayofyear==timstampMin.dayofyear,'date_heure']=dfValide.loc[dfValide.date_heure.
        dt.dayofyear==timstampMin.dayofyear].apply(lambda x : datetime.combine(timstampMax.date(),x['date_heure'].time()),axis=1)
    return dfValide

def GroupeCompletude(dfValide, frequence='1H', vitesse=False):
    """
    Pour les donnees individuelles uniquement
    a partir des donnees nettoyees sur le temps, regouper les donnees par tranche horaire, type et sens 
    in : 
        dfValide : df nettoyees avec les attributs date_heure, nbVeh et sens, cf fonction NettoyageTemps()
        vitese : booelean : traduit si les donnees de vitesse doivent etre associees ou non
        frequence : string de description du regroupement par durée. par défaut : '1H', cf https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases pour les alias
    out : 
        dfHeureTypeSens : df avec attribut date_heure, type_veh, sens, nbVeh et si vitesse : V10, V50, V85
    """
    O.checkAttributsinDf(dfValide, ['date_heure', 'sens', 'type_veh', 'nbVeh'])
    if vitesse : 
        O.checkAttributsinDf(dfValide, 'vitesse')
    print(dfValide.sens.unique())    
    O.checkAttributValues(dfValide, 'sens', 'sens1', 'sens2')
    O.checkAttributValues(dfValide, 'type_veh', 'TV', 'VL', 'PL', '2R')
        
    dfGroupTypeHeure=dfValide.set_index('date_heure').groupby([pd.Grouper(freq=frequence),'type_veh','sens'])['nbVeh'].count().reset_index().sort_values('date_heure')
    #completude des données
    #ajout des données horaires à 0 si aucun type de vehicules mesures
    date_range=pd.date_range(dfGroupTypeHeure.date_heure.min(),dfGroupTypeHeure.date_heure.max(),freq=frequence)
    #df de comparaison
    dfComp=pd.DataFrame({'type_veh':['2R','PL','VL']}).assign(key=1).merge(pd.DataFrame({'date_heure':date_range}).assign(key=1), on='key').merge(
    pd.DataFrame({'sens':['sens1','sens2']}).assign(key=1)).sort_values(['date_heure', 'type_veh','sens'])[['date_heure','type_veh','sens']]
    #df des données amnquantes
    dfManq=dfComp.loc[dfComp.apply(lambda x : (x['date_heure'],x['type_veh'],x['sens']) not in zip(dfGroupTypeHeure.date_heure.tolist(),dfGroupTypeHeure.type_veh.tolist(),
                      dfGroupTypeHeure.sens.tolist()), axis=1)].copy()
    dfHeureTypeSens=pd.concat([dfManq,dfGroupTypeHeure],axis=0).fillna(0).sort_values(['date_heure', 'type_veh','sens'])
    dfHeureTypeSens['jour']=dfHeureTypeSens.date_heure.dt.dayofweek
    dfHeureTypeSens['jourAnnee']=dfHeureTypeSens.date_heure.dt.dayofyear
    dfHeureTypeSens['heure']=dfHeureTypeSens.date_heure.dt.hour
    dfHeureTypeSens['date']=pd.to_datetime(dfHeureTypeSens.date_heure.dt.date)
    dfHeureTypeSens=pd.concat([dfHeureTypeSens.groupby(['date_heure','sens']).agg({'nbVeh':'sum','heure':lambda x : x.unique()[0], 
                        'jour':lambda x : x.unique()[0],'jourAnnee':lambda x : x.unique()[0],'date':lambda x : x.unique()[0]}
                        ).assign(type_veh='TV').reset_index(),dfHeureTypeSens], axis=0, sort=False).sort_values(['date_heure', 'type_veh'])
    if vitesse : 
        dfGroupVtsHeure=pd.concat([dfValide.set_index('date_heure').groupby([pd.Grouper(freq=frequence),'sens']).agg(
            **{'v10': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,10)),
               'v50': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,50)),
               'v85': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,85))}).reset_index().sort_values('date_heure').assign(type_veh='TV'),
           dfValide.set_index('date_heure').groupby([pd.Grouper(freq=frequence),'sens', 'type_veh']).agg(
            **{'v10': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,10)),
               'v50': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,50)),
               'v85': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,85))}).reset_index().sort_values('date_heure')])
        dfHeureTypeSens=dfHeureTypeSens.merge(dfGroupVtsHeure, on=['date_heure','sens', 'type_veh'],how='left').sort_values(['date_heure', 'type_veh', 'sens'])
    
    #si un sens est completement à O on le vire
    dfTestSens=dfHeureTypeSens.groupby('sens').nbVeh.sum().reset_index()
    if (dfTestSens.nbVeh==0).any() :
        dfHeureTypeSens=dfHeureTypeSens.loc[dfHeureTypeSens.sens==dfTestSens.loc[dfTestSens.nbVeh!=0].sens.values[0]]
       
    return dfHeureTypeSens

def donneesIndiv2HoraireBdd(dfHeureTypeSens):
    """
    transformer une df de donnees individuelles en df format horaire de la Bdd
    in :
        dfHeureTypeSens : dataframe comrenant les attributs principaux des donnees individuelle. issue de dfHeureTypeSens
    out : 
        dataframe avec date et indictauer + heures en atteributs
    """
    O.checkAttributsinDf(dfHeureTypeSens, ['date', 'heure', 'nbVeh'])
    dfHoraireTmja=dfHeureTypeSens.loc[dfHeureTypeSens.type_veh.str.lower()!='tv'].groupby(['date', 'heure']).nbVeh.sum().reset_index().rename(
        columns={'date':'jour','nbVeh':'valeur'}).assign(indicateur='TV').pivot(index=['jour', 'indicateur'], columns='heure', values='valeur'
                                                                               ).rename(columns={n:f'h{n}_{n+1}' for n in range(24)})
    dfHorairePl=dfHeureTypeSens.loc[dfHeureTypeSens.type_veh.str.lower()=='pl'].groupby(['date', 'heure']).nbVeh.sum().reset_index().rename(
        columns={'date':'jour','nbVeh':'valeur'}).assign(indicateur='PL').pivot(index=['jour', 'indicateur'], columns='heure', values='valeur'
                                                                               ).rename(columns={n:f'h{n}_{n+1}' for n in range(24)})
    return pd.concat([dfHoraireTmja, dfHorairePl])
    

def NombreDeJours(dfHeureTypeSens):
    """
    a partir d'une df groupees par heure et complete, deduire le nombre de jours plein de comptage, ouvres, samedi, dimanche...
    in :
        dfHeureTypeSens : df avec ena ttribut : date_heure (timestamp horaire), type_veh ('2r',vl,pl,tv), sens (sens1, sens2), nbVeh (integer) cf GroupeCompletude()
    out : 
        un dico avec en key nbJours, nbJoursOuvres,samedi,dimanche
    """
    dfNbJours=dfHeureTypeSens.set_index('date_heure').resample('1D').count().reset_index().date_heure.dt.dayofweek.value_counts().rename('nbOcc')
    return {'dfNbJours':dfNbJours,
            'nbJours':dfHeureTypeSens.date_heure.dt.dayofyear.nunique(),
            'nbJoursOuvres' : dfHeureTypeSens.loc[dfHeureTypeSens.date_heure.dt.dayofweek.isin(range(5))].date_heure.dt.dayofyear.nunique()}

def semaineMoyenne(dfHeureTypeSens, vitesse=False):
    """
    a partir des donnees pour chaque heure, type de veh et sens, et en fonction du nombre de jours de mesure,
    obtenir un semaine moyenne
    """
    dicoNbJours=NombreDeJours(dfHeureTypeSens)
    dfMoyenne=dfHeureTypeSens.groupby(['jour','heure','type_veh','sens']).nbVeh.sum().reset_index().merge(dicoNbJours['dfNbJours'],left_on='jour', right_index=True)#bon là je pense qu'on peut remplacer ça et la ligne en dessous par nbVeh.mean(). a verifier
    dfMoyenne['nbVeh']=dfMoyenne['nbVeh']/dfMoyenne['nbOcc']
    if vitesse : 
        dfVitesseMoyenne=dfHeureTypeSens.groupby(['jour','heure','sens']).agg({'v10':lambda x : x.quantile(0.5),'v50':lambda x : x.quantile(0.5) ,
                                                                       'v85':lambda x : x.quantile(0.5) }).reset_index()
        dfMoyenne=dfMoyenne.merge(dfVitesseMoyenne, on=['jour','heure','sens'], how='left')
    return dfMoyenne
    
def IndicsGraphs(dfMoyenne, typesVeh, typesDonnees='all', sens='all', typeSource='indiv'):
    """
    fonction de creation des donnes tabulees et graphs de rendu des donnes de trafic
    in :
       dfMoyenne :  df des données agregées par type de jour, heure, type de veuhicules et sens, isu de semaineMoyenne
       types veh : list des types de vehciules souhaites parmi 'tv','vl','pl','2r'
       typesDonnees : list des types de donnée souhiates parmi mja, mjo, samedi, dimanche)
       sens : list des sens que l'on souhaite, parmi : sens1, sens2, 2sens
    out : 
        dicoHoraire : dictionnaire imbriqué à 3 niveaux avec le type de données, le sens, le type de rendu
                     (exemple : pour avoir les données en mja, pour les 2sens : dico['mja']['2sens']['donnees']
    """
    dicoHoraire={e[0]:{'nbJour':e[1],'listJours':e[2] } for e in tupleParams}
    dicoJournalier={e[0]:{'nbJour':e[1],'listJours':e[2] } for e in tupleParams if e[0] in ('mja','mjo')}
    #generralisation si all : 
    sens=np.append(dfMoyenne.sens.unique(),'2sens') if sens=='all' else [sens,]
    typesDonnees=dicoHoraire.keys() if typesDonnees=='all' else typesDonnees
    
    
    #calcul des donnees
    for t in  typesDonnees : 
        for s in sens :     
            dicoHoraire[t][s]={}
            dicoHoraire[t][s]['donnees']=round((dfMoyenne.loc[(dfMoyenne.jour.isin(dicoHoraire[t]['listJours'])) &
                                            (dfMoyenne.type_veh.isin(typesVeh))].groupby(['heure','type_veh','sens']).
                                             nbVeh.sum()/dicoHoraire[t]['nbJour']),0).reset_index() if s=='2sens' else round((
                                            dfMoyenne.loc[(dfMoyenne.jour.isin(dicoHoraire[t]['listJours'])) &
                                            (dfMoyenne.type_veh.isin(typesVeh)) & (dfMoyenne.sens==s)
                                            ].groupby(['heure','type_veh','sens']).
                                             nbVeh.sum()/dicoHoraire[t]['nbJour']),0).reset_index()            
            dicoHoraire[t][s]['graph']=px.line(dicoHoraire[t][s]['donnees'].loc[dicoHoraire[t][s]['donnees'].type_veh.isin(typesVeh)].groupby(
                                       ['heure','type_veh']).sum().reset_index(),x='heure', y='nbVeh', color='type_veh',
                                       title=f'Evolution horaire moyenne {t} {s}') if s=='2sens' else px.line(
                         dicoHoraire[t][s]['donnees'].loc[(dicoHoraire[t][s]['donnees'].type_veh.isin(typesVeh)) & 
                         (dicoHoraire[t][s]['donnees'].sens==s)].groupby(['heure','type_veh']).sum().reset_index(),
                         x='heure', y='nbVeh', color='type_veh', title=f'Donnees horaire {t} {s}')
            if t == 'mja' :
                dicoJournalier[t][s]={}
                dicoJournalier[t][s]['donnees']=round(dfMoyenne.loc[(dfMoyenne.jour.isin(dicoHoraire[t]['listJours'])) & 
                                                (dfMoyenne.type_veh.isin(typesVeh))].groupby(['jour','type_veh','sens']).nbVeh.sum(),0
                                ).reset_index() if s=='2sens' else round(dfMoyenne.loc[(dfMoyenne.jour.isin(dicoHoraire[t]['listJours'])) 
                                                 & (dfMoyenne.sens==s) & (dfMoyenne.type_veh.isin(typesVeh))].groupby(
                                                     ['jour','type_veh','sens']).nbVeh.sum(),0).reset_index()
                dicoJournalier[t][s]['donnees'].sort_values(['jour'], inplace=True)
                dicoJournalier[t][s]['donnees']['jour']=dicoJournalier[t][s]['donnees']['jour'].replace(
                    {0:'lundi', 1:'mardi', 2:'mercredi', 3:'jeudi',4:'vendredi',5:'samedi',6:'dimanche'}) 
                dicoJournalier[t][s]['graph']=px.bar(dicoJournalier[t][s]['donnees'].loc[dicoJournalier[t][s]['donnees'].type_veh.
                                              isin(typesVeh)].groupby(['jour','type_veh']).nbVeh.sum().reset_index(),x="jour", 
                                              y="nbVeh", color="type_veh", barmode="group", title=f'Donnees Journalieres par type de vehicules {s}',
                                              category_orders={"jour": ["lundi", "mardi", "mercredi", "jeudi",'vendredi', 'samedi', 'dimanche']}) if s=='2sens' else px.bar(
                                              dicoJournalier[t][s]['donnees'].loc[(dicoJournalier[t][s]['donnees'].type_veh.
                                               isin(typesVeh)) & (dicoJournalier[t][s]['donnees'].sens==s)],x="jour", 
                                              y="nbVeh", color="type_veh", barmode="group",title=f'Donnees Journalieres par type de vehicules {s}',
                                              category_orders={"jour": ["lundi", "mardi", "mercredi", "jeudi",'vendredi', 'samedi', 'dimanche']})
                
                if s=='2sens' : 
                    dicoJournalier[t]['compSens']={}
                    dicoJournalier[t]['compSens']['donnees']=round((dfMoyenne.loc[(dfMoyenne.jour.isin(dicoHoraire[t]['listJours'])) & (dfMoyenne.type_veh.isin(typesVeh))
                        ].groupby(['jour','sens']).nbVeh.sum()),0).reset_index()
                    dicoJournalier[t]['compSens']['donnees'].sort_values('jour', inplace=True)
                    dicoJournalier[t]['compSens']['donnees']['jour']=dicoJournalier[t]['compSens']['donnees']['jour'].replace(
                        {0:'lundi', 1:'mardi', 2:'mercredi', 3:'jeudi',4:'vendredi',5:'samedi',6:'dimanche'}) 
                    dicoJournalier[t]['compSens']['graph']=px.bar(dicoJournalier[t]['compSens']['donnees'],
                                                                  x="jour", y="nbVeh", color="sens", barmode="group",
                                                                  title=f' Comparaison journaliere des sens',
                                                                  category_orders={"jour": ["lundi", "mardi", "mercredi", "jeudi",'vendredi', 'samedi', 'dimanche']})            
    return dicoHoraire, dicoJournalier

def graphsGeneraux(dfMoyenne,dicoHoraire, dicoJournalier,typesVeh, vitesse=False):
    nbSens=dfMoyenne.sens.nunique()
    if nbSens==2 :
        sens='2sens'
        nbRows=5 
        subplot_titles=('évolutions horaires moyenne Jours Ouvrable','évolution moyenne des vitesses Jours Ouvrable sens 1',
                        'évolution moyenne des vitesses Jours Ouvrable sens 2','évolution journaliere moyenne', 'comparaison des sens de circulation')
    else : 
        sens=dfMoyenne.sens.values[0]
        nbRows=3
        subplot_titles=('évolutions horaires moyenne Jours Ouvrable','évolution moyenne des vitesses',
                        'évolution journaliere moyenne')
        
    figSyntheses = make_subplots(rows=nbRows, cols=1,subplot_titles=subplot_titles,
                                                        horizontal_spacing=0.05,vertical_spacing =0.05)

    for i in range(len(typesVeh)) :
        figSyntheses.add_trace(dicoHoraire['mjo'][sens]['graph']['data'][i], row=1, col=1)
        figSyntheses.add_trace(dicoJournalier['mja'][sens]['graph']['data'][i], row=4, col=1)
    
    if vitesse :
        for v in ('v10','v50','v85') : 
            vts=dfMoyenne.loc[(dfMoyenne.jour.isin(dicoHoraire['mjo']['listJours'])) & (dfMoyenne.sens=='sens1')
               ].groupby(['heure','sens'])[v].mean().reset_index()
            figSyntheses.add_trace(go.Scatter(x=vts['heure'], y=vts[v], name=v),row=2, col=1)
        for v in ('v10','v50','v85') : 
            vts=dfMoyenne.loc[(dfMoyenne.jour.isin(dicoHoraire['mjo']['listJours'])) & (dfMoyenne.sens=='sens2')
               ].groupby(['heure','sens'])[v].mean().reset_index()
            figSyntheses.add_trace(go.Scatter(x=vts['heure'], y=vts[v], name=v),row=3, col=1)
    else : 
        figSyntheses.add_trace(go.Scatter(x=[1,],y=[1],mode="markers+text",name="Markers and Text",text=["PAS DE DONNEES VITESSES"],textfont_size=40),row=2, col=1)
        figSyntheses.add_trace(go.Scatter(x=[1,],y=[1],mode="markers+text",name="Markers and Text",text=["PAS DE DONNEES VITESSES"],textfont_size=40),row=3, col=1)                
    
    for i in [0,1] :
        figSyntheses.add_trace(dicoJournalier['mja']['compSens']['graph']['data'][i], row=5, col=1)
    figSyntheses.update_layout(height=2000, width=1500,title_text="Données synthétiques")
    
    figJournaliere = make_subplots(rows=7, cols=2, specs=[[{"secondary_y": True},{"secondary_y": True}],[{"secondary_y": True},{"secondary_y": True}],[{"secondary_y": True},{"secondary_y": True}],[{"secondary_y": True},{"secondary_y": True}],
               [{"secondary_y": True},{"secondary_y": True}],[{"secondary_y": True},{"secondary_y": True}],[{"secondary_y": True},{"secondary_y": True}]],
               subplot_titles=[j+' '+s for j in ('lundi', 'mardi', 'mercredi', 'jeudi','vendredi','samedi','dimanche') for s in ('sens 1','sens 2')],
               horizontal_spacing=0.05,
               vertical_spacing =0.05)
    #donnees
    data=dfMoyenne[['jour','heure','sens','v85']].drop_duplicates() if vitesse else dfMoyenne[['jour','heure','sens']].drop_duplicates()
    #ajout des differents cas : pour chaque jour et cahque sens
    for i in range(7) :
        for j in range(1,3) : 
            traf=dfMoyenne.loc[(dfMoyenne.jour==i) & (dfMoyenne.sens==f'sens{j}') & (dfMoyenne.type_veh.isin(typesVeh))][['jour','heure','sens','type_veh','nbVeh']]
            data_j=data.loc[(data.jour==i) & (data.sens==f'sens{j}')]
            if vitesse :
                fig1 = px.line(data_j,x='heure', y='v85')
                f1=fig1['data'][0]
                figJournaliere.add_trace(f1, row=i+1, col=j,secondary_y=True)
            fig2 = px.bar(traf,x='heure', y='nbVeh', color='type_veh',barmode='group' )
            dicoTypeVeh={t:fig2['data'][i] for i,t in enumerate(traf.type_veh.unique())}
            if i!=0 or j!=1 : 
                for v in dicoTypeVeh.values() : 
                    v.showlegend=False
            for v in dicoTypeVeh.values() :
                figJournaliere.add_trace(v, row=i+1, col=j)
            figJournaliere.update_xaxes(title_text="heure", row=i, col=j)
    figJournaliere.update_layout(height=3000, width=1500,title_text="Données journalieres")
    figJournaliere.add_annotation(xref='paper',yref='paper',x=0, y=1, text="Nombre de Veh",arrowhead=2)
    figJournaliere.add_annotation(xref='paper',yref='paper',x=0.5, y=1, text="Nombre de Veh",arrowhead=2)
    if vitesse :
        figJournaliere.add_annotation(xref='paper',yref='paper',x=0.43, y=1, text="vitesse", arrowhead=2)
        figJournaliere.add_annotation(xref='paper',yref='paper',x=0.93, y=1, text="vitesse", arrowhead=2)
    
    for i in range(len(figSyntheses['data'])) : 
        if isinstance(figSyntheses['data'][1],go.Bar) : 
            figSyntheses['data'][1]['x']=np.array(["lundi", "mardi", "mercredi", "jeudi",'vendredi', 'samedi', 'dimanche']) 
            
    return figSyntheses, figJournaliere

def IndicsPeriodes(dfMoyenne):
    """
    fournir les indicateurs suer periode hpm, hps, jour et nuit, sous frome de df par sens
    in :
        dfMoyenne :  df des données agregées par type de jours, heure, type de veuhicules et sens, iisu de semaineMoyenne()
    """
    hpm=round(dfMoyenne.loc[dfMoyenne.heure.isin(range(7,10))].groupby(['sens','type_veh']).nbVeh.sum()/3,0).reset_index()
    hps=round(dfMoyenne.loc[dfMoyenne.heure.isin(range(16,19))].groupby(['sens','type_veh']).nbVeh.sum()/3,0).reset_index()
    nuit=round(dfMoyenne.loc[dfMoyenne.heure.isin([a for a in range(6)]+[22,23])].groupby(['sens','type_veh']).nbVeh.sum()/8,0).reset_index()
    jour=round(dfMoyenne.loc[dfMoyenne.heure.isin(range(6,22))].groupby(['sens','type_veh']).nbVeh.sum()/16,0).reset_index()
    dfMoyenneHorairesSpeciales=pd.concat([hpm.assign(typeHeure='HPM'),hps.assign(typeHeure='HPS'),jour.assign(typeHeure='Jour'),
                                          nuit.assign(typeHeure='Nuit')])
    return hpm,hps,nuit,jour,dfMoyenneHorairesSpeciales

def JoursCharges(dfHeureTypeSens):
    """
    pourchaque type de vehicule connaite le jour le plus charge
    in  :
        dfHeureTypeSens :  df des données agregées par heure, type de veuhicules et sens, iisu de GroupeCompletude
    out : 
        df avec date, type de veh et nombre de veh
    """
    data_temp=dfHeureTypeSens.set_index('date_heure').groupby([pd.Grouper(freq='1D'),'type_veh']).nbVeh.sum().reset_index()
    return data_temp.loc[data_temp.nbVeh==data_temp.groupby('type_veh').nbVeh.transform(max)]

class Mixtra(object):
    '''
    Donnees issues de compteurs a tubes
    pour chaque point de comptage on peut avoir 1 ou 2 sens. 
    pour chaque sens on peut avoir 1 ou plusieurs fichiers 
    '''
    
    def __init__(self, fichier):
        '''
        Constructor
        in : 
            listFichiersSens1 : la liste des fcihiers utilisé pour le sens 1
            listFichiersSens 2 : la liste des fcihiers utilisés pour le sens 2
        attributs : 
            dfFichier : dataframe de sortie, une ligne par veh
        '''
        self.fichier=fichier
        self.dfFichier=self.NettoyageDonnees(self.ouvrirFichier())
        
    def ouvrirFichier(self):
        """
        ouvrir le ou les fichiers
        """
        try : 
            df=pd.read_csv(self.fichier,delimiter='\0|\t', encoding='latin_1', engine='python'
                           ).dropna(axis=0, how='all')
            df.columns=['ASupr']+[c for c in df.columns][:-1]
            df.drop('ASupr', axis=1, inplace=True)
        except pd._libs.parsers.ParserError :
            df=pd.read_excel(self.fichier, dtype={'Horodate de passage':str})
            df['Horodate de passage']=df['Horodate de passage'].str[:16]
        return df
    
    def NettoyageDonnees(self,dfFichier):
        """
        a partir du fichier2Sens, virer les veh non valide et ajouter un attribut  sur le type de veh et un horadatage complet
        """
        def type_vehicule(silhouette) : 
            """
            deduire le type de v�hicule � partir de la classification par silhouette
            in : 
                silhouette : int
            """
            if silhouette in (1,12) : 
                return 'VL'
            elif silhouette==13 : 
                return '2R'
            else : return 'PL'
        dfVehiculesValides=dfFichier.loc[dfFichier['Véhicule Valide']==1].copy()
        dfVehiculesValides['date_heure']=pd.to_datetime(dfVehiculesValides['Horodate de passage']+':'+dfVehiculesValides['Seconde'].
                                                        astype(int).astype(str)+'.'+dfVehiculesValides['Centième'].astype(int).astype(str), dayfirst=True)
        dfVehiculesValides['type_veh']=dfVehiculesValides.Silhouette.apply(lambda x : type_vehicule(x))
        dfVehiculesValides.rename(columns={'Véhicule Valide':'nbVeh','Vitesse (km/h)':'vitesse'}, inplace=True)
        dfVehiculesValides['sens'] = dfVehiculesValides['Voie'].apply(lambda x: f'sens{int(x+1)}')
        return dfVehiculesValides      
        
class Viking(object):
    '''
    Donnees issues de compteurs radar
    pour chaque point de comptage on peut avoir 1 ou 2 sens. 
    pour chaque sens on peut avoir 1 ou plusieurs fichiers 
    '''
    
    def __init__(self, fichier):
        '''
        Constructor
        in : 
            fichier : raw string du fcihiers utilisé
        attributs : 
            fichier
            dfFichier : dataframe de sortie, une ligne par veh
            anneeDeb
            moisDeb
            jourDeb
        '''
        self.fichier=fichier
        self.dfFichier, self.anneeDeb,self.moisDeb,self.jourDeb=self.ouvrirFichier()
        self.formaterDonnees()
        
    def ouvrirFichier(self):
        """
        pour chaque sens, les regrouper et mettre ne form les attributs
        """
        with open(self.fichier) as f :
                entete=[e.strip() for e in f.readlines()][0]
        anneeDeb,moisDeb,jourDeb=(entete.split('.')[i].strip() for i in range(5,8)) 
        print(entete)
        dfFichier=pd.read_csv(self.fichier,delimiter=' ',skiprows=1, 
                                 names=['sens', 'jour', 'heureMin','secCent', 'vts', 'ser', 'type_veh'],dtype={'heureMin':str,'secCent':str})
        return dfFichier,anneeDeb,moisDeb,jourDeb
    
    
    def creerDate(self):
        """
        creer les attributs relatif a l'horodatage
        """
        # ramener le jour de l'enregistrement suivant
        self.dfFichier['jour_supp'] = self.dfFichier.jour-self.dfFichier.jour.shift(1)
        #corriger les changements de mois
        jourNouveauMois = self.dfFichier.loc[self.dfFichier.jour_supp < 0, 'jour']
        self.dfFichier.loc[self.dfFichier.jour_supp < 0, 'jour_supp'] = 1 + jourNouveauMois - 1
        # calcul de la nouvelle date
        self.dfFichier['date_ref'] = self.dfFichier.iloc[0, self.dfFichier.columns.get_loc('date_heure')]
        self.dfFichier['jour_supp_tot'] = self.dfFichier.jour_supp.cumsum()
        self.dfFichier.jour_supp_tot.fillna(method='bfill', inplace=True)
        self.dfFichier['jour_supp_tot'] = self.dfFichier['jour_supp_tot'].apply(lambda x: pd.to_timedelta(str(x)+'D'))
        self.dfFichier['date_increment'] = (self.dfFichier['jour_supp_tot']+self.dfFichier['date_ref']).dt.date
        self.dfFichier['heureNew'] = self.dfFichier.apply(lambda x: time(int(x.heureMin[:2]), int(x.heureMin[2:]), int(x.secCent[:2]), int(x.secCent[2:])*10000), axis=1)
        self.dfFichier['date_heure'] = self.dfFichier.apply(lambda x: pd.Timestamp.combine(x.date_increment, x.heureNew), axis=1)


    def formaterDonnees(self):
        """
        ajouter l'attribut de date, modifier le sens et le type de vehicule pour coller au format de la classe Mixtra
        """
        def creer_date(jourDeb,moisDeb,anneeDeb, jourMesure,heureMin,secCent) : 
            """
            creer la date d'acquisition. Attention : si comptage sur un mois entier ça ne marche pas
            """
            # gerer le changement d'annee
            if jourMesure<int(jourDeb) : 
                if 1 <= int(moisDeb) < 12:
                    moisModif = str(int(moisDeb)+1)
                    anneeModif = anneeDeb
                elif int(moisDeb) == 12:
                    moisModif = '1'
                    anneeModif = str(int(anneeDeb)+1)
                else:
                    raise ValueError("le mois n'est pas entre 1 et 12")
            else:
                anneeModif = anneeDeb
                moisModif = moisDeb
            return pd.to_datetime(f'20{anneeModif}-{moisModif}-{jourMesure} {str(heureMin)[:2]}:{str(heureMin)[2:]}:{str(secCent)[:2]}.{str(secCent)[2:]}')
        
        
        self.dfFichier['date_heure']=self.dfFichier.apply(lambda x : creer_date(self.jourDeb,self.moisDeb,self.anneeDeb, 
                            x['jour'],x['heureMin'],x['secCent']), axis=1)
        self.creerDate()
        self.dfFichier['type_veh']=self.dfFichier['type_veh'].str.upper()
        self.dfFichier['nbVeh']=1
        self.dfFichier['vitesse']=self.dfFichier.vts.str[2:].astype(int)
        self.dfFichier['sens']=self.dfFichier['sens'].apply(lambda x: f'sens{x}')
            
class ComptageDonneesIndiv(object):
    """
    a partir de données individuelles de sens 1 et parfois sens 2, creer les donnees 2 sens
    ces donnees indiv peuvent etre issue que de Mixtra ou que de Viking ou de melange
    """
    def __init__(self,dossier, vitesse=False, verifNbJoursComptage=False):
        """
        in : 
           dossier : raw string du dossier qui conteint les données individuelles 
           vitesse : boolean : traiter ou non les vitesse
        atributs : 
            dossier : raw string de l'emplacement du dossier
            vitesse : boolean : traiter ou non les vitesse
            verifNbJoursComptage : boolean sdi True, on verifie que le nb de jours est suffisant (erreursi pas suffisant). si false on prend tel quel
            df2SensBase : df des passages d echaque vehicule poyr les 2 sens avec horodatage et type_veh
            dfHeureTypeSens : regrouepement des passages par heure, type de vehicule et sens cf GroupeCompletude()
            dicoNbJours : dictionnaire caracterisant les nombr de jours par type sur la periode de dcomptage, cf NombreDeJours()a
            dfSemaineMoyenne : dataframe horaire moyenne par jour (contient les 7 jours de la semaine avec 24 h par jour)
        """
        self.dossier=dossier
        self.vitesse=vitesse
        self.verifNbJoursComptage = verifNbJoursComptage
        self.df2SensBase= self.dfSens(self.analyseSensFichiers())
        self.dfHeureTypeSens,self.dicoNbJours,self.dfSemaineMoyenne=self.MettreEnFormeDonnees()
        
   
    def analyseSensFichiers(self):
        """
        connaitre qui est le sens1, qui est le sens2.
        les fichiers doivent contenir s1 ou s2 ou sens1 ou sens2 (pas de pb de casse)
        si personne erreur
        out :
            listFichiersSens1 : list de chemin en raw string
            listFichiersSens2 : list de chemin en raw string
        """
        with os.scandir(self.dossier) as it:
            listFichiers = set([os.path.join(self.dossier,f.name) for f in it 
                                          if f.is_file() and f.name.lower().endswith(('.vik','.xls'))])
        return listFichiers
            
    def dfSens(self,listFichiers):
        """
        pour chaque sens obtenir la liste des fichiers Mixtra ou Viking ou les deux
        in :
            listFichiers : list de chemin en raw string, issu de analyseSensFichiers()
        """
        listDfSens = []
        listAttributs=['sens', 'date_heure','nbVeh','type_veh'] if not self.vitesse else ['sens','date_heure','nbVeh','type_veh','vitesse'] 
        for f in listFichiers : 
            if f.lower().endswith('.xls') : 
                listDfSens.append(Mixtra(f).dfFichier[listAttributs])
            elif f.lower().endswith('.vik') :
                listDfSens.append(Viking(f).dfFichier[listAttributs])
        df2Sens = pd.concat(listDfSens, axis=0)
        return df2Sens
    
       
    def MettreEnFormeDonnees(self, frequence='1H'):
        """
        pour chaque sens et 2 sens confondus : gerer les cas ou l enb de jours n'est pas o cf NettoyageTemps()k
       ensuite on regroupe les donnees par heure et type de vehicule et sens, cf GroupeCompletude() 
        """
        if self.verifNbJoursComptage:
            dfValide = NettoyageTemps(self.df2SensBase)
        else:
            dfValide = self.df2SensBase
        dfHeureTypeSens=GroupeCompletude(dfValide, frequence=frequence, vitesse=self.vitesse)
        dicoNbJours=NombreDeJours(dfHeureTypeSens)
        dfMoyenne=semaineMoyenne(dfHeureTypeSens,self.vitesse)
        return dfHeureTypeSens,dicoNbJours,dfMoyenne
    
    def graphsSynthese(self,typesVeh, typesDonnees='all', sens='all', vitesse=False, synthese=False):
        """
        creer les graphs pour visu
        """
        self.dicoHoraire,self.dicoJournalier=IndicsGraphs(self.dfSemaineMoyenne,typesVeh,typesDonnees,sens)
        figSyntheses, figJournaliere=graphsGeneraux(self.dfSemaineMoyenne,self.dicoHoraire, self.dicoJournalier,typesVeh, vitesse)          
        return figSyntheses, figJournaliere

class FichierComptageIndiv(ComptageDonneesIndiv):
    """
    a partir de données individuelles d'1fichier, creer une structure de table pour graph comme pour la classe ComptageDonneesIndiv
    """
    def __init__(self,fichier, vitesse=False):
        """
        attributs : 
            fichier : string chemin du nom de fichier
            vitesse : boolean : traiter ou non les vitesse
        """
        self.fichier=fichier
        self.vitesse=vitesse
        self.listAttributs=super().calculListAttributs()
        self.df2SensBase=self.fichierType()
        self.nbsens=self.df2SensBase.sens.nunique()
        self.sens=self.df2SensBase.sens.unique()[0] if self.nbsens==1 else self.df2SensBase.sens.unique()
        
        self.dfHeureTypeSens,self.dicoNbJours,self.dfSemaineMoyenne=self.MettreEnFormeDonnees()
        
           
    def fichierType(self):
        """
        connaitre le type de fcihier selon l'exten
        """
        if self.fichier.lower().endswith('.vik') :
            df=Viking(self.fichier).dfFichier
        elif self.fichier.lower().endswith('.xls') : 
            df=Mixtra(self.fichier).dfFichier
        else : 
            raise NotImplementedError('fichier autre que vik et mixtra pas encore implemente')
        if 'sens' in df.columns : 
            df['sens']=df.sens.apply(lambda x : 'sens'+str(x) if 'sens' not in str(x) else str(x))
            return df[self.listAttributs+['sens',]]
        else : #ATTENTION, SI MIXTRA ON PEUT AVOIR 1 FICHIER ET 2 SENS ?
            return df[self.listAttributs].assign(sens='sens1')
        
    def graphsSynthese(self,typesVeh, typesDonnees='all', sens='sens2', vitesse=False, synthese=False):
        """
        creer les graphs pour visu
        """
        self.dicoHoraire,self.dicoJournalier=IndicsGraphs(self.dfSemaineMoyenne,typesVeh,typesDonnees,sens)
        figSyntheses, figJournaliere=graphsGeneraux(self.dfSemaineMoyenne,self.dicoHoraire, self.dicoJournalier,typesVeh, vitesse)          
        return figSyntheses, figJournaliere
        
    
        
    
             

class FIM():
    """
    classe dedié aux fichiers FIM de comptage brut
    attributs : 
        verifQualite ; value parmi 'Bloque' ou 'Message : la verif de qualite fait remonter une erreur et bloque la fonction ou la verif qualite affiche un message seulement
        dico_corresp_type_veh : dico poutr determiner le type de vehicule selon en-tete
        dico_corresp_type_fichier : dico poutr determiner le mode de comptag selon en-tete
        pas_temporel : pas de concatenation des donnes, issu de en-tete (cf fonction params_fim())
        date_debut : date de debut du comptage, (cf fonction params_fim())
        mode : mod de cimptage (cf fonction params_fim())
        taille_donnees : integer : taille des blocs de donnees
        df_tot_heure : df avec dateIndex et valeur horaire VL ou TV et PL et tot si calculée
        df_tot_jour : df avec dateIndex et valeur journaliere TV et PL
        tmja
        pl,
        pc_pl
        sens_uniq : booleen
        sens_uniq_nb_blocs : si sens_uniq : nb de bloc de donnees
        dfHeureTypeSens
        dfHoraire2Sens : df au format ancienne bdd
        dfSemaineMoyenne : 
        periode : de forme YYYY/MM/DD-YYYY/MM/DD
    """
    def __init__(self, fichier, gest=None, verifQualite='Bloque'):
        O.checkParamValues(verifQualite, ('Bloque', 'Message'))
        self.verifQualite=verifQualite
        self.fichier_fim=fichier
        self.dico_corresp_type_veh={'TV':('1.T','2.','1.'),'VL':('2.V','4.V'),'PL':('3.P','2.P','4.P')}
        self.dico_corresp_type_fichier={'mode3' : ('1.T','3.P'), 'mode4' : ('2.V','2.P','4.V', '4.P'), 'mode2':('2.',), 'mode1' : ('1.',)}
        self.gest=gest
        self.lignes=self.ouvrir_fim()
        self.pas_temporel,self.date_debut,self.mode, self.geoloc, self.geom_l93=self.params_fim(self.lignes)
        self.liste_lign_titre,self.sens_uniq,self.sens_uniq_nb_blocs=self.liste_carac_fichiers(self.lignes)
        self.taille_donnees=self.taille_bloc_donnees()
        self.isoler_bloc(self.lignes, self.liste_lign_titre)
        self.dfHeureTypeSens,self.dfHoraire2Sens, self.periode = self.traficsHoraires()
        self.qualiteComptage()
        self.dfSemaineMoyenne,self.tmja,self.pc_pl, self.pl =self.calcul_indicateurs_agreges()

    def ouvrir_fim(self):
        """
        ouvrir le fichier txt et en sortir la liste des lignes
        """
        with open(self.fichier_fim) as f :
            lignes=[e.strip() for e in f.readlines()]
        return lignes
    
    def corriger_mode(self,mode):
        """
        correction du fichier fim si mode = 4. dans le fichiers, pour pouvoir diiférencier VL et PL
        """
        i=0
        for e,l in enumerate(self.lignes) :
            if mode=='4.' : #porte ouvert pour d'auter corrections si beoisn
                if '   4.   ' in l : 
                    if i% 2==0 :
                        self.lignes[e]=l.replace('   4.   ','   4.V   ')
                        i+=1
                    else : 
                        self.lignes[e]=l.replace('   4.   ','   4.P   ') 
                        i+=1

    def params_fim(self,lignes):
        """
        obtenir les infos générales du fichier : date_debut(anne, mois, jour, heure, minute), mode, geolocalisation
        """
        lign0Splitpoint=self.lignes[0].split('.')
        annee,mois,jour,heure,minute,pas_temporel=(int(lign0Splitpoint[i].strip()) for i in range(5,11))
        #particularite CD16 : l'identifiant route et section est present dans le FIM
        if self.gest=='CD16' : 
            self.section_cp='_'.join([str(int(lign0Splitpoint[a].strip())) for a in (2,3)])
        date_debut=pd.to_datetime(f'{jour}-{mois}-{annee} {heure}:{minute}', dayfirst=True)
        mode=self.lignes[0].split()[9]
        if mode in ['4.',] : #correction si le mode est de type 4. sans distinction exlpicite de VL TV PL. porte ouvert à d'autre cas si besoin 
            self.corriger_mode(mode)
            mode=self.lignes[0].split()[9]
        mode=[k for k,v in self.dico_corresp_type_fichier.items() if any([e == mode for e in v])][0]
        if not mode : 
            raise self.fim_TypeModeError
        
        geoloc=re.search('(?P<lat>(\-|\+)[0-9]{1,3}\.([0-9]{4}\.){2})(?P<long>(\-|\+)[0-9]{1,3}\.([0-9]{4}\.){2})', self.lignes[0].split()[-1])
        wgs84Proj = pyproj.CRS('EPSG:4326')
        l93Proj = pyproj.CRS('EPSG:2154')
        project = pyproj.Transformer.from_crs(wgs84Proj, l93Proj, always_xy=True).transform
        if geoloc: 
            longitude = geoloc.group('long')
            latitude = geoloc.group('lat')
            x_wgs84=float(re.sub('(\+|-)(.*\.)(.*)(\.)(.*)(\.)', '\g<1>\g<2>\g<3>\g<5>', longitude))
            y_wgs84=float(re.sub('(\+|-)(.*\.)(.*)(\.)(.*)(\.)', '\g<1>\g<2>\g<3>\g<5>', latitude))
            geom_wgs84=Point(x_wgs84, y_wgs84)
            geom_l93 = transform(project, geom_wgs84)
            geoloc=True
        else : 
            geoloc=False 
            geom_l93=None
        
        return pas_temporel,date_debut,mode, geoloc, geom_l93
        

    def fim_type_veh(self, ligne):
        """
        savoir si le fichier fim concerne les TV, VL ou PL
        in : 
            ligne : ligne du fichier
        """

        for k,v  in self.dico_corresp_type_veh.items() : 
            if any([e+' ' in ligne for e in v]) :
                return [cle for cle, value in self.dico_corresp_type_veh.items() for e in value  if e==ligne.split()[9]][0]
    
    def liste_carac_fichiers(self,lignes):
        """
        creer une liste des principales caracteristiques 
        """
        liste_lign_titre=[]
        for i,ligne in enumerate(lignes) : 
            type_veh=self.fim_type_veh(ligne)
            if type_veh : 
                sens=ligne.split('.')[4].strip()
                liste_lign_titre.append([i, type_veh,sens])
        sens_uniq=True if len(set([e[2] for e in liste_lign_titre]))==1 else False
        sens_uniq_nb_blocs=len(liste_lign_titre) if sens_uniq else np.NaN 
        return liste_lign_titre,sens_uniq,sens_uniq_nb_blocs

    def taille_bloc_donnees(self) : 
        """
        verifier que les blocs de donnees ont tous la mm taille
        in : 
            lignes_fichiers : toute les lignes du fichiers, issu de f.readlines()
        """
        taille_donnees=tuple(set([self.liste_lign_titre[i+1][0]-(self.liste_lign_titre[i][0]+1) for i in range(len(self.liste_lign_titre)-1)]+
                           [len(self.lignes)-1-self.liste_lign_titre[len(self.liste_lign_titre)-1][0]]))
        if len(taille_donnees)>1 : 
            raise self.fim_TailleBlocDonneesError(taille_donnees)
        else : 
            return taille_donnees[0]

    def isoler_bloc(self,lignes, liste_lign_titre) : 
        """
        isoler les blocs de données des lignes de titre,en fonction du mode de comptage
        """
        for i,e in enumerate(liste_lign_titre) :
            if self.mode in ('mode3','mode1') :
                e.append([int(b) for c in [a.split('.') for a in [a.strip() for a in lignes[e[0]+1:e[0]+1+self.taille_donnees]]] for b in c if b])
            elif self.mode in ('mode4', 'mode2') :
                e.append([sum([int(e) for e in b if e]) for b in [a.split('.') for a in [a.strip() for a in lignes[e[0]+2:e[0]+1+self.taille_donnees]]]])
        return
        
    def traficsHoraires(self):
        """
        creer une df des donnes avec un index datetimeindex de freq basee sur le pas temporel et rnvoi la date de fin
        """
        freq=str(int(self.pas_temporel))+'T'
        """
        for i,e in enumerate(liste_lign_titre) :
            df=pd.DataFrame(liste_lign_titre[i][3], columns=[liste_lign_titre[i][1]+'_sens'+liste_lign_titre[i][2]])
            df.index=pd.date_range(self.date_debut, periods=len(df), freq=freq)
            if i==0 : 
                self.df_heure_brut=df
            else : 
                self.df_heure_brut=self.df_heure_brut.merge(df, left_index=True, right_index=True)
        self.date_fin=self.df_heure_brut.index.max()
        """
        dfHeureTypeSens=pd.concat([pd.DataFrame({'date_heure':pd.date_range(self.date_debut, periods=len(self.liste_lign_titre[i][3]), freq=freq),'nbVeh':self.liste_lign_titre[i][3]})
                   .assign(type_veh=self.liste_lign_titre[i][1].lower(),sens='sens'+self.liste_lign_titre[i][2].lower()) for i in range(len(self.liste_lign_titre))],
                  axis=0)
        dfHeureTypeSens['jour']=dfHeureTypeSens.date_heure.dt.dayofweek
        dfHeureTypeSens['jourAnnee']=dfHeureTypeSens.date_heure.dt.dayofyear
        dfHeureTypeSens['heure']=dfHeureTypeSens.date_heure.dt.hour
        dfHeureTypeSens['date']=pd.to_datetime(dfHeureTypeSens.date_heure.dt.date)
        dfHoraire2Sens=dfHeureTypeSens.groupby(['date_heure','type_veh','jour','jourAnnee','heure','date']).nbVeh.sum().reset_index()
        dfHoraire2Sens['fichier']=os.path.basename(self.fichier_fim)
        dfHoraire2Sens=dfHoraire2Sens.pivot(index=['date', 'type_veh', 'fichier'], columns='heure', values='nbVeh').reset_index().rename(
            columns={k: v for k, v in zip(['date', 'type_veh']+[e for e in range(24)], ['jour', 'indicateur']+attributsHoraire)})
        dfHoraire2Sens['indicateur']=dfHoraire2Sens.indicateur.str.upper()
        periode = f"{dfHoraire2Sens.jour.min().date().strftime('%Y/%m/%d')}-{dfHoraire2Sens.jour.max().date().strftime('%Y/%m/%d')}"
        return dfHeureTypeSens,dfHoraire2Sens, periode

    def calcul_indicateurs_agreges(self):
        """
        calculer le tmjs, pl et pc_pl pour un fichier
        """
        #calcul d'une semaine moyenne
        
        dfSemaineMoyenne=semaineMoyenne(NettoyageTemps(self.dfHeureTypeSens))
        #TMJA
        if 'tv' in dfSemaineMoyenne.type_veh.unique() : 
            tmja=round(dfSemaineMoyenne.loc[dfSemaineMoyenne['type_veh']=='tv'].nbVeh.sum()/7,0)
        else :
            tmja=round(dfSemaineMoyenne.nbVeh.sum()/7,0)
        #PL & PC_PL
        if self.mode not in ('mode2','mode1') : 
            pl=round(dfSemaineMoyenne.loc[dfSemaineMoyenne['type_veh']=='pl'].nbVeh.sum()/7,0)
            pc_pl=pl/tmja*100
        else : 
            pl,pc_pl=np.NaN, np.NaN
        return dfSemaineMoyenne,tmja,pc_pl, pl
    
    def qualiteComptage(self):
        """
        selon le nombre de jours comptés, forcer une qualité faible ou non
        """
        if self.verifQualite=='Message' : 
            try : 
                verifNbJour(self.dfHeureTypeSens)#verif nb jours ok
                self.qualite=None
            except  PasAssezMesureError as e : 
                print(e)
                self.qualite=1
        else : 
            verifNbJour(self.dfHeureTypeSens)
        return 
        
class ComptageFim(object):
    """
    classe qui permet si on a un fichier fim dans un sens et un autre pour l'autre, d'agreger les deux
    """
    def __init__(self, dossier):
        """
        attributs : 
        dossier : raw string du chemin du dossier contenant le(s) fim
        dfHeureTypeSens
        dfSemaineMoyenne
        """
        self.dossier=dossier
        self.dfHeureTypeSens=self.analyseNbFichier()
        verifNbJour(self.dfHeureTypeSens)#verif nb jours ok
        self.dfSemaineMoyenne=semaineMoyenne(NettoyageTemps(self.dfHeureTypeSens))
    
    def analyseNbFichier(self):
        """
        trouver le nombre de fichiers FIM, corriger les sens si necessaires
        """
        with os.scandir(self.dossier) as it:
            listFichiersFim=[os.path.join(self.dossier,f.name) for f in it if f.is_file() and f.name.lower().endswith('.fim')]
            if len(listFichiersFim)==1 : 
                objetFim=FIM(listFichiersFim[0])
                dfHeureTypeSens=objetFim.dfHeureTypeSens
            elif len(listFichiersFim)==2 : # 2 fichiers 
                if (any([a in re.sub('( |_)','',e.lower()) for e in listFichiersFim for a in ['s1','sens1']]) and# les noms doivent contenir une refernce au sens
                 any([a in re.sub('( |_)','',e.lower()) for e in listFichiersFim for a in ['s2','sens2']])) :
                    dicoSens={} #calcul de la df par heure, sens, jour, type_veh
                    for i in (1,2) : 
                        if any([a in re.sub('( |_)','',listFichiersFim[i-1].lower()) for a in [f's{i}',f'sens{i}']]) :
                            objetFim=FIM(listFichiersFim[i-1])
                            dfHeureTypeSens=objetFim.dfHeureTypeSens
                            dfHeureTypeSens['sens']=f'sens{i}'
                            dicoSens[f'sens{i}']=dfHeureTypeSens
                    dfHeureTypeSens=pd.concat([a for a in dicoSens.values()], axis=0).groupby(['date_heure','jour','heure','jourAnnee','date','type_veh',
                                                                                               'sens']).nbVeh.sum().reset_index()
                else : 
                    raise ValueError('les noms de fcihiers ne contiennent pas s1 ou sens1 ou s2 ou sens2, caseless')
            else : 
                raise AttributeError(f'trop de fichier fim : {len(listFichiersFim)} dans le dossier {self.dossier}')
        return dfHeureTypeSens
    
    def graphsSynthese(self,typesVeh, typesDonnees='all', sens='all', vitesse=False, synthese=False):
        """
        creer les graphs pour visu
        """
        self.dicoHoraire,self.dicoJournalier=IndicsGraphs(self.dfSemaineMoyenne,typesVeh,typesDonnees,sens)
        figSyntheses, figJournaliere=graphsGeneraux(self.dfSemaineMoyenne,self.dicoHoraire, self.dicoJournalier,typesVeh, vitesse)          
        return figSyntheses, figJournaliere
    
class MHCorbin(object):
    """
    Classe de caracterisation / traitement des fihciers issus du logiciel MHCorbin, founis par la ville d'Anglet et d'Angouleme notamment
    les fihiers peuvent etre des .mdb (microsoftDabase) ou des pdf. 
    pour les .mdb on s'appuie sur Pandas et la librairie Perso de connexion à une bdd
    on peut acceder a une comparaison du fichier mdb et du fichier sequential produit via le package Data ou via la fonction dfRessourceCorrespondance
    """
    
    def __init__(self, fichier, nature=None):
        """
        attributes : 
            fichier : rawString de parh complet
            nature : None ou Test : si test, le parametre fichier est ignore et les donnees de test "rue de souche du 31 01 au 07 02 2020.mdb" sont utilise
            fichierCourt : string du nom du fichier sans path
            tables : liste des tables conotenues dans le fichier mdb
            dico_tables : dico des tables avec en clé (et en value la df correspolnvdante) a1, a2, c1, c2, e1, e2, v1, v2, pe1, pe2, hdhdr : tables contenues dans le fihciers mdb. Décrivent :
                - les donnees individuelle pour chaque sens ('a')
                - les donnes parclasse de vitesse ('c') et par sens
                - les donnees d etemperatur et de sens ('e') par periode de regroupement et par sens
                - les donnees de nb de vehicules par periode de regroupement ('v') et par sens
                - les donnees techniques du compteuir par sens (pe)
                - la description du point de comptage (hshdr) : denomination des sens, et autres 
            listTablesManquante : list de string : list des tables ci-dessus non présentes dans le fichier
            nbSens  : integer : nombre de sens mesures 
            dfAgreg2Sens : df des 2 sens de circulation, brutes sans nettoyages,avec attribut 'fail_type' type tuple avec erreur parmi 'adviceCode', 'longueur', 'vitesse', 'value0'
            indicQualite : integer parmi 1,2,3, traduit sla qualite surla base des notes en bdd otv schema qualite table enum_qualite
        """
        self.fichier=fichier    
        self.fichierCourt=os.path.basename(self.fichier)
        self.ouvrirMdb()
        self.calculNbSens()
        self.verifTables()
        self.filtreDonneesAberrantes()
        self.dfAgreg2Sens=self.calculLongueurVitesse()
        self.indicQualite, self.comQualite=self.qualificationQualite() 
            
    
    def ouvrirMdb(self):
        """
        ouvrir le fichier mdb et retourner les tables non système
        """
        self.dicoTables={}
        self.listTablesManquante=[]
        with ct.ConnexionBdd('mdb', fichierMdb=self.fichier) as c:
            self.tables = list(c.mdbCurs.tables())
            for k in [t[2] for t in self.tables if t[2].lower() not in ('msysaces','msysobjects','msysqueries','msysrelationships')] :
                try : 
                    self.dicoTables[k.lower()]=pd.read_sql(f"SELECT * FROM {k}", c.connexionMdb)
                except DatabaseError:
                    self.listTablesManquante.append(k)
     
    def calculNbSens(self): 
        """
        verifier que le comptage concerne 1 ou 2 sens
        """    
        nbSens=len(self.dicoTables['hshdr'])
        if nbSens in (1,2) : 
            self.nbSens=nbSens
        elif nbSens==3 and len(self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Lane.apply(lambda x : not (re.match('^ *$', x) and not pd.isnull(x)))])==2: 
            self.nbSens=nbSens
            self.dicoTables['hshdr']=self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Lane.apply(
                lambda x : not (re.match('^ *$', x) and not pd.isnull(x)))].copy()
        else : 
            raise ValueError("il n'y aucun sens dans le fihcier hshdr ou plus de 2")
        return
            
    def verifTables(self):
        """
        en fonction du nombre de sens, verifier que toute les tables sont presentes 
        """
        if not all([len([t[2] for t in self.tables if t[2].lower() not in ('msysaces','msysobjects','msysqueries','msysrelationships') 
                     and t[2].lower()[0]==l])==self.nbSens for l in ('a', 'v', 'c')]): 
            raise DataSensError()
        elif any([len([t[2] for t in self.tables if t[2].lower() not in ('msysaces','msysobjects','msysqueries','msysrelationships') 
                       and t[2].lower()[0]==l])==2 for l in ('a', 'v', 'c')]) and self.nbSens==1:
            raise DataSensError()
        elif any([len([t[2] for t in self.tables if t[2].lower() not in ('msysaces','msysobjects','msysqueries','msysrelationships') 
                       and t[2].lower()[0]==l])==2 for l in ('a', 'v', 'c')]) and self.nbSens==1:
            raise DataSensError()
        return
        
            
    def dfRessourceCorrespondance(self):
        """
        ouvrir la ressource sur Niort (fichier sequential et fichier .mdb) et fournir une df qui permet de visauliser 
        pour chaque vehicule les donnees en mdb et en sequential
        out : 
            dataframe des donnes de comparaison brutes
            dataframe des donnees de comparaison filtrees selon les criteres du modules DonneesSOurcesParams
        """
        return (pd.read_json(resources.read_text('data.MHCorbin','compSequentialMdb.json' )),
                pd.read_json(resources.read_text('data.MHCorbin','compSequentialMdbNettoyee.json' )))
    
    def dfRessourceTests(self):
        """
        ouvrir les donnees necessaires aux tests unitaires
        out : 
            df de verif diu format de conversion vers donneesIndiv
        """   
        return pd.read_json(resources.read_text('data.MHCorbin','verifFormat2Sens.json' ), convert_dates=['date_heure'])
        
    
    def agreg2Sens(self):
        """
        Agreger les donnees brutes stockees dans les tables a1 et a2, avec ajout des champs sens, sensTxt, obs_supl et nb_sens
        out : 
            concatenation brutes de a1 et a2 si deux , sens, sinon a1
        """
        if self.nbSens==2: 
            listSens=[t[2].lower() for t in self.tables if t[2].lower() not in ('msysaces','msysobjects','msysqueries','msysrelationships') and t[2].lower()[0]=='a']
            listNum=[int(s[-1]) for s in listSens]
            return pd.concat([self.dicoTables[listSens[0]].assign(sens=1,sensTxt=f"sens 1 : {self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==listNum[0], 'Lane'].values[0]}", 
                    obs_supl=','.join(set([self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==i, 'Street'].values[0] for i in listNum])),
                    nb_sens='double sens'),
              self.dicoTables[listSens[1]].assign(sens=2, sensTxt=f"sens 2 : {self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==listNum[1], 'Lane'].values[0]}", 
                    obs_supl=','.join(set([self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==i, 'Street'].values[0] for i in listNum])),
                    nb_sens='double sens')], axis=0, sort=False).reset_index(drop=True)
        elif self.nbSens==3 and len(self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Lane.apply(lambda x : not (re.match('^ *$', x) and not pd.isnull(x)))])==2:
            listSens=[f'a{n}' for n in self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Lane.apply(lambda x : not (re.match('^ *$', x) and not pd.isnull(x)))].Rangenum.values]
            listNum=[int(s[-1]) for s in listSens]
            return pd.concat([self.dicoTables[listSens[0]].assign(sens=1,sensTxt=f"sens 1 : {self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==listNum[0], 'Lane'].values[0]}", 
                                obs_supl=','.join(set([self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==i, 'Street'].values[0] for i in listNum])),
                                nb_sens='double sens'),
                          self.dicoTables[listSens[1]].assign(sens=2, sensTxt=f"sens 2 : {self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==listNum[1], 'Lane'].values[0]}", 
                                obs_supl=','.join(set([self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==i, 'Street'].values[0] for i in listNum])),
                                nb_sens='double sens')], axis=0, sort=False).reset_index(drop=True)
        else : 
            listSens=[t[2].lower() for t in self.tables if t[2].lower() not in ('msysaces','msysobjects','msysqueries','msysrelationships') and t[2].lower()[0]=='a']
            listNum=[int(s[-1]) for s in listSens]
            return self.dicoTables[listSens[0]].assign(sens=1,sensTxt=f"sens 1 : {self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==listNum[0], 'Lane'].values[0]}", 
                    obs_supl=self.dicoTables['hshdr'].loc[self.dicoTables['hshdr'].Rangenum==listNum[0], 'Street'].values[0],
                    nb_sens='sens unique')
            
    def filtreDonneesAberrantes(self):
        """
        sur la base des valeurs de parametres aberrants du module DonneesSOurcesParams, enlever les donnees de valeurs aberrantes 
        et fournir une df filtree, une df des donnees aberrantes 
        """
        #calculer les df de chauqe cas
        dfAgreg2Sens=self.agreg2Sens()
        dfAdviceCodeFail=dfAgreg2Sens.loc[(dfAgreg2Sens.AdviceCode==MHCorbinFailAdviceCode)]
        dfLengthFail=dfAgreg2Sens.loc[(dfAgreg2Sens.Length>=MHcorbinMaxLength)]
        dfSpeedFail=dfAgreg2Sens.loc[(dfAgreg2Sens.Length>=MHcorbinMaxSpeed)]
        dfValue0=dfAgreg2Sens.loc[(dfAgreg2Sens.Length==MHCorbinValue0)|(dfAgreg2Sens.Speed==MHCorbinValue0)]
        #la df filmtree
        dfFail=pd.concat([dfAdviceCodeFail.assign(fail_type='adviceCode'),
            dfLengthFail.assign(fail_type='longueur'),
            dfSpeedFail.assign(fail_type='vitesse'),
           dfValue0.assign(fail_type='value0')])
        dfFailCause=dfFail.drop('fail_type', axis=1).merge(dfFail.groupby(level=0).agg({'fail_type':lambda x : tuple(x)}), left_index=True, right_index=True)
        df2SensFail=dfFailCause.reset_index().drop_duplicates('index').set_index('index')
        self.dfAgreg2Sens=dfAgreg2Sens.merge(df2SensFail[['fail_type']],left_index=True, right_index=True, how='left')
        return 
    
    def creerModelePredictionAttribut(self, attribut):
        """
        a partir des donnees ressources de Niort, creer le modele de prediction des donnees
        in : 
            attribut : string parmi 'Length' ou 'Speed'
        out :     
            modelPoly4Inf10k : modele polynomial entraine avec les donnees ressources
            linearSup10k : modele lineaire entraine avec les donnees resosurces de longueur inf a 10k
            Y : series de la''tribu
        """
        if attribut not in ('Length', 'Speed') : 
            raise ValueError("la valeur d'attribut doit etre parmi 'Length' ou 'Speed' ")
        else : 
            attributX=attribut+'_x'
            attributY=attribut+'_y'
        concatNettoyees=self.dfRessourceCorrespondance()[1]
        #creation des donnees : on va séparer le jeu de donnes en avant et après 10 000, car ça matche mieux en polynomial avant 10k et linear apres 10k
        XInf10k=concatNettoyees.loc[concatNettoyees[attributY]<10000][[attributY]].copy()
        YInf10k=concatNettoyees.loc[concatNettoyees[attributY]<10000][[attributX]].copy()
        XSup10k=concatNettoyees.loc[concatNettoyees[attributY]>=10000][[attributY]].copy()
        YSup10k=concatNettoyees.loc[concatNettoyees[attributY]>=10000][[attributX]].copy()
        #separation en train et test
        X_trainSup10k, X_testSup10k, y_trainSup10k, y_testSup10k=train_test_split(XSup10k, YSup10k, test_size=0.2)
        X_trainInf10k, X_testInf10k, y_trainInf10k, y_testInf10k=train_test_split(XInf10k, YInf10k, test_size=0.2)
        #preparation de plusieurs methode de test
        poly4=Pipeline([('poly',PolynomialFeatures(degree=4)), ('linear', LinearRegression())])
        linearSup10k=LinearRegression()
        #entrainement
        poly4.fit(X_trainInf10k, y_trainInf10k)
        linearSup10k.fit(X_trainSup10k, y_trainSup10k)
        return poly4, linearSup10k
    
    def predireAttribut(self, Series,nomAttribut, poly4,linearSup10k ):
        """
        predire les valeurs correspndantes des attribts longueur ou vitesse.Utilise les modeles crees par creerModelePredictionAttribut
        in : 
           Series : df avec 1 seul attribut, celui a convertir 
           nomAttribut: string : nom de l'attribut a convertir
           poly4 : scikit learn regression model (polynomial 4) entraine. issu de creerModelePredictionAttribut
           linearSup10k : scikit learn regression model (polynomial 4) entraine. issu de creerModelePredictionAttribut
        out:
            dfPrediction : dartaframe des resultats avec attribut 'prediction'
            resultsPoly4TestInf10k : list des valeurs résultat pour la valeur d'attribut <10000
            resultsLinearTestSup10k : list des valeurs résultat pour la valeur d'attribut >=10000
        """
        X_testInf10k=Series.loc[Series[nomAttribut]<10000]
        X_testSup10k=Series.loc[Series[nomAttribut]>=10000]
        resultsPoly4TestInf10k=poly4.predict(X_testInf10k)
        resultsLinearTestSup10k=linearSup10k.predict(X_testSup10k)
        dfPrediction=pd.concat([X_testInf10k.assign(prediction=resultsPoly4TestInf10k), 
                              X_testSup10k.assign(prediction=resultsLinearTestSup10k)]
           ).merge(Series, left_index=True, right_index=True).drop([nomAttribut+'_x', nomAttribut+'_y'], axis=1).rename(columns={'prediction':nomAttribut+'_calc'})
        return dfPrediction, resultsPoly4TestInf10k, resultsLinearTestSup10k
     
    
    def conversionAttribut(self,Series,nomAttribut):
        """
        convertir les longueur de la base du fihcier mdb en longueur en m. On utilise le fichier sequential fourni sur Niort
        pour calculer un modele de prediction basé sur du polynomial avant 10k et du lineaire apres 10k
        in : 
            dfACalculer : df issue du fichier mdb 
            Series : df avec 1 seul attribut, celui a convertir 
            nomAttribut: string : nom de l'attribut a convertir
        """
        poly4, linearSup10k=self.creerModelePredictionAttribut(nomAttribut)
        dfPrediction=self.predireAttribut(Series,nomAttribut,poly4, linearSup10k)[0]
        return dfPrediction
    
    def calculLongueurVitesse(self):
        """
        ajouter une vitesse et une longueur calculee et corrigee a la df self.dfAgreg2Sens
        """
        dfLongueurCalc=self.conversionAttribut(self.dfAgreg2Sens[['Length']], 'Length')
        dfVitesseCalc=self.conversionAttribut(self.dfAgreg2Sens[['Speed']], 'Speed')
        return self.correctionPrediction(dfLongueurCalc,dfVitesseCalc )
        
    
    def correctionPrediction(self,dfLongueurCalc,dfVitesseCalc  ):
        """
        une fois les predictions faite et jointe a la table de base, il faut corriger les valeurs aberrantes liees 
        aux lignes de la table df2SensFail : si la vitesse ou la longuer ont trop importante ou egale a 0 on les passe a NaN
        in : 
            dfLongueurCalc : dataframe issu de conversionAttribut
            dfVitesseCalc : dataframe issu de conversionAttribut
        out : 
            dfCorrection : jointure entre l'attribut d'instance dfAgrege2Sens et les valeur calculees de vitesse et longueur
        """
        dfCorrection=self.dfAgreg2Sens.merge(dfLongueurCalc, left_index=True, right_index=True).merge(
            dfVitesseCalc, left_index=True, right_index=True)
        for attr in ('Length_calc', 'Speed_calc') : 
            dfCorrection.loc[dfCorrection.fail_type.apply(lambda x : any([e in x for e in ('longueur', 'vitesse', 'value0')]) if not pd.isnull(x) else False),attr]=np.nan
        return dfCorrection
    
    def qualificationQualite(self):
        """
        en fonction du ration nombr d'objet qui présente un fail (cf fonction filtreDonneesAberrantes) / nb objet total, affecter une 
        valeur de qualite selon la liste enumeree en base (0:NC, 1:faible, 2:moyen, 3:bonne)
        """
        dfFail=self.dfAgreg2Sens.loc[~self.dfAgreg2Sens.fail_type.isna()]
        #note de qualite sur le nombre de vehicule ayant des valeurs aberrantes
        if len(dfFail)/len(self.dfAgreg2Sens) >= 0.3 :
            noteQualiteFail = 1
            commQualiteFail="plus de 30% de valeur de longueur ou de vitesse aberrantes ou a 0, ou d'adviceCode speciaux"
        elif 0.1 < len(dfFail)/len(self.dfAgreg2Sens) < 0.3 : 
            noteQualiteFail = 2
            commQualiteFail="entre 10% et 30% de valeur de longueur ou de vitesse aberrantes ou a 0, ou d'adviceCode speciaux"
        else : 
            noteQualiteFail = 3
            commQualiteFail="moins de 10% de valeur de longueur ou de vitesse aberrantes ou a 0, ou d'adviceCode speciaux"
        #note de qualite sur le nombre de sens et la qualite de leur descriptions
        if self.nbSens in (1,2) and not self.dicoTables['hshdr'].Lane.isna().any() : 
            noteSens=3
            commQualiteSens="1 ou 2 sens nommé"
        elif self.nbSens in (1,2) and self.dicoTables['hshdr'].Lane.isna().any() : 
            noteSens=2
            commQualiteSens="1 ou 2 sens, certains ou tous non nommés"
        else : 
            noteSens=1
            commQualiteSens="0 ou plus de 2 sens, peu importe le nom" 
        #note et com finale
        if noteQualiteFail == 1 or noteSens == 1 : 
            noteFinale=1
        elif noteQualiteFail != noteSens :
            noteFinale=2
        else : 
            noteFinale=noteQualiteFail
        comFinal=commQualiteFail+' ; '+commQualiteSens
        return noteFinale, comFinal
        
    def formaterDonneesIndiv(self, df):
        """
        passer du format MHCorbin au format des donnees indiv ci-dessus
        in : 
            df : df des donnees a transformer, generalement ce sera dfAgreg2Sens
        out : 
            df2sensFormatIndiv : modificarion de format (nom de champ, ajout de champ, valeurs)
        """
        df2sensFormatIndiv=self.dfAgreg2Sens.copy()
        df2sensFormatIndiv.loc[(df2sensFormatIndiv.Length_calc<6) | (df2sensFormatIndiv.Length_calc.isna()), 'type_veh']='VL'
        df2sensFormatIndiv.loc[df2sensFormatIndiv.Length_calc>=6, 'type_veh']='PL'
        df2sensFormatIndiv.rename(columns={'ATime':'date_heure'}, inplace=True)
        df2sensFormatIndiv['nbVeh']=1
        df2sensFormatIndiv.sens.replace([1,2], ['sens1', 'sens2'], inplace=True)
        return df2sensFormatIndiv  
    
    def formaterDonneesHoraires(self, df): 
        """
        passer les donnees indiv au format de donnees horaires de la Bdd Otv
        in : 
            df : df au format donnees indiv, issue de formaterDonneesIndiv
        out : 
            dfHoraireBdd : 
        """ 
        try : 
            verifNbJour(df)#verif nb jours ok
        except PasAssezMesureError : 
            print(f'pas assez de mesure sur le fichier {self.fichierCourt}')
            self.indcQualite=1
            self.comQualite="moins de 7 jours de mesure"
        dfFormatGroupe=GroupeCompletude(NettoyageTemps(df))
        dfHoraireBdd=donneesIndiv2HoraireBdd(dfFormatGroupe)
        return dfHoraireBdd
        
        
class PasAssezMesureError(Exception):
    """
    Exception levee si le fichier comport emoins de 7 jours
    """     
    def __init__(self, nbjours):
        Exception.__init__(self,f'le fichier comporte moins de 7 jours complets de mesures. Nb jours complets : {nbjours} ')   
        
class DataSensError(Exception):
    """
    Exception levee pour classe MHCorbin si le fichier comporte des sens dans la table hshdr mais que  les tables a ou c ou v ne sont pas présentes
    """     
    def __init__(self, nbjours):
        Exception.__init__(self,f'Il manque la table a, c ou v dans le fichier.mdb')     
        
        