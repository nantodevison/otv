# -*- coding: utf-8 -*-
'''
Created on 12 oct. 2020

@author: martin.schoreisz
Traitementsdes donnes individuelles fournies par a DREAM TEAM en mixtra ou vikings
'''

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import os, re

#tuple avec le type de jours, le nombre de jours associes et les jours (de 0à6)
tupleParams=(('mja',7,range(7)),('mjo',5,range(5)),('lundi',1,[0]), ('mardi',1,[1]), ('mercredi',1,[2]),('jeudi',1,[3]),('vendredi',1,[4]),('samedi',1,[5]),('dimanche',1,[6]))

def NettoyageTemps(dfVehiculesValides):
    """
    Analyser le nb de jours dispo, lever un erreur si inférieur à 7, si 7 verifier que recouvrement sinon erreur
    ensuite si besoin fusion du 1er et dernier jours, sinon suppression jours incomplet
    in  : 
        dfVehiculesValides : données issues de NettoyageDonnees()
    """
    #analyse du nombre de jours mesurés 
    nbJours=dfVehiculesValides.date_heure.dt.dayofyear.nunique()
    timstampMin=dfVehiculesValides.date_heure.min()
    timstampMax=dfVehiculesValides.date_heure.max()
    #si nb jour==8 : (sinon <8 erreur )
    if nbJours==8 : 
        #vérif qu'il y a recouvrement
        if timstampMin.time()>timstampMax.time() : 
            PasAssezMesureError(nbJours-1) #sinon renvoyer sur l'erreur ci-dessus
        #on relimite la df : 
        dfValide=dfVehiculesValides.loc[dfVehiculesValides.date_heure>datetime.combine(timstampMin.date(),timstampMax.time())].copy()
        #puis on triche et on modifie la date du dernier jour pour le mettre sur celle du 1er, en conservant l'heure
        dfValide.loc[dfValide.date_heure.dt.dayofyear==timstampMax.dayofyear,'date_heure']=dfValide.loc[dfValide.date_heure.
           dt.dayofyear==timstampMax.dayofyear].apply(lambda x : datetime.combine(timstampMin.date(),x['date_heure'].time()),axis=1)    
    elif nbJours>8 :#si nb jour >8 on enleve les premier ete dernier jours
        dfValide=dfVehiculesValides.loc[~dfVehiculesValides.date_heure.dt.dayofyear.isin((timstampMin.dayofyear,timstampMax.dayofyear))].copy()
    else : 
        nbJours=nbJours if nbJours!=7 else nbJours-1
        raise PasAssezMesureError(nbJours)
    return dfValide

def GroupeCompletude(dfValide, vitesse=False):
    """
    Pour les donnees individuelles uniquement
    a partir des donnees nettoyees sur le temps, regouper les donnees par tranche horaire, type et sens 
    in : 
        dfValide : df nettoyees avec les attributs date_heure, nbVeh et sens, cf fonction NettoyageTemps()
        vitese : booelean : traduit si les donnees de vitesse doivent etre associees ou non
    out : 
        dfHeureTypeSens : df avec attribut date_heure, type_veh, sens, nbVeh et si vitesse : V10, V50, V85
    """
    dfGroupTypeHeure=dfValide.set_index('date_heure').groupby([pd.Grouper(freq='1H'),'type_veh','sens'])['nbVeh'].count().reset_index().sort_values('date_heure')
    #completude des données
    #ajout des données horaires à 0 si aucun type de vehicules mesures
    date_range=pd.date_range(dfGroupTypeHeure.date_heure.min(),dfGroupTypeHeure.date_heure.max(),freq='1H')
    #df de comparaison
    dfComp=pd.DataFrame({'type_veh':['2r','pl','vl']}).assign(key=1).merge(pd.DataFrame({'date_heure':date_range}).assign(key=1), on='key').merge(
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
                        ).assign(type_veh='tv').reset_index(),dfHeureTypeSens], axis=0, sort=False).sort_values(['date_heure', 'type_veh'])
    if vitesse : 
        dfGroupVtsHeure=dfValide.set_index('date_heure').groupby([pd.Grouper(freq='1H'),'sens']).agg(
            **{'v10': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,10)),
               'v50': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,50)),
               'v85': pd.NamedAgg(column='vitesse',aggfunc=lambda x : np.percentile(x,85))}).reset_index().sort_values('date_heure')
        dfHeureTypeSens=dfHeureTypeSens.merge(dfGroupVtsHeure, on=['date_heure','sens'],how='left')
    
    #si un sens est completement à O on le vire
    dfTestSens=dfHeureTypeSens.groupby('sens').nbVeh.sum().reset_index()
    if (dfTestSens.nbVeh==0).any() :
        dfHeureTypeSens=dfHeureTypeSens.loc[dfHeureTypeSens.sens==dfTestSens.loc[dfTestSens.nbVeh!=0].sens.values[0]]
       
    return dfHeureTypeSens

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
            df=pd.read_csv( self.fichier, delimiter='\t', encoding='latin_1')
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
                return 'vl'
            elif silhouette==13 : 
                return '2r'
            else : return 'pl'
        dfVehiculesValides=dfFichier.loc[dfFichier['Véhicule Valide']==1].copy()
        dfVehiculesValides['date_heure']=pd.to_datetime(dfVehiculesValides['Horodate de passage']+':'+dfVehiculesValides['Seconde'].
                                                        astype(int).astype(str)+'.'+dfVehiculesValides['Centième'].astype(int).astype(str), dayfirst=True)
        dfVehiculesValides['type_veh']=dfVehiculesValides.Silhouette.apply(lambda x : type_vehicule(x))
        dfVehiculesValides.rename(columns={'Véhicule Valide':'nbVeh','Vitesse (km/h)':'vitesse'}, inplace=True)
        return dfVehiculesValides      
        
class Viking(object):
    '''
    Donn�es issues de compteurs � tubes
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
        
        dfFichier=pd.read_csv(self.fichier,delimiter=' ',skiprows=1, 
                                 names=['sens', 'jour', 'heureMin','secCent', 'vts', 'ser', 'type_veh'],dtype={'heureMin':str,'secCent':str})
        return dfFichier,anneeDeb,moisDeb,jourDeb
    
    def formaterDonnees(self):
        """
        ajouter l'attribut de date, modifier le sens et le type de vehicule pour coller au format de la classe Mixtra
        """
        def creer_date(jourDeb,moisDeb,anneeDeb, jourMesure,heureMin,secCent) : 
            """
            creer la date d'acquisition. Attention : si comptage sur un mois entier ça ne marche pas
            """
            if jourMesure<int(jourDeb) : 
                moisDeb=str(int(moisDeb)+1)
            return pd.to_datetime(f'20{anneeDeb}-{moisDeb}-{jourMesure} {str(heureMin)[:2]}:{str(heureMin)[2:]}:{str(secCent)[:2]}.{str(secCent)[2:]}')
        self.dfFichier['date_heure']=self.dfFichier.apply(lambda x : creer_date(self.jourDeb,self.moisDeb,self.anneeDeb, 
                            x['jour'],x['heureMin'],x['secCent']), axis=1)
        self.dfFichier['type_veh']=self.dfFichier['type_veh'].str.lower()
        self.dfFichier['nbVeh']=1
        self.dfFichier['vitesse']=self.dfFichier.vts.str[2:].astype(int)
            
class ComptageDonneesIndiv(object):
    """
    a partir de données individuelles de sens 1 et parfois sens 2, creer les donnees 2 sens
    ces donnees indiv peuvent etre issue que de Mixtra ou que de Viking ou de melange
    """
    def __init__(self,dossier, vitesse=False):
        """
        in : 
           dossier : raw string du dossier qui conteint les données individuelles 
           vitesse : boolean : traiter ou non les vitesse
        atributs : 
            dossier : raw string de l'emplacement du dossier
            vitesse : boolean : traiter ou non les vitesse
            df2SensBase : df des passages d echaque vehicule poyr les 2 sens avec horodatage et type_veh
            dfHeureTypeSens : regrouepement des passages par heure, type de vehicule et sens cf GroupeCompletude()
            dicoNbJours : dictionnaire caracterisant les nombr de jours par type sur la periode de dcomptage, cf NombreDeJours()a
            dfSemaineMoyenne : dataframe horaire moyenne par jour (contient les 7 jours de la semaine avec 24 h par jour)
            listAttributs : list des attributs de la df de base avant mise en forme
        """
        self.dossier=dossier
        self.vitesse=vitesse
        self.listAttributs=self.calculListAttributs()
        self.df2SensBase= self.dfSens(*self.analyseSensFichiers())
        self.dfHeureTypeSens,self.dicoNbJours,self.dfSemaineMoyenne=self.MettreEnFormeDonnees()
        
    
    def calculListAttributs(self):
        return ['date_heure','nbVeh','type_veh'] if not self.vitesse else ['date_heure','nbVeh','type_veh','vitesse']
    
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
            listFichiersSens1=list(set([os.path.join(self.dossier,f.name) for f in it for a in ['s1','sens1'] if f.is_file() and f.name.lower().endswith(('.vik','.xls'))  and a in f.name.lower()]))
        with os.scandir(self.dossier) as it:
            listFichiersSens2=list(set([os.path.join(self.dossier,f.name) for f in it for a in ['s2','sens2'] if f.is_file() and f.name.lower().endswith(('.vik','.xls'))  and a in f.name.lower()]))
        return listFichiersSens1,listFichiersSens2
            
    def dfSens(self,listFichiersSens1,listFichiersSens2):
        """
        pour chaque sens obtenir la liste des fichiers Mixtra ou Viking ou les deux
        in :
            listFichiersSens1 : list de chemin en raw string, issu de analyseSensFichiers()
            listFichiersSens2 : list de chemin en raw string
        """
        listDfSens1,listDfSens2=[],[]
        for f in listFichiersSens1 : 
            if f.lower().endswith('.xls') : 
                listDfSens1.append(Mixtra(f).dfFichier[self.listAttributs])
            elif f.lower().endswith('.vik') :
                listDfSens1.append(Viking(f).dfFichier[self.listAttributs])
        for f in listFichiersSens2 : 
            if f.lower().endswith('.xls') : 
                listDfSens2.append(Mixtra(f).dfFichier[self.listAttributs])
            elif f.lower().endswith('.vik') :
                listDfSens2.append(Viking(f).dfFichier[self.listAttributs])
        return pd.concat([pd.concat(listDfSens1, axis=0).assign(sens='sens1'),pd.concat(listDfSens2, axis=0).assign(sens='sens2')], axis=0)
        
    def MettreEnFormeDonnees(self):
        """
        pour chaque sens et 2 sens confondus : gerer les cas ou l enb de jours n'est pas o cf NettoyageTemps()k
       ensuite on regroupe les donnees par heure et type de vehicule et sens, cf GroupeCompletude() 
        """
        
        dfValide=NettoyageTemps(self.df2SensBase)
        dfHeureTypeSens=GroupeCompletude(dfValide,self.vitesse)
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
        date_fin
        dfHeureTypeSens
        dfHoraire2Sens
        dfSemaineMoyenne
    """
    def __init__(self, fichier, gest=None):
        self.fichier_fim=fichier
        self.dico_corresp_type_veh={'TV':('1.T','2.','1.'),'VL':('2.V','4.V'),'PL':('3.P','2.P','4.P')}
        self.dico_corresp_type_fichier={'mode3' : ('1.T','3.P'), 'mode4' : ('2.V','2.P','4.V', '4.P'), 'mode2':('2.',), 'mode1' : ('1.',)}
        self.gest=gest
        lignes=self.ouvrir_fim()
        self.pas_temporel,self.date_debut,self.mode=self.params_fim(lignes)
        liste_lign_titre,self.sens_uniq,self.sens_uniq_nb_blocs=self.liste_carac_fichiers(lignes)
        self.taille_donnees=self.taille_bloc_donnees(lignes,liste_lign_titre)
        self.isoler_bloc(lignes, liste_lign_titre)
        self.dfHeureTypeSens,self.dfHoraire2Sens=self.traficsHoraires(liste_lign_titre)
        self.dfSemaineMoyenne,self.tmja,self.pc_pl, self.pl=self.calcul_indicateurs_agreges()

    def ouvrir_fim(self):
        """
        ouvrir le fichier txt et en sortir la liste des lignes
        """
        with open(self.fichier_fim) as f :
            lignes=[e.strip() for e in f.readlines()]
        return lignes
    
    def corriger_mode(self,lignes, mode):
        """
        correction du fichier fim si mode = 4. dans le fichiers, pour pouvoir diiférencier VL et PL
        """
        i=0
        for e,l in enumerate(lignes) :
            if mode=='4.' : #porte ouvert pour d'auter corrections si beoisn
                if '   4.   ' in l : 
                    if i% 2==0 :
                        lignes[e]=l.replace('   4.   ','   4.V   ')
                        i+=1
                    else : 
                        lignes[e]=l.replace('   4.   ','   4.P   ') 
                        i+=1

    def params_fim(self,lignes):
        """
        obtenir les infos générales du fichier : date_debut(anne, mois, jour, heure, minute), mode
        """
        annee,mois,jour,heure,minute,pas_temporel=(int(lignes[0].split('.')[i].strip()) for i in range(5,11))
        #particularite CD16 : l'identifiant route et section est present dans le FIM
        if self.gest=='CD16' : 
            self.section_cp='_'.join([str(int(lignes[0].split('.')[a].strip())) for a in (2,3)])
        date_debut=pd.to_datetime(f'{jour}-{mois}-{annee} {heure}:{minute}', dayfirst=True)
        mode=lignes[0].split()[9]
        if mode in ['4.',] : #correction si le mode est de type 4. sans distinction exlpicite de VL TV PL. porte ouvert à d'autre cas si besoin 
            self.corriger_mode(lignes, mode)
            mode=lignes[0].split()[9]
        mode=[k for k,v in self.dico_corresp_type_fichier.items() if any([e == mode for e in v])][0]
        if not mode : 
            raise self.fim_TypeModeError
        return pas_temporel,date_debut,mode
        

    def fim_type_veh(self,ligne):
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

    def taille_bloc_donnees(self,lignes_fichiers,liste_lign_titre) : 
        """
        verifier que les blocs de donnees ont tous la mm taille
        in : 
            lignes_fichiers : toute les lignes du fichiers, issu de f.readlines()
        """
        taille_donnees=tuple(set([liste_lign_titre[i+1][0]-(liste_lign_titre[i][0]+1) for i in range(len(liste_lign_titre)-1)]+
                           [len(lignes_fichiers)-1-liste_lign_titre[len(liste_lign_titre)-1][0]]))
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
        
    def traficsHoraires(self,liste_lign_titre):
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
        dfHeureTypeSens=pd.concat([pd.DataFrame({'date_heure':pd.date_range(self.date_debut, periods=len(liste_lign_titre[i][3]), freq=freq),'nbVeh':liste_lign_titre[i][3]})
                   .assign(type_veh=liste_lign_titre[i][1].lower(),sens='sens'+liste_lign_titre[i][2].lower()) for i in range(len(liste_lign_titre))],
                  axis=0)
        dfHeureTypeSens['jour']=dfHeureTypeSens.date_heure.dt.dayofweek
        dfHeureTypeSens['jourAnnee']=dfHeureTypeSens.date_heure.dt.dayofyear
        dfHeureTypeSens['heure']=dfHeureTypeSens.date_heure.dt.hour
        dfHeureTypeSens['date']=pd.to_datetime(dfHeureTypeSens.date_heure.dt.date)
        dfHoraire2Sens=dfHeureTypeSens.groupby(['date_heure','type_veh','jour','jourAnnee','heure','date']).nbVeh.sum().reset_index()
        return dfHeureTypeSens,dfHoraire2Sens

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
      
class PasAssezMesureError(Exception):
    """
    Exception levee si le fichier comport emoins de 7 jours
    """     
    def __init__(self, nbjours):
        Exception.__init__(self,f'le fichier comporte moins de 7 jours complets de mesures. Nb jours complets : {nbjours} ')        
        
        