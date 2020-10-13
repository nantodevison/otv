# -*- coding: utf-8 -*-
'''
Created on 12 oct. 2020

@author: martin.schoreisz
Traitementsdes donnes individuelles fournies par a DREAM TEAM en mixtra ou vikings
'''

import pandas as pd
import altair as alt
from datetime import datetime


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

def GroupeCompletude(dfValide):
    """
    a partir des donnees nettoyees sur le temps, regouper les donnees par tranche horaire, type et sens 
    in : 
        dfValide : df nettoyees avec les attributs date_heure, nbVeh et sens, cf fonction NettoyageTemps()
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
    return dfHeureTypeSens

def NombreDeJours(dfHeureTypeSens):
    """
    a partir d'une df groupees par heure et complete, deduire le nombre de jours plein de comptage, ouvres, samedi, dimanche...
    in :
        dfHeureTypeSens : df avec ena ttribut : date_heure (timestamp horaire), type_veh ('2r',vl,pl,tv), sens (sens1, sens2), nbVeh (integer)
    out : 
        un dico avec en key nbJours, nbJoursOuvres,samedi,dimanche
    """
    dfNbJours=dfHeureTypeSens.set_index('date_heure').resample('1D').count().reset_index().date_heure.dt.dayofweek.value_counts().rename('nbOcc')
    return {'dfNbJours':dfNbJours,
            'nbJours':dfHeureTypeSens.date_heure.dt.dayofyear.nunique(),
            'nbJoursOuvres' : dfHeureTypeSens.loc[dfHeureTypeSens.date_heure.dt.dayofweek.isin(range(5))].date_heure.dt.dayofyear.nunique()}
    
def IndicsGraphs(dfHeureTypeSens, typesVeh, typesDonnees, sens):
    """
    fonction de creation des donnes tabulees et graphs de rendu des donnes de trafic
    in :
       dfHeureTypeSens :  df des données agregées par heure, type de veuhicules et sens, iisu de GroupeCompletude
       types veh : list des types de vehciules souhaites parmi 'tv','vl','pl','2r'
       typesDonnees : list des types de donnée souhiates parmi mja, mjo, samedi, dimanche)
       sens : list des sens que l'on souhaite, parmi : sens1, sens2, 2sens
    out : 
        dicoHoraire : dictionnaire imbriqué à 3 niveaux avec le type de données, le sens, le type de rendu
                     (exemple : pour avoir les données en mja, pour les 2sens : dico['mja']['2sens']['donnees']
    """
    #paramètres selon ce que l'on demande
    dicoNbJours=NombreDeJours(dfHeureTypeSens)
    tupleParams=(('mja',dicoNbJours['nbJours'],range(7)),('mjo',dicoNbJours['nbJoursOuvres'],range(5)),
                 ('samedi',dicoNbJours['dfNbJours'].loc[5],[5]),('dimanche',dicoNbJours['dfNbJours'].loc[6],[6]))
    dicoHoraire={e[0]:{'nbJour':e[1],'listJours':e[2] } for e in tupleParams}
    dicoJournalier={e[0]:{'nbJour':e[1],'listJours':e[2] } for e in tupleParams}
    
    #calcul des donnees
    for t in  typesDonnees : 
        for s in sens :
            dicoHoraire[t][s]={}
            dicoHoraire[t][s]['donnees']=round((dfHeureTypeSens.loc[(dfHeureTypeSens.jour.isin(dicoHoraire[t]['listJours'])) &
                                        (dfHeureTypeSens.type_veh.isin(typesVeh))].groupby(['heure','type_veh','sens']).
                                         nbVeh.sum()/dicoHoraire[t]['nbJour']),0).reset_index() if s=='2sens' else round((
                                        dfHeureTypeSens.loc[(dfHeureTypeSens.jour.isin(dicoHoraire[t]['listJours'])) &
                                        (dfHeureTypeSens.type_veh.isin(typesVeh)) & (dfHeureTypeSens.sens==s)
                                        ].groupby(['heure','type_veh','sens']).
                                         nbVeh.sum()/dicoHoraire[t]['nbJour']),0).reset_index()
            dicoHoraire[t][s]['graph']=alt.Chart(dicoHoraire[t][s]['donnees'].loc[dicoHoraire[t][s]['donnees'].type_veh.isin(typesVeh)].groupby(
                                    ['heure','type_veh']).sum().reset_index()).mark_line().encode(
                                     x='heure:O', y='nbVeh:Q',color='type_veh') if s=='2sens' else alt.Chart(
                                         dicoHoraire[t][s]['donnees'].loc[(dicoHoraire[t][s]['donnees'].type_veh.isin(typesVeh)) & 
                                       (dicoHoraire[t][s]['donnees'].sens==s)].groupby(['heure','type_veh']).sum().reset_index()).mark_line().encode(
                                           x='heure:O', y='nbVeh:Q',color='type_veh')
            
            dicoJournalier[t][s]={}
            data_temp=round(dfHeureTypeSens.loc[(dfHeureTypeSens.jour.isin(dicoHoraire[t]['listJours']))].groupby(['jour','type_veh','sens']).nbVeh.sum(),0
                            ).reset_index().merge(dicoNbJours['dfNbJours'],left_on='jour', right_index=True) if s=='2sens' else round(
                    dfHeureTypeSens.loc[(dfHeureTypeSens.jour.isin(dicoHoraire[t]['listJours'])) & (dfHeureTypeSens.sens==s)].
                    groupby(['jour','type_veh','sens']).nbVeh.sum(),0).reset_index().merge(
                        dicoNbJours['dfNbJours'],left_on='jour', right_index=True)
            data_temp['nbVeh']=data_temp['nbVeh']/data_temp['nbOcc']
            dicoJournalier[t][s]['donnees']=data_temp
            dicoJournalier[t][s]['donnees'].sort_values(['jour'], inplace=True)
            dicoJournalier[t][s]['donnees']['jour']=dicoJournalier[t][s]['donnees']['jour'].replace(
                {0:'lundi', 1:'mardi', 2:'mercredi', 3:'jeudi',4:'vendredi',5:'samedi',6:'dimanche'}) 
            dicoJournalier[t][s]['graph']=alt.Chart(dicoJournalier[t][s]['donnees'].loc[dicoJournalier[t][s]['donnees'].type_veh.
                                    isin(typesVeh)]).mark_bar().encode(
                                alt.X('type_veh:N', axis=alt.Axis(title=''), sort=['tv','vl','pl','2r']),
                                alt.Y('nbVeh:Q', axis=alt.Axis(title='Nombre de véhicules', grid=False)),
                                color=alt.Color('type_veh:N'),
                                column=alt.Column('jour:O', sort=['lundi', 'mardi', 'mercredi', 'jeudi','vendredi','samedi','dimanche'])
                            ).configure_view(stroke='transparent') if s=='2sens' else alt.Chart(
                                dicoJournalier[t][s]['donnees'].loc[(dicoJournalier[t][s]['donnees'].type_veh.
                                    isin(typesVeh)) & (dicoJournalier[t][s]['donnees'].sens==s)]).mark_bar().encode(
                                alt.X('type_veh:N', axis=alt.Axis(title=''), sort=['tv','vl','pl','2r']),
                                alt.Y('nbVeh:Q', axis=alt.Axis(title='Nombre de véhicules', grid=False)),
                                color=alt.Color('type_veh:N'),
                                column=alt.Column('jour:O', sort=['lundi', 'mardi', 'mercredi', 'jeudi','vendredi','samedi','dimanche'])
                            ).configure_view(stroke='transparent')
            if s=='2sens' : 
                dicoJournalier[t]['compSens']={}
                data_temp=round((dfHeureTypeSens.loc[dfHeureTypeSens.jour.isin(dicoHoraire[t]['listJours'])
                    ].groupby(['jour','sens']).nbVeh.sum()),0).reset_index().merge(dicoNbJours['dfNbJours'],left_on='jour', right_index=True)
                data_temp['nbVeh']=data_temp['nbVeh']/data_temp['nbOcc']
                dicoJournalier[t]['compSens']['donnees']=data_temp
                dicoJournalier[t]['compSens']['donnees'].sort_values('jour', inplace=True)
                dicoJournalier[t]['compSens']['donnees']['jour']=dicoJournalier[t]['compSens']['donnees']['jour'].replace(
                    {0:'lundi', 1:'mardi', 2:'mercredi', 3:'jeudi',4:'vendredi',5:'samedi',6:'dimanche'}) 
                
                dicoJournalier[t]['compSens']['graph']=alt.Chart(
                        dicoJournalier[t]['compSens']['donnees'], title='Trafic moyen journalier par sens').mark_bar().encode(
                        alt.X('sens:N', axis=alt.Axis(title=''), sort=['tv','vl','pl','2r']),
                        alt.Y('nbVeh:Q', axis=alt.Axis(title='Nombre de véhicules', grid=False)),
                        color=alt.Color('sens:N'),
                        column=alt.Column('jour:O', sort=['lundi', 'mardi', 'mercredi', 'jeudi','vendredi','samedi','dimanche'])
                    ).configure_view(stroke='transparent')                      
    return dicoHoraire, dicoJournalier

def IndicsPeriodes(dfHeureTypeSens):
    """
    fournir les indicateurs suer periode hpm, hps, jour et nuit, sous frome de df par sens
    """
    dicoNbJours=NombreDeJours(dfHeureTypeSens)
    hpm=round(dfHeureTypeSens.loc[dfHeureTypeSens.heure.isin(range(7,10))].groupby(['sens','type_veh']).nbVeh.sum()/dicoNbJours['nbJoursOuvres'],0).reset_index()
    hps=round(dfHeureTypeSens.loc[dfHeureTypeSens.heure.isin(range(16,19))].groupby(['sens','type_veh']).nbVeh.sum()/dicoNbJours['nbJoursOuvres'],0).reset_index()
    nuit=round(dfHeureTypeSens.loc[dfHeureTypeSens.heure.isin([a for a in range(6)]+[22,23])].groupby(['sens','type_veh']).nbVeh.sum()/dicoNbJours['nbJoursOuvres'],0).reset_index()
    jour=round(dfHeureTypeSens.loc[dfHeureTypeSens.heure.isin(range(6,22))].groupby(['sens','type_veh']).nbVeh.sum()/dicoNbJours['nbJoursOuvres'],0).reset_index()
    return hpm,hps,nuit,jour

class Mixtra(object):
    '''
    Donn�es issues de compteurs a tubes
    pour chaque point de comptage on peut avoir 1 ou 2 sens. 
    pour chaque sens on peut avoir 1 ou plusieurs fichiers 
    '''
    
    def __init__(self, listFichiersSens1,listFichiersSens2=None):
        '''
        Constructor
        in : 
            listFichiersSens1 : la liste des fcihiers utilisé pour le sens 1
            listFichiersSens 2 : la liste des fcihiers utilisés pour le sens 2
        '''
        self.fichier2sens=self.RegrouperSens(listFichiersSens1,listFichiersSens2)
    
    def RegrouperSens(self,listFichiersSens1,listFichiersSens2 ):
        """
        pour chaque sens, les regrouper et nettoyer
        """
        fichierSens1=pd.concat([pd.read_csv(a, delimiter='\t', encoding='latin_1') 
                               for a in listFichiersSens1]) if len(listFichiersSens1) > 1 else pd.read_csv(
                                   listFichiersSens1[0], delimiter='\t', encoding='latin_1') 
        fichierSens1['sens']='sens1'
        if listFichiersSens2 :
            fichierSens2=pd.concat([pd.read_csv(a, delimiter='\t', encoding='latin_1') 
                                   for a in listFichiersSens2], axis=0) if len(listFichiersSens2) > 1 else pd.read_csv(
                                       listFichiersSens2[0], delimiter='\t', encoding='latin_1')
            fichierSens2['sens']='sens2'
            return pd.concat([fichierSens1,fichierSens2], axis=0)
        else : 
            return fichierSens1
    
    def NettoyageDonnees(self):
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
        dfVehiculesValides=self.fichier2sens.loc[self.fichier2sens['Véhicule Valide']==1].copy()
        dfVehiculesValides['date_heure']=pd.to_datetime(dfVehiculesValides['Horodate de passage']+':'+dfVehiculesValides['Seconde'].
                                                        astype(int).astype(str)+'.'+dfVehiculesValides['Centième'].astype(int).astype(str))
        dfVehiculesValides['type_veh']=dfVehiculesValides.Silhouette.apply(lambda x : type_vehicule(x))
        dfVehiculesValides.rename(columns={'Véhicule Valide':'nbVeh'}, inplace=True)
        return dfVehiculesValides      
        
class Viking(object):
    '''
    Donn�es issues de compteurs � tubes
    pour chaque point de comptage on peut avoir 1 ou 2 sens. 
    pour chaque sens on peut avoir 1 ou plusieurs fichiers 
    '''
    
    def __init__(self, fichiersSens1,fichiersSens2=None):
        '''
        Constructor
        in : 
            FichiersSens1 : raw string du fcihiers utilisé pour le sens 1
            FichiersSens2 : raw string du fcihiers utilisés pour le sens 2
        '''
        self.fichier2sens, self.anneeDeb,self.moisDeb,self.jourDeb=self.RegrouperSens(fichiersSens1,fichiersSens2)
        self.formaterDonnees()
        
    def RegrouperSens(self,fichiersSens1,fichiersSens2 ):
        """
        pour chaque sens, les regrouper et mettre ne form les attributs
        """
        with open(fichiersSens1) as f :
                entete=[e.strip() for e in f.readlines()][0]
        anneeDeb,moisDeb,jourDeb=(entete.split('.')[i].strip() for i in range(5,8)) 
        
        if fichiersSens2 :
            dfFichier2Sens = pd.concat([pd.read_csv(f,delimiter=' ',skiprows=1,
                            names=['sens', 'jour', 'heureMin','secCent', 'vts', 'ser', 'type_veh'],
                            dtype={'heureMin':str,'secCent':str, 'sens':str})
                            for f in (fichiersSens1,fichiersSens2)], axis=0) 
        else : 
            dfFichier2Sens=pd.read_csv(fichiersSens1,delimiter=' ',skiprows=1, 
                                 names=['sens', 'jour', 'heureMin','secCent', 'vts', 'ser', 'type_veh'],dtype={'heureMin':str,'secCent':str})
        return dfFichier2Sens,anneeDeb,moisDeb,jourDeb
    
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
        
        self.fichier2sens['date_heure']=self.fichier2sens.apply(lambda x : creer_date(self.jourDeb,self.moisDeb,self.anneeDeb, 
                            x['jour'],x['heureMin'],x['secCent']), axis=1)
        self.fichier2sens['sens']='sens'+self.fichier2sens['sens']
        self.fichier2sens['type_veh']=self.fichier2sens['type_veh'].str.lower()
        self.fichier2sens['nbVeh']=1
            
        
class PasAssezMesureError(Exception):
    """
    Exception levee si le fichier comport emoins de 7 jours
    """     
    def __init__(self, nbjours):
        Exception.__init__(self,f'le fichier comporte moins de 7 jours complets de mesures. Nb jours complets : {nbjours} ')        
        
        