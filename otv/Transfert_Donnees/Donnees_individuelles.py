# -*- coding: utf-8 -*-
'''
Created on 12 oct. 2020

@author: martin.schoreisz
Traitementsdes donnes individuelles fournies par a DREAM TEAM en mixtra ou vikings
'''

import pandas as pd
import plotly.express as px
from datetime import datetime
import os

#tuple avec le type de jours, le nombre de jours associes et les jours (de 0à6)
tupleParams=(('mja',7,range(7)),('mjo',5,range(5)),('samedi',1,[5]),('dimanche',1,[6]))

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

def semaineMoyenne(dfHeureTypeSens,dicoNbJours):
    """
    a partir des donnees pour chaque heure, type de veh et sens, et en fonction du nombre de jours de mesure,
    obtenir un semaine moyenne
    """
    dfMoyenne=dfHeureTypeSens.groupby(['jour','heure','type_veh','sens']).nbVeh.sum().reset_index().merge(dicoNbJours['dfNbJours'],left_on='jour', right_index=True)
    dfMoyenne['nbVeh']=dfMoyenne['nbVeh']/dfMoyenne['nbOcc']
    return dfMoyenne
    
def IndicsGraphs(dfMoyenne, typesVeh, typesDonnees, sens):
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
    dicoJournalier={e[0]:{'nbJour':e[1],'listJours':e[2] } for e in tupleParams}
    
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
                         x='heure', y='nbVeh', color='type_veh')
            
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
                                          y="nbVeh", color="type_veh", barmode="group") if s=='2sens' else px.bar(
                                          dicoJournalier[t][s]['donnees'].loc[(dicoJournalier[t][s]['donnees'].type_veh.
                                           isin(typesVeh)) & (dicoJournalier[t][s]['donnees'].sens==s)],x="jour", 
                                          y="nbVeh", color="type_veh", barmode="group")
            
            if s=='2sens' : 
                dicoJournalier[t]['compSens']={}
                dicoJournalier[t]['compSens']['donnees']=round((dfMoyenne.loc[(dfMoyenne.jour.isin(dicoHoraire[t]['listJours'])) & (dfMoyenne.type_veh.isin(typesVeh))
                    ].groupby(['jour','sens']).nbVeh.sum()),0).reset_index()
                dicoJournalier[t]['compSens']['donnees'].sort_values('jour', inplace=True)
                dicoJournalier[t]['compSens']['donnees']['jour']=dicoJournalier[t]['compSens']['donnees']['jour'].replace(
                    {0:'lundi', 1:'mardi', 2:'mercredi', 3:'jeudi',4:'vendredi',5:'samedi',6:'dimanche'}) 
                dicoJournalier[t]['compSens']['graph']=px.bar(dicoJournalier[t]['compSens']['donnees'],
                                                              x="jour", y="nbVeh", color="sens", barmode="group",
                                                              title=f' Comparaison des sens ; {t}')              
    return dicoHoraire, dicoJournalier

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
        dfVehiculesValides.rename(columns={'Véhicule Valide':'nbVeh'}, inplace=True)
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
            
class ComptageDonneesIndiv(object):
    """
    a partir de données individuelles de sens 1 et parfois sens 2, creer les donnees 2 sens
    ces donnees indiv peuvent etre issue que de Mixtra ou que de Viking ou de melange
    """
    def __init__(self,dossier):
        """
        en entree on attend une ou deux dfavec le mm format, issus des classes Viking ou Mixtr
        atributs : 
            dossier : raw string de l'emplacement du dossier
            df2SensBase : df des passages d echaque vehicule poyr les 2 sens avec horodatage et type_veh
            dfHeureTypeSens : regrouepement des passages par heure, type de vehicule et sens cf GroupeCompletude()
            dicoNbJours : dictionnaire caracterisant les nombr de jours par type sur la periode de dcomptage, cf NombreDeJours()a
        """
        self.dossier=dossier
        self.df2SensBase= self.dfSens(*self.analyseSensFichiers())
        self.dfHeureTypeSens,self.dicoNbJours,self.dfMoyenne=self.MettreEnFormeDonnees()
    
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
                listDfSens1.append(Mixtra(f).dfFichier[['date_heure','nbVeh','type_veh']])
            elif f.lower().endswith('.vik') :
                listDfSens1.append(Viking(f).dfFichier[['date_heure','nbVeh','type_veh']])
        for f in listFichiersSens2 : 
            if f.lower().endswith('.xls') : 
                listDfSens2.append(Mixtra(f).dfFichier[['date_heure','nbVeh','type_veh']])
            elif f.lower().endswith('.vik') :
                listDfSens2.append(Viking(f).dfFichier[['date_heure','nbVeh','type_veh']])
        return pd.concat([pd.concat(listDfSens1, axis=0).assign(sens='sens1'),pd.concat(listDfSens2, axis=0).assign(sens='sens2')], axis=0)
        
    def MettreEnFormeDonnees(self):
        """
        pour chaque sens et 2 sens confondus : gerer les cas ou l enb de jours n'est pas o cf NettoyageTemps()k
       ensuite on regroupe les donnees par heure et type de vehicule et sens, cf GroupeCompletude() 
        """
        
        dfValide=NettoyageTemps(self.df2SensBase)
        dfHeureTypeSens=GroupeCompletude(dfValide)
        dicoNbJours=NombreDeJours(dfHeureTypeSens)
        dfMoyenne=semaineMoyenne(dfHeureTypeSens, dicoNbJours)
        return dfHeureTypeSens,dicoNbJours,dfMoyenne
    
    def graphs(self,typesVeh, typesDonnees, sens):
        """
        creer les graphs pour visu
        """
        self.dicoHoraire,self.dicoJournalier=IndicsGraphs(self.dfMoyenne,typesVeh,typesDonnees,sens)
       
class PasAssezMesureError(Exception):
    """
    Exception levee si le fichier comport emoins de 7 jours
    """     
    def __init__(self, nbjours):
        Exception.__init__(self,f'le fichier comporte moins de 7 jours complets de mesures. Nb jours complets : {nbjours} ')        
        
        