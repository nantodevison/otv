# -*- coding: utf-8 -*-
'''
Created on 12 oct. 2020

@author: martin.schoreisz
Traitementsdes donnes individuelles fournies par a DREAM TEAM en mixtra ou vikings
'''

import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime
import os, statistics

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
    Pour les donnees individuelles uniquement
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
        dfHeureTypeSens : df avec ena ttribut : date_heure (timestamp horaire), type_veh ('2r',vl,pl,tv), sens (sens1, sens2), nbVeh (integer) cf GroupeCompletude()
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
    """
    def __init__(self, fichier, gest=None):
        self.fichier_fim=fichier
        self.dico_corresp_type_veh={'TV':('1.T','2.','1.'),'VL':('2.V','4.V'),'PL':('3.P','2.P','4.P')}
        self.dico_corresp_type_fichier={'mode3' : ('1.T','3.P'), 'mode4' : ('2.V','2.P','4.V', '4.P'), 'mode2':('2.',), 'mode1' : ('1.',)}
        self.gest=gest

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
        annee,mois,jour,heure,minute,self.pas_temporel=(int(lignes[0].split('.')[i].strip()) for i in range(5,11))
        #particularite CD16 : l'identifiant route et section est present dans le FIM
        if self.gest=='CD16' : 
            self.section_cp='_'.join([str(int(lignes[0].split('.')[a].strip())) for a in (2,3)])
        self.date_debut=pd.to_datetime(f'{jour}-{mois}-{annee} {heure}:{minute}', dayfirst=True)
        mode=lignes[0].split()[9]
        if mode in ['4.',] : #correction si le mode est de type 4. sans distinction exlpicite de VL TV PL. porte ouvert à d'autre cas si besoin 
            self.corriger_mode(lignes, mode)
            mode=lignes[0].split()[9]
        self.mode=[k for k,v in self.dico_corresp_type_fichier.items() if any([e == mode for e in v])][0]
        if not self.mode : 
            raise self.fim_TypeModeError

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
        self.sens_uniq=True if len(set([e[2] for e in liste_lign_titre]))==1 else False
        self.sens_uniq_nb_blocs=len(liste_lign_titre) if self.sens_uniq else np.NaN 
        return liste_lign_titre

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
        else : self.taille_donnees=taille_donnees[0]

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
        
    def df_trafic_brut_horaire(self,liste_lign_titre):
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
        self.dfHeureTypeSens=pd.concat([pd.DataFrame({'date_heure':pd.date_range(self.date_debut, periods=len(liste_lign_titre[i][3]), freq=freq),'nbVeh':liste_lign_titre[i][3]})
                   .assign(type_veh=liste_lign_titre[i][1].lower(),sens='sens'+liste_lign_titre[i][2].lower()) for i in range(len(liste_lign_titre))],
                  axis=0)
        self.dfHeureTypeSens['jour']=self.dfHeureTypeSens.date_heure.dt.dayofweek
        self.fHeureTypeSens['jourAnnee']=self.dfHeureTypeSens.date_heure.dt.dayofyear
        self.dfHeureTypeSens['heure']=self.dfHeureTypeSens.date_heure.dt.hour
        self.dfHeureTypeSens['date']=pd.to_datetime(self.dfHeureTypeSens.date_heure.dt.date)

    def calcul_indicateurs_horaire(self):
        """
        ajouter le tmja et le nb PL au xdonnes de df_trafic_brut_horaire
        """
        self.df_tot_heure=self.df_tot_heure.reset_index().rename(columns={'index':'date'})

        def sommer_trafic_h(dateindex):
            if not self.sens_uniq : 
                if self.mode=='mode3' : 
                    return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens2.values[0],
                            self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens2.values[0])
                elif self.mode=='mode4' :
                    colVL1,colVL2=[e for e in self.df_tot_heure.columns if 'VL' in e][0], [e for e in self.df_tot_heure.columns if 'VL' in e][1]
                    colPL1,colPL2=[e for e in self.df_tot_heure.columns if 'PL' in e][0], [e for e in self.df_tot_heure.columns if 'PL' in e][1]
                    return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex][colVL1].values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex][colVL2].values[0]+
                            self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex][colPL1].values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex][colPL2].values[0],
                            self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex][colPL1].values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex][colPL2].values[0])
                elif self.mode=='mode2':
                    return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens2.values[0],np.NaN)
                elif self.mode=='mode1':
                    return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens2.values[0],np.NaN)
            else : 
                if self.sens_uniq_nb_blocs==4:
                    if self.mode=='mode3' : 
                        return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1_x.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1_y.values[0],
                                self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1_x.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1_y.values[0])
                    elif self.mode=='mode4' :
                        return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].VL_sens1_x.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].VL_sens1_y.values[0]+
                                self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1_x.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1_y.values[0],
                                self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1_x.values[0]+self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1_y.values[0])
                    elif self.mode=='mode2':
                        return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1_x.values[0]+
                                self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1_y.values[0],np.NaN)
                elif self.sens_uniq_nb_blocs==2:
                    if self.mode=='mode3' : 
                        return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1.values[0],
                                self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1.values[0])
                    elif self.mode=='mode4' :
                        return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].VL_sens1.values[0]+
                                self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1.values[0],
                                self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].PL_sens1.values[0])
                    elif self.mode=='mode2':
                        return (self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1.values[0]+
                                self.df_tot_heure.loc[self.df_tot_heure['date']==dateindex].TV_sens1.values[0],np.NaN)
                else : 
                    raise self.fimNbBlocDonneesError(self.mode)
                    
                
        self.df_tot_heure['tv_tot']=self.df_tot_heure.apply(lambda x : sommer_trafic_h(x.date)[0],axis=1)
        self.df_tot_heure['pl_tot']=self.df_tot_heure.apply(lambda x : sommer_trafic_h(x.date)[1] if self.mode not in ('mode2','mode1') else
                                                            np.NaN ,axis=1)
        self.df_tot_heure['pc_pl_tot']=self.df_tot_heure.apply(lambda x : x['pl_tot']*100/x['tv_tot'] if x['tv_tot']!=0 else 0 if self.mode not in ('mode2','mode1') else
                                                            np.NaN,axis=1)
        self.df_tot_heure.set_index('date', inplace=True)

    def calcul_indicateurs_agreges(self):
        """
        calculer le tmjs, pl et pc_pl pour un fichier
        """
        #regrouper par jour et calcul des indicateurs finaux
        self.df_tot_jour=self.df_tot_heure[['tv_tot','pl_tot']].resample('1D').sum() if self.mode not in ('mode2','mode1') else self.df_tot_heure[['tv_tot']].resample('1D').sum()
        #selon le nombre de jour comptés on prend soit tous les jours sauf les 1ers et derniers, soit on somme les 1ers et derniers
        #si le nb de jours est inéfrieur à 7 on leve une erreur
        if len(self.df_tot_jour)<7 : 
            raise PasAssezMesureError(len(self.df_tot_jour))
        elif len(self.df_tot_jour) in (7,8) : 
            traf_list=self.df_tot_jour.tv_tot.tolist()
            self.tmja=int(statistics.mean([traf_list[0]+traf_list[-1]]+traf_list[1:-1]))
            pl_list=self.df_tot_jour.pl_tot.tolist() if self.mode not in ('mode2','mode1') else np.NaN
            self.pl=int(statistics.mean([pl_list[0]+pl_list[-1]]+pl_list[1:-1])) if self.mode not in ('mode2','mode1') else np.NaN
        else : 
            self.tmja=int(self.df_tot_jour.iloc[1:-1].tv_tot.mean())
            self.pl=int(self.df_tot_jour.iloc[1:-1].pl_tot.mean()) if self.mode not in ('mode2','mode1') else np.NaN
        if self.tmja==0 : 
            self.pc_pl=0
        else :
            self.pc_pl=round(self.pl*100/self.tmja,1) if self.mode not in ('mode2','mode1') else np.NaN
    
    def resume_indicateurs(self):
        """
        procedure complete de calcul des indicateurs agreges
        """
        lignes=self.ouvrir_fim()
        self.params_fim(lignes)
        liste_lign_titre=self.liste_carac_fichiers(lignes)
        self.taille_bloc_donnees(lignes,liste_lign_titre)
        self.isoler_bloc(lignes, liste_lign_titre)
        self.df_trafic_brut_horaire(liste_lign_titre)
        self.calcul_indicateurs_horaire()
        self.calcul_indicateurs_agreges()

      
class PasAssezMesureError(Exception):
    """
    Exception levee si le fichier comport emoins de 7 jours
    """     
    def __init__(self, nbjours):
        Exception.__init__(self,f'le fichier comporte moins de 7 jours complets de mesures. Nb jours complets : {nbjours} ')        
        
        