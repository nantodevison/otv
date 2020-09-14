# -*- coding: utf-8 -*-
'''
Created on 14 sept 2020

@author: martin.schoreisz

module d'importation des données de trafics forunies par les gestionnaires
'''

import pandas as pd
import geopandas as gp
import numpy as np
import os, re, csv,statistics,filecmp, unidecode
from geoalchemy2 import Geometry,WKTElement
from shapely.geometry import Point, LineString
from shapely.ops import transform
from itertools import combinations
from collections import Counter

import Connexion_Transfert as ct
import Outils as O

def statsHoraires(df_horaire,attributHeure, typeVeh,typeJour='semaine'):
    """
    Calculer qq stats à partir d'une df des données horaires formattée comme dans la Bdd  
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

def correctionHoraire(df_horaire):
    """
    corriger une df horaire en passant à -99 les valeurs qui semble non correlées avec le reste des valeusr
    """
    #corriger les valuers inférieures à moyenne-2*ecart_type
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
