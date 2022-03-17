# -*- coding: utf-8 -*-
'''
Created on 18 nov. 2021

@author: martin.schoreisz
'''


dico_mois={'janv':[1,'Janv','Janvier', 'janu'], 'fevr':[2,'Fév','Février','févr','Fevrier', 'febr'], 'mars':[3,'Mars','Mars', 'marc'], 'avri':[4,'Avril','Avril', 'apri' ], 'mai':[5,'Mai','Mai', 'may'], 'juin':[6,'Juin','Juin', 'june'], 
           'juil':[7,'Juill','Juillet', 'juil', 'july'], 'aout':[8,'Août','Aout', 'augu'], 'sept':[9,'Sept','Septembre', 'sept'], 'octo':[10,'Oct','Octobre', 'octo'], 'nove':[11,'Nov','Novembre', 'nove'], 'dece':[12,'Déc','Décembre','Decembre', 'dece']}

   
def renommerMois(df):
    """
    dans une df mensuelle, renommer les mois pour coller aux cles du dico_mois
    in :     
        df :dataframe contenant des references aux mois selon les valeurs attendus dans dico_mois
    out : 
        nouvelle df avec les noms renommes
    """
    return df.rename(columns={c : k for k, v in dico_mois.items() for c in df.columns if c in v})
    