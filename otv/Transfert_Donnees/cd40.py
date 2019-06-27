# -*- coding: utf-8 -*-
'''
Created on 18 mars 2019

@author: martin.schoreisz

Rappatrier les donnees du CD40
'''

import pandas as pd
import os
import Connexion_Transfert as ct
import Outils as O

#lire les fichiers
for chemin, dossier, files in os.walk(r"Q:\DAIT\TI\DREAL33\2019\C19SA0035_OTR-NA\Doc_travail\Donnees_source\CD40\Trafics 2018 Landes\comptage_B152") :
    for fichier in files : 
        if fichier.endswith('.xls') :
            print(chemin,fichier)
            path_donnees=os.path.join(chemin,fichier)
            df_fichier_xls=df=pd.read_excel(path_donnees,headers=None, skiprows=1) #les 1eres lignes mettent le bordel dans la definition des colonnes
            
            #epurer les donn�es
            df2=df.dropna(how='all').dropna(axis=1,how='all')
            
            #d�finir les variables globales
            df2=df.dropna(how='all').dropna(axis=1,how='all')
            compteur='040.'+df2.loc[0,'Unnamed: 125'].split(' ')[1]
            vma=int(df2.loc[4,'Unnamed: 0'].split(' : ')[1][:2])
            voie=O.epurationNomRoute(df2.loc[4,'Unnamed: 141'].split(' ')[1])
            pr,absice=df2.loc[4,'Unnamed: 125'].split(' ')[1],df2.loc[4,'Unnamed: 125'].split(' ')[2]
            dep,gest, reseau,concession,type_poste,annee_cpt='40','CD40','D','N','permanent','2018'
            id_comptag=dep+'-'+voie+'-'+pr+'+'+absice
            tmja=df2.loc[18,'Unnamed: 107']
            pc_pl=df2.loc[19,'Unnamed: 107']
            
            #recuperer les donnees mensuelles
            donnees=df2.loc[[7,18,19],['Unnamed: 13', 'Unnamed: 23', 'Unnamed: 30',
                   'Unnamed: 37', 'Unnamed: 44', 'Unnamed: 51', 'Unnamed: 58',
                   'Unnamed: 65', 'Unnamed: 72', 'Unnamed: 79', 'Unnamed: 86',
                   'Unnamed: 93', 'Unnamed: 100', 'Unnamed: 107']].dropna(axis=1,how='all')
                #renommer les colonnes
            donnees.columns=[element.replace('é','e').replace('.','').lower() for element in list(donnees.loc[7])]
                #remplacer l'annee en string et ne conserver 
            donnees=donnees.drop(7).replace(['D.Moy.Jour', '% PL'],['tmja','pc_pl'])
                #inserer les valeusr qui vont bien
            donnees['annee']='2018'
            donnees['id_comptag']=id_comptag
                #r�arranger les colonnes
            cols=donnees.columns.tolist()
            cols_arrangees=cols[-1:]+cols[:1]+cols[-2:-1]+cols[1:-2]
            donnees=donnees[cols_arrangees]
            donnees.columns=['id_comptag','donnees_type','annee','janv','fevr','mars','avri','mai','juin','juil','aout','sept','octo','nove','dece']
            
            #inserer les donn�es
            with ct.ConnexionBdd('local_otv') as c : 
                c.curs.execute("select distinct id_comptag from comptage.na_2010_2017_p")
                if id_comptag in [record[0] for record in c.curs] :
                    c.curs.execute("update comptage.na_2010_2017_p set tmja_2018=%s, pc_pl_2018=%s, id_cpt=%s, ann_cpt=%s where id_comptag=%s",(tmja, pc_pl,compteur,annee_cpt,id_comptag))
                else : 
                    c.curs.execute("insert into comptage.na_2010_2017_p (id_comptag, dep, route, pr, abs, reseau, gestionnai, concession,type_poste, id_cpt, ann_cpt, tmja_2018, pc_pl_2018) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(id_comptag,dep,voie,pr,absice,reseau,gest,concession,type_poste,compteur,annee_cpt, tmja, pc_pl))
                c.connexionPsy.commit()
                donnees.to_sql('na_2010_2017_mensuel', c.sqlAlchemyConn,schema='comptage',if_exists='append',index=False)