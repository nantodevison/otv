# -*- coding: utf-8 -*-
'''
Created on 27 juin 2019

@author: martin.schoreisz

module d'importation des données de trafics forunies par les gestionnaires
'''

import pandas as pd
import os, re
import Connexion_Transfert as ct
import Outils as O


def cd40():
    """
    fonction faite vie fait pour transférer les données stockée dans un répertoire vers la bdd OTV sur la station GTI
    il faudrait ajouter les paramètres d'année, de dossier / fichiers source au moins
    """
    #lire les fichiers
    for chemin, dossier, files in os.walk(r"Q:\DAIT\TI\DREAL33\2019\C19SA0035_OTR-NA\Doc_travail\Donnees_source\CD40\Trafics 2018 Landes\comptage_B152") :
        for fichier in files : 
            if fichier.endswith('.xls') :
                path_donnees=os.path.join(chemin,fichier)
                df_fichier_xls=df=pd.read_excel(path_donnees,headers=None, skiprows=1) #les 1eres lignes mettent le bordel dans la definition des colonnes
                
                #epurer les donn�es
                df2=df.dropna(how='all').dropna(axis=1,how='all')
                
                #d�finir les variables globales
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
                with ct.ConnexionBdd('gti_otv') as c : 
                    c.curs.execute("select distinct id_comptag from comptage.na_2010_2018_p")
                    if id_comptag in [record[0] for record in c.curs] :
                        print(f'update {chemin+fichier}')
                        c.curs.execute("update comptage.na_2010_2018_p set tmja_2018=%s, pc_pl_2018=%s, id_cpt=%s, ann_cpt=%s where id_comptag=%s",(tmja, pc_pl,compteur,annee_cpt,id_comptag))
                    else : 
                        c.curs.execute("insert into comptage.na_2010_2018_p (id_comptag, dep, route, pr, abs, reseau, gestionnai, concession,type_poste, id_cpt, ann_cpt, tmja_2018, pc_pl_2018) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(id_comptag,dep,voie,pr,absice,reseau,gest,concession,type_poste,compteur,annee_cpt, tmja, pc_pl))
                        print(f'insert {chemin+fichier}')                    
                    c.connexionPsy.commit()
                    donnees.to_sql('na_2010_2018_mensuel', c.sqlAlchemyConn,schema='comptage',if_exists='append',index=False)
                    
def cd17(fichier):
    """
    fonction d'extraction des donnees numeriques liees au comptages cponctuels issus de brochures du CD17:
    P:\DAIT\TI\Donnees\1-ROUTIER\3 - Poitou-Charentes\17 - Charente-Maritime\2015\2015_CD17_carte_comptage_routiers.pdf
    P:\DAIT\TI\Donnees\1-ROUTIER\3 - Poitou-Charentes\17 - Charente-Maritime\2016\Brochure Comptages 2016.pdf
    en entree, le nom complet du fichier en raw string
    il faut que le fichier soit creer par ouvertur du pdf dans firefox pour permettre le copier-coller, copier-coller dans un fihcier .txt
    via notepad++, enregistrement du fichier.txt. le fichier .txt ne contient pas les en-tete de colonne
    """
    # ouvrir le fichier et trouver voie, pr et abscisse
    with open(fichier, encoding="utf-8") as fichier :  # ouvrir l fichier
        liste = [element.replace('\n', ' ').replace('    ', ' ').replace('   ', ' ').replace('  ', ' ') for element in fichier]
        liste_decomposee_ligne = re.split(r'(Janvier|Février|Mars|Avril|Mai|Juin|Juillet|Août|Sept.|Oct.|Nov.|Déc.)', "".join(liste))  # permet de conserver le mois
        liste_decomposee_ligne = [liste_decomposee_ligne[i] + liste_decomposee_ligne[i + 1] for i in range(0, len(liste_decomposee_ligne) - 1, 2)]  # necessaier pour repasser le mois dans le string
        liste_decomposee_ligne[0] = ' ' + liste_decomposee_ligne[0]  # uniformité des données
        liste_decomposee_ligne = list(filter(None, liste_decomposee_ligne))
        voie, pr, abscisse = [element.split(' ')[1]for element in liste_decomposee_ligne], [element.split(' ')[2]for element in liste_decomposee_ligne], [element.split(' ')[3]for element in liste_decomposee_ligne]
        # pour le tmj et %PL c'est plus compliqué car la taille de la cellule localisation varie, son délimiteur aussi et les chiffres peuvent être entier ou flottant, don on va se baser sur le fait
        # que la rechreche d'un nombre a virgule renvoi le %PL, sinon la vitesse, et si c'est la vitesse, alors ca créer une value error en faisant le float sur l'element + 1, donc on 
        # sait que c'est la vitesse
        pc_pl, v85, tmj = [], [], []
        for element in liste_decomposee_ligne : 
            element_decompose = element.split()
            nombre_a_virgule = re.search('[0-9]{1,}\,[0-9]{1,}', element)  # rechreche un truc avec deux chiffres séparés par une virgule : renvoi un objet match si ok, none sinon
            if nombre_a_virgule : 
                try : 
                    v85.append(float(element_decompose[element_decompose.index(nombre_a_virgule.group()) + 1].replace(',', '.')))  # idem tmj
                    pc_pl.append(float(nombre_a_virgule.group().replace(',', '.'))) 
                    tmj.append(int(element_decompose[element_decompose.index(nombre_a_virgule.group()) - 2]))  # donc on en deduit le tmja selon al position (car seare par un ' ' dans le fichier de base) 
                except ValueError :
                    pc_pl.append(float(element_decompose[element_decompose.index(nombre_a_virgule.group()) - 1].replace(',', '.')))
                    v85.append(float(nombre_a_virgule.group().replace(',', '.')))
                    tmj.append(int(element_decompose[element_decompose.index(nombre_a_virgule.group()) - 3]))            
            else :  # si les deux données sont des entiers
                liste_nombre = []  # la liste des nombres dans la ligne de données :
                for objet in element_decompose : 
                    try :  # comme ça on ne garde que les nombre
                        liste_nombre.append(float(objet))
                    except ValueError : 
                        pass
                tmj, pc_pl, v85 = liste_nombre[-4], liste_nombre[-2], liste_nombre[-1]        
        # plus que les dates de mesure !!
        mois = [element.split()[-1] for element in liste_decomposee_ligne]
        periode = [element.split()[-3] + '-' + element.split()[-2] for element in liste_decomposee_ligne]
        # concatenation por verif
        donnees = zip(voie, pr, abscisse, tmj, pc_pl, v85, periode, mois)

        with ct.ConnexionBdd('local_otv') as c:
            for i in range(len(voie)) :
                c.curs.execute("INSERT INTO comptage.na_2010_2017_p (id_comptag,dep, route, pr, abs, reseau, gestionnai, concession,type_poste, tmja_2015, pc_pl_2015, obs_2015) VALUES ('17-'||%s||'-'||%s||'+'||%s,'17', %s, %s,%s,'RD','CD17','N','ponctuel',%s,%s,'nouveau point,'||%s||',v85_tv '||%s)", (voie[i], pr[i], abscisse[i], voie[i], pr[i], abscisse[i], tmj[i], pc_pl[i], periode[i], v85[i]))
                c.connexionPsy.commit()

def cd23(fichier=r'Q:\DAIT\TI\DREAL33\2019\C19SA0035_OTR-NA\Doc_travail\Donnees_source\CD23\2018_CD23_trafics.xls'):
    """
    Import des données de la creuse
    attention : pour le point de comptage D941 6+152 à Aubusson, le pR est 32 et non 6. il faut donc corriger à la main le fihcier excel
    Pour le moment tous les points sont déjà dans  la base, dc pas de traitement de type insert prévus
    en entree : 
        fichier : raw string le nom du tableur excel contenant les données
    """
    # ouvrir le classeur
    df_excel=pd.read_excel(fichier,skiprows=11)
    # renomer les champs
    df_excel_rennome=df_excel.rename(columns={'1er trimestre  du 01 janvier au 31 mars':'trim1_TV', 'Unnamed: 9':'trim1_pcpl',
                             '2ème trimestre du 01 avril au 30 juin':'trim2_TV', 'Unnamed: 11':'trim2_pcpl',
                             '3ème trimestre du 01 juillet au 30 septembre':'trim3_TV', 'Unnamed: 13':'trim3_pcpl',
                             '4ème trimestre du 01 octobre au 31 décembre':'trim4_TV', 'Unnamed: 15':'trim4_pcpl',
                             'Unnamed: 17':'pc_pl', 'TMJA 2018':'tmja'})
    #supprimer la 1ere ligne
    df_excel_filtre=df_excel_rennome.loc[1:,:].copy()
    #mise en forme attribut
    df_excel_filtre['Route']=df_excel_filtre.apply(lambda x : str(x['Route']).upper(), axis=1)
    annee_cpt='2018'
    #attribut id_comptag
    for i in ['DEP','PR','ABS'] : 
        df_excel_filtre[i]=df_excel_filtre.apply(lambda x : str(int(x[i])),axis=1)
    df_excel_filtre['id_comptag']=df_excel_filtre.apply(lambda x : '-'.join([x['DEP'],'D'+str(x['Route']),
                                                                         x['PR']+'+'+x['ABS']]),axis=1)
    
    #donnees_mensuelles
    list_id_comptag=[val for val in df_excel_filtre.id_comptag.tolist() for _ in (0, 1)]
    donnees_type=['tmja','pc_pl']*len(df_excel_filtre.id_comptag.tolist())
    annee_df=['2018']*2*len(df_excel_filtre.id_comptag.tolist())
    janv, fev, mars,avril,mai,juin,juil,aout,sept,octo,nov,dec=[],[],[],[],[],[],[],[],[],[],[],[]
    for i in range(len(df_excel_filtre.id_comptag.tolist())) :
        for j in (janv, fev, mars) :
            j.extend([df_excel_filtre.trim1_TV.tolist()[i],df_excel_filtre.trim1_pcpl.tolist()[i]])
        for k in (avril,mai,juin) :
            k.extend([df_excel_filtre.trim2_TV.tolist()[i],df_excel_filtre.trim2_pcpl.tolist()[i]])
        for l in (juil,aout,sept) :
            l.extend([df_excel_filtre.trim3_TV.tolist()[i],df_excel_filtre.trim3_pcpl.tolist()[i]])
        for m in (octo,nov,dec) :
            m.extend([df_excel_filtre.trim4_TV.tolist()[i],df_excel_filtre.trim4_pcpl.tolist()[i]])
    donnees_mens=pd.DataFrame({'id_comptag':list_id_comptag,'donnees_type':donnees_type,'annee':annee_df,'janv':janv,'fevr':fev,'mars':mars,'avri':avril,
                  'mai':mai,'juin':juin,'juil':juil,'aout':aout,'sept':sept,'octo':octo,'nove':nov,'dece':dec})
    
    #Mise à jour bdd
    with ct.ConnexionBdd('gti_otv') as c :
        c.curs.execute("select distinct id_comptag from comptage.na_2010_2018_p where dep='23' order by id_comptag")
        listerecord=[record[0] for record in c.curs]
        for id_comptag,tmja, pc_pl  in zip(df_excel_filtre.id_comptag.tolist(), df_excel_filtre.tmja.tolist(),df_excel_filtre.pc_pl.tolist()) : 
            if id_comptag in listerecord :
                c.curs.execute("update comptage.na_2010_2018_p set tmja_2018=%s, pc_pl_2018=%s, ann_cpt=%s where id_comptag=%s",(tmja, pc_pl,annee_cpt,id_comptag))
            else : 
                print (f'{id_comptag} nouveau, à traiter')
        print('fini')
        c.connexionPsy.commit()
        donnees_mens.to_sql('na_2010_2018_mensuel', c.sqlAlchemyConn,schema='comptage',if_exists='append',index=False)
    
    