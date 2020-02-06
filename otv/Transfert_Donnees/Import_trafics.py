# -*- coding: utf-8 -*-
'''
Created on 27 juin 2019

@author: martin.schoreisz

module d'importation des données de trafics forunies par les gestionnaires
'''

import pandas as pd
import geopandas as gp
import numpy as np
import os, re, csv

import Connexion_Transfert as ct
import Outils as O
from Base_BdTopo import Import_outils as io
from Base_BdTopo import Rond_points as rp
from Base_BdTopo import Regroupement_correspondance as rc


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

def cd19(fichier=r'Q:\DAIT\TI\DREAL33\2019\C19SA0035_OTR-NA\Doc_travail\Donnees_source\CD19\2018_Recensement_trafic.xls'):    
    """
    je le met en bloc mais ça meriterait d'etre passe en classe comme le 17
    """
    #importer fichier
    fichier=r'Q:\DAIT\TI\DREAL33\2019\C19SA0035_OTR-NA\Doc_travail\Donnees_source\CD19\2018_Recensement_trafic.xls'
    donnees_brutes=pd.read_excel(fichier, skiprows=6)
    donnees_filtrees=donnees_brutes.rename(columns={'N° R.D.':'route','P.R.':'pr',2018:'ann_2018'})[['route','pr','ann_2018']]
    donnees_filtrees=donnees_filtrees.loc[~donnees_filtrees.pr.isna()].copy()
    
    #mettre à jour les champs et pereparer les donnees
    def id_comptage(route,pr) : 
        route=str(route).strip()
        pr=str(int(pr.split('+')[0]))+'+0' if int(pr.split('+')[1])==0 else str(int(pr.split('+')[0]))+'+'+str(int(pr.split('+')[1]))
        return '19-D'+route+'-'+pr
    
    donnees_filtrees['idcomptag']=donnees_filtrees.apply(lambda x : id_comptage(x['route'],x['pr']), axis=1)
    donnees_filtrees['tmja']=donnees_filtrees.ann_2018.apply(lambda x : 0 if (pd.isna(x) or x=='x') else int(x.split('\n')[0]))
    donnees_filtrees['pc_pl']=donnees_filtrees.ann_2018.apply(lambda x : 0 if (pd.isna(x) or x=='x') else float(x.split('\n')[1].split('%')[0].replace(',','.')))
    donnees_transfert=donnees_filtrees.loc[donnees_filtrees['tmja']>0].copy()
    
    #pour interactions avec Bdd
    bdd='gti_otv_pg11'
    
    #prise en compte variation id_comptag
    rqt_corresp_comptg='select * from comptage.corresp_id_comptag'
    with ct.ConnexionBdd(bdd) as c:
        corresp_comptg=pd.read_sql(rqt_corresp_comptg, c.sqlAlchemyConn)
    donnees_transfert['idcomptag']=donnees_transfert.apply(lambda x : corresp_comptg.loc[corresp_comptg['id_gest']==x['idcomptag']].id_gti.values[0] 
                                            if x['idcomptag'] in corresp_comptg.id_gest.tolist() else x['idcomptag'], axis=1)
    
    #Recherche des points existants
    comptage=Comptage(r'Q:\DAIT\TI\DREAL33\2019\C19SA0035_OTR-NA\Doc_travail\Donnees_source\CD19\2018_Recensement_trafic.xls')
    cpt_existant=comptage.comptag_existant_bdd('gti_otv_pg11','na_2010_2018_p',dep='19')
    #identification des nouveaux points
    points_a_inserer=donnees_transfert.loc[~donnees_transfert['idcomptag'].isin(cpt_existant.id_comptag.tolist())].copy()
    #identification des points à mettre a jour
    points_a_mettre_a_jour=donnees_transfert.loc[donnees_transfert['idcomptag'].isin(cpt_existant.id_comptag.tolist())]
    
    #mettre a jour
    valeurs_txt=str(tuple([(elem[0],elem[1], elem[2]) for elem in zip(
               points_a_mettre_a_jour.idcomptag.tolist(), points_a_mettre_a_jour.tmja.tolist(), 
                points_a_mettre_a_jour.pc_pl.tolist(), )]))[1:-1]
    rqt=f"""update comptage.na_2010_2018_p  as c 
                    set tmja_2018=v.tmja,pc_pl_2018=v.pc_pl from (values {valeurs_txt}) as v(id_comptag,tmja,pc_pl)
                    where v.id_comptag=c.id_comptag"""
    with ct.ConnexionBdd(bdd) as c:
        c.sqlAlchemyConn.execute(rqt)
        
    #inserer
    #mise en forme
    points_a_inserer.rename(columns={'idcomptag':'id_comptag','tmja':'tmja_2018','pc_pl':'pc_pl_2018'}, inplace=True)
    points_a_inserer.drop(['route','pr','ann_2018'], axis=1, inplace=True)
    points_a_inserer['type_poste']='tournant'
    points_a_inserer['dep']='19'
    points_a_inserer['reseau']='RD'
    points_a_inserer['gestionnai']='CD19'
    points_a_inserer['concession']='N'
    with ct.ConnexionBdd(bdd) as c:
        points_a_inserer.to_sql('na_2010_2018_p',c.sqlAlchemyConn,schema='comptage',if_exists='append', index=False )
    
    #mise à jhour geom : auto puis le reste en manuel
    comptage.maj_geom(bdd,'comptage','na_2010_2018_p','19')

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
                    
class Comptage():
    """
    classe comprenant les attributs et méthode commune à tous les comptages de tous les departements
    """
    def __init__(self, fichier):
        self.fichier=fichier
        
    def ouvrir_csv(self):
        """
        ouvrir un fichier csv en supprimant les caracteres accentué et en remplaçant les valeurs vides par np.naN
        """
        with open(self.fichier, newline='') as csvfile : 
            reader = csv.reader(csvfile, delimiter=';')
            fichier=([[re.sub(('é|è|ê'),'e',a) for a in row] for row in reader])
            df=pd.DataFrame(data=fichier[1:], columns=fichier[0])
            df.replace('', np.NaN, inplace=True)
        return  df
    
    def comptag_existant_bdd(self,bdd, table, schema='comptage',dep=False, type_poste=False):
        """
        recupérer les comptages existants dans une df
        en entree : 
            bdd : string selon le fichier 'Id_connexions' du Projet Outils
            table : string : nom de la table
            schema : string : nom du schema
            dep : string, code du deprtament sur 2 lettres
            type_poste : string ou list de string: type de poste dans 'permanent', 'tournant', 'ponctuel'
        en sortie : 
            existant : df selon la structure de la table interrogée
        """
        if not dep and not type_poste : rqt=f"select * from {schema}.{table}"
        elif dep and not type_poste : rqt=f"select * from {schema}.{table} where dep='{dep}'"
        elif dep and isinstance(type_poste, str) : rqt=f"select * from {schema}.{table} where dep='{dep}' and type_poste='{type_poste}'"
        elif dep and isinstance(type_poste, list) : 
            list_type_poste='\',\''.join(type_poste)
            rqt=f"""select * from {schema}.{table} where dep='{dep}' and type_poste in ('{list_type_poste}')"""
        with ct.ConnexionBdd(bdd) as c:
            self.existant=gp.GeoDataFrame.from_postgis(rqt, c.sqlAlchemyConn, geom_col='geom',crs={'init': 'epsg:2154'})
    
    def maj_geom(self,bdd, schema, table, dep=False):
        """
        mettre à jour les lignes de geom null
        en entree : 
            bdd: txt de connexion à la bdd que l'on veut (cf ficchier id_connexions)
            schema : string nom du schema de la table
            table : string : nom de la table
            dep : string : code departement sur 2 chiffres
        """
        if dep : 
            rqt=f""" update {schema}.{table}
              set geom=(select geom_out  from comptage.geoloc_pt_comptag(id_comptag))
              where dep='{dep}' and geom is null"""
        else :
            rqt=f""" update {schema}.{table}
              set geom=(select geom_out  from comptage.geoloc_pt_comptag(id_comptag))
              where geom is null"""    
        with ct.ConnexionBdd(bdd) as c:
                c.sqlAlchemyConn.execute(rqt)
    
    def creer_valeur_txt_update(self, df, liste_attr):
        """
        a partir d'une df cree un tuple selon les vaelur que l'on va vouloir inserer dans la Bdd
        en entree : 
            df: df des donnees de base
            liste_attr : liste des attributs que l'on souhaite transferer dans la bdd
        """
        valeurs_txt=str(tuple([ tuple([elem[i] for i in range(len(liste_attr))]) 
                               for elem in zip(*[df[a].tolist() for a in liste_attr])]))[1:-1]
        return valeurs_txt
    
    def update_bdd(self,bdd, schema, table, valeurs_txt,dico_attr_modif):
        """
        mise à jour des id_comptag deja presents dans la base
        en entree : 
            bdd: txt de connexion à la bdd que l'on veut (cf ficchier id_connexions)
            schema : string nom du schema de la table
            table : string : nom de la table
            valeurs_txt : tuple des valeurs pour mise à jour, issu de creer_valeur_txt_update
            dico_attr_modif : dico de traing avec en clé les nom d'attribut à mettre à jour, en value des noms des attributs source dans la df
        """
        rqt_attr=','.join(f'{attr_b}=v.{attr_f}' for (attr_b,attr_f) in dico_attr_modif.items())
        attr_fichier=','.join(f'{attr_f}' for attr_f in dico_attr_modif.values())
        rqt_base=f'update {schema}.{table}  as c set {rqt_attr} from (values {valeurs_txt}) as v(id_comptag,{attr_fichier}) where v.id_comptag=c.id_comptag'
        with ct.ConnexionBdd(bdd) as c:
                c.sqlAlchemyConn.execute(rqt_base)

    def insert_bdd(self,bdd, schema, table, df):
        """
        insérer les données dans la bdd et mettre à jour la geometrie
        en entree : 
            bdd: txt de connexion à la bdd que l'on veut (cf ficchier id_connexions)
            schema : string nom du schema de la table
            table : string : nom de la table
        """
        with ct.ConnexionBdd(bdd) as c:
            df.to_sql(table,c.sqlAlchemyConn,schema=schema,if_exists='append', index=False )

class Comptage_cd17(Comptage) :
    """
    Classe d'ouvertur de fichiers de comptage du CD17
    en entree : 
        fichier : raw string de chemin du fichier
        type_fichier : type e fichier parmi ['brochure', 'permanent_csv',tournant_xls_bochure]
        annee : integer : annee des points de comptages
    """  
            
    def __init__(self,fichier, type_fichier, annee):
        Comptage.__init__(self, fichier)
        self.annee=annee
        self.liste_type_fichier=['brochure_pdf', 'permanent_csv','tournant_xls_bochure','ponctuel_xls_bochure']
        if type_fichier in self.liste_type_fichier :
            self.type_fichier=type_fichier#pour plus tard pouvoir différencier les fichiers de comptage tournant / permanents des brochures pdf
        else : 
            raise Comptage_cd17.CptCd17_typeFichierError(type_fichier)
        if self.type_fichier=='brochure_pdf' :
            self.fichier_src=self.lire_borchure_pdf()
        elif self.type_fichier=='permanent_csv' : 
            self.fichier_src=self.ouvrir_csv()
        elif self.type_fichier=='tournant_xls_bochure' : 
            self.fichier_src=self.ouvrir_xls_tournant_brochure()
        elif self.type_fichier=='ponctuel_xls_bochure' :
            self.fichier_src=self.ouvrir_xls_ponctuel_brochure()
            
        
        
    def lire_borchure_pdf(self):
        """
        Transformer les données contenues dans les borchures pdf en list de données
        POur ça il faut copier-coller les données de la brohcure pdf en l'ouvrant avec firfox puis cole dans notepad puis enregistrer en .txt. 
        Pas d'en tete de colonne
        attention à la dénomination des mois,la vérifier
        """
        with open(self.fichier, encoding="utf-8") as fichier :  # ouvrir l fichier
            liste = [element.replace('\n', ' ').replace('    ', ' ').replace('   ', ' ').replace('  ', ' ') for element in fichier]
            liste_decomposee_ligne = re.split(r'(Janvier|Février|Mars|Avril|Mai|Juin|Juillet|Août|Sept.|Oct.|Nov.|Déc.|Jan.)', "".join(liste))  # permet de conserver le mois
            liste_decomposee_ligne = [liste_decomposee_ligne[i] + liste_decomposee_ligne[i + 1] for i in range(0, len(liste_decomposee_ligne) - 1, 2)]  # necessaier pour repasser le mois dans le string
            liste_decomposee_ligne[0] = ' ' + liste_decomposee_ligne[0]  # uniformité des données
            liste_decomposee_ligne = list(filter(None, liste_decomposee_ligne))
            liste_decomposee_ligne=[e.strip() for e in liste_decomposee_ligne]
        return liste_decomposee_ligne
    
    def brochure_pdf_voie_pr_abs(self):
        """
        extraire les voies, pr et abscisse de la liste decomposee issue de lire_borchure_pdf
        en entree : 
            self.fichier_src : liste des element issu du fichier.txt issu du pdf 
        en sortie : 
            voie : list de string : dénomination de la voie au format D000X
            pr : list d'integer : point de repere
            abs : list d'integer : absice
        """
        return [element.split(' ')[1] for element in self.fichier_src], [element.split(' ')[2]for element in self.fichier_src], [element.split(' ')[3]for element in self.fichier_src]
    
    def brochure_pdf_tmj_pcpl_v85(self):
        """
        extraire le tmj, %pl et v85 de la liste decomposee issue de lire_borchure_pdf
        en entree : 
            self.fichier_src : liste des element issu du fichier.txt issu du pdf
        en sortie : 
            tmj : list de numeric : trafic moyen journalier
            pc_pl : list de numeric : point de repere
            v85 : list de numeric : absice
        """
        # pour le tmj et %PL c'est plus compliqué car la taille de la cellule localisation varie, son délimiteur aussi et les chiffres peuvent être entier ou flottant, don on va se baser sur le fait
        # que la rechreche d'un nombre a virgule renvoi le %PL, sinon la vitesse, et si c'est la vitesse, alors ca créer une value error en faisant le float sur l'element + 1, donc on 
        # sait que c'est la vitesse
        pc_pl, v85, tmj = [], [], []
        for element in self.fichier_src : 
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
        return tmj, pc_pl, v85
    
    def brochure_pdf_mois_periode(self):
        """
        extraire le mois et la periode de mesure issue de lire_borchure_pdf
        en entree : 
            self.fichier_src : liste des element issu du fichier.txt issu du pdf
        en sortie : 
            tmj : list de numeric : trafic moyen journalier
            pc_pl : list de numeric : point de repere
            v85 : list de numeric : absice
        """
        # plus que les dates de mesure !!
        mois = [element.split()[-1] for element in self.fichier_src]
        periode = [element.split()[-3] + '-' + element.split()[-2] for element in self.fichier_src]
        return mois, periode

    def brochure_pdf_tt_attr(self):
        """
        sort une dataframe des voie, pr, abs, tmj, pc_pl, v85, periode et mois
        """
        voie, pr, absc=self.brochure_pdf_voie_pr_abs()
        tmj, pcpl, v85=self.brochure_pdf_tmj_pcpl_v85()
        mois, periode=self.brochure_pdf_mois_periode()
        return pd.DataFrame({'route': voie, 'pr':pr,'abs':absc,'tmja_'+str(self.annee):tmj, 'pc_pl_'+str(self.annee):pcpl, 'v85':v85,'mois':mois, 'periode':periode}) 
    
    def permanent_csv_attr(self) :
        """
        sort une dataframe des voie, pr, abs, tmj, pc_pl, v85, periode et mois pour les fichiers csv de comptag permanent
        """
        fichier_src_2sens=self.fichier_src.loc[self.fichier_src['Sens']=='3'].copy()
        liste_attr=([a for a in fichier_src_2sens.columns if a[:6]=='MJM TV']+['Route','PRC','ABC']+['MJA TV TCJ '+str(self.annee),
                                                                            'MJA %PL TCJ '+str(self.annee),'MJAV85 TV TCJ '+str(self.annee)])
        liste_nom=(['janv', 'fevr', 'mars', 'avri', 'mai', 'juin', 'juil', 'aout', 'sept', 'octo', 'nove', 'dece']+['route', 'pr','abs']+[
                                                                            'tmja_'+str(self.annee), 'pc_pl_'+str(self.annee), 'v85'])
        dico_corres_mois={a:b for a,b in zip(liste_attr,liste_nom)}
        fichier_filtre=fichier_src_2sens[liste_attr].rename(columns=dico_corres_mois).copy()
        fichier_filtre=fichier_filtre.loc[~fichier_filtre['tmja_'+str(self.annee)].isna()].copy()
        fichier_filtre['tmja_'+str(self.annee)]=fichier_filtre['tmja_'+str(self.annee)].apply(lambda x : int(x))
        fichier_filtre['pc_pl_'+str(self.annee)]=fichier_filtre['pc_pl_'+str(self.annee)].apply(lambda x : float(x.strip().replace(',','.')))
        fichier_filtre['route']=fichier_filtre.route.apply(lambda x : x.split(' ')[1]) 
        return fichier_filtre

    def ouvrir_xls_tournant_brochure (self):
        """
        ouvrir le fichier et y ajouter un id_comptag
        """
        donnees_brutes=pd.read_excel(self.fichier, skiprows=1)
        donnees_brutes.dropna(axis=1, how='all',inplace=True)
        donnees_brutes.columns=['identifiant','localisation','type','route','pr','tmja_2018','tmja_2017','evol_2017_2018','tmja_2016','tmja_2015','tmja_2014']
        def pr(pr):
            pr=str(pr)
            ptpr=pr.split('.')[0]
            absc=pr.split('.')[1]
            if len(absc)<4 : 
                absc=int(absc+((4-len(absc))*'0'))
            else : 
                absc=int(absc)
            pr=ptpr+'+'+str(absc)
            return pr
        donnees_brutes['pr']=donnees_brutes.apply(lambda x : pr(x['pr']), axis=1)
        donnees_brutes['id_comptag']=donnees_brutes.apply(lambda x : '17-'+O.epurationNomRoute(x['route'][2:].strip())+'-'+x['pr'],axis=1)
        return donnees_brutes
    
    def ouvrir_xls_ponctuel_brochure(self):
        """
        ouvrir le fichier, ajouter un id_comptag et obs, filtrer les points en juillet / aout
        """
        donnees_brutes=pd.read_excel(self.fichier, skiprows=2)
        donnees_brutes.columns=['route','pr','absc','localisation','tmja','pl','pc_pl','v85','agence','zone','debut_periode','fin_periode']
        donnees_brutes['id_comptag']=donnees_brutes.apply(lambda x : '17-'+O.epurationNomRoute(x['route'][2:].strip())+'-'+str(x['pr'])+'+'+str(x['absc']),axis=1)
        donnees_brutes['obs']=donnees_brutes.apply(lambda x : 'nouveau_point,'+x['debut_periode'].strftime('%d/%m/%Y')+'-'+
                                                   x['fin_periode'].strftime('%d/%m/%Y')+',v85_tv '+str(x['v85']), axis=1)
        return donnees_brutes
    
    def conversion_id_comptg_existant_xls_brochure (self,bdd):
        """
        prendre den compte si des id_comptages ont unnom differents entre le CD17 et le Cerema
        in : 
            bdd : l'identifiant de la Bdd pour recuperer la table de correspondance
            donnees_brutes : df issue de ouvrir_xls_tournant_brochure
        """
        rqt_corresp_comptg='select * from comptage.corresp_id_comptag'
        with ct.ConnexionBdd(bdd) as c:
            corresp_comptg=pd.read_sql(rqt_corresp_comptg, c.sqlAlchemyConn)
        self.df_attr=self.fichier_src.copy()
        self.df_attr['id_comptag']=self.df_attr.apply(lambda x : corresp_comptg.loc[corresp_comptg['id_gest']==x['id_comptag']].id_gti.values[0] 
                                                    if x['id_comptag'] in corresp_comptg.id_gest.tolist() else x['id_comptag'], axis=1)
    
    def filtrer_periode_ponctuels_xls_brochure(self):
        """
        filtrer des periodesde vacances
        """
        #filtrer pt comptage pendant juillet aout
        self.df_attr=self.df_attr.loc[self.df_attr.apply(lambda x : x['debut_periode'].month not in [7,8] and x['fin_periode'].month not in [7,8], 
                                                         axis=1)].copy()
            
    def carac_xls_brochure(self):
        """
        separer les donnes entre les pt de comptage deja dans la base et les autres (fonctionne pour tout type de comptage)
        """
        #identification des nouveaux points
        self.df_attr_insert=self.df_attr.loc[~self.df_attr['id_comptag'].isin(self.existant.id_comptag.tolist())].copy()
        #identification des points à mettre a jour
        self.df_attr_update=self.df_attr.loc[self.df_attr['id_comptag'].isin(self.existant.id_comptag.tolist())]
  
    def mises_forme_bdd(self, bdd, schema, table, dep, type_poste):
        """
        mise en forme et decompoistion selon comptage existant ou non dans Bdd
        en sortie : 
            df_attr_insert : df des pt de comptage a insrere
            df_attr_update : df des points de comtage a mettre a jour
            bdd: txt de connexion à la bdd que l'on veut (cf ficchier id_connexions)
        """ 
        #mise en forme
        if self.type_fichier=='brochure' : 
            df_attr= self.brochure_pdf_tt_attr()
            df_attr['type_poste']=type_poste
            df_attr['obs_'+str(self.annee)]=df_attr.apply(lambda x : 'nouveau point,'+x['periode']+',v85_tv '+str(x['v85']),axis=1)
            df_attr.drop(['v85', 'mois','periode'], axis=1, inplace=True)
        elif self.type_fichier=='permanent_csv' : 
            df_attr=self.permanent_csv_attr()
            df_attr['type_poste']=type_poste
            df_attr['obs_'+str(self.annee)]=df_attr.apply(lambda x : 'v85_tv '+str(x['v85']),axis=1)
            df_attr.drop('v85',axis=1, inplace=True)
        df_attr['id_comptag']=df_attr.apply(lambda x : '17-'+O.epurationNomRoute(x['route'])+'-'+str(x['pr'])+'+'+str(x['abs']),axis=1)
        df_attr['dep']='17'
        df_attr['reseau']='RD'
        df_attr['gestionnai']='CD17'
        df_attr['concession']='N'
            #verif que pas de doublons et seprartion si c'est le cas
        existant=self.comptag_existant_bdd(bdd, table, schema, dep, type_poste)
        df_attr_insert=df_attr.loc[~df_attr['id_comptag'].isin(existant.id_comptag.to_list())]
        df_attr_update=df_attr.loc[df_attr['id_comptag'].isin(existant.id_comptag.to_list())]
        self.df_attr, self.df_attr_insert, self.df_attr_update=df_attr, df_attr_insert,df_attr_update
                  
    def localiser_comptage_a_inserer(self,bdd, schema_temp,nom_table_temp):
        """
        récupérer la geometrie de pt de comptage à inserer dans une df sans les inserer dans la Bdd
        """
        with ct.ConnexionBdd(bdd) as c:
            #passer les données dans Bdd
            c.sqlAlchemyConn.execute(f'drop table if exists {schema_temp}.{nom_table_temp}')
            self.df_attr_insert.to_sql(nom_table_temp,c.sqlAlchemyConn,schema_temp)   
            #ajouter une colonne geometrie
            rqt_ajout_geom=f"""ALTER TABLE {schema_temp}.{nom_table_temp} ADD COLUMN geom geometry('POINT',2154)""" 
            c.sqlAlchemyConn.execute(rqt_ajout_geom)
            #mettre a jour la geometrie. attention, il faut un fichier de référentiel qui va bein, cf fonction geoloc_pt_comptag dans la Bdd
            rqt_maj_geom=f"""update {schema_temp}.{nom_table_temp}
                             set geom=(select geom_out  from comptage.geoloc_pt_comptag(id_comptag))
                             where geom is null"""
            c.sqlAlchemyConn.execute(rqt_maj_geom)
            points_a_inserer=gp.GeoDataFrame.from_postgis(f'select * from {nom_table_temp}', c.sqlAlchemyConn, geom_col='geom',crs={'init': 'epsg:2154'})
            return points_a_inserer
        
    def donnees_existantes(self, bdd, table_linearisation_existante):
        """
        recuperer une linearisation existante et les points de comptages existants
        in : 
            bdd : string : identifiant de connexion à la bdd
            table_linearisation_existante : string : schema-qualified table de linearisation de reference
        """    
        with ct.ConnexionBdd(bdd) as c:
            lin_precedente=gp.GeoDataFrame.from_postgis(f'select * from {table_linearisation_existante}',c.sqlAlchemyConn, 
                                                    geom_col='geom',crs={'init': 'epsg:2154'})
        return lin_precedente
    
    def plus_proche_voisin_comptage_a_inserer(self,bdd, schema_temp,nom_table_temp,table_linearisation_existante):
        """
        trouver si nouveau point de comptage est sur  un id_comptag linearise
        in:
            schema_temp : string : nom du schema en bdd opur calcul geom, cf localiser_comptage_a_inserer
            nom_table_temp : string : nom de latable temporaire en bdd opur calcul geom, cf localiser_comptage_a_inserer
            table_linearisation_existante : string : schema-qualified table de linearisation de reference cf donnees_existantes
        """
        #mettre en forme les points a inserer
        points_a_inserer=self.localiser_comptage_a_inserer(bdd, schema_temp,nom_table_temp)
        #recuperer les donnees existante
        lin_precedente=self.donnees_existantes(bdd, table_linearisation_existante)
        
        #ne conserver que le spoints a inserer pour lesquels il y a une geometrie
        points_a_inserer_geom=points_a_inserer.loc[~points_a_inserer.geom.isna()].copy()
        #recherche de la ligne la plus proche pour chaque point a inserer
        ppv=O.plus_proche_voisin(points_a_inserer_geom,lin_precedente[['id_ign','geom']],5,'id_comptag','id_ign')
        #verifier si il y a un id comptage sur la ligne la plus proche, ne conserver que les lignes où c'est le cas, et recuperer la geom de l'id_comptag_lin 
        #(les points issu de lin sans geom ne sont pas conserves, et on a besoin de passer les donnees en gdf)
        ppv_id_comptagLin=ppv.merge(lin_precedente[['id_ign','id_comptag']].rename(columns={'id_comptag':'id_comptag_lin'}), on='id_ign')
        ppv_id_comptagLin=ppv_id_comptagLin.loc[~ppv_id_comptagLin.id_comptag_lin.isna()].copy().merge(self.existant[['geom','id_comptag']].rename(columns=
                                                {'geom':'geom_cpt_lin','id_comptag':'id_comptag_lin'}),on='id_comptag_lin')
        ppv_id_comptagLin_p=gp.GeoDataFrame(ppv_id_comptagLin.rename(columns={'id_ign':'id_ign_cpt_new'}),geometry=ppv_id_comptagLin.geom_cpt_lin)
        ppv_id_comptagLin_p.crs = {'init' :'epsg:2154'}
        #si il y a un id_comptag linearisation : trouver le troncon le plus proche de celui-ci
        ppv_total=O.plus_proche_voisin(ppv_id_comptagLin_p,lin_precedente[['id_ign','geom']],5,'id_comptag_lin','id_ign')
        ppv_final=ppv_total.merge(ppv_id_comptagLin_p,on='id_comptag_lin').rename(columns={'id_ign':'id_ign_lin'})
        return ppv_final
        
    
    
    def troncon_elemntaires(self,bdd, schema, table_graph,table_vertex,liste_lignes):    
        """
        trouver les troncons elementaires d'une liste de ligne
        se base sur travail interne cf Base_BdTopo Modules regroupement_Correspondace, Import_outils, Rond_Points
        """
        def troncon_elementaires_params(self,bdd, schema, table_graph,table_vertex):
            """
            construire les parametres de determination des troncons elementaires
            """    
            df=io.import_donnes_base(bdd,schema, table_graph,table_vertex)
            df2_chaussees=df.loc[df.nature.isin(['Autoroute', 'Quasi-autoroute', 'Route à 2 chaussées'])]
            df_avec_rd_pt,carac_rd_pt,lign_entrant_rdpt=rp.identifier_rd_pt(df)
            return df_avec_rd_pt, carac_rd_pt,df2_chaussees
        
        df_avec_rd_pt, carac_rd_pt,df2_chaussees=troncon_elementaires_params(self,bdd, schema, table_graph,table_vertex)
        dico_corresp={}
        for id_ign_lin in set(liste_lignes) :
            try : 
                dico_corresp[id_ign_lin]=rc.regrouper_troncon([id_ign_lin], df_avec_rd_pt, carac_rd_pt,df2_chaussees,[])[0].id.tolist()
            except rc.PasAffectationError : 
                continue
        return dico_corresp
    
    
    def correspondance_ancien_nouveau_comptage(self,bdd, schema_temp,nom_table_temp,table_linearisation_existante,
                                               schema_te, table_graph,table_vertex):
        """
        obtenir la table de correspondance entre les id_comptage des points a inserer et les id_comptage des points existants, pour les points
        se situant sur le même troncon elementaire
        in : 
            bdd : string :identifiant de la Bdd avec la quelle on interagit, cf module Id_connexions
            schema_temp :string :pour la geolov des points de comptage a tester cf plus_proche_voisin_comptage_a_inserer
            nom_table_temp : string :pour la geolov des points de comptage a tester cf plus_proche_voisin_comptage_a_inserer
            table_linearisation_existante
            schema_te : string : pour le calcul des traoncon elemntaires : nom du schema cf troncon_elemntaires
            table_graph :string : pour le calcul des traoncon elemntaires : nom de la table avec les colonnes de topologie cf troncon_elemntaires
            table_vertex : string : pour le calcul des traoncon elemntaires : nom de la table avec les valeur de count des vertex cf troncon_elemntaires
        """
        #calcul de la table de correspondance de base
        ppv_final=self.plus_proche_voisin_comptage_a_inserer(bdd,schema_temp,nom_table_temp,table_linearisation_existante)
        dico_corresp=self.troncon_elemntaires(bdd, schema_te, table_graph,table_vertex,ppv_final.id_ign_lin.tolist())
        #rappatrimeent des id des tronc elem pour comparaison, creation d'un attribut booleen pour comparaison
        table_correspondance=ppv_final.merge(pd.DataFrame.from_dict({k:(tuple(v),) for k,v in dico_corresp.items()}, orient='index', columns=['id_ign_te']).reset_index(),left_on='id_ign_lin',
                        right_on='index').drop_duplicates()
        table_correspondance['cpt_redondant']=table_correspondance.apply(lambda x : x['id_ign_cpt_new'] in x['id_ign_te'], axis=1)
        #creation de la table de comparaison pouyr les id_comptages qui se chevauchent
        table_correspondance_finale=table_correspondance.loc[table_correspondance.cpt_redondant][['id_comptag_lin','id_comptag']].merge(self.existant[['type_poste','id_comptag']].rename(
                                                    columns={'id_comptag':'id_comptag_lin','type_poste':'type_poste_lin'}),on='id_comptag_lin')
        return table_correspondance_finale
        
        
    def insert_bdd_mens(self,bdd, schema, table) :
        """
        insérer des données dans la table des comptages mensuels
        en entree : 
            bdd: txt de connexion à la bdd que l'on veut (cf ficchier id_connexions)
            schema : string nom du schema de la table
            table : string : nom de la table
        """ 
        list_attr_mens=['janv', 'fevr', 'mars', 'avri', 'mai', 'juin', 'juil', 'aout', 'sept', 'octo', 'nove', 'dece', 'id_comptag', 'donnees_type', 'annee']
        mens=self.df_attr.copy()
        mens['donnees_type']='tmja' #a travailler plus tatrd si on doit extraire le tmja à partir des noms de colonnes, en lien du coup avec permanent_csv_attr()
        mens['annee']=str(self.annee)
        mens_fin=mens[list_attr_mens].copy()
        with ct.ConnexionBdd(bdd) as c:
            mens_fin.to_sql(table,c.sqlAlchemyConn,schema=schema,if_exists='append', index=False )
        
        
    class CptCd17_typeFichierError(Exception):  
        """
        Exception levee si la recherched'une parrallele ne donne rien
        """     
        def __init__(self, type_fichier):
            Exception.__init__(self,f'type de fichier "{type_fichier}" non présent dans {Comptage_cd17.liste_type_fichier} ')
            
class Comptage_cd47(Comptage):
    """
    traiter les données du CD47
    PLUS TARD ON POURRA AJOUTER LA RECUPDES DONNEES HORAIRES
    """
    def __init__(self,dossier) : 
        #liste des dossiers contenant du permanent
        self.liste_dossiers_perm=[os.path.join(root,dir) for root, dirs, files in 
                     os.walk(dossier) 
                     for directory in dirs if 'UD' in root and 'TRAFICS PERMANENTS' in root]
        
    

    




















