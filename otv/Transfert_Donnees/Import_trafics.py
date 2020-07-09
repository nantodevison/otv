# -*- coding: utf-8 -*-
'''
Created on 27 juin 2019

@author: martin.schoreisz

module d'importation des données de trafics forunies par les gestionnaires
'''

import pandas as pd
import geopandas as gp
import numpy as np
import os, re, csv,statistics,filecmp, unidecode
from geoalchemy2 import Geometry,WKTElement
from shapely.geometry import Point

import Connexion_Transfert as ct
import Outils as O
from Decorateurs import concat_df
from Base_BdTopo import Import_outils as io
from Base_BdTopo import Rond_points as rp
from Base_BdTopo import Regroupement_correspondance as rc

dico_mois={'janv':[1,'Janv','Janvier'], 'fevr':[2,'Fév','Février','févr'], 'mars':[3,'Mars','Mars'], 'avri':[4,'Avril','Avril'], 'mai':[5,'Mai','Mai'], 'juin':[6,'Juin','Juin'], 
           'juil':[7,'Juill','Juillet', 'juil'], 'aout':[8,'Août','Aout'], 'sept':[9,'Sept','Septembre'], 'octo':[10,'Oct','Octobre'], 'nove':[11,'Nov','Novembre'], 'dece':[12,'Déc','Décembre']}

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

class FIM():
    """
    classe dediée aux fichiers FIM de comptage brut
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
        for i,e in enumerate(liste_lign_titre) :
            df=pd.DataFrame(liste_lign_titre[i][3], columns=[liste_lign_titre[i][1]+'_sens'+liste_lign_titre[i][2]])
            df.index=pd.date_range(self.date_debut, periods=len(df), freq=freq)
            if i==0 : 
                self.df_tot_heure=df
            else : 
                self.df_tot_heure=self.df_tot_heure.merge(df, left_index=True, right_index=True)
        self.date_fin=self.df_tot_heure.index.max()

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
        
        
    class fim_TailleBlocDonneesError(Exception):
        """
        Exception levee si la taile des blocs de donnees entre fichiers fim, varie
        """     
        def __init__(self, taille_donnees):
            Exception.__init__(self,f'taille multiple de blocs de donnees dans le fichier : {taille_donnees} ')

    class fim_TypeModeError(Exception):
        """
        Exception levee si le mode n'a pas pu etre detreminé
        """     
        def __init__(self):
            Exception.__init__(self,f'le mode n\'est pas reconnu, cf focntion params_fim()')

    
    class fimNbBlocDonneesError(Exception):
        """
        Exception levee si le  nb de blocs du fihchier est égal à 1
        """     
        def __init__(self, mode):
            Exception.__init__(self,f'le fichier ne comporte qu\'un seul bloc en mode {mode} ')
            
                  
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
    
    def corresp_nom_id_comptag(self,bdd,df):
        """
        pour les id_comptag dont on sait que les noms gti et gestionnaire diffèrent mais que ce sont les memes (cf table comptage.corresp_id_comptag), 
        on remplace le nom gest par le nom_gti, pour pouvoir faire des jointure ensuite
        in : 
            bdd :  string : l'identifiant de la Bdd pour recuperer la table de correspondance
            df : dataframe des comptage du gest. attention doit contenir l'attribut 'id_comptag', ene général prendre df_attr
        """
        rqt_corresp_comptg='select * from comptage.corresp_id_comptag'
        with ct.ConnexionBdd(bdd) as c:
            corresp_comptg=pd.read_sql(rqt_corresp_comptg, c.sqlAlchemyConn)
        df['id_comptag']=df.apply(lambda x : corresp_comptg.loc[corresp_comptg['id_gest']==x['id_comptag']].id_gti.values[0] 
                                                    if x['id_comptag'] in corresp_comptg.id_gest.tolist() else x['id_comptag'], axis=1)
    
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
            rqt_geom=f""" update {schema}.{table}
              set geom=(select geom_out  from comptage.geoloc_pt_comptag(id_comptag))
              where dep='{dep}' and geom is null"""
            rqt_attr=f""" update {schema}.{table}
              set src_geo='pr+abs', x_l93=round(st_x(geom)::numeric,3), y_l93=round(st_y(geom)::numeric,3)
              where dep='{dep}' and src_geo is null and geom is not null"""
        else :
            rqt_geom=f""" update {schema}.{table}
              set geom=(select geom_out  from comptage.geoloc_pt_comptag(id_comptag))
              where geom is null""" 
            rqt_attr=f""" update {schema}.{table}
              set src_geo='pr+abs', x_l93=round(st_x(geom)::numeric,3), y_l93=round(st_y(geom)::numeric,3)
              where src_geo is null and geom is not null"""   
              
        with ct.ConnexionBdd(bdd) as c:
                c.sqlAlchemyConn.execute(rqt_geom)
                c.sqlAlchemyConn.execute(rqt_attr)
                
    def localiser_comptage_a_inserer(self,df,bdd, schema_temp,nom_table_temp):
        """
        récupérer la geometrie de pt de comptage à inserer dans une df sans les inserer dans la Bdd
        """
        with ct.ConnexionBdd(bdd) as c:
            #passer les données dans Bdd
            c.sqlAlchemyConn.execute(f'drop table if exists {schema_temp}.{nom_table_temp}')
            df.to_sql(nom_table_temp,c.sqlAlchemyConn,schema_temp)   
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

    def filtrer_periode_ponctuels(self):
        """
        filtrer des periodesde vacances
        """
        #filtrer pt comptage pendant juillet aout
        self.df_attr=self.df_attr.loc[self.df_attr.apply(lambda x : x['debut_periode'].month not in [7,8] and x['fin_periode'].month not in [7,8], 
                                                         axis=1)].copy()
    
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
    
    def plus_proche_voisin_comptage_a_inserer(self,df,bdd, schema_temp,nom_table_temp,table_linearisation_existante):
        """
        trouver si nouveau point de comptage est sur  un id_comptag linearise
        in:
            df : les données de comptage nouvelles, normalement c'est self.df_attr_insert (cf Comptage_Cd17 ou Compatge cd47
            schema_temp : string : nom du schema en bdd opur calcul geom, cf localiser_comptage_a_inserer
            nom_table_temp : string : nom de latable temporaire en bdd opur calcul geom, cf localiser_comptage_a_inserer
            table_linearisation_existante : string : schema-qualified table de linearisation de reference cf donnees_existantes
        """
        #mettre en forme les points a inserer
        points_a_inserer=self.localiser_comptage_a_inserer(df,bdd, schema_temp,nom_table_temp)
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
        ppv_final=ppv_total.merge(ppv_id_comptagLin_p,on='id_comptag_lin').merge(df[['id_comptag','type_poste']]
                    ,on='id_comptag').rename(columns={'id_ign':'id_ign_lin','type_poste':'type_poste_new'}).drop_duplicates()
        return ppv_final
    
    def troncon_elemntaires(self,bdd, schema, table_graph,table_vertex,liste_lignes,id_name):    
        """
        trouver les troncons elementaires d'une liste de ligne
        se base sur travail interne cf Base_BdTopo Modules regroupement_Correspondace, Import_outils, Rond_Points
        in : 
            id_name : string : nom de l'integer identifiant uniq de la table_graph
        """
        def troncon_elementaires_params(self,bdd, schema, table_graph,table_vertex):
            """
            construire les parametres de determination des troncons elementaires
            """    
            df=io.import_donnes_base(bdd,schema, table_graph,table_vertex)
            df2_chaussees=df.loc[df.nature.isin(['Autoroute', 'Quasi-autoroute', 'Route à 2 chaussées'])]
            df_avec_rd_pt,carac_rd_pt,lign_entrant_rdpt=rp.identifier_rd_pt(df)
            return df_avec_rd_pt, carac_rd_pt,df2_chaussees
        
        O.epurer_graph(bdd,id_name, schema, table_graph,table_vertex)
        df_avec_rd_pt, carac_rd_pt,df2_chaussees=troncon_elementaires_params(self,bdd, schema, table_graph,table_vertex)
        dico_corresp={}
        for id_ign_lin in set(liste_lignes) :
            try : 
                dico_corresp[id_ign_lin]=rc.regrouper_troncon([id_ign_lin], df_avec_rd_pt, carac_rd_pt,df2_chaussees,[])[0].id.tolist()
            except rc.PasAffectationError : 
                continue
        return dico_corresp
    
    def corresp_old_new_comptag(self,bdd, schema_temp,nom_table_temp,table_linearisation_existante,
                                schema, table_graph,table_vertex,id_name):
        """
        trouver la correspndance entre des comptages gestionnaires nouveau et les données dans la base gti de comptage, pour des 
        comptages n'ayant pas tout a fait les mm pr+abs.
        attention, traitement de plusieurs heures possible.
        Attention, si le comptage de la base linearise existante est aussi dans les données de comptage, en plus du nouveau point, alors on le conserve dans les points
        a inserer
        ON PEUT NE FAIRE LA CORRESPONDANCE QU'AVEC LES IMPORTANCES 1,2,3,4 pour les comptages perm et tourn, mais pourça il faut recreer un graph ou mettre a jour l'existant
        in : 
            bdd : string : id de la base a laquelle se connecter (cf module id_connexion)
            schema_temp : string : nom du schema en bdd opur calcul geom, cf localiser_comptage_a_inserer
            nom_table_temp : string : nom de latable temporaire en bdd opur calcul geom, cf localiser_comptage_a_inserer
            table_linearisation_existante : string : schema-qualified table de linearisation de reference cf donnees_existantes
            schema : string : nom du schema contenant la table qui sert de topologie
            table_graph : string : nom de la table topologie (normalement elle devrait etre issue de table_linearisation_existante
            table_vertex : string : nom de la table des vertex de la topoolgie
            id_name : nom de l'identifiant uniq en integer de la table_graoh
             : 
        """
        
        def pt_corresp(id_ign_lin,id_ign_cpt_new,dico_corresp) : 
            if id_ign_cpt_new in dico_corresp[id_ign_lin] : 
                return True
            else : return False
           
        #verif que les colonnes necessaires sont presentes dans le fichier de base
        flag_col, col_manquante=O.check_colonne_in_table_bdd(bdd, schema, table_graph,*io.list_colonnes_necessaires)
        if not flag_col : 
            raise io.ManqueColonneError(col_manquante)

        ppv_final=self.plus_proche_voisin_comptage_a_inserer(self.df_attr_insert,bdd, schema_temp,nom_table_temp,table_linearisation_existante)
        print('plus proche voisin fait')
        dico_corresp=self.troncon_elemntaires(bdd, schema, table_graph,table_vertex,ppv_final.id_ign_lin.tolist(),id_name)
        print('tronc elem fait')
        ppv_final['correspondance']=ppv_final.apply(lambda x : pt_corresp(x['id_ign_lin'],x['id_ign_cpt_new'],dico_corresp),axis=1)
        df_correspondance=ppv_final.loc[(ppv_final['correspondance']) & 
              (~ppv_final['id_comptag_lin'].isin(self.df_attr.id_comptag.tolist())) &
              (ppv_final.type_poste_new.isin(['permanent','tournant']))
             ].copy()[['id_comptag_lin','id_comptag','type_poste_new']]
        return df_correspondance
    
    def creer_valeur_txt_update(self, df, liste_attr):
        """
        a partir d'une df cree un tuple selon les vaelur que l'on va vouloir inserer dans la Bdd
        en entree : 
            df: df des donnees de base
            liste_attr : liste des attributs que l'on souhaite transferer dans la bdd (avec id_comptag)
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
            dico_attr_modif : dico de string avec en clé les nom d'attribut à mettre à jour, en value des noms des attributs source dans la df (ne pas mettre id_comptag,
                            garder les attributsdans l'ordre issu de creer_valeur_txt_update)
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
        if isinstance(df, gp.GeoDataFrame) : 
            if df.geometry.name!='geom':
                df=O.gp_changer_nom_geom(df, 'geom')
                df.geom=df.apply(lambda x : WKTElement(x['geom'].wkt, srid=2154), axis=1)
            with ct.ConnexionBdd(bdd) as c:
                df.to_sql(table,c.sqlAlchemyConn,schema=schema,if_exists='append', index=False,
                          dtype={'geom': Geometry('POINT', srid=2154)} )
        elif isinstance(df, pd.DataFrame) : 
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
  
    def mises_forme_bdd_brochure_pdf(self, bdd, schema, table, dep, type_poste):
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
        Exception levee si le type de fcihier n'est pas dans la liste self.liste_type_fichier
        """     
        def __init__(self, type_fichier):
            Exception.__init__(self,f'type de fichier "{type_fichier}" non présent dans {Comptage_cd17.liste_type_fichier} ')

class Comptage_cd19(Comptage):
    """
    Dans le 19 pour le moment on qu'un simple fichier xls des comptages perm ou tourn
    """
    def __init__(self,fichier,annee) :
        Comptage.__init__(self, fichier)
        self.annee=annee
    
    def comptage_forme(self):
        
        def id_comptage(route,pr) : 
            route=str(route).strip()
            pr=str(int(pr.split('+')[0]))+'+0' if int(pr.split('+')[1])==0 else str(int(pr.split('+')[0]))+'+'+str(int(pr.split('+')[1]))
            return '19-D'+route+'-'+pr
        
        donnees_brutes=pd.read_excel(self.fichier, skiprows=6)
        donnees_filtrees=donnees_brutes.rename(columns={' RD':'route','P.R.':'pr',self.annee:f'ann_{self.annee}'})[['route','pr',f'ann_{self.annee}']]
        donnees_filtrees=donnees_filtrees.loc[~donnees_filtrees.pr.isna()].copy()
        donnees_filtrees['route']=donnees_filtrees.route.apply(lambda x : x.strip().replace(' ','') if isinstance(x,str) else x)
        donnees_filtrees['id_comptag']=donnees_filtrees.apply(lambda x : id_comptage(x['route'],x['pr']), axis=1)
        donnees_filtrees['tmja']=donnees_filtrees[f'ann_{self.annee}'].apply(lambda x : 0 if (pd.isna(x) or x=='x') else int(x.split('\n')[0]))
        donnees_filtrees['pc_pl']=donnees_filtrees[f'ann_{self.annee}'].apply(lambda x : 0 if (pd.isna(x) or x=='x') else float(x.split('\n')[1].split('%')[0].replace(',','.')))
        donnees_filtrees['type_poste']='tournant'
        donnees_filtrees['src']='tableur_2019'
        donnees_transfert=donnees_filtrees.loc[donnees_filtrees['tmja']>0].copy()
        self.df_attr= donnees_transfert.copy()
    
    def classer_comptage_update_insert(self,bdd,table_cpt,schema_cpt,
                                       schema_temp,nom_table_temp,table_linearisation_existante,
                                       schema_graph, table_graph,table_vertex,id_name) : 
        """
        classer les comptage a insrere ou mettre a jour, selon leur correspondance ou non avec des comptages existants.
        se base sur la creation de graph variable selon le type de comptage, pour prendsre en compte ou non certaines categories de vehicules
        """
        #creation de l'existant
        self.comptag_existant_bdd(bdd, schema=schema_cpt, table=table_cpt,dep='19', type_poste=False)
        #mise a jour des noms selon la table de corresp existnate
        self.corresp_nom_id_comptag(bdd,self.df_attr)
        #classement
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        #recherche de correspondance
        corresp_cd19=self.corresp_old_new_comptag(bdd, schema_temp,nom_table_temp,table_linearisation_existante,
                                       schema_graph, table_graph,table_vertex,id_name)
        #insertion des correspondance dans la table dediee
        self.insert_bdd(bdd,schema_cpt,'corresp_id_comptag',corresp_cd19[['id_comptag','id_comptag_lin']].rename(columns={'id_comptag':'id_gest','id_comptag_lin':'id_gti'}))
        #ajout de correspondance manuelle (3, à cause de nom de voie notamment) et creation d'un unique opint a insert
        #nouvelle mise a jour de l'id comptag suivant correspondance
        self.comptag_existant_bdd(bdd, schema=schema_cpt, table=table_cpt,dep='19', type_poste=False)
        self.corresp_nom_id_comptag(bdd,self.df_attr)
        #puis nouveau classement
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()

    def update_bdd_19(self,bdd, schema, table):
        valeurs_txt=self.creer_valeur_txt_update(self.df_attr_update,['id_comptag','tmja','pc_pl', 'src'])
        dico_attr_modif={'tmja_2019':'tmja', 'pc_pl_2019':'pc_pl', 'src_2019':'src'}
        self.update_bdd(bdd, schema, table, valeurs_txt,dico_attr_modif)

class Comptage_cd40(Comptage):
    """
    Les donnees du CD40 sont habituelement fournies au format B152, i.e u dossier par site, chaque dossier contenant des sous dossiers qui menent a un fihcier xls
    """
    def __init__(self,dossier) : 
        self.dossier=dossier
        self.liste_fichier=[os.path.join(chemin,fichier) for chemin, dossier, files in os.walk(dossier) for fichier in files if fichier.endswith('.xls')]
     
    def type_fichier(self,fichier) : 
        """
        récupérer le type de fichier selon la case A5
        """
        return pd.read_excel(fichier,headers=None, skiprows=1).iloc[2,0]
    
    def verif_liste_fichier(self):
        """
        trier la liste des fichiers fournis pour ne garder que le B152
        """
        return [fichier for fichier in self.liste_fichier if self.type_fichier(fichier)=='B152']
            
    def extraire_donnees_annuelles(self, fichier):
        """
        recuperer les donnes annuelles et de caract des comptage
        """
        df=pd.read_excel(fichier,headers=None, skiprows=1) #les 1eres lignes mettent le bordel dans la definition des colonnes
        df2=df.dropna(how='all').dropna(axis=1,how='all')
        compteur='040.'+df2.loc[0,'Unnamed: 125'].split(' ')[1]
        vma=int(df2.loc[4,'Unnamed: 0'].split(' : ')[1][:2])
        voie=O.epurationNomRoute(df2.loc[4,'Unnamed: 141'].split(' ')[1])
        pr,absice=df2.loc[4,'Unnamed: 125'].split(' ')[1],df2.loc[4,'Unnamed: 125'].split(' ')[2]
        dep,gest, reseau,concession,type_poste='40','CD40','D','N','permanent'
        annee_cpt=df2.loc[18,'Unnamed: 6']
        id_comptag=dep+'-'+voie+'-'+pr+'+'+absice
        tmja=df2.loc[18,'Unnamed: 107']
        pc_pl=round(df2.loc[19,'Unnamed: 107'],2)
        return compteur,vma, voie, pr, absice,dep,gest, reseau,concession,type_poste,annee_cpt,id_comptag,tmja,pc_pl,df2
    
    def remplir_dico(self, dico, *donnees):
        dico['compteur'].append(donnees[0])
        dico['vma'].append(donnees[1])
        dico['voie'].append(donnees[2])
        dico['pr'].append(donnees[3])
        dico['absice'].append(donnees[4])
        dico['dep'].append(donnees[5])
        dico['gest'].append(donnees[6])
        dico['reseau'].append(donnees[7])
        dico['concession'].append(donnees[8])
        dico['type_poste'].append(donnees[9])
        dico['annee_cpt'].append(donnees[10])
        dico['id_comptag'].append(donnees[11])
        dico['tmja'].append(donnees[12])
        dico['pc_pl'].append(donnees[13])
    
    def donnees_mensuelles(self, df2,id_comptag, annee):
        """
        extraire les donnes mens d'un fichiere te les mettre ne forme
        """
        donnees_mens=df2.loc[[7,18,19],['Unnamed: 13', 'Unnamed: 23', 'Unnamed: 30',
                           'Unnamed: 37', 'Unnamed: 44', 'Unnamed: 51', 'Unnamed: 58',
                           'Unnamed: 65', 'Unnamed: 72', 'Unnamed: 79', 'Unnamed: 86',
                           'Unnamed: 93', 'Unnamed: 100', 'Unnamed: 107']].dropna(axis=1,how='all')
            #renommer les colonnes
        donnees_mens.columns=[element.replace('é','e').replace('.','').lower() for element in list(donnees_mens.loc[7])]
            #remplacer l'annee en string et ne conserver 
        donnees_mens=donnees_mens.drop(7).replace(['D.Moy.Jour', '% PL'],['tmja','pc_pl'])
            #inserer les valeusr qui vont bien
        donnees_mens['annee']=annee
        donnees_mens['id_comptag']=id_comptag
            #r�arranger les colonnes
        cols=donnees_mens.columns.tolist()
        cols_arrangees=cols[-1:]+cols[:1]+cols[-2:-1]+cols[1:-2]
        donnees_mens=donnees_mens[cols_arrangees]
        donnees_mens.columns=['id_comptag','donnees_type','annee','janv','fevr','mars','avri','mai','juin','juil','aout','sept','octo','nove','dece']
        return donnees_mens
    
    def comptage_forme(self):
        """
        mise en forme des comptages dans une df pour les donnees annuelles et une autre pour les donnees mensuelles
        """     
        #ouvrir un fichier et modifier son cotenu
        dico_annee={'compteur':[],'vma':[],'voie':[],'pr':[],'absice':[],'dep':[],'gest':[],'reseau' :[],'concession':[],'type_poste':[],'annee_cpt':[],'id_comptag':[],'tmja':[],'pc_pl':[]}
        for i,fichier in enumerate(self.verif_liste_fichier()) :
            #traitement des donnees agregee a l'annee
            compteur,vma, voie, pr, absice,dep,gest, reseau,concession,type_poste,annee_cpt,id_comptag,tmja,pc_pl,df2=self.extraire_donnees_annuelles(fichier)
            self.remplir_dico(dico_annee,compteur,vma, voie, pr, absice,dep,gest, reseau,concession,type_poste,annee_cpt,id_comptag,tmja,pc_pl)
            #traiteent des donnees mensuelles
            donnees_mens=self.donnees_mensuelles(df2,id_comptag,annee_cpt)
            if i==0 : 
                donnees_mens_tot=donnees_mens.copy()
            else : 
                donnees_mens_tot=pd.concat([donnees_mens_tot,donnees_mens], sort=False, axis=0)
        #attributs finaux
        self.df_attr=pd.DataFrame(dico_annee)
        self.df_attr_mens=donnees_mens_tot.copy()
    
    def classer_comptage_insert_update(self,bdd,table, schema):
        """
        repartir les donnes selon si les comptages sont a insrere ou mette a jour
        là c'est un peu biaisé car on a que des comptages permanents sans nouveaux
        """
        #creation de l'existant
        self.comptag_existant_bdd(bdd, table, schema=schema,dep='40', type_poste=False)
        #mise a jour des noms selon la table de corresp existnate
        self.corresp_nom_id_comptag(bdd,self.df_attr)
        #classement
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        
    def update_bdd_40(self,bdd, schema, table):
        valeurs_txt=self.creer_valeur_txt_update(self.df_attr_update,['id_comptag','tmja','pc_pl'])
        dico_attr_modif={'tmja_2019':'tmja', 'pc_pl_2019':'pc_pl'}
        self.update_bdd(bdd, schema, table, valeurs_txt,dico_attr_modif)
    
class Comptage_cd47(Comptage):
    """
    traiter les données du CD47, selon les traitement on creer les attributs :
    -type_cpt
    -dico_perm
    -dico_periodq
    -dico_tempo
    -dico_tot
    PLUS TARD ON POURRA AJOUTER LA RECUPDES DONNEES HORAIRES
    """
    def __init__(self,dossier,type_cpt,annee ) :
        self.dossier=dossier 
        self.annee=annee
        self.liste_type_cpt=['TRAFICS PERIODIQUES','TRAFICS PERMANENTS','TRAFICS TEMPORAIRES']
        if type_cpt in self.liste_type_cpt:
            self.type_cpt=type_cpt
        else : 
            raise Comptage_cd47.CptCd47_typeCptError(type_cpt)
        #liste des dossiers contenant du permanent
    
    def modifier_type_cpt(self,new_type_cpt):
        """
        mettre à jour le type_cpt de l'objet
        """
        if new_type_cpt in self.liste_type_cpt:
            self.type_cpt=new_type_cpt
        else : 
            raise Comptage_cd47.CptCd47_typeCptError(new_type_cpt)
    
    def liste_dossier(self):
        """
        recupérer la liste des dossier contenant les comptages en fonction du type_cpt
        """
        liste_dossiers=[os.path.join(root,directory) for root, dirs, files in 
                 os.walk(self.dossier) 
                 for directory in dirs if 'UD' in root and self.type_cpt.upper() in root]
        return liste_dossiers
    
    def dico_fichier(self,liste_dossiers):
        """
        creer un dico contenant la liste des fichier contenu dans chaque dossier qui contient des fichiers de comptage
        in : 
            liste_dossiers : list des dossiers contenant des fichiers de commtage, cf liste_dossier
        """
        if self.type_cpt.upper()=='TRAFICS PERMANENTS':
            dico_fichiers={dossier : {'fichier' : O.ListerFichierDossier(dossier,'.xlsx')} for dossier in liste_dossiers}
        elif self.type_cpt.upper()=='TRAFICS PERIODIQUES': 
            dico_fichiers={dossier : {'fichier' : [[fichier for fichier in O.ListerFichierDossier(dossier,'.xlsx') 
                        if uniq in fichier  and re.search('[T][1-4]',fichier) and 'vitesse' not in fichier.lower()] 
                        for uniq in set([re.split('[T,V][1-4]',fichier)[0] 
                        for fichier in O.ListerFichierDossier(dossier,'.xlsx')]) ] } for dossier in liste_dossiers}
        elif self.type_cpt.upper()=='TRAFICS TEMPORAIRES' :
            dico_fichiers={dossier : {'fichier' : [[fichier for fichier in O.ListerFichierDossier(dossier,'.xlsx') 
                        for uniq in [re.split(' T.xls',fichier)[0] 
                        for fichier in O.ListerFichierDossier(dossier,'.xlsx')]
                        if uniq in fichier  and re.search(' T.xls',fichier) and 'V.xls' not in fichier
                        and re.search('D[ ]{0,1}[0-9]+',fichier)]] } for dossier in self.liste_dossier()}
        return dico_fichiers
    
    def ouverture_fichier(self,dossier,fichier):
        """
        ouvrir un fichier, prendre la bonne feuille, virer les NaN et renommer les colonnes
        """
        colonne=['jour','type_veh']+[str(i)+'_'+str(i+1)+'h' for i in range(24)]+['total','pc_pl']
        toutes_feuilles=pd.read_excel(os.path.join(dossier,fichier),sheet_name=None, header=None)
        try : 
            data=toutes_feuilles[[a for a in list(toutes_feuilles.keys())[::-1] if a[:2]=='S_'][0]]#on ouvre la premiere feuille ne partany de ladroite qui a un prefixe avec S_
        except IndexError : 
            data=toutes_feuilles[[a for a in list(toutes_feuilles.keys())[::-1] if 'sens 3' in a.lower().strip()][0]]
        data.dropna(axis=0,how='all', inplace=True)
        data.columns=colonne
        return data
    
    def id_comptag(self, data):
        """
        definir l'id_comptag à patir d'une df issue d'un fcihier xls (cf ouverture_fichier)
        """
        localisation=data.loc[1,'jour']
        if 'PR' in localisation :
            pr_abs=localisation.split(' PR')[1].strip()[:-1].split(' ')[0]
            pr=int(pr_abs.split('+')[0])
            absc=int(pr_abs.split('+')[1])
            route=re.search('D[ ]{0,1}[0-9]+',localisation.split(' PR ')[0].split("(")[1].strip())[0].replace(' ','')
            id_comptag='47-'+route+'-'+str(pr)+'+'+str(absc)
        else : 
            pr=None
            absc=None
            route=re.search('D[ ]{0,1}[0-9]+',localisation)
            id_comptag=localisation
        return id_comptag,pr,absc,route
    
    def donnees_generales(self,data):
        """
        recuperer le pc_pl et le tmja et les periodes de mesures à patir d'une df issue d'un fcihier xls (cf ouverture_fichier)
        """
        
        for c in data.columns : #pour les cas bizrres ou les données ne sont pas à leurs places et  / ou sans calcul du pc_pl
            try : 
                tmja=int(data.loc[data['jour']=='Moyenne journalière : ',c].values[0].split(' ')[0])
                try : 
                    pc_pl=float(data.loc[data.loc[data['jour']=='Moyenne journalière : ',c].index+2,c].values[0].split('(')[1][1:-3].replace(',','.'))
                except IndexError : 
                    pc_pl=int(data.loc[data.loc[data['jour']=='Moyenne journalière : ',c].index+2,c].values[0].split(' ')[0])/tmja*100
                break
            except (AttributeError,ValueError) :
                continue
        
        dico_date={a:b for (a,b) in zip(['janvier','février','mars' ,'avril','mai','juin','juillet','août','septembre','octobre','novembre','décembre'],
                           [a.lower() for a in ['January','February','March', 'April','May','June','July','August','September','October','November','December']])}
        #trouver date de mesures peu importe la lign / colonne
        test=data.loc[data.apply(lambda x : any(['Période ' in a for a in x.values if isinstance(a,str)]), axis=1)]
        b_mask=test.apply(lambda x : 'Période ' in str(x.values), axis=0).to_numpy()
        date_mes=test.to_numpy()[:,b_mask][0][0]
        
        debut_periode=date_mes.lower().split('du')[1].split('au')[0].strip()
        fin_periode=date_mes.lower().split('du')[1].split('au')[1].strip()
        for k,v in dico_date.items() :
            debut_periode=debut_periode.replace(k,v)
            fin_periode=fin_periode.replace(k,v)
        debut_periode=pd.to_datetime(debut_periode)
        fin_periode=pd.to_datetime(fin_periode)
        return tmja, pc_pl,debut_periode,fin_periode
    
    def donnees_horaires(self, data):
        """
        Récuperer les valeurs unitaires de trafics
        """
        data_filtree=data.loc[data['jour']!='jour'].copy().reset_index(drop=True)
        data_filtree['jour'].fillna(method='pad', inplace=True)
        data_horaires=data_filtree.iloc[:data_filtree.loc[data_filtree['jour'].apply(lambda x : x[:7]=='Moyenne' if isinstance(x,str) else False)].index[0]].copy()
        data_horaires.jour=pd.to_datetime(data_horaires.jour)
        data_horaires=data_horaires[data_horaires.columns[:-2]].set_index('jour')
        data_horaires=data_horaires.loc[data_horaires.type_veh.isin(['TV','PL'])].copy()
        return data_horaires
    
    def remplir_dico_fichier(self):
        """
        creer un dico avec les valeusr de tmja et pc_pl par id_comptage
        """
        dico_final={}
        dico=self.dico_fichier(self.liste_dossier())
        if self.type_cpt.upper()=='TRAFICS PERMANENTS' : 
            for k,v in dico.items():
                v['mensuel']={'donnees_type':['tmja', 'pc_pl']}
                for i,fichier in enumerate(v['fichier']):
                    data=self.ouverture_fichier(k,fichier)
                    id_comptag,pr,absc,route=self.id_comptag(data)
                    if not 'id_comptag' in v.keys() : 
                        v['id_comptag']=id_comptag
                    tmja, pc_pl,debut_periode=self.donnees_generales(data)[:3]
                    mois=unidecode.unidecode(debut_periode.month_name(locale='French')[:4].lower())
                    v['mensuel'][mois]=[tmja,pc_pl]
                    if i==0 : 
                        v['horaire']=self.donnees_horaires(data.iloc[4:])
                    else : 
                        v['horaire']=pd.concat([v['horaire'],self.donnees_horaires(data.iloc[4:])], sort=False, axis=0)
                v['tmja']=int(statistics.mean([a[0] for a in v['mensuel'].values() if not isinstance(a[0],str)]))
                v['pc_pl']=round(float(statistics.mean([a[1] for a in v['mensuel'].values() if not isinstance(a[0],str)])),1)
                v['pr'], v['absc'],v['route'] =pr, absc, route
            dico_final={v['id_comptag']:{'tmja':v['tmja'],'pc_pl':v['pc_pl'], 'type_poste' : 'permanent','debut_periode':None,
                                         'fin_periode':None,'pr':v['pr'],'absc':v['absc'],'route':v['route'], 'horaire':v['horaire'], 'mensuel':v['mensuel']} for k,v in dico.items()}
        elif self.type_cpt.upper()=='TRAFICS PERIODIQUES' :
            for k,v in dico.items() :
                for liste_fichier in v['fichier'] :
                    liste_tmja=[]
                    liste_pc_pl=[]
                    i=0
                    for fichier in liste_fichier :
                        if 'Vitesse' not in fichier : 
                            data=self.ouverture_fichier(k,fichier)
                            try :
                                id_comptag,pr,absc,route=self.id_comptag(data) 
                            except :
                                continue
                            tmja, pc_pl=self.donnees_generales(data)[0:2]
                            if i==0 : 
                                horaire=self.donnees_horaires(data.iloc[4:])
                            else : 
                                horaire=pd.concat([horaire,self.donnees_horaires(data.iloc[4:])], sort=False, axis=0)
                            i+=1
                            liste_tmja.append(tmja)
                            liste_pc_pl.append(pc_pl)       
                    dico_final[id_comptag]={'tmja' : int(statistics.mean(liste_tmja)), 'pc_pl' : round(float(statistics.mean(liste_pc_pl)),1), 
                                            'type_poste' : 'tournant','debut_periode':None,'fin_periode':None,'pr':pr,'absc':absc,'route':route, 'horaire':horaire}  
        elif self.type_cpt.upper()=='TRAFICS TEMPORAIRES' : 
            for k,v in dico.items():
                for liste_fichier in v['fichier']:
                    if liste_fichier :
                        if len(liste_fichier)>1 : 
                            for fichier in liste_fichier :
                                data=self.ouverture_fichier(k,fichier)
                                id_comptag,pr,absc,route=self.id_comptag(data)
                                tmja, pc_pl,debut_periode,fin_periode=self.donnees_generales(data)
                                horaire=self.donnees_horaires(data.iloc[4:])
                                dico_final[id_comptag]={'tmja':tmja, 'pc_pl':pc_pl,'type_poste' : 'ponctuel','debut_periode':debut_periode,
                                                        'fin_periode':fin_periode,'pr':pr,'absc':absc,'route':route,'horaire':horaire}
                        else : 
                            data=self.ouverture_fichier(k,liste_fichier[0])
                            id_comptag,pr,absc,route=self.id_comptag(data)
                            tmja, pc_pl,debut_periode,fin_periode=self.donnees_generales(data)
                            horaire=self.donnees_horaires(data.iloc[4:])
                            dico_final[id_comptag]={'tmja':tmja, 'pc_pl':pc_pl,'type_poste' : 'ponctuel','debut_periode':debut_periode,
                                                        'fin_periode':fin_periode,'pr':pr,'absc':absc,'route':route,'horaire':horaire}
        return dico_final

    def regrouper_dico(self):
        """
        regrouper les dico creer dans remplir_dico_fichier et sortir une df
        """
        self.type_cpt='TRAFICS PERMANENTS'
        self.dico_perm=self.remplir_dico_fichier()
        self.modifier_type_cpt('TRAFICS PERIODIQUES')
        self.dico_perdq=self.remplir_dico_fichier()
        self.modifier_type_cpt('TRAFICS TEMPORAIRES')
        self.dico_tempo=self.remplir_dico_fichier()   
        self.dico_tot={}
        for d in [self.dico_perm, self.dico_perdq, self.dico_tempo]:
            self.dico_tot.update(d)
     
    def dataframe_dico(self, dico):
        """
        conversion d'un dico en df
        """
        df_agrege=pd.DataFrame.from_dict({k:{x:y for x,y in v.items() if x!='horaire'} for k, v in dico.items()},
                                        orient='index').reset_index().rename(columns={'index':'id_comptag'})
        df_agrege['src']='donnees_xls_sources'
        
        for (i,(k, v)) in enumerate(dico.items()) : 
            for x,y in v.items()  : 
                if x=='horaire' :
                    df_horaire=y
                    df_horaire['id_comptag']=k
            if i==0 : 
                df_horaire_tot=df_horaire.copy()
            else : 
                df_horaire_tot=pd.concat([df_horaire_tot,df_horaire], sort=False, axis=0)
        df_horaire_tot.columns=['type_veh','h0_1','h1_2','h2_3','h3_4','h4_5','h5_6','h6_7','h7_8','h8_9','h9_10','h10_11','h11_12','h12_13',
                                'h13_14','h14_15','h15_16','h16_17','h17_18','h18_19','h19_20','h20_21','h21_22','h22_23','h23_24','id_comptag']
        df_horaire_tot.reset_index(inplace=True)
        
        for (i,(k, v)) in enumerate(dico.items()) : 
            for x,y in v.items()  : 
                if x=='mensuel' :
                    df_mensuel=pd.DataFrame(y, index=range(2))
                    df_mensuel['id_comptag']=k
                    df_mensuel['annee']=str(self.annee)
            if i==0 : 
                df_mensuel_tot=df_mensuel.copy()
            else : 
                df_mensuel_tot=pd.concat([df_mensuel_tot,df_mensuel], sort=False, axis=0)
        df_mensuel_tot['annee']=str(self.annee)
               
        return df_agrege,df_horaire_tot, df_mensuel_tot
 #   
    def classer_comptage_update_insert(self,bdd):
        """
        a prtir du dico tot (regrouper_dico), separer les comptages a mettre a jour et ceux à inserer dans les attributs
        df_attr_update et df_attr_insert
        """
        #creer le dico_tot
        self.regrouper_dico()
        self.df_attr, self.df_attr_horaire, self.df_attr_mens=self.dataframe_dico(self.dico_tot)
        #prende en compte les variation d'id_comptag en gti et le gest
        self.corresp_nom_id_comptag(bdd,self.df_attr)
        #compârer avec les donnees existantes
        self.comptag_existant_bdd(bdd, 'na_2010_2019_p', dep='47')
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
    
    def update_bdd_47(self,bdd,schema,table,df):
        """
        mise a jour base sur la fonction update bdd
        """
        val_txt=self.creer_valeur_txt_update(df, ['id_comptag','tmja','pc_pl','src'])
        self.update_bdd(bdd,schema,table, val_txt,{f'tmja_{str(self.annee)}':'tmja',f'pc_pl_{str(self.annee)}':'pc_pl',f'src_{str(self.annee)}':'src'})
        
    def mise_en_forme_insert(self):
        """
        ajout des attributs à self.df_attr_insert attendu dans la table comptag avant transfert dans bdd
        in : 
            annee : string : annee sur 4 lettres pour mise enf orme nom attr
        """
        if not len(str(self.annee))==4 : 
            raise TypeError('annee doit un string ou entier sur 4 caracteres')
        self.df_attr_insert['dep']='47'
        self.df_attr_insert['reseau']='RD'
        self.df_attr_insert['gestionnai']='CD47'
        self.df_attr_insert['concession']='N'
        self.df_attr_insert['obs']=self.df_attr_insert.apply(lambda x : f"""nouveau_point,{x['debut_periode'].strftime("%d/%m/%Y")}-{x['fin_periode'].strftime("%d/%m/%Y")}""" if not (pd.isnull(x['debut_periode']) and  pd.isnull(x['fin_periode'])) else None,axis=1)
        self.df_attr_insert.rename(columns={'absc' : 'abs', 'tmja':'tmja_'+str(self.annee),'pc_pl':'pc_pl_'+str(self.annee),'obs':'obs_'+str(self.annee)},inplace=True)
        self.df_attr_insert.drop(['debut_periode','fin_periode'],axis=1,inplace=True)
        
    class CptCd47_typeCptError(Exception):  
        """
        Exception levee si le type de comptage n'est pas dans la liste self.self.type_cpt
        """     
        def __init__(self, type_cpt):
            Exception.__init__(self,f'type de comptage "{type_cpt}" non présent dans {Comptage_cd47.liste_type_cpt} ')


class Comptage_cd87(Comptage):
    """
    les données fournies par le CD87 sont des fichiers fim bruts
    attributs : 
        dossier : nom du dossier contenant les fchiers
        liste_nom_simple : list edes fihciers avce un n om explicite
        liste_nom_foireux : liste des fichiers avec un nom pourri
    """
    def __init__(self,dossier):
        self.dossier=dossier
        self.liste_fichier=O.ListerFichierDossier(dossier,'.FIM')
    

    def classer_fichiers_par_nom(self):
        """
        classer les fihciers selon le nom : avec RD et PR ou sans
        """
        self.liste_nom_simple=[fichier for fichier in self.liste_fichier 
                  if re.search('[D][( |_)]{0,1}[0-9]{1,5}([A-O]|[Q-Z]|[a-o]|[q-z]){0,3}.*PR[( |_)]{0,1}[0-9]{1,4}',fichier)]
        self.liste_nom_foireux=[fichier for fichier in self.liste_fichier 
                    if re.search('^[0-9]{1,4}([A-Z]|[a-z]){0,3}[( |_)]{0,1}[0-9]{4,5}',fichier) and fichier not in self.liste_nom_simple]

        
    def verif_fichiers_dico(self):
        """
        verifier que les fichiers des listes (cf classer_fichiers_par_nom()) sont tous dans le dico
        """    
        #verif sur le nombre de fichier
        if not len(self.liste_nom_simple)+len(self.liste_nom_foireux)==len([e for a in [b['fichiers'] for a in self.dico_voie.values() for b in a] for e in a]) : 
            nbfichierManquants=abs((len(self.liste_nom_simple)+len(self.liste_nom_foireux))-len([e for a in [b['fichiers'] for a in self.dico_voie.values() for b in a] for e in a]) )
            raise self.CptCd87_ManqueFichierDansDicoError(nbfichierManquants)
        else : print('OK : tous fichiers dans dico')
        
    def supprimer_fichiers_doublons(self):
        """
        supprimer dans les listes de fichiers presentes dans le dico, les fihciers qui sont les mm
        """
        for k,v in self.dico_voie.items() :
            for e in v :
                liste_fichiers=e['fichiers']
                liste_fichier_dbl=[]
                dico_corresp_fichier_dbl={}
                for i,f_a_tester in enumerate(liste_fichiers) :
                    if i!=0:
                        if f_a_tester in [a for b in dico_corresp_fichier_dbl.values() for a in b] :
                            continue
                    for f_test in liste_fichiers :
                        if f_test!=f_a_tester and filecmp.cmp(os.path.join(self.dossier,f_a_tester),os.path.join(self.dossier,f_test),shallow=True):
                            liste_fichier_dbl.append(f_test)
                    dico_corresp_fichier_dbl[f_a_tester]=liste_fichier_dbl
                    liste_fichier_dbl=[]
                e['fichiers']=[a for a in dico_corresp_fichier_dbl.keys()] 
        
        
    def dico_pt_cptg(self):
        """
        creer un dico de caracterisation pdes pt de comptag : pr, absc, fihciers.
        le dico supprime les fihciers doublons du dossier source
        """
        #recuperer les liste de fichiers : 
        self.classer_fichiers_par_nom()
        #associer les fichiers à des voies_pr_absc pour la liste_nom_simple 
        self.dico_voie={}
        for fichier in self.liste_nom_simple :
            nom_voie=O.epurationNomRoute(re.sub('( |_)','',re.search('[D][( |_)]{0,1}[0-9]{1,5}([A-O]|[Q-Z]|[a-o]|[q-z]){0,3}',fichier).group(0))).upper()
            pr=re.search('(PR[( |_)]{0,1})([0-9]{1,2})([+]{0,1})([0-9]{0,3})',re.sub("(!|\()",'+',fichier)).group(2)
            absc=re.search('(PR[( |+)]{0,1})([0-9]{1,2})([+]{0,1})([0-9]{0,3})',re.sub("(!|\(|_|x)",'+',fichier)).group(4)
            if len(absc)<3 :
                absc, pr = int(pr[-(3-len(absc))] + absc), int(pr[0:-(3-2)])
            else : 
                absc, pr=int(absc),int(pr)
            if nom_voie in self.dico_voie.keys():
                for e in self.dico_voie[nom_voie] :
                    if pr==e['pr'] : 
                        if absc==e['abs'] : 
                            e['fichiers'].append(fichier)
                            break
                        else : 
                            self.dico_voie[nom_voie].append({'pr':pr,'abs':absc,'fichiers':[fichier]})
                            break
                else : 
                    self.dico_voie[nom_voie].append({'pr':pr,'abs':absc,'fichiers':[fichier]})
            else : 
                self.dico_voie[nom_voie]=[{'pr':pr,'abs':absc,'fichiers':[fichier]},]
                
        #pour la liste_nom_foireux : 
        for fichier in self.liste_nom_foireux : 
            nom_voie=O.epurationNomRoute('D'+re.split('( |_)',fichier)[0]).upper()
            absc, pr=int(re.split('( |_)',fichier)[2][-3:]), int(re.split('( |_)',fichier)[2][:-3])
            if nom_voie in self.dico_voie.keys():
                for e in self.dico_voie[nom_voie] :
                    if pr==e['pr'] : 
                        if absc==e['abs'] : 
                            e['fichiers'].append(fichier)
                            break
                        else : 
                            self.dico_voie[nom_voie].append({'pr':pr,'abs':absc,'fichiers':[fichier]})
                            break
                else : 
                    self.dico_voie[nom_voie].append({'pr':pr,'abs':absc,'fichiers':[fichier]})
            else : 
                self.dico_voie[nom_voie]=[{'pr':pr,'abs':absc,'fichiers':[fichier]},]
        
        self.verif_fichiers_dico()#verfi que tous fihciers dans dico
        self.supprimer_fichiers_doublons()
        
    def remplir_indicateurs_dico(self):
        """
        ajouter eu dico issu de dico_pt_cptg les données de tmja, pc_pl, date_debut, date_fin
        """
        for k, v in self.dico_voie.items() : 
            for i,e in enumerate(v) : 
                if len(e['fichiers'])==1 : 
                    print(e['fichiers'][0])
                    obj_fim=FIM(os.path.join(self.dossier,e['fichiers'][0]))
                    try : 
                        obj_fim.resume_indicateurs()
                    except PasAssezMesureError : 
                        continue
                    except Exception as ex : 
                        print(f"erreur : {ex} \n dans fichier : {e['fichiers'][0]}")
                    e['tmja'], e['pc_pl'], e['date_debut'], e['date_fin']=obj_fim.tmja, obj_fim.pc_pl, obj_fim.date_debut,obj_fim.date_fin
                elif len(e['fichiers'])>1 :
                    list_tmja=[]
                    list_pc_pl=[]
                    for f in e['fichiers'] : 
                        obj_fim=FIM(os.path.join(self.dossier,f))
                        print(f)
                        try : 
                            obj_fim.resume_indicateurs()
                        except (PasAssezMesureError,obj_fim.fimNbBlocDonneesError)  : 
                            continue
                        except Exception as ex : 
                            print(f"erreur : {ex} \n dans fichier : {f}")
                        list_tmja.append(obj_fim.tmja)
                        list_pc_pl.append(obj_fim.pc_pl)
                    #pour faire les moyennes et gérer les valeurs NaN de pc_pl
                    list_pc_pl=[p for p in list_pc_pl if p>0]
                    e['tmja'], e['date_debut'], e['date_fin']=int(statistics.mean(list_tmja)),np.NaN, np.NaN
                    e['pc_pl']=round(statistics.mean([p for p in list_pc_pl if p>0]),2) if list_pc_pl else np.NaN
    
    def remplir_type_poste_dico(self):
        """
        ajouter eu dico issu de remplir_indicateurs_dico le type de poste selon le nb de fichiers ayant servi à calculer le tmja
        """        
        for k, v in self.dico_voie.items() : 
            for e in v : 
                if len(e['fichiers']) > 4 :
                    e['type_poste']='permanent'
                elif 1<len(e['fichiers'])<=4 : 
                    e['type_poste']='tournant'
                elif len(e['fichiers'])== 1 :
                    e['type_poste']='ponctuel'
                else : 
                    e['type_poste']='NC'
    
    def dataframe_dico(self):
        """
        obtenir une df à partir du dico issu de remplir_type_poste_dico
        """
        self.df_attr=pd.DataFrame([[k, e['pr'], e['abs'], e['tmja'], e['pc_pl'], e['type_poste'],
                      e['date_debut'],e['date_fin']] for k, v in self.dico_voie.items() for e in v if 'tmja' in e.keys()], 
             columns=['route','pr','absc','tmja','pc_pl','type_poste','date_debut','date_fin'])
        self.df_attr['id_comptag']=self.df_attr.apply(lambda x :'87-'+x['route']+'-'+str(x['pr'])+'+'+str(x['absc']), axis=1)
        
    def filtrer_periode_ponctuels(self):
        """
        filtrer des periodesde vacances
        """
        #filtrer pt comptage pendant juillet aout
        self.df_attr=self.df_attr.loc[self.df_attr.apply(lambda x : x['date_debut'].month not in [7,8] and x['date_fin'].month not in [7,8], 
                                                         axis=1)].copy()
    
    def dataframe_dico_glob(self):
        """
        fonction globla de création d'une df
        """
        self.remplir_indicateurs_dico()
        self.remplir_type_poste_dico()
        self.dataframe_dico()
        self.filtrer_periode_ponctuels()
    
    def classer_comptage_update_insert(self,bdd,table_cpt,schema_cpt,
                                       schema_temp,nom_table_temp,table_linearisation_existante,
                                       schema_graph, table_graph,table_vertex,id_name):
        """
        classer la df des comptage (self.df_attr) selon le spoints à mettre à jour et ceux à inserer, en prenant en compte les points diont l'id_comptag
        diffère mais qui sont sur le mm troncon elemnraire
        in :
            bdd :string :id de la bdd
            table_cpt : string : nom de la table dans la bdd contenar les cpt
            schema_cpt :string : nom du schma contenant la table
            schema_temp : string : nom du schema en bdd opur calcul geom, cf localiser_comptage_a_inserer
            nom_table_temp : string : nom de latable temporaire en bdd opur calcul geom, cf localiser_comptage_a_inserer
            table_linearisation_existante : string : schema-qualified table de linearisation de reference cf donnees_existantes
            schema_graph : string : nom du schema contenant la table qui sert de topologie
            table_graph : string : nom de la table topologie (normalement elle devrait etre issue de table_linearisation_existante
            table_vertex : string : nom de la table des vertex de la topoolgie
            id_name : nom de l'identifiant uniq en integer de la table_graoh
        """
        #fare le tri avec les comptages existants : 
        #recuperer les compmtages existants
        self.comptag_existant_bdd(bdd, table_cpt, schema=schema_cpt,dep='87', type_poste=False)
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        #obtenir une cle de correspondace pour les comptages tournants et permanents
        df_correspondance=self.corresp_old_new_comptag(bdd, schema_temp,nom_table_temp,table_linearisation_existante,
                                        schema_graph, table_graph,table_vertex,id_name)
        #passer la df de correspondance dans le table corresp_id_comptage
        self.insert_bdd(bdd, schema_cpt, 'corresp_id_comptag', 
               df_correspondance.rename(columns={'id_comptag_lin':'id_gti','id_comptag':'id_gest'})[['id_gest','id_gti']])
        #faire la correspondance entre les noms de comptage
        self.corresp_nom_id_comptag(bdd,self.df_attr)
        #recalculer les insert et update
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
    
    def update_bdd_d87(self,bdd,table_cpt,schema_cpt):
        """
        mettre à jour la bdd avec df_attr_update, en ayant au préalbale traite les NaN
        """
        #mettre en forme pour update
        self.df_attr_update['obs']=self.df_attr_update.apply(lambda x : x['date_debut'].strftime('%d/%m/%Y')+'-'+ x['date_fin'].strftime('%d/%m/%Y') if not pd.isnull(x['date_debut']) else '', axis=1)
        self.df_attr_update.loc[self.df_attr_update.pc_pl.isna(),'obs']='pc_pl inconnu'
        self.df_attr_update.loc[self.df_attr_update.pc_pl.isna(),'pc_pl']=-99
        #preparer update
        valeurs_txt=self.creer_valeur_txt_update(self.df_attr_update, ['id_comptag','tmja','pc_pl','obs'])
        dico_attr={'tmja_2018':'tmja','pc_pl_2018':'pc_pl','obs_2018':'obs'}
        #update
        self.update_bdd(bdd, schema_cpt, table_cpt, valeurs_txt,dico_attr)
        
    def insert_bdd_d87(self,bdd,table_cpt,schema_cpt):
        """
        inserer les point df_attr_insert qui n'était pas à mettre à jour
        """
        #mettre en forme le insert
        dbl=self.df_attr_insert.loc[self.df_attr_insert.duplicated('id_comptag', False)].copy()
        ss_dbl=self.df_attr_insert.loc[~self.df_attr_insert.index.isin(dbl.index.tolist())].copy()
        dbl=dbl.dropna()
        dbl_traite=dbl.loc[dbl.tmja==dbl.groupby('id_comptag').tmja.transform(max)].drop_duplicates().copy()
        self.df_attr_insert=pd.concat([dbl_traite,ss_dbl], axis=0, sort=False)
        self.df_attr_insert.pc_pl.fillna(-99, inplace=True)
        annee='2018'
        self.df_attr_insert['dep']='87'
        self.df_attr_insert['reseau']='RD'
        self.df_attr_insert['gestionnai']='CD87'
        self.df_attr_insert['concession']='N'
        self.df_attr_insert['obs']=self.df_attr_insert.apply(lambda x : f"""nouveau_point,{x['date_debut'].strftime("%d/%m/%Y")}-{x['date_fin'].strftime("%d/%m/%Y")}""" if not (pd.isnull(x['date_debut']) and  pd.isnull(x['date_fin'])) else None,axis=1)
        self.df_attr_insert.rename(columns={'absc' : 'abs', 'tmja':'tmja_'+annee,'pc_pl':'pc_pl_'+annee,'obs':'obs_'+annee},inplace=True)
        self.df_attr_insert.drop(['date_debut','date_fin','route'],axis=1,inplace=True)
        self.insert_bdd(self,bdd,table_cpt,schema_cpt, self.df_attr_insert)
        #mettre à jour la geom
        self.maj_geom(bdd, schema_cpt, table_cpt, dep='87')
    
    class CptCd87_ManqueFichierDansDicoError(Exception):  
        """
        Exception levee si des fichiers des listes ne sont pas presents dans le dico
        """     
        def __init__(self, nbfichierManquants):
            Exception.__init__(self,f'il manque {nbfichierManquants} fichiers présents dans liste_nom_simple ou liste_nom_foireux dans le dico_voie')


class Comptage_cd16(Comptage):
    """
    les données fournies par le CD16 sont des fichiers excel pour les compteurs permanents, et des fichiers FIM pour les comptages temporaires.
    il y a aussi des données sur PIGMA qu'il a tout prix télécharger, car les données FIM ne sont géolocalisable que par PIGMA
    en 2018 on ne traite pas les FIM mais ils permettront d'obtenir des données horaires plus tard.
    Normalement dans ce dept on a que des comptage en update
        fichier_b15_perm : nom complet du fichier des comptages permanents
        fichier_cpt_lgn : nom complet du fihcier de ligne contenant tous les comptages issu de pigma
        fichier_station_cpt : nom complet du fihcier de points contenant tous les points comptages issu de pigma
    """
    def __init__(self,fichier_b15_perm,fichier_cpt_lgn,fichier_station_cpt,annee):
        self.fichier_b15_perm=fichier_b15_perm
        self.fichier_cpt_lgn=fichier_cpt_lgn
        self.fichier_station_cpt=fichier_station_cpt
        self.annee=annee
        
    def cpt_perm_xls(self, skiprows, cpt_a_ignorer=()):
        """
        mettre en forme les comptages permannets issu des fichiers de comptages au format xls B15 de route plus
        in : 
            skiprows : nb de lignes du fichiers excel a ignorer
            cpt_a_ignorer : tuple de string des idc_omptag a ignorer au vu des commentaires du CD16
        """
        donnees_brutes=pd.read_excel(self.fichier_b15_perm, skiprows=skiprows)
        donnees_filtrees=donnees_brutes[[a for a in donnees_brutes.columns if not isinstance(a, int) and 'Unnamed' not in a]].copy()
        # traiter et mettre en forme
        tmja=donnees_filtrees.loc[donnees_filtrees .apply(lambda x : x['Identif. Local.'][-2:]==' 3', axis=1)].copy()
        tmja['pr']=tmja['PR+Distance'].apply(lambda x : re.split('(\.|\+|,)',str(x))[0])
        tmja['absc']=tmja['PR+Distance'].apply(lambda x : int(re.split('(\.|\+|,)',str(x))[2]+'0'*(4-len(re.split('(\.|\+|,)',str(x))[2]))) if re.search('(\.|\+|,)', str(x)) else 0)
        tmja['route']=tmja['Route'].apply(lambda x : O.epurationNomRoute(x[3:]))
        tmja['id_comptag']=tmja.apply(lambda x : f"16-{x['route']}-{x['pr']}+{x['absc']}", axis=1)
        tmja_final=tmja[['pr', 'absc', 'route', 'id_comptag', 'Année']].loc[(tmja['Année']!=' ') & (~tmja['Année'].isna())].rename(columns={'Année':'tmja'})
        tmja_mens=tmja[[c for c in tmja.columns if c in [a[2] for a in dico_mois.values()]]+['id_comptag']].copy()
        tmja_mens=tmja_mens.rename(columns={a:k for a in tmja_mens.columns if a!='id_comptag' for k,v in dico_mois.items() if a==v[2] })
        tmja_mens['donnees_type']='tmja'
        tmja_mens['annee']=str(self.annee)
        index_tmja=tmja.index.tolist()
        index_ppl=[a+1 for a in index_tmja]
        ppl=donnees_filtrees.loc[index_ppl].copy()
        ppl['pr']=ppl['PR+Distance'].apply(lambda x : re.split('(\.|\+|,)',str(x))[0])
        ppl['absc']=ppl['PR+Distance'].apply(lambda x : int(re.split('(\.|\+|,)',str(x))[2]) if re.search('(\.|\+|,)', str(x)) else 0)
        ppl['route']=ppl['Route'].apply(lambda x : O.epurationNomRoute(x[3:]))
        ppl['id_comptag']=ppl.apply(lambda x : f"16-{x['route']}-{x['pr']}+{x['absc']}", axis=1)
        ppl['Année']=ppl['Année'].apply(lambda x : round(float(x),2) if x!=' ' else np.NaN)
        ppl_mens=ppl[[c for c in ppl.columns if c in [a[2] for a in dico_mois.values()]]+['id_comptag']].copy()
        ppl_mens=ppl_mens.rename(columns={a:k for a in ppl_mens.columns if a!='id_comptag' for k,v in dico_mois.items() if a==v[2] })
        ppl_mens['donnees_type']='pc_pl'
        ppl_mens['annee']=str(self.annee)
        ppl_mens=ppl_mens.applymap(lambda x : round(x,2) if isinstance(x,float) else x)
        ppl_final=ppl[['pr', 'absc', 'route', 'id_comptag', 'Année']].rename(columns={'Année':'pc_pl'})
        df_trafic=tmja_final.merge(ppl_final[['pc_pl','id_comptag']], on='id_comptag')
        donnees_mens=pd.concat([tmja_mens,ppl_mens], axis=0, sort=False)
        #filtrer selon comm CD16
        df_compt_perm=df_trafic.loc[~df_trafic.id_comptag.isin(cpt_a_ignorer)].copy()
        donnees_mens=donnees_mens.loc[~donnees_mens.id_comptag.isin(cpt_a_ignorer)].copy()
        df_compt_perm['type_poste']='Per'
        df_compt_perm['src']='tableau B15'
        return df_compt_perm, donnees_mens
    
    def cpt_tmp_pigma(self):
        """
        mettre en forme les comptages temporaires issu de pigma
        """
        donnees_brutes_tmp_lgn=gp.read_file(self.fichier_cpt_lgn)
        donnees_brutes_tmp_pt=gp.read_file(self.fichier_station_cpt)
        
        donnees_brutes_tmp=donnees_brutes_tmp_pt[['AXE','PLOD','ABSD','SECTION']].merge(donnees_brutes_tmp_lgn[['AXE','TRAFIC_PL','TMJA','ANNEE_COMP', 'PRC','ABC','TYPE_COMPT','SECTION_CP']], 
                                                                                        left_on=['AXE','PLOD','ABSD'],right_on=['AXE','PRC','ABC'])
        donnees_tmp_liees=donnees_brutes_tmp.loc[(donnees_brutes_tmp['ANNEE_COMP']==self.annee) & (donnees_brutes_tmp['TYPE_COMPT']!='Per')].copy()
        donnees_tmp_filtrees=donnees_tmp_liees.rename(columns={'AXE':'route','PRC':'pr','ABC':'absc','TMJA':'tmja','TRAFIC_PL':'pc_pl','TYPE_COMPT':'type_poste'}).drop(['PLOD','ABSD','SECTION','ANNEE_COMP'], axis=1)
        donnees_tmp_filtrees['id_comptag']=donnees_tmp_filtrees.apply(lambda x : f"16-{x['route']}-{x['pr']}+{x['absc']}", axis=1)
        donnees_tmp_filtrees['src']='sectionnement'
        return donnees_tmp_filtrees
    
    def donnees_horaires(self, donnees_tmp_filtrees, dossier):
        """
        à partir des FIM et du fichier geolocalise des comptages, creer le fichier horaire par pt de comptag
        in : 
            dossier : dosssier contenant les fichers FIM de donnees hoarires
        """
        def mise_en_forme_fim_horaire(donnees, id_comptag):
            """
            modifier la forme horaire des FIM pour le transfert dans la table des comptages horaires
            """
            donnees.reset_index(inplace=True)
            donnees['jour']=donnees['date'].apply(lambda x : pd.to_datetime(x.strftime('%Y-%m-%d')))
            donnees['heure']=donnees['date'].apply(lambda x : f"h{x.hour}_{x.hour+1 if x.hour!=24 else 24}")
            donnees_tv=donnees[['jour', 'heure', 'tv']].pivot(index='jour',columns='heure', values='tv')
            donnees_tv['type_veh']='TV'
            donnees_pl=donnees[['jour', 'heure', 'pl']].pivot(index='jour',columns='heure', values='pl')
            donnees_pl['type_veh']='PL'
            donnees=pd.concat([donnees_tv,donnees_pl], axis=0, sort=False).reset_index()
            donnees['id_comptag']=id_comptag
            return donnees
        
        for e,fichier in enumerate(O.ListerFichierDossier(dossier,'')) : 
            print(fichier)
            fichier_fim=FIM(os.path.join(dossier, fichier), gest='CD16')
            try : 
                fichier_fim.resume_indicateurs()
            except PasAssezMesureError as e : 
                print(e, fichier)
                continue
            id_comptag=donnees_tmp_filtrees.loc[donnees_tmp_filtrees['SECTION_CP']==fichier_fim.section_cp].id_comptag.values[0]
            donnees=fichier_fim.df_tot_heure[['tv_tot','pl_tot']].rename(columns={'tv_tot':'tv','pl_tot':'pl'}).copy()
            donnees=mise_en_forme_fim_horaire(donnees,id_comptag)
            if e==0: 
                fichier_tot=donnees.copy()
            else : 
                fichier_tot=pd.concat([fichier_tot,donnees.copy()], sort=False, axis=0)
        
        return fichier_tot
    
    def comptage_forme(self, skiprows, cpt_a_ignorer, dossier_cpt_fim):
        """
        fusion des données de cpt_tmp_pigma() et cpt_perm_xls()
        in : 
            cf fonction cpt_perm_xls
        """
        df_compt_perm, donnees_mens=self.cpt_perm_xls(skiprows, cpt_a_ignorer)
        donnees_tmp_filtrees=self.cpt_tmp_pigma()
        self.df_attr=pd.concat([df_compt_perm,donnees_tmp_filtrees],sort=False, axis=0)
        self.df_attr_mens=donnees_mens
        self.df_attr_horaire=self.donnees_horaires(donnees_tmp_filtrees, dossier_cpt_fim)

    def classer_comptage_update_insert(self,bdd,table_cpt):
        """
        faire le tri entre les comptages à mettre a jour et ceux à inserer. Pour arppel dans le 16 normalement on a que du update
        in :
            bdd : string, descriptf bdd (cf module Connexion_transfert de Outils
            table_cpt : string : nom e la table des compatges que l'on va chercher a modifer
        """
        self.comptag_existant_bdd(bdd, table_cpt, dep='16')
        self.corresp_nom_id_comptag(bdd, self.df_attr)
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()

    def update_bdd_16(self,bdd, schema, table):
        """
        mettre à jour la table des comptages dans le 16
        """
        valeurs_txt=self.creer_valeur_txt_update(self.df_attr_update,['id_comptag','tmja','pc_pl','src'])
        dico_attr_modif={'tmja_'+str(self.annee):'tmja', 'pc_pl_'+str(self.annee):'pc_pl','src_'+str(self.annee):'src'}
        self.update_bdd(bdd, schema, table, valeurs_txt,dico_attr_modif)
    

class Comptage_cd86(Comptage):
    """
    les données fournies par le CD86 sont des fichiers excel pour les compteurs permanents et secondaires,
    il y a un petit pb sur les compteurs permanents entre la donnees pr+abs chez nous et la leur dans le tableau, donc il faut la premiere fois (2018) tout passer dans la table de correspondance
    si un seul fichier avec 2 feuilles : renseigner le mm nom de fichier pour perm etsecondaire, et indiuqer les feuills
        fichier_perm : nom complet du fichier des comptages permanents
        fichier_secondaire : nom complet du fihcier des comptages secondaires
        annee : integer annee des comptages
        feuil_perm : si c le mm fihciers pour les comptages perm et secondaiere, nom de la feuille avec les comptages perm 
        feuil_secondaire : si c le mm fihciers pour les comptages perm et secondaiere, nom de la feuille avec les comptages secondaires
    """
    def __init__(self,fichier_perm,fichier_secondaire,annee, feuil_perm=None, feuil_secondaire=None):
        self.fichier_perm=fichier_perm
        self.fichier_secondaire=fichier_secondaire
        self.annee=annee
        self.feuil_perm=feuil_perm
        self.feuil_secondaire=feuil_secondaire
    
    def ouvrir_cpt_perm_xls(self):
        """
        mettre en forme les comptages permannets
        """
        donnees_brutes_perm=pd.read_excel(self.fichier_perm)
        donnees_brutes_perm['route']=donnees_brutes_perm['AXE'].apply(lambda x : O.epurationNomRoute(x[3:]))
        donnees_brutes_perm['pr']=donnees_brutes_perm.apply(lambda x : int(x['CUMULD']//1000) if not pd.isnull(x['CUMULD']) else x['PLOD'], axis=1)
        donnees_brutes_perm['absc']=donnees_brutes_perm.apply(lambda x : int(x['CUMULD']%1000) if not pd.isnull(x['CUMULD']) else 0, axis=1)
        donnees_brutes_perm['id_gest']=donnees_brutes_perm.apply(lambda x : f"86-{x['route']}-{x['pr']}+{x['absc']}", axis=1)
        return donnees_brutes_perm

    def corresp_perm(self, bdd, table):
        """
        creer le dico de corresp des compt perm fouri en 2018 avec ceux existants
        """
        self.comptag_existant_bdd(bdd, table, schema='comptage',dep='86')
        corresp=self.existant[['id_comptag','route','pr','abs']].merge(self.ouvrir_cpt_perm_xls(), left_on=['route','pr'], right_on=['route','PLOD'], how='right').sort_values('id_comptag')
        corresp['id_comptag']=corresp.apply(lambda x : x['id_comptag'] if not pd.isnull(x['id_comptag']) else f"86-{x['route']}-{x['pr_y']}+{x['absc']}", axis=1)
        #le dico de correspondance à inserer dans corresp_id_comptage
        corresp_id_comptag=corresp[['id_comptag', 'id_gest']].loc[corresp['id_comptag']!=corresp['id_gest']].rename(columns={'id_comptag':'id_gti'})
        return corresp_id_comptag
    
    def forme_cpt_perm_xls(self):
        #la future df_attr, avec que les perm
        donnees_brutes_perm=self.ouvrir_cpt_perm_xls()
        df_cpt_perm=donnees_brutes_perm[['id_gest','TMJA','POURCENTAGE_PL','TYPE_POSTE','route','pr','absc']].rename(columns={'id_gest':'id_comptag','TMJA':'tmja','POURCENTAGE_PL':'pc_pl','TYPE_POSTE':'type_poste'})
        df_cpt_perm['type_poste']='permanent'
        return df_cpt_perm

    def cpt_second_xls(self):
        """
        mettre en forme les comptages secondaires
        """  
        donnees_brutes_second=pd.read_excel(self.fichier_secondaire)
        donnees_brutes_second['route']=donnees_brutes_second.apply(lambda x : 'D'+str(x['RD']).strip(), axis=1)
        donnees_brutes_second['pr']=donnees_brutes_second.apply(lambda x : re.split('(\.|\+)',str(x['PR']))[0], axis=1)
        donnees_brutes_second['absc']=donnees_brutes_second.apply(lambda x : int(re.split('(\.|\+|,)',str(x['PR']))[2]+'0'*(3-len(re.split('(\.|\+|,)',str(x['PR']))[2]))) if re.search('(\.|\+)',str(x['PR'])) else 0, axis=1)
        donnees_brutes_second['id_comptag']=donnees_brutes_second.apply(lambda x : f"86-{x['route']}-{x['pr']}+{x['absc']}", axis=1)
        donnees_brutes_second['type_poste']='tournant'
        donnees_brutes_second['% PL']=donnees_brutes_second['% PL']*100
        df_cpt_second=donnees_brutes_second.rename(columns={'TV':'tmja','% PL':'pc_pl'}).drop(['LIEUX', 'RD','PR','n° de compteur','PL'], axis=1)     
        return  df_cpt_second
    
    def ouvrir_fichier_unique(self) : 
        donnees_brutes_perm=pd.read_excel(self.fichier_perm, sheet_name=self.feuil_perm)
        donnees_brutes_perm=donnees_brutes_perm.loc[(donnees_brutes_perm['ANNEE_TRAFIC']==self.annee) & (~donnees_brutes_perm['PLOD'].isna()) &
                            (~donnees_brutes_perm['ABSD'].isna()) & (~donnees_brutes_perm['TMJA'].isna()) & (~donnees_brutes_perm['POURCENTAGE_PL'].isna())].copy()
        donnees_brutes_perm['route']=donnees_brutes_perm['AXE'].apply(lambda x : O.epurationNomRoute(x[3:]))
        donnees_brutes_perm['pr_cumuld']=donnees_brutes_perm.apply(lambda x : int(x['CUMULD']//1000) if not pd.isnull(x['CUMULD']) else np.NaN, axis=1)
        donnees_brutes_perm['absc_cumuld']=donnees_brutes_perm.apply(lambda x : int(x['CUMULD']%1000) if not pd.isnull(x['CUMULD']) else np.NaN, axis=1)
        donnees_brutes_perm['id_gest_cumuld']=donnees_brutes_perm.apply(lambda x : f"86-{x['route']}-{x['pr_cumuld']}+{x['absc_cumuld']}", axis=1)
        donnees_brutes_perm['pr_plod']=donnees_brutes_perm.apply(lambda x : int(x['PLOD']) if not pd.isnull(x['PLOD']) else np.NaN, axis=1)
        donnees_brutes_perm['absc_absd']=donnees_brutes_perm.apply(lambda x : int(x['ABSD']) if not pd.isnull(x['ABSD']) else np.NaN, axis=1)
        donnees_brutes_perm['id_gest_plod']=donnees_brutes_perm.apply(lambda x : f"86-{x['route']}-{x['pr_plod']}+{x['absc_absd']}", axis=1)
        donnees_brutes_perm['POURCENTAGE_PL']=donnees_brutes_perm['POURCENTAGE_PL']*100
        donnees_brutes_second=pd.read_excel(self.fichier_perm, sheet_name='secondaires', names=donnees_brutes_perm.columns, header=None)
        donnees_brutes_second=donnees_brutes_second.loc[(donnees_brutes_second['ANNEE_TRAFIC']==2019) & (~donnees_brutes_second['PLOD'].isna()) &
                            (~donnees_brutes_second['ABSD'].isna()) & (~donnees_brutes_second['TMJA'].isna()) & (~donnees_brutes_second['POURCENTAGE_PL'].isna())].copy()
        donnees_brutes_second['route']=donnees_brutes_second['AXE'].apply(lambda x : O.epurationNomRoute(x[3:]))
        donnees_brutes_second['pr_cumuld']=donnees_brutes_second.apply(lambda x : int(x['CUMULD']//1000) if not pd.isnull(x['CUMULD']) else np.NaN, axis=1)
        donnees_brutes_second['absc_cumuld']=donnees_brutes_second.apply(lambda x : int(x['CUMULD']%1000) if not pd.isnull(x['CUMULD']) else np.NaN, axis=1)
        donnees_brutes_second['id_gest_cumuld']=donnees_brutes_second.apply(lambda x : f"86-{x['route']}-{x['pr_cumuld']}+{x['absc_cumuld']}", axis=1)
        donnees_brutes_second['pr_plod']=donnees_brutes_second.apply(lambda x : int(x['PLOD']) if not pd.isnull(x['PLOD']) else np.NaN, axis=1)
        donnees_brutes_second['absc_absd']=donnees_brutes_second.apply(lambda x : int(x['ABSD']) if not pd.isnull(x['ABSD']) else np.NaN, axis=1)
        donnees_brutes_second['id_gest_plod']=donnees_brutes_second.apply(lambda x : f"86-{x['route']}-{x['pr_plod']}+{x['absc_absd']}", axis=1)
        donnees_brutes_second['POURCENTAGE_PL']=donnees_brutes_second['POURCENTAGE_PL']*100
        donnees_concat=pd.concat([donnees_brutes_perm,donnees_brutes_second],axis=0, sort=False)
        dico_corresp_type_poste={'Permanent':'permanent', 'Tournant':'ponctuel', 'Secondaire':'tournant'}
        donnees_concat.TYPE_POSTE=donnees_concat.TYPE_POSTE.apply(lambda x : dico_corresp_type_poste[x])
        return donnees_brutes_perm,donnees_brutes_second, donnees_concat
    
    def equivalence_id_comptag(self,bdd,table_cpt, df) : 
        """
        pour conserver soit l'id_comptag issu du plod, soit celui issu du cumul, si il est présent dans la liste des id_coptage ou dans liste des id_cgest de la table corresp
        """
        rqt_corresp_comptg='select * from comptage.corresp_id_comptag'
        rqt_id_comtg=f"select * from comptage.{table_cpt} where dep='86'"
        with ct.ConnexionBdd(bdd) as c:
            list_corresp_comptg=pd.read_sql(rqt_corresp_comptg, c.sqlAlchemyConn).id_gest.tolist()
            list_id_comptg=gp.GeoDataFrame.from_postgis(rqt_id_comtg, c.sqlAlchemyConn, geom_col='geom',crs={'init': 'epsg:2154'}).id_comptag.tolist()
        
        def maj_id_comptg(id_cpt_plod,id_cpt_cumuld, list_id_comptag,list_id_gest) : 
            for a in  (id_cpt_plod,id_cpt_cumuld) : 
                if a in list_id_comptag or a in list_id_gest : 
                    return a
            else : return id_cpt_plod
    
        df['id_comptag']=df.apply(lambda x : maj_id_comptg(x['id_gest_plod'], x['id_gest_cumuld'], list_id_comptg,list_corresp_comptg ), axis=1)
    
    def comptage_forme(self,bdd, table_cpt) : 
        """
        creer le df_attr a partir des donnees secondaire et permanent
        2 cas de figuer pour le moment : en 2018 ajout des pr + abs issus de plod et abscd dans la liste de correspondance, en 2019 conservation des 2 colonnes et recherche si existants
        """
        if not self.feuil_perm : 
            self.df_attr=pd.concat([self.forme_cpt_perm_xls(),self.cpt_second_xls()], axis=0, sort=False)
            self.df_attr=self.df_attr.loc[~self.df_attr.isna().any(axis=1)].copy()
        else : 
            donnees_concat=self.ouvrir_fichier_unique()[2]
            self.equivalence_id_comptag(bdd, table_cpt, donnees_concat)
            self.df_attr=donnees_concat[['TMJA','POURCENTAGE_PL','TYPE_POSTE','route','id_comptag']].rename(columns={a:a.lower() for a 
                                        in donnees_concat[['TMJA','POURCENTAGE_PL','TYPE_POSTE','route','id_comptag']].columns}).rename(columns={'pourcentage_pl':'pc_pl'})
            self.df_attr['pr']=self.df_attr.id_comptag.apply(lambda x : int(x.split('-')[2].split('+')[0]))
            self.df_attr['absc']=self.df_attr.id_comptag.apply(lambda x : int(x.split('-')[2].split('+')[1]))
        self.df_attr['src']='tableur'
    
    
    def classer_comptage_update_insert(self,bdd, table_cpt):
        self.comptag_existant_bdd(bdd, table_cpt, dep='86')
        self.corresp_nom_id_comptag(bdd, self.df_attr)
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        
    def update_bdd_86(self,bdd, schema, table):
        """
        mettre à jour la table des comptages dans le 16
        """
        valeurs_txt=self.creer_valeur_txt_update(self.df_attr_update,['id_comptag','tmja','pc_pl','src'])
        dico_attr_modif={f'tmja_{str(self.annee)}':'tmja', f'pc_pl_{str(self.annee)}':'pc_pl',f'src_{str(self.annee)}':'src'}
        self.update_bdd(bdd, schema, table, valeurs_txt,dico_attr_modif)
    
    def insert_bdd_86(self,bdd, schema_cpt, table_cpt):   
        self.df_attr_insert['dep']='86'
        self.df_attr_insert['reseau']='RD'
        self.df_attr_insert['gestionnai']='CD86'
        self.df_attr_insert['concession']='N'
        self.df_attr_insert['obs']="nouveau_point 2019, denominationCD86 ='tournant'"
        self.df_attr_insert.rename(columns={'absc' : 'abs', 'tmja':'tmja_'+str(self.annee),'pc_pl':'pc_pl_'+str(self.annee),'obs':'obs_'+str(self.annee), 'src':'src_'+str(self.annee)},inplace=True)
        self.insert_bdd(bdd,schema_cpt,table_cpt, self.df_attr_insert)
        #mettre à jour la geom
        self.maj_geom(bdd, schema_cpt, table_cpt, dep='86')
        
        
class Comptage_cd24(Comptage):
    """
    pour le moment on ne traite que les cpt perm de 2018 issus de D:\temp\otv\Donnees_source\CD24\2018_CD24_trafic.csv
    """ 
    def __init__(self,fichier_perm):
        Comptage.__init__(self, fichier_perm)
    
    def cpt_perm_csv(self):
        donnees_brutes=self.ouvrir_csv()
        donnees_traitees=donnees_brutes.loc[~donnees_brutes.MJA.isna()].copy()
        donnees_traitees=donnees_traitees.loc[donnees_traitees['Sens']=='3'].rename(columns={'MJA' : 'tmja', 'MJAPPL':'pc_pl'}).copy()
        donnees_traitees['id_comptag']=donnees_traitees.apply(lambda x : f"24-{x['Data']}-{x['PRC']}+{x['ABC']}", axis=1)
        donnees_traitees['Latitude']=donnees_traitees.Latitude.apply(lambda x : float(x.replace(',','.')))
        donnees_traitees['Longitude']=donnees_traitees.Longitude.apply(lambda x : float(x.replace(',','.')))
        donnees_traitees['tmja']=donnees_traitees.tmja.apply(lambda x : int(x))
        donnees_traitees['pc_pl']=donnees_traitees.pc_pl.apply(lambda x : float(x.replace(',','.')))
        donnees_traitees.rename(columns={'Data':'route', 'PRC':'pr', 'ABC':'abs'}, inplace=True)
        gdf_finale = gp.GeoDataFrame(donnees_traitees, geometry=gp.points_from_xy(donnees_traitees.Longitude, donnees_traitees.Latitude), crs={'init': 'epsg:4326'})
        gdf_finale=gdf_finale.to_crs({'init': 'epsg:2154'})
        gdf_finale['type_poste']='permanent'
        gdf_finale['geometry']=gdf_finale.apply(lambda x : None if x['Latitude']==0 else x['geometry'], axis=1)
        gdf_finale['src']='tableau cpt permanent 2018'
        return gdf_finale
    
    def comptage_forme(self):
        donnees_finales=self.cpt_perm_csv()
        donnees_finales=donnees_finales[['id_comptag', 'tmja', 'pc_pl','route', 'pr', 'abs','src', 'geometry', 'type_poste']].copy()
        self.df_attr=donnees_finales
        
    def classer_comptage_update_insert(self,bdd, table_cpt):
        """
        attention, on aurait pu ajouter un check des comptages deja existant et rechercher les correspondances comme dans le 87,
        mais les données PR de l'IGN sont trop pourries dans ce dept, dc corresp faite à la main en amont
        """
        self.corresp_nom_id_comptag(bdd,self.df_attr)
        self.comptag_existant_bdd(bdd, table_cpt, dep='24')
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        """
        #on peut tenter un dico de correspondance, mais les données PR de l'IGN sont trop fausses pour faire confaince
        dico_corresp=cd24.corresp_old_new_comptag('local_otv_station_gti', 'public','cd24_perm', 'lineaire.traf2017_bdt24_ed17_l',
             'referentiel','troncon_route_bdt24_ed17_l','troncon_route_bdt24_ed17_l_vertices_pgr','id')
        """
        
    def update_bdd_24(self,bdd, schema, table):
        valeurs_txt=self.creer_valeur_txt_update(self.df_attr_update,['id_comptag','tmja','pc_pl','src'])
        dico_attr_modif={'tmja_2018':'tmja', 'pc_pl_2018':'pc_pl','src_2018':'src'}
        self.update_bdd(bdd, schema, table, valeurs_txt,dico_attr_modif) 
    
    def insert_bdd_24(self,bdd, schema, table):
        self.df_attr_insert.loc[self.df_attr_insert['id_comptag']=='24-D939-4+610', 'geometry']=Point(517579.400,6458283.399)
        self.df_attr_insert.loc[self.df_attr_insert['id_comptag']=='24-D939-4+610', 'obs_geo']='mano'
        self.df_attr_insert.loc[self.df_attr_insert['id_comptag']=='24-D939-4+610', 'src_geo']='tableau perm 2018'
        self.df_attr_insert.loc[self.df_attr_insert['id_comptag']=='24-D710-32+270', 'src_geo']='tableau perm 2018'
        self.df_attr_insert['dep']='24'
        self.df_attr_insert['reseau']='RD'
        self.df_attr_insert['gestionnai']='CD24'
        self.df_attr_insert['concession']='N'
        self.df_attr_insert=self.df_attr_insert.rename(columns={'tmja':'tmja_2018', 'pc_pl':'pc_pl_2018','src':'src_2018'})
        self.df_attr_insert['x_l93']=self.df_attr_insert.apply(lambda x : round(x['geometry'].x,3), axis=1)
        self.df_attr_insert['y_l93']=self.df_attr_insert.apply(lambda x : round(x['geometry'].y,3), axis=1)
        self.insert_bdd(bdd, schema, table,self.df_attr_insert)
    
class Comptage_vinci(Comptage):
    """
    inserer les donnees de comptage de Vinci
    POur info il y a une table de correspondance entre les donnees fournies par Vinci et les notre dans la base otv, scham source table asf_otv_tmja_2017
    """  
    def __init__(self,fichier_perm, annee):
        self.fichier_perm=fichier_perm
        self.annee=annee
        self.comptage_forme()
    
    def ouvrir_fichier(self):
        donnees_brutes=pd.read_excel(self.fichier_perm).rename(columns={'(*) PR début':'pr_deb'})
        donnees_brutes=donnees_brutes.loc[~donnees_brutes.pr_deb.isna()].copy()
        return donnees_brutes
    
    def importer_donnees_correspondance(self):
        with ct.ConnexionBdd('local_otv_station_gti') as c :
            rqt=f"""select * from source.asf_otv_tmja_2017"""
            base=pd.read_sql(rqt, c.sqlAlchemyConn).rename(columns={'(*) PR début':'pr_deb'})
            base=base.loc[~base.pr_deb.isna()].copy()
            base['pr_deb']=base.pr_deb.apply(lambda x : float(x.strip()))
        return base
    
    def comptage_forme(self):
        donnees_brutes=self.ouvrir_fichier()
        base=self.importer_donnees_correspondance() 
        self.df_attr=donnees_brutes[['pr_deb',f'TMJA {str(self.annee)}',f'Pc PL {str(self.annee)}']].merge(base[['pr_deb','ID']], on='pr_deb').rename(columns=
                                                            {'ID':'id_comptag',f'TMJA {str(self.annee)}':'tmja',f'Pc PL {str(self.annee)}':'pc_pl'})
        self.df_attr['pc_pl']=self.df_attr.pc_pl.apply(lambda x : round(x,2))
        self.df_attr['src']='tableur'
        
        tmjm_mens=donnees_brutes[[a for a in donnees_brutes.columns if a =='pr_deb' or 'TMJM '+str(self.annee) in a]].merge(base[['pr_deb','ID']], on='pr_deb').rename(columns=
                                                            {'ID':'id_comptag'}).drop('pr_deb', axis=1)
        tmjm_mens.rename(columns={c:k for c in tmjm_mens.columns if c!='id_comptag' for k, v in dico_mois.items()  if v[0]==int(c[-2:])}, inplace=True)
        tmjm_mens=tmjm_mens.applymap(lambda x : int(x) if isinstance(x, float) else  x)
        tmjm_mens['donnees_type']='tmja'
        pc_pl_mens=donnees_brutes[[a for a in donnees_brutes.columns if a == 'pr_deb' or re.search('Pc PL [0-9]{2}$',a)]].merge(base[['pr_deb','ID']], on='pr_deb').rename(columns=
                                                                    {'ID':'id_comptag'}).drop('pr_deb', axis=1)
        pc_pl_mens.rename(columns={c:k for c in pc_pl_mens.columns if c!='id_comptag' for k, v in dico_mois.items()  if v[0]==int(c[-2:])}, inplace=True)
        pc_pl_mens['donnees_type']='pc_pl'
        self.df_attr_mens=pd.concat([tmjm_mens,pc_pl_mens], axis=0, sort=False).sort_values('id_comptag')
        self.df_attr_mens['annee']=str(self.annee)
        
    def classer_comptage_update_insert(self,bdd, table_cpt):
        """
        uniquement pour verif, car normalemnet df_attr=df_atr_update et df_attr_insertest vide
        """
        self.comptag_existant_bdd(bdd, table_cpt)
        self.df_attr_update=self.df_attr.loc[self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        self.df_attr_insert=self.df_attr.loc[~self.df_attr.id_comptag.isin(self.existant.id_comptag.tolist())].copy()
        
        
    def update_bdd_Vinci(self,bdd, schema, table):
        val_txt=self.creer_valeur_txt_update(self.df_attr, ['id_comptag','tmja','pc_pl', 'src'])
        self.update_bdd(bdd, schema, table, val_txt,{f'tmja_{str(self.annee)}':'tmja',f'pc_pl_{str(self.annee)}':'pc_pl', f'src_{str(self.annee)}':'src'})
        
class Comptage_alienor(Comptage):    
    def __init__(self,fichier_perm, annee):
        self.fichier_perm=fichier_perm
        self.annee=annee
    
    def ouvrir_et_separe_donnees(self):
        donnees=pd.read_excel(self.fichier_perm).rename(columns={'ID':'id_comptag'})
        donnees_agregees=donnees[[a for a in donnees.columns if a in ('id_comptag','TMJA_'+str(self.annee),'Pc_PL_'+str(self.annee))]].copy()
        donnees_agregees.rename(columns={a:a.lower() for a in donnees_agregees.columns}, inplace=True)
        donnees_agregees['tmja_'+str(self.annee)]=donnees_agregees['tmja_'+str(self.annee)].apply(lambda x : int(x))
        donnees_agregees['src']='tableur'
        tmjm_mens=donnees[[a for a in donnees.columns if a == 'id_comptag' or 'TMJM' in a]].copy()#or re.search('Pc_PL_[0-9]{4}_[0-9]{2}',a)
        tmjm_mens.rename(columns={c:k for c in tmjm_mens.columns if c!='id_comptag' for k, v in dico_mois.items()  if v[0]==int(c[-2:])}, inplace=True)
        tmjm_mens=tmjm_mens.applymap(lambda x : int(x) if isinstance(x, float) else  x)
        tmjm_mens['donnees_type']='tmja'
        pc_pl_mens=donnees[[a for a in donnees.columns if a == 'id_comptag' or re.search('Pc_PL_[0-9]{4}_[0-9]{2}',a)]].copy()
        pc_pl_mens.rename(columns={c:k for c in pc_pl_mens.columns if c!='id_comptag' for k, v in dico_mois.items()  if v[0]==int(c[-2:])}, inplace=True)
        pc_pl_mens['donnees_type']='pc_pl'
        donnees_mens=pd.concat([tmjm_mens,pc_pl_mens], axis=0, sort=False).sort_values('id_comptag')
        donnees_mens['annee']=str(self.annee)
        self.df_attr=donnees_agregees.copy()
        self.df_attr_mens=donnees_mens.copy()
    
    def update_bdd_Alienor(self,bdd, schema, table):
        val_txt=self.creer_valeur_txt_update(self.df_attr, ['id_comptag','tmja_'+str(self.annee),'pc_pl_'+str(self.annee), 'src'])
        self.update_bdd(bdd, schema, table, val_txt,{'tmja_'+str(self.annee):'tmja_'+str(self.annee),'pc_pl_'+str(self.annee):'pc_pl_'+str(self.annee), 
                                                     'src_'+str(self.annee):'src'})    

class Comptage_atlandes(Comptage):    
    def __init__(self,fichier_perm, annee):
        self.fichier_perm=fichier_perm
        self.annee=annee   
        
    def donnees_mens(self):
        donnees=pd.read_excel(self.fichier_perm)
        donnees=donnees.loc[(~donnees['TMJA 2 sens confondus'].isna()) & (~donnees['TMJM VL'].isna())].copy()
        val_l0=donnees.iloc[0].values
        col_tmjm_vl=donnees.columns[donnees.columns.tolist().index('TMJM VL'):donnees.columns.tolist().index('TMJM PL')]
        dico_nom_tmjm_vl={a:'tmjm_vl_'+b for a, b in zip(col_tmjm_vl,val_l0[donnees.columns.tolist().index('TMJM VL'):donnees.columns.tolist().index('TMJM PL')])}
        col_tmjm_pl=donnees.columns[donnees.columns.tolist().index('TMJM PL'):]
        dico_nom_tmjm_pl={a:'tmjm_pl_'+b for a, b in zip(col_tmjm_pl,val_l0[donnees.columns.tolist().index('TMJM VL'):donnees.columns.tolist().index('TMJM PL')])}
        donnees=donnees.rename(columns=dico_nom_tmjm_vl).rename(columns=dico_nom_tmjm_pl)
        donnees=donnees.iloc[1:].copy()
        donnees['id_comptag']=donnees.apply(lambda x : f"40-A63-{str(x['Echangeur de début de section Trafic'])}+{str(x['Unnamed: 2'])}"if x['Echangeur de début de section Trafic'] != 36 else
                                          f"33-A63-{str(x['Echangeur de début de section Trafic'])}+{str(x['Unnamed: 2'])}", axis=1)
        donnees_mens=donnees[[a for a in donnees.columns if a=='id_comptag' or 'tmjm' in a ]].copy()
        #calcul du tmjm_tv
        for m in dico_mois.values():
            donnees_mens[f'tmjm_tv_{m[1]}']=donnees_mens[f'tmjm_vl_{m[1]}']+donnees_mens[f'tmjm_pl_{m[1]}']
            donnees_mens[f'tmjm_pc_pl_{m[1]}']=donnees_mens.apply(lambda x : round(x[f'tmjm_pl_{m[1]}']/x[f'tmjm_tv_{m[1]}']*100,2), axis=1)
        tmjm_tv_mens=donnees_mens[[a for a in donnees_mens.columns if 'tv' in a or a=='id_comptag']].copy()
        tmjm_tv_mens.rename(columns={c:k for c in tmjm_tv_mens.columns if c!='id_comptag' for k, v in dico_mois.items()  if v[1]==c.split('_')[-1]}, inplace=True)
        tmjm_tv_mens['donnees_type']='tmja'
        tmjm_pl_mens=donnees_mens[[a for a in donnees_mens.columns if 'pc_pl_' in a or a=='id_comptag']].copy()
        tmjm_pl_mens.rename(columns={c:k for c in tmjm_pl_mens.columns if c!='id_comptag' for k, v in dico_mois.items()  if v[1]==c.split('_')[-1]}, inplace=True)
        tmjm_pl_mens['donnees_type']='pc_pl'
        donnees_mens_final=pd.concat([tmjm_tv_mens,tmjm_pl_mens], axis=0, sort=False).sort_values('id_comptag')
        donnees_mens_final['annee']=str(self.annee)
        self.df_attr_mens=donnees_mens_final.copy() 
        
class Comptage_GrandPoitiers(Comptage):
    def __init__(self,dossier, annee):
        self.dossier=dossier
        self.fichiers=O.ListerFichierDossier(dossier, 'xlsx')
        self.annee=annee
        
    def donnnees_agregees(self, fichier):
        donnees_agregees=pd.read_excel(os.path.join(self.dossier,fichier), sheet_name='Informations')  
        tmja=int(donnees_agregees.iloc[10,5])
        pc_pl=round(donnees_agregees.iloc[10,8]*100,2)
        pl=int(donnees_agregees.iloc[10,7])
        rue=donnees_agregees.iloc[1,1].split('(')[0].strip()
        return donnees_agregees, rue, tmja, pc_pl, pl
    
    def donnees_horaires(self, fichier,start=8, step=34):
        donnees_detaillees=pd.read_excel(os.path.join(self.dossier,fichier), sheet_name='Tableau', header=None)  
        donnees_detaillees=donnees_detaillees.loc[:,[1,26,27]].copy()
        donnees_detaillees.columns=['heure','tv','pl']
        for i,j in enumerate(range(start,len(donnees_detaillees),step)):
            date=donnees_detaillees.iloc[j,0]
            donnees=donnees_detaillees.iloc[j+2:j+26,1:3]
            date_index=pd.date_range(date, periods=24, freq='H')
            donnees.index=date_index
            donnees=donnees.reset_index().rename(columns={'index':'date'})
            donnees['jour']=donnees['date'].apply(lambda x : pd.to_datetime(x.strftime('%Y-%m-%d')))
            donnees['heure']=donnees['date'].apply(lambda x : f'h{x.hour}_{x.hour+1 if x.hour!=24 else 24}')
            donnees=pd.concat([donnees[['jour', 'heure', 'tv']].pivot(index='jour',columns='heure', values='tv'),
            donnees[['jour', 'heure', 'pl']].pivot(index='jour',columns='heure', values='pl')], axis=0, sort=False).reset_index()
            donnees['type_veh']=['TV','PL']
            if i==0 : 
                donnees_horaire=donnees.copy()
            else : 
                donnees_horaire=pd.concat([donnees_horaire,donnees], axis=0, sort=False)
            donnees_horaire.index.name='index' #juste pour lisibilite
            donnees_horaire.columns.name=None
        return donnees_horaire
    
    def cumul_sens(self, ref_pt, id_comptag):
        """
        faire la somme des deux sens quand on a une liste des deux fichiers de comptage
        in : 
            ref_pt : string des points relatif à un id_comptag, issu de la Bdd: comptag.gp_tmja_2015_p.id_comp
            id_comptag : id_comptag auquel associe les fichiers
        """
        liste_tmja=[]
        liste_pl=[]
        liste_donnees_horaire=[]
        if len(ref_pt)==1 :
            fichiers_sens=[f for f in self.fichiers if f'Poste {ref_pt[0]} - sens' in f] if ref_pt[0]!='20' else [f for f in self.fichiers if re.search('Poste 20[A,B] - sens',f)]
        elif len(ref_pt)==2 : 
            fichiers_sens=[f for f in self.fichiers if (f'Poste {ref_pt[0]} - sens' in f or f'Poste {ref_pt[1]} - sens' in f)]
        else  : 
            print(f'pb nb de fichier : {ref_pt}')
            raise Exception
        
        if not fichiers_sens : 
            raise self.PasDeFichierError(ref_pt)
        
        for f in fichiers_sens : 
            rue, tmja, pc_pl,pl=self.donnnees_agregees(f)[1:]
            liste_tmja.append(tmja)
            liste_pl.append(pl)
            liste_donnees_horaire.append(self.donnees_horaires(f))
        tmja=sum(liste_tmja) 
        pc_pl=round(100*sum(liste_pl)/tmja, 2)
        donnees_horaires_f=liste_donnees_horaire[0].set_index('jour').add(liste_donnees_horaire[1].set_index('jour')).reset_index() if len(fichiers_sens)>1 else liste_donnees_horaire[0]
        donnees_horaires_f.type_veh=donnees_horaires_f.type_veh.apply(lambda x : x[:2])
        donnees_horaires_f['id_comptag']=id_comptag
        return tmja,pc_pl, donnees_horaires_f,fichiers_sens
    
    def comptage_forme(self, bdd):
        with ct.ConnexionBdd(bdd) as c : 
            rqt="select * from comptage.gp_tmja_2015_p"
            points_existants=gp.read_postgis(rqt,c.sqlAlchemyConn)
        i=0
        dico_cpt={}
        for p in points_existants.point_comp.tolist():
            if isinstance(p, str) : #ne pas prendre en compte les points sans références
                ref_pt=p.split('_')
                id_comptag=points_existants.loc[points_existants['point_comp']==p].id_comptag.to_numpy()[0]
                try :
                    tmja,pc_pl, donnees_horaires_f,fichiers_sens=self.cumul_sens(ref_pt,id_comptag)
                except self.PasDeFichierError : 
                    continue
                dico_cpt[id_comptag]={'tmja':tmja, 'pc_pl':pc_pl, 'src':f"tableurs : {', '.join(fichiers_sens)}"}
                if i==0 :
                    donnees_horaires_tot=donnees_horaires_f.copy()
                else : donnees_horaires_tot=pd.concat([donnees_horaires_tot, donnees_horaires_f], axis=0, sort=False)
                i+=1
        self.df_attr=pd.DataFrame.from_dict(dico_cpt, orient='index').reset_index().rename(columns={'index':'id_comptag'})
        self.df_attr_horaire=donnees_horaires_tot.copy()
        
    def update_bdd_grdPoi(self,bdd, schema, table):
        val_txt=self.creer_valeur_txt_update(self.df_attr, ['id_comptag','tmja','pc_pl', 'src'])
        self.update_bdd(bdd, schema, table, val_txt,{f'tmja_{str(self.annee)}':'tmja',f'pc_pl_{str(self.annee)}':'pc_pl', f'src_{str(self.annee)}':'src'})
    
    class PasDeFichierError(Exception): 
        def __init__(self, ref_pt):
            Exception.__init__(self,f'pas de fihcier pour les postes de comptage {ref_pt} ')  
        

class Comptage_Niort(Comptage): 
    def __init__(self,dossier, annee) : 
        """
        debut en 2020, fichiers csv fourni par sens, les fichiers csv fournis ressemble aux fichiers mdb de la ville d'Anglet et Angouleme
        on part comme criter de VL ou PL un longueur à 7,2 m
        # voiture : <5,2m
        # camionnete : >=5.2 à <7.2
        # bus et PL : >=7.2 à <10.9
        # PL remorque : >=10.9 à <22
        """
        self.dossier=dossier
        self.annee=annee
        
    def creer_dico(self, dico_id_comptag):
        """
        creeru dico avec en cle les id_comptag et en value les fchiers concernes
        dico_id_comptag : dico de correspondance entre les dossier fournis par la ville de Niort et les id_comptag
        """
        dico_fichier={}
        for chemin, dossier, files in os.walk(self.dossier) : 
            for d in dossier : 
                dico_fichier[d]=[chemin+os.sep+d+os.sep+f for f in O.ListerFichierDossier(os.path.join(chemin,d),'csv')]
        #pour les dossier cotenat plus de 2 fichiers csv (i.e plus de 1 pt de comptage
        dico_fichier_separe={}
        for k, v in dico_fichier.items() : 
            if len(v)>2 : 
                dico_fichier_separe[k+'_1']=[a for a in v if '1.csv' in a or '2.csv' in a]
                dico_fichier_separe[k+'_2']=[a for a in v if '3.csv' in a or '4.csv' in a]
            else :
                dico_fichier_separe[k]=v
                
        dico_fichiers_final={}
        for k, v in dico_fichier_separe.items() : 
            for c,val in dico_id_comptag.items() : 
                if k==val :
                    dico_fichiers_final[c]=v
        
        return dico_fichiers_final
    
    def formater_fichier_csv(self, fichier):
        """
        à partir des fichiers csv, obtenir une df avec modification de la date si le fichier comporte uniquement 8 jours, i.e : 6 jours plein et 2 demi journée
        """    
        fichier=pd.read_csv(fichier)
        fichier.DateTime=pd.to_datetime(fichier.DateTime)
        nb_jours_mesure=len(fichier.set_index('DateTime').resample('D'))
        jourMin=fichier.DateTime.apply(lambda x : x.dayofyear).min()
        jourMax=fichier.DateTime.apply(lambda x : x.dayofyear).max()
        if nb_jours_mesure==8 : #si le nombre total de jours est inférieur à 9, il faudra regrouper les jours de début et de fin pour avoir une journée complete, ou alors c'est qu'une erreur a ete commise lors de la mesure de trafic (duree < 7 jours)
            fichier.loc[fichier.DateTime.apply(lambda x : x.dayofyear==jourMin), 'DateTime']=fichier.loc[fichier.DateTime.apply(lambda x : x.dayofyear==jourMin)].DateTime.apply(lambda x : x+pd.Timedelta('7D'))
        elif nb_jours_mesure>8 : 
            fichier=fichier.loc[fichier.DateTime.apply(lambda x : (x.dayofyear!=jourMin) & (x.dayofyear!=jourMax))].copy()
        else : 
            raise PasAssezMesureError(nb_jours_mesure)
        return fichier
             
    def csv_identifier_type_veh(self,df_brute):
        """
        separer les vl des pl à partir de la df de données brutes creer avec formater_fichier_csv
        """        
        vl=df_brute.loc[(df_brute['Length']<7.2) & (df_brute['AdviceCode']!=128)].copy()
        pl=df_brute.loc[(df_brute['Length']>=7.2) & (df_brute['Length']<22) & (df_brute['AdviceCode']!=128) ].copy()
        return vl,pl
    
    def csv_agrege_donnees_brutes(self,vl,pl):
        """
        prendre les donnees issues de identifier_type_veh et les regrouper par heure et jour
        """
        vl_agrege_h=vl.set_index('DateTime').resample('H').AdviceCode.count().reset_index().rename(columns={'AdviceCode':'nb_veh'})
        vl_agrege_j=vl.set_index('DateTime').resample('D').AdviceCode.count().reset_index().rename(columns={'AdviceCode':'nb_veh'})
        pl_agrege_h=pl.set_index('DateTime').resample('H').AdviceCode.count().reset_index().rename(columns={'AdviceCode':'nb_veh'})
        pl_agrege_j=pl.set_index('DateTime').resample('D').AdviceCode.count().reset_index().rename(columns={'AdviceCode':'nb_veh'})
        return vl_agrege_h,vl_agrege_j, pl_agrege_h, pl_agrege_j
    
    def csv_indicateurs_agrege(self,vl_agrege_j,pl_agrege_j ):
        """
        calcul des indicateurs tmja et nb_pl
        """
        nb_vl=round(vl_agrege_j.mean().values[0])
        nb_pl=round(pl_agrege_j.mean().values[0])
        tmja=nb_vl+nb_pl
        pc_pl=round(nb_pl*100/tmja,2)
        return nb_vl, nb_pl, tmja, pc_pl
    
    def csv_donnees_horaires(self,vl_agrege_h,pl_agrege_h):
        """
        renvoyer la df des donnees horaires en tv et pl
        """
        vl_agrege_h['jour']=vl_agrege_h['DateTime'].apply(lambda x : pd.to_datetime(x.strftime('%Y-%m-%d')))
        vl_agrege_h['heure']=vl_agrege_h['DateTime'].apply(lambda x : f'h{x.hour}_{x.hour+1 if x.hour!=24 else 24}')
        pl_agrege_h['jour']=pl_agrege_h['DateTime'].apply(lambda x : pd.to_datetime(x.strftime('%Y-%m-%d')))
        pl_agrege_h['heure']=pl_agrege_h['DateTime'].apply(lambda x : f'h{x.hour}_{x.hour+1 if x.hour!=24 else 24}')
        
        pl_agrege_h=pl_agrege_h[['jour', 'heure', 'nb_veh']].pivot(index='jour',columns='heure', values='nb_veh')
        calcul_agreg_h=pd.concat([vl_agrege_h[['jour', 'heure', 'nb_veh']].pivot(index='jour',columns='heure', values='nb_veh'),
                  pl_agrege_h], axis=0, sort=False)
        tv_agrege_h=calcul_agreg_h.groupby('jour').sum()
        tv_agrege_h['type_veh']='TV'
        pl_agrege_h['type_veh']='PL'
        donnees_horaires=pd.concat([tv_agrege_h,pl_agrege_h], axis=0, sort=False).fillna(0).reset_index()
        return donnees_horaires
    
    def traiter_csv_sens_unique(self, fichier):
        """
        concatenation des ofnctions permettant d'aboutir aux donnees agrege et horaire d'un fichier csv de comptage pour un sens
        """
        f=self.formater_fichier_csv(fichier)
        vl,pl=self.csv_identifier_type_veh(f)
        vl_agrege_h,vl_agrege_j, pl_agrege_h, pl_agrege_j=self.csv_agrege_donnees_brutes(vl,pl)
        nb_vl, nb_pl, tmja, pc_pl=self.csv_indicateurs_agrege(vl_agrege_j,pl_agrege_j)
        donnees_horaires=self.csv_donnees_horaires(vl_agrege_h,pl_agrege_h)
        return nb_vl, nb_pl, tmja, pc_pl, donnees_horaires
    
    def horaire_2_sens(self, id_comptag,df_sens1, df_sens2):
        """
        creer une df des donnes horaires pour les 2 sens d'un id_comptage
        """
        df_finale=pd.concat([df_sens1, df_sens2], axis=0, sort=False)
        df_finale=df_finale.groupby(['jour','type_veh']).sum().reset_index()
        df_finale['id_comptag']=id_comptag
        return df_finale
    
    def horaire_tout_cpt(self,dico_fichiers_final):
        """
        creation de la df horaire pour tout les cpt
        in :
            dico_fichiers_final : dico d'association de l'id_comptag et de sfichiers creer par creer_dico
        """
        self.df_attr_horaire=pd.concat([self.horaire_2_sens(k,*[self.traiter_csv_sens_unique(f)[4] for f in v]) for k, v in dico_fichiers_final.items()], axis=0, sort=False)
    

    def indic_agreg_2sens_1_cpt(self,id_comptag, fichiers):
        list_resultat=[self.traiter_csv_sens_unique(f)[1:3] for f in fichiers]
        tmja, nb_pl=[list_resultat[0][1],list_resultat[1][1]], [list_resultat[0][0],list_resultat[1][0]]
        tmja=sum(tmja)
        pc_pl=sum(nb_pl)/tmja*100
        src=list(set([os.path.split(f)[0] for f in fichiers]))[0]
        return pd.DataFrame.from_dict({'id_comptag':[id_comptag,],'tmja':[tmja,],'pc_pl':pc_pl, 'src':src})
    
    
    def agrege_tout_cpt(self,dico_fichiers_final ):
        """
        creation de la df des donnees agregees pour tout les indicatuers
        """
        @concat_df
        def indic_agreg_2sens(id_comptag, fichiers):
            list_resultat=[self.traiter_csv_sens_unique(f)[1:3] for f in fichiers]
            tmja, nb_pl=[list_resultat[0][1],list_resultat[1][1]], [list_resultat[0][0],list_resultat[1][0]]
            tmja=sum(tmja)
            pc_pl=sum(nb_pl)/tmja*100
            src=list(set([os.path.split(f)[0] for f in fichiers]))[0]
            return pd.DataFrame.from_dict({'id_comptag':[id_comptag,],'tmja':[tmja,],'pc_pl':pc_pl, 'src':src})
        
        self.df_attr=indic_agreg_2sens(list(dico_fichiers_final.items()))
        
class Comptage_GrandDax(Comptage): 
    def __init__(self, fichiers_pt_comptages):
        """
        pour le moment je me base sur un fichier de geolocalisation creer à la main qui reprend aussi les principales info 
        pour creer l'di_comptag et les noms de fichiers a utiliser pour creer les donnees
        """
        self.fichiers_pt_comptages=gp.read_file(fichiers_pt_comptages, encoding='UTF-8')
        self.MaJ_fichiers_pt_comptages()
    
    def MaJ_fichiers_pt_comptages(self):
        """
        ajouter les attributs necessaires au fichier de pt de comptage
        """
        self.fichiers_pt_comptages['x_wgs84']=self.fichiers_pt_comptages.geometry.to_crs({'proj':'longlat', 'ellps':'WGS84', 'datum':'WGS84'}).apply(lambda x: round(x.x,4))
        self.fichiers_pt_comptages['y_wgs84']=self.fichiers_pt_comptages.geometry.to_crs({'proj':'longlat', 'ellps':'WGS84', 'datum':'WGS84'}).apply(lambda x: round(x.y,4))
        self.fichiers_pt_comptages['id_comptag']=self.fichiers_pt_comptages.apply(lambda x : f'GrdDax-{x.nom_rte}-{str(x.x_wgs84)};{str(x.y_wgs84)}', axis=1)
        self.fichiers_pt_comptages['dep']='40'
        self.fichiers_pt_comptages['reseau']='VC'
        self.fichiers_pt_comptages['gestionnai']='GrdDax'
        self.fichiers_pt_comptages['concession']='N'
        self.fichiers_pt_comptages['type_poste']='ponctuel'
        self.fichiers_pt_comptages['src_geo']='plan pdf'
        self.fichiers_pt_comptages['x_l93']=self.fichiers_pt_comptages.geometry.apply(lambda x : round(x.x,3))
        self.fichiers_pt_comptages['y_l93']=self.fichiers_pt_comptages.geometry.apply(lambda x : round(x.y,3))
     
    def ouvrir_feuille_infos(self, dossier, fichier):
        donnees_generale=pd.read_excel(os.path.join(dossier, fichier), sheet_name='Informations')
        donnees_gal_sansNa=donnees_generale.dropna(how='all').dropna(axis=1,how='all')
        return donnees_generale,donnees_gal_sansNa 
    
    def recup_infos(self,donnees_gal_sansNa, lgn_date_deb, lgn_date_fin):
        #dates et duree
        def dates_cpt(ligne, donnees_gal_sansNa):
            txt=donnees_gal_sansNa.loc[ligne,'Unnamed: 7'].split()
            for k, v in dico_mois.items():
                if txt[2] in [a.lower() for a in v if isinstance(a, str)] : 
                    mois=str(v[0])
            return pd.to_datetime('-'.join([txt[1],mois,txt[3]]), dayfirst=True)
    
        date_debut=dates_cpt(lgn_date_deb,donnees_gal_sansNa)
        date_fin=dates_cpt(lgn_date_fin,donnees_gal_sansNa)
        duree_cpt=int(donnees_gal_sansNa.loc[15,'Unnamed: 29'])
        annee=str(date_debut.year)
        return date_debut, date_fin, duree_cpt, annee
    
    def recup_donnees_agregees(self,donnees_gal_sansNa):
        #donnees_trafic
        tmja_vl=round(donnees_gal_sansNa.loc[19,'Unnamed: 12'],0)
        tmjo_vl=round(donnees_gal_sansNa.loc[20,'Unnamed: 12'],0)
        tmja_pl=round(donnees_gal_sansNa.loc[19,'Unnamed: 23'],0)
        tmjo_pl=round(donnees_gal_sansNa.loc[20,'Unnamed: 23'],0)
        pc_pl_ja=round((donnees_gal_sansNa.loc[19,'Unnamed: 32'])*100,1)
        pc_pl_jo=round((donnees_gal_sansNa.loc[20,'Unnamed: 32'])*100,1)
        tmja=tmja_vl+tmja_pl
        tmjo=tmjo_vl+tmjo_pl
        return tmja_vl,tmjo_vl, pc_pl_ja, tmjo_pl, tmjo, pc_pl_jo, tmja, tmja_pl
        
    def creer_df_agrege(self):
        """
        creer la df de tout les points de comptage avec indicateur agreges
        """
        #importer fichiers georeferencedes pt de comptag contenat id_comptag et reference vers le dossier et ou fichier      
        dico={}
        dico['id_comptag']=[]
        dico['obs_geo']=[]
        for a in range(2011,2021) : 
            dico[f'tmja_{str(a)}']=[]
            dico[f'pc_pl_{str(a)}']=[]
            dico[f'src_{str(a)}']=[]
            
        for dossier, fichiers, id_comptag in zip(self.fichiers_pt_comptages.dossier.tolist(),self.fichiers_pt_comptages.nom_fich.tolist(),
                                                 self.fichiers_pt_comptages.id_comptag.tolist()) : 
            dico['id_comptag'].append(id_comptag)
            dico['obs_geo'].append(dossier)
            if fichiers==None : #si pas de fichiers renseigne normelent seulement 2 IFX dans le dossier
                fichiers=O.ListerFichierDossier(dossier,extension='.IFX')
            else : 
                fichiers=fichiers.split(';')
    
            for f in fichiers : # si j'ai oublie l'extension 
                if not f.endswith('.IFX') : 
                    fichiers[fichiers.index(f)]+='.IFX'
    
            # recup des infos de dates
            donnees_gal_sansNa=self.ouvrir_feuille_infos(dossier, fichiers[0])[1]
            duree_cpt, annee=self.recup_infos(donnees_gal_sansNa, 10, 11)[2:]
    
    
            donnees_agregees=[self.recup_donnees_agregees(self.ouvrir_feuille_infos(dossier, f)[1])[6:] for f in fichiers]
            tmja=sum(i[0] for i in donnees_agregees)
            pc_pl=round(sum(i[1] for i in donnees_agregees)/tmja*100,1)
    
            for a in range(2011,2021) : 
                if int(annee)==a : 
                    dico[f'tmja_{a}'].append(tmja)
                    dico[f'pc_pl_{a}'].append(pc_pl)
                    dico[f'src_{a}'].append(';'.join([os.path.join(dossier, f) for f in fichiers]))
                else : 
                    dico[f'tmja_{a}'].append(np.NaN)
                    dico[f'pc_pl_{a}'].append(np.NaN)
                    dico[f'src_{a}'].append(np.NaN)
            
        self.df_attr=self.fichiers_pt_comptages[['id_comptag','geometry','dep','reseau','gestionnai','concession','type_poste','src_geo','x_l93','y_l93']].merge(pd.DataFrame.from_dict(dico), on='id_comptag')
        
    def creer_donnees_source(self, dossier, fichier,date_debut,duree_cpt):
        return [(pd.read_excel(os.path.join(dossier, fichier), sheet_name=f'Tableau {i+1}'),date_debut+pd.Timedelta(f'{i}D')) for i in range(duree_cpt)]
    
    def creer_df_horaire_brutes(self,donnees_horaires_brutes, date) : #pour 1 journee
        donnees=([donnees_horaires_brutes,'VL','Unnamed: 29'], [donnees_horaires_brutes,'PL','Unnamed: 30'])
        
        def df_horaire_brute(donnees_horaires_brutes,type_veh,attr) : #pourcahque type de veh
            test=donnees_horaires_brutes.loc[15:38,['TABLEAU HORAIRE DE SYNTHESE DES RESULTATS JOURNALIERS',attr]].copy()
            test['type_veh']=type_veh
            test.rename(columns={'TABLEAU HORAIRE DE SYNTHESE DES RESULTATS JOURNALIERS':'heure',attr:'nb_veh'}, inplace=True)
            return test
        
    
        test=pd.concat([df_horaire_brute(*d) for d in donnees], axis=0, sort=False).reset_index(drop=True)
        df_horaire=pd.concat([test.loc[test.type_veh==v[1]].pivot(index='type_veh',columns='heure', values='nb_veh') for v in donnees], axis=0, sort=False)
        df_horaire.loc['TV',:]=df_horaire.sum(axis=0)
        df_horaire.reset_index(inplace=True)
        df_horaire.drop(0, inplace=True)
        df_horaire['jour']=date
        return df_horaire
    
    def creer_df_horaire_tt_jour(self, donnees_tout_jour):
        return pd.concat([self.creer_df_horaire_brutes(*d) for d in donnees_tout_jour], axis=0, sort=False)
    
    #pour 2 fihciers (2sens)
    def creer_df_horaire_2_sens(self, dossier,liste_fichiers, id_comptag):
        if liste_fichiers==None : #si pas de fichiers renseigne normelent seulement 2 IFX dans le dossier
            liste_fichiers=O.ListerFichierDossier(dossier,extension='.IFX')
        else : 
            liste_fichiers=liste_fichiers.split(';')
        for f in liste_fichiers : # si j'ai oublie l'extension 
            if not f.endswith('.IFX') : 
                liste_fichiers[liste_fichiers.index(f)]+='.IFX'     
        donnees_gal_sansNa=self.ouvrir_feuille_infos(dossier, liste_fichiers[0])[1]
        date_debut, date_fin, duree_cpt, annee=self.recup_infos(donnees_gal_sansNa, 10, 11)
        
        donnees_tout_jour=(self.creer_donnees_source(dossier, f,date_debut,duree_cpt) for f in liste_fichiers)
        df_2_sens = pd.concat([self.creer_df_horaire_tt_jour(d) for d in donnees_tout_jour], axis=0, sort=False)
        df_2_sens=df_2_sens.groupby(['jour', 'type_veh']).sum().reset_index()
        df_2_sens['id_comptag']=id_comptag
        df_2_sens.rename(columns={k:f"h{k.replace('H','').replace('-','_')}" for k in df_2_sens.columns if k[-1]=='H'},inplace=True )
        return df_2_sens
    
    def df_horaire_final(self):
        self.df_attr_horaire=pd.concat([self.creer_df_horaire_2_sens(dossier,fichiers, id_comptag) 
                     for dossier, fichiers, id_comptag in zip(self.fichiers_pt_comptages.dossier.tolist(),self.fichiers_pt_comptages.nom_fich.tolist(),
                                                              self.fichiers_pt_comptages.id_comptag.tolist())],
                     axis=0, sort=False)
        
class PasAssezMesureError(Exception):
    """
    Exception levee si le fichier comport emoins de 7 jours
    """     
    def __init__(self, nbjours):
        Exception.__init__(self,f'le fichier comporte moins de 7 jours de mesures. Nb_jours: : {nbjours} ')   
        
