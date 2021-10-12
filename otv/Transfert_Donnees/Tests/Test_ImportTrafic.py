# -*- coding: utf-8 -*-
'''
Created on 17 sept. 2021

@author: martin.schoreisz
'''
import unittest
import pandas as pd
import numpy as np
import geopandas as gp
from Tests_Connexion_Transfert import TestConnexionBdd
from Import_trafics import Comptage, Comptage_cd40, Comptage_Dira, Comptage_Dirco, dico_mois

tableauCptAnnuelDira=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRA\0_tmja_dira_par_section_20210101.ods'
tableaucptCartoDira=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRA\dira_tmja_2020.ods'
dossierCptHoraireDira=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRA\GrosFichiers - JP CASSOU\0_Annee_Complete_2020'

fichierMjaDirco=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRCO\TMJA DIRCO 2020 NA.ods'
fichierMjMDirco=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRCO\TMJM DIRCO 2020 NA.ods'
dossierHoraireDirco=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRCO\Re_ Observatoire des trafics routiers DREAL NA 2020'

annee='2020'
bdd='local_otv_boulot'

class TestComptage(unittest.TestCase):
    """
    test des methodes communes a tous les comptages
    """
    
    def setUp(self):
        self.cpt=Comptage('fichier')
        
    def test_renommerMois(self):
        """
        vérifier qu'une df avec des noms de mois complet ressort bien les noms de moins selon le dico_mois
        """
        dfJson=pd.read_json('{"Janvier":{"93":8682,"95":902},"F\\u00e9vrier":{"93":9067,"95":922},"Mars":{"93":5418,"95":614},"Avril":{"93":2830,"95":329},"Mai":{"93":5839,"95":773},"Juin":{"93":8684,"95":1035},"Juillet":{"93":8997,"95":1174},"Ao\\u00fbt":{"93":8813,"95":1148},"Septembre":{"93":9202,"95":1057},"Octobre":{"93":8842,"95":1008},"Novembre":{"93":6196,"95":578},"D\\u00e9cembre":{"93":8009,"95":762}}')
        self.assertTrue(all([c in dico_mois.keys() for c in self.cpt.renommerMois(dfJson)]))
        
    def test_comptag_existant_bdd_typeTableGeom(self):
        """
        vérifier que la fonction comptag_existant_bdd renvoi bien une gdf si 'compteur'
        """
        self.cpt.comptag_existant_bdd()
        self.assertIsInstance(self.cpt.existant, gp.GeoDataFrame, 'la donnees renvoyeee par la table compteur n\'est pas une gdf')
    
    def test_comptag_existant_bdd_typeTableNonGeom(self):
        """
        vérifier que la fonction comptag_existant_bdd renvoi bien une gdf si 'compteur'
        """
        self.cpt.comptag_existant_bdd('comptage')
        self.assertIsInstance(self.cpt.existant, pd.DataFrame, 'la donnees renvoyeee par la table compteur n\'est pas une gdf')    
    
    def testCreerComptageTypeVeh(self):
        """
        verifier que le type_veh renvoi erreur si fausse valeur
        """
        self.assertRaises(ValueError,self.cpt.creer_comptage,'cpt', annee, 'src', 'obs', 'toto')
        
    def testCreerComptageAnneeFuture(self):
        """
        verifier que le type_veh renvoi erreur si fausse valeur
        """
        self.assertRaises(ValueError,self.cpt.creer_comptage,'cpt', '2050', 'src', 'obs', 'toto')
        
    def testCreerComptageAnneePassee(self):
        """
        verifier que le type_veh renvoi erreur si fausse valeur
        """
        self.assertRaises(ValueError,self.cpt.creer_comptage,'cpt', '1984', 'src', 'obs', 'toto')
    
    def testCreerComptagePeriodeFausse(self):
        """
        verifier que le type_veh renvoi erreur si fausse valeur
        """
        for p in ('2020/50/03-2020/09/10', '1980/09/20-1980-09-27','2020/01/00-2020/09/10', '2020/01/01-2020/00/10',
                  '2020/01/01-2020/13/10', '2020/01/41-2020/09/10', '2020/01/01-2020/13/10','2020/01/33-2020/13/10') :
            with self.subTest(p=p):
                self.assertRaises(ValueError,self.cpt.creer_comptage,'cpt', annee, 'src', 'obs', 'tv/pl', p)
                
    def testSructureBddOld2NewFormAttrObligatoires(self):
        """
        vérifier que les erreurs sont bien levées si les conditions ne sont pas rempli sur les attributs obligatoires
        """
        self.assertRaises(AttributeError,self.cpt.structureBddOld2NewForm,'df', annee, ['listAttrFixe, id_comptag'],['tmja'], 'agrege')
        
    def testSructureBddOld2NewFormResultNonVide(self):
        """
        vérifier que les erreurs sont bien levées si les conditions ne sont pas rempli sur les attributs obligatoires
        """
        dfTest=pd.DataFrame({'id_comptag':['XXX-DXXXX-XX+XXX',], 'annee':annee, 'tmja':10000})
        self.assertRaises(ValueError,self.cpt.structureBddOld2NewForm,dfTest, annee, ['annee', 'id_comptag'],['tmja'], 'agrege')
        
    def testStructureBddOld2NewFormTypeIndic(self):
        self.assertRaises(ValueError,self.cpt.structureBddOld2NewForm,'df', annee, ['annee', 'id_comptag'],['tmja'], 'toto')
        
    def test_scinderComptagExistant_idComptagFail(self):
        """
        verifier que si la df passée ne contient pas de colonne id_comptag alors erreur
        """
        self.assertRaises(AttributeError, self.cpt.scinderComptagExistant,pd.DataFrame({'nom':['JP', 'Toto']}), annee )
        
    def test_scinderComptagExistant_ScinderOk(self):
        """
        verifier que si la df passée ne contient pas de colonne id_comptag alors erreur
        """
        dfATester=pd.DataFrame({'id_comptag':['33-A65-0+0', 'Toto']})
        dfIdsConnus, dfIdsInconnus=self.cpt.scinderComptagExistant(dfATester, annee)
        self.assertTrue(len(dfIdsConnus)+len(dfIdsInconnus)==len(dfATester))
        

class TestComptageCd40(unittest.TestCase):
    
    def testConnexionBddreuse(self):
        """
        reprend les test du module de connexion
        """
        TestConnexionBdd()

    def testComptageCd40DonneesType(self):
        """
        verifie que le type de donnees est bien dans ceux attendu
        """
        self.assertRaises(ValueError,Comptage_cd40,'chemin',annee, 'toto')

class TestComptageDira(unittest.TestCase):
    
    def setUp(self):
        self.dira=Comptage_Dira(tableauCptAnnuelDira,dossierCptHoraireDira,tableaucptCartoDira,annee,bdd, 'compteur')
    
    def test_cptCartoVerif_Unequal(self):
        """
        verifier que si une df sans correspondance est passé on a bien une erreur
        """
        for df in (pd.DataFrame({f'carto_{self.dira.annee}':['t', 'f'], 'src':[np.nan, 'carto_2020'], 'diffusable':['f', 't']}),
                   pd.DataFrame({f'carto_{self.dira.annee}':['t', 'f'], 'src':['carto_2020',np.nan ], 'diffusable':['f', 't']})):
              
                self.assertRaises(ValueError,self.dira.cptCartoVerif,df)
                
    def test_cptCartoVerif_Equal(self):
        """
        verifier que si une df avec correspondance est passé on a bien une df non vide
        """
        df=pd.DataFrame({f'carto_{self.dira.annee}':['t', 'f'], 'src':['carto_2020',np.nan ], 'diffusable':['t', 'f']})
        self.assertFalse(self.dira.cptCartoVerif(df).empty)
        
    def test_cptCartoForme_tmjaNullObsDND(self):
        """
        verfier que une erreur est levee si les ligne ayant obs a 'Donnees Non Disponibles' n'ont pas de tmja, et inversement
        """
        tableurCarto=pd.DataFrame({f'carto_{self.dira.annee}':['t', 't', 't', 't'], 
                                 'src':['carto_2020', 'carto_2020','carto_2020','carto_2020'  ], 
                                 'diffusable':['t', 't','t', 't'], 
                                 'tmja':[1000, np.nan, np.nan, 1000], 
                                'obs':['Donnees Non Disponibles', None,'Donnees Non Disponibles', 'Donnees Non Disponibles'], 
                                 'id_comptag':['c1', 'c1', 'c2', 'c2'], 'pc_pl':[10,20, 10, 20]})
        self.assertRaises(ValueError,self.dira.cptCartoForme,tableurCarto)
        
    def tes_cptCartoForme_corrDND(self):
        """
        verufier que les DND sont bein en nan en sortie
        """
        tableurCarto=pd.DataFrame({f'carto_{self.dira.annee}':['t', 't', 't', 't'], 
                                 'src':['carto_2020', 'carto_2020','carto_2020','carto_2020'  ], 
                                 'diffusable':['t', 't','t', 't'], 
                                 'tmja':[1000, 2000, 1000, np.nan], 
                                'obs':[None, None,None, 'Donnees Non Disponibles'], 
                                 'id_comptag':['c1', 'c1', 'c2', 'c2'], 'pc_pl':[10,20, 10, 20]})
        for i in ('tmja', 'pc_pl') : 
            with self.subTest(i=i) :
                dfTest=self.dira.cptCartoForme(tableurCarto)
                self.assertTrue(dfTest.loc[dfTest.obs.apply(lambda x : 'Donnees Non Disponibles' in x if not pd.isnull(x) else False)][i].all())

class TestComptageDirco(unittest.TestCase):   
    
    def setUp(self):
        self.dirco=Comptage_Dirco(fichierMjaDirco, fichierMjMDirco, dossierHoraireDirco, annee)
        
    def test_miseEnFormeMJA_NomAttr(self):
        """
        test que les attributs souhaites soient bien presents dans le jeu de donnees
        """
        self.assertTrue(all([e in self.dirco.miseEnFormeMJA().columns for e in ['id_comptag', 'tmja', 'pc_pl', 'obs_supl', 'fichier']])) 
        
    def test_miseEnFormeFichierTmjaPourHoraire_id_comptagVide(self):
        """
        tester qu'avec un ficier maitrise on a bien tous les id_comptag et idfihcier
        """
        dfFichierTmja=self.dirco.miseEnFormeFichierTmjaPourHoraire()
        self.assertTrue(dfFichierTmja.loc[dfFichierTmja.id_comptag.isna()].empty)
        
            
    

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()