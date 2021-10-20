# -*- coding: utf-8 -*-
'''
Created on 12 oct. 2021

@author: martin.schoreisz
'''
import unittest
from Donnees_sources import MHCorbin, NettoyageTemps, PasAssezMesureError, GroupeCompletude
import datetime as dt
import pandas as pd
import Outils as O

fichierMHCorbinTest=r'C:\Users\martin.schoreisz\git\otv\otv\Transfert_Donnees\data\MHCorbin\rue de souche du 31 01 au 07 02 2020.mdb'
fichierMHCorbinAmpute=r'C:\Users\martin.schoreisz\git\otv\otv\Transfert_Donnees\data\MHCorbin\test_Tables_Manquantes.mdb'

class testFonctionGenerales(unittest.TestCase):
    
    def test_NettoyageTemps_NbJourInf8(self):
        self.assertRaises(PasAssezMesureError,NettoyageTemps,pd.DataFrame(O.random_dates('2019-05-27 0:0:0', '2019-05-30 0:0:0', 30), columns=['date_heure']))
        
    def test_NettoyageTemps_NbJourEgal8(self): 
        self.assertRaises(PasAssezMesureError,NettoyageTemps,pd.DataFrame(O.random_dates('2019-05-27 13:0:0', '2019-06-03 11:0:0', 30), columns=['date_heure']))  
        
    def test_NettoyageTemps_modifie1Jour(self):  
        testDf=pd.DataFrame(O.random_dates('2019-05-27 01:0:0', '2019-06-03 23:0:0', 200), columns=['date_heure']).sort_values('date_heure') 
        self.assertTrue(NettoyageTemps(testDf).date_heure.dt.date.max()==dt.date(2019, 6, 2))
        
    def test_NettoyageTemps_SuprJourExtrem(self):
        testDf=pd.DataFrame(O.random_dates('2019-05-27', '2019-06-06 23:0:0', 400), columns=['date_heure']).sort_values('date_heure')
        self.assertTrue(NettoyageTemps(testDf).date_heure.dt.date.min()==dt.date(2019, 5, 28) and NettoyageTemps(testDf).date_heure.dt.date.max()==dt.date(2019, 6, 5))
        
    def test_TestGroupeComletude_NbJours(self):
        """
        verifier que le niombr ede ligne renvoye par la fonction correspond bien au nombre de jopurs total de la donnees de depart
        """
        testDf=pd.DataFrame({'date_heure':pd.date_range('2019-05-27','2019-06-04', periods=200), 
                     'nbVeh':1,
                     'type_veh':['vl','pl']*100,
                     'sens':['sens1','sens2']*100}).sort_values('date_heure') 
        TestGroupeComletude=GroupeCompletude(NettoyageTemps(testDf))
        self.assertTrue(len(TestGroupeComletude)/24/4/2==TestGroupeComletude.date_heure.dt.dayofyear.nunique() 
                        if TestGroupeComletude.date_heure.dt.dayofyear.nunique()>8 else len(TestGroupeComletude)/24/4/2==7)
        
class TestMhCorbin(unittest.TestCase):
    
    def setUp(self):
        self.mhc=MHCorbin(fichierMHCorbinTest) 

    def testRessource(self):
        lg=len(self.mhc.dfRessourceCorrespondance()[0])
        self.assertTrue(lg==12483)
        
    def test_nbSens_egal2(self):
        self.assertTrue(self.mhc.nbSens==2)
        
    def test_verifTables_2sens(self):
        self.assertRaises(ValueError,MHCorbin,fichierMHCorbinAmpute )
        
    def test_qualificationQualite(self):
        """
        verifier que l'inidcateur qualite est bien celui attendu
        """
        self.assertTrue(self.mhc.indicQualite==3)
        
    def test_calculLongueurVitesse(self):
        """
        verifier que toute les lignes qui ont une erreur en vitesse ou longueur ou value0 ont bien les vitesses ou longueurs en NaN
        """
        self.assertTrue(self.mhc.dfAgreg2Sens.loc[self.mhc.dfAgreg2Sens.fail_type.apply(lambda x : any([e in x for e in ('longueur', 'vitesse', 'value0')]) 
                                                                  if not pd.isnull(x) else False)].Length_calc.isna().all() and 
                        self.mhc.dfAgreg2Sens.loc[self.mhc.dfAgreg2Sens.fail_type.apply(lambda x : any([e in x for e in ('longueur', 'vitesse', 'value0')]) 
                                                                  if not pd.isnull(x) else False)].Speed_calc.isna().all(), 'Length_calc ou Speed_calc non null avec fail_type en erreur')
    
    def test_formaterDonneesIndiv(self):
        """
        verifier qu'avec le fichier de test on obtient bien la dF attaendue (issue des ressources)
        """
        ref=self.mhc.dfRessourceTests()
        comp=self.mhc.formaterDonneesIndiv(self.mhc.dfAgreg2Sens)
        ref=ref[[c for c in ref.columns if c not in ('Length_calc', 'Speed_calc','fail_type' )]]
        comp=comp[[c for c in ref.columns if c not in ('Length_calc', 'Speed_calc','fail_type' )]]
        self.assertTrue((ref==comp).all().all())

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()