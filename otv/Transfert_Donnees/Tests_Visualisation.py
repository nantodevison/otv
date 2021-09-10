# -*- coding: utf-8 -*-
'''
Created on 1 sept. 2021

@author: martin.schoreisz
module de test des fonctions et classes de l'OTV
'''
import unittest
import pandas as pd
import Connexion_Transfert as ct
from Tests_Connexion_Transfert import TestConnexionBdd
from Visualisation import IdComptage, IdComptagInconnuError


class TestIdComptage(unittest.TestCase):
    
    def TestConnexionBddreuse(self):
        """
        reprend les test du module de connexion
        """
        TestConnexionBdd()
    
    def testIdComptageNonNull(self):
        """
        verifie que si le comptage n'existe pas alors erreur
        """
        self.assertRaises(IdComptagInconnuError,IdComptage,'toto')
        
    def testIdComptagListIndicValues(self):
        """
        verifie que les valuers de la liste de valeurs sont bien conformes 
        """
        with ct.ConnexionBdd('local_otv_boulot') as c : 
            listValeur=pd.read_sql('select code from comptage_new.enum_indicateur', c.sqlAlchemyConn).code.to_list()
        self.idComptage=IdComptage('17-D939-47+803')
        self.assertTrue(all([e in listValeur for e in self.idComptage.listIndic]))
        
    def testGraphTraficTmjaSeulemnt(self):
        """
        verfie qu'un comptage sans pcpl ne contient bien que des donnees tmja
        """
        self.idComptage=IdComptage('86-D97-18+0')
        self.idComptage.graphTrafic()
        self.assertTrue(('tmja',)==self.idComptage.listIndic)
    
    def testGraphTraficTmjaPcpl(self):
        """
        verifie qu'un comptage qui continent tmja et pc_pl renvoi bien les deux dans ses donnees sources
        """
        self.idComptage=IdComptage('16-D1-17+0')    
        self.assertTrue(('pc_pl', 'tmja')==self.idComptage.listIndic)

    def testGraphTraficAnneeMaxExclueDown(self):
        """
        verifie que si l'anneeMax est inferieure a la derniere date connu on a une ValueErreur
        """
        self.idComptage=IdComptage('86-D97-18+0')
        self.assertRaises(ValueError,self.idComptage.graphTrafic,1000)
        
    def testGraphTraficAnneeMaxExclueUp(self):
        """
        verifie que si l'anneeMax est inferieure a la derniere date connu on a une ValueErreur
        """
        self.idComptage=IdComptage('86-D97-18+0')
        self.assertRaises(UserWarning,self.idComptage.graphTrafic,3000)
        
    def testRegressionTmjaAnneeMaxExclueDown(self):
        """
        verifie que si l'anneeMax est inferieure a la derniere date connu on a une ValueErreur
        """
        self.idComptage=IdComptage('86-D97-18+0')
        self.assertRaises(ValueError,self.idComptage.regressionTmja,1000)
        
    def testRegressionTmjaAnneeMaxExclueUp(self):
        """
        verifie que si l'anneeMax est inferieure a la derniere date connu on a une ValueErreur
        """
        self.idComptage=IdComptage('86-D97-18+0')
        self.assertRaises(UserWarning,self.idComptage.regressionTmja,3000)
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()