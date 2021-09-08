# -*- coding: utf-8 -*-
'''
Created on 1 sept. 2021

@author: martin.schoreisz
module de test des fonctions et classes de l'OTV
'''
import unittest
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
        
    def testGraphTraficTmjaSeulemnt(self):
        """
        verfie qu'un comptage sans pcpl ne contient bien que des donnees tmja
        """
        self.idComptage=IdComptage('86-D97-18+0')
        self.idComptage.graphTrafic()
        self.assertTrue(all(['tmja'== e for e in self.idComptage.chartTrafic.data.indicateur.tolist()]))
    
    def testGraphTraficTmjaPcpl(self):
        """
        verifie qu'un comptage qui continent tmja et pc_pl renvoi bien les deux dans ses donnees sources
        """
        self.idComptage=IdComptage('16-D1-17+0')    
        self.assertTrue(['pc_pl', 'tmja']==sorted(list(self.idComptage.dfIdComptagBdd.indicateur.unique())))

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