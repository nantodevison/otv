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
        TestConnexionBdd()
    
    def testIdComptageNonNull(self):
        self.assertRaises(IdComptagInconnuError,IdComptage,'toto')
        
    def testGraphTraficTmjaSeulemnt(self):
        self.idComptage=IdComptage('86-D97-18+0')
        self.idComptage.graphTrafic()
        self.assertTrue(all(['tmja'== e for e in self.idComptage.chartTrafic.data.indicateur.tolist()]))
    
    def testGraphTraficTmjaPcpl(self):
        self.idComptage=IdComptage('16-D1-17+0')    
        self.assertTrue(['pc_pl', 'tmja']==sorted(list(self.idComptage.dfIdComptagBdd.indicateur.unique())))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()