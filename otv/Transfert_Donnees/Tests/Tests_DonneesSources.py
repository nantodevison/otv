# -*- coding: utf-8 -*-
'''
Created on 12 oct. 2021

@author: martin.schoreisz
'''
import unittest
from Donnees_sources import MHCorbin

fichierMHCorbinTest=r'C:\Users\martin.schoreisz\git\otv\otv\Transfert_Donnees\data\MHCorbin\rue de souche du 31 01 au 07 02 2020.mdb'
fichierMHCorbinAmpute=r'C:\Users\martin.schoreisz\git\otv\otv\Transfert_Donnees\data\MHCorbin\test_Tables_Manquantes.mdb'

class TestMhCorbin(unittest.TestCase):
    
    def setUp(self):
        self.mhc=MHCorbin(fichierMHCorbinTest) 

    def testRessource(self):
        lg=len(self.mhc.dfRessourceCorrespondance())
        self.assertTrue(lg==12483)
        
    def test_nbSens_egal2(self):
        self.assertTrue(self.mhc.nbSens==2)
        
    def test_verifTables_2sens(self):
        self.assertRaises(ValueError,MHCorbin,fichierMHCorbinAmpute )


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()