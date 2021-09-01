'''
Created on 1 sept. 2021

@author: martin.schoreisz
module de test des fonctions et classes de l'OTV
'''
import unittest
from Visualisation import IdComptage, IdComptagInconnuError


class TestIdComptage(unittest.TestCase):

    def testIdComptageNonNull(self):
        try : 
            idComptage=IdComptage('16-D1-17+0')
        except IdComptagInconnuError :
            self.assertTrue(True)
            return
        self.assertFalse(True, 'message d\'erreur')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()