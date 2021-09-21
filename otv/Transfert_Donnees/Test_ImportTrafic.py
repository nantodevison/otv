# -*- coding: utf-8 -*-
'''
Created on 17 sept. 2021

@author: martin.schoreisz
'''
import unittest
import pandas as pd
import Connexion_Transfert as ct
from Tests_Connexion_Transfert import TestConnexionBdd
from Import_trafics import Comptage, Comptage_cd40
from Visualisation import IdComptage, IdComptagInconnuError, Otv

class TestComptage(unittest.TestCase):
    """
    test des methodes communes a tous les comptages
    """
    
    def setUp(self):
        self.cpt=Comptage('fichier')
    
    def testCreerComptageTypeVeh(self):
        """
        verifier que le type_veh renvoi erreur si fausse valeur
        """
        self.assertRaises(ValueError,self.cpt.creer_comptage,'cpt', '2020', 'src', 'obs', 'toto')
        
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
                self.assertRaises(ValueError,self.cpt.creer_comptage,'cpt', '2020', 'src', 'obs', 'tv/pl', p)
                
    def testSructureBddOld2NewFormAttrObligatoires(self):
        """
        vérifier que les erreurs sont bien levées si les conditions ne sont pas rempli sur les attributs obligatoires
        """
        self.assertRaises(AttributeError,self.cpt.structureBddOld2NewForm,'df', '2020', ['listAttrFixe, id_comptag'],['tmja'], 'agrege')
        
    def testSructureBddOld2NewFormResultNonVide(self):
        """
        vérifier que les erreurs sont bien levées si les conditions ne sont pas rempli sur les attributs obligatoires
        """
        dfTest=pd.DataFrame({'id_comptag':['XXX-DXXXX-XX+XXX',], 'annee':'2020', 'tmja':10000})
        self.assertRaises(ValueError,self.cpt.structureBddOld2NewForm,dfTest, '2020', ['annee', 'id_comptag'],['tmja'], 'agrege')
        
    def testStructureBddOld2NewFormTypeIndic(self):
        self.assertRaises(ValueError,self.cpt.structureBddOld2NewForm,'df', '2020', ['annee', 'id_comptag'],['tmja'], 'toto')
        

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
        self.assertRaises(ValueError,Comptage_cd40,'chemin','2020', 'toto')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()