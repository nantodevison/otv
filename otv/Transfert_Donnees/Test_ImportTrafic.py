# -*- coding: utf-8 -*-
'''
Created on 17 sept. 2021

@author: martin.schoreisz
'''
import unittest
import pandas as pd
import numpy as np
from Tests_Connexion_Transfert import TestConnexionBdd
from Import_trafics import Comptage, Comptage_cd40, Comptage_Dira

tableauCptAnnuelDira=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRA\0_tmja_dira_par_section_20210101.ods'
tableaucptCartoDira=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRA\dira_tmja_2020.ods'
dossierCptHoraireDira=r'C:\Users\martin.schoreisz\Documents\temp\OTV\DIRA\GrosFichiers - JP CASSOU\0_Annee_Complete_2020'
annee='2020'
bdd='local_otv_boulot'

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
    
    
            
    

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()