# -*- coding: utf-8 -*-
'''
Created on 9 ao�t 2022

@author: martin.schoreisz
parametres liés aux gestionniaires
'''

#########
# CD16
#########
cd16_columnsFichierPtPigma = ['AXE', 'PLOD', 'ABSD', 'MATERIEL', 'IDENTIFIAN', 'LIMITE_VIT']
cd16_columnsFichierLgnPigma = ['AXE', 'TRAFIC_PL', 'TMJA', 'MJE', 'MJEPPL', 'MJHE', 'MJHEPPL', 'VMOY', 'V85' , 'ANNEE_COMP', 'PRC',
                               'ABC', 'TYPE_COMPT', 'SECTION_CP', 'PERIODE_DE', 'PERIODE_FI']
cd16_dicoCorrespTypePoste = {'Tmp': 'tournant', 'CuP': 'permanent', 'Per': 'permanent'}
cd16_dicoCorrespTechno = {'MIXTRA et/ou ALPHA': 'tube', 'PRMX': 'boucle_electromagnetique', 'PHOENIX': 'boucle_electromagnetique',
                     'PRMX RADAR': 'boucle_electromagnetique', 'CAMERA': 'camera', 'SAM': 'boucle_electromagnetique'}
cd16_dicoCorrespNomColums = {'AXE': 'route', 'PRC': 'pr', 'ABC': 'abs', 'TMJA': 'tmja', 'TRAFIC_PL': 'pc_pl', 'TYPE_COMPT': 'type_poste', 'MATERIEL': 'techno',
                        'IDENTIFIAN': 'id_cpt', 'LIMITE_VIT': 'vma', 'SECTION_CP': 'id_sect', 'ANNEE_COMP': 'annee', 'MJE': 'tmje',
                        'MJEPPL': 'pc_pl_e', 'MJHE': 'tmjhe', 'MJHEPPL': 'pc_pl_he', 'VMOY': 'vmoy', 'V85': 'v85'}
cd16_columnsASuppr = ['PLOD', 'ABSD', 'PERIODE_DE', 'PERIODE_FI']
cd16_attrIndicAgregePigma = ['tmja', 'tmje', 'tmjhe', 'pc_pl', 'pc_pl_e', 'pc_pl_he', 'vmoy', 'vma_vl', 'vma_pl', 'v85']