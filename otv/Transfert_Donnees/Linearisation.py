# -*- coding: utf-8 -*-
'''
Created on 14 oct. 2019

@author: martin.schoreisz
'''

"""
La linéarisation c'est quoi : 
des point de comptages issus de la Bdd Nouvelle-Aquitaine, un linéaire qui contient la linéarisation précédente et des données sur les troncon élémentaires de trafic.
Donc, dans un departement on a : 1 df des pts de comptages qui vient forcémentde al Bdd, une df des lignes, qui veint d'un fichier ou d'une Bdd
""" 

"""
Pour l'ensemble des départements il faut vérifier que les id_comptag sont bien toujours continus, cela se traduit par un nombre de lignes touché à chaque vertex
supérieur ou égal à 2 pour toute les lignes de l'id_comptag sauf 2 celle de début et celle de fin, sinon on lève une alerte.
"""
