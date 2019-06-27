# -*- coding: utf-8 -*-
'''
Created on 25 janv. 2019
@author: martin.schoreisz
Module pour importer les donnees de comptages issues des brochures du CD17 : 
P:\DAIT\TI\Donnees\1-ROUTIER\3 - Poitou-Charentes\17 - Charente-Maritime\2015\2015_CD17_carte_comptage_routiers.pdf
P:\DAIT\TI\Donnees\1-ROUTIER\3 - Poitou-Charentes\17 - Charente-Maritime\2016\Brochure Comptages 2016.pdf
'''

import re
import Connexion_Transfert as ct


def extraction_comptages_ponctuel(fichier):
    """
    fonction d'extraction des donnees numeriques liees au comptages cponctuels
    en entree, le nom complet du fichier en raw string
    il faut que le fichier soit creer par ouvertur du pdf dans firefox pour permettre le copier-coller, copier-coller dans un fihcier .txt
    via notepad++, enregistrement du fichier.txt. le fichier .txt ne contient pas les en-tete de colonne
    """
    # ouvrir le fichier et trouver voie, pr et abscisse
    with open(fichier, encoding="utf-8") as fichier :  # ouvrir l fichier
        liste = [element.replace('\n', ' ').replace('    ', ' ').replace('   ', ' ').replace('  ', ' ') for element in fichier]
        liste_decomposee_ligne = re.split(r'(Janvier|Février|Mars|Avril|Mai|Juin|Juillet|Août|Sept.|Oct.|Nov.|Déc.)', "".join(liste))  # permet de conserver le mois
        liste_decomposee_ligne = [liste_decomposee_ligne[i] + liste_decomposee_ligne[i + 1] for i in range(0, len(liste_decomposee_ligne) - 1, 2)]  # necessaier pour repasser le mois dans le string
        liste_decomposee_ligne[0] = ' ' + liste_decomposee_ligne[0]  # uniformité des données
        liste_decomposee_ligne = list(filter(None, liste_decomposee_ligne))
        voie, pr, abscisse = [element.split(' ')[1]for element in liste_decomposee_ligne], [element.split(' ')[2]for element in liste_decomposee_ligne], [element.split(' ')[3]for element in liste_decomposee_ligne]
        # pour le tmj et %PL c'est plus compliqué car la taille de la cellule localisation varie, son délimiteur aussi et les chiffres peuvent être entier ou flottant, don on va se baser sur le fait
        # que la rechreche d'un nombre a virgule renvoi le %PL, sinon la vitesse, et si c'est la vitesse, alors ca créer une value error en faisant le float sur l'element + 1, donc on 
        # sait que c'est la vitesse
        pc_pl, v85, tmj = [], [], []
        for element in liste_decomposee_ligne : 
            element_decompose = element.split()
            nombre_a_virgule = re.search('[0-9]{1,}\,[0-9]{1,}', element)  # rechreche un truc avec deux chiffres séparés par une virgule : renvoi un objet match si ok, none sinon
            if nombre_a_virgule : 
                try : 
                    v85.append(float(element_decompose[element_decompose.index(nombre_a_virgule.group()) + 1].replace(',', '.')))  # idem tmj
                    pc_pl.append(float(nombre_a_virgule.group().replace(',', '.'))) 
                    tmj.append(int(element_decompose[element_decompose.index(nombre_a_virgule.group()) - 2]))  # donc on en deduit le tmja selon al position (car seare par un ' ' dans le fichier de base) 
                except ValueError :
                    pc_pl.append(float(element_decompose[element_decompose.index(nombre_a_virgule.group()) - 1].replace(',', '.')))
                    v85.append(float(nombre_a_virgule.group().replace(',', '.')))
                    tmj.append(int(element_decompose[element_decompose.index(nombre_a_virgule.group()) - 3]))            
            else :  # si les deux données sont des entiers
                liste_nombre = []  # la liste des nombres dans la ligne de données :
                for objet in element_decompose : 
                    try :  # comme ça on ne garde que les nombre
                        liste_nombre.append(float(objet))
                    except ValueError : 
                        pass
                tmj, pc_pl, v85 = liste_nombre[-4], liste_nombre[-2], liste_nombre[-1]        
        # plus que les dates de mesure !!
        mois = [element.split()[-1] for element in liste_decomposee_ligne]
        periode = [element.split()[-3] + '-' + element.split()[-2] for element in liste_decomposee_ligne]
        # concatenation por verif
        donnees = zip(voie, pr, abscisse, tmj, pc_pl, v85, periode, mois)

        with ct.ConnexionBdd('local_otv') as c:
            for i in range(len(voie)) :
                c.curs.execute("INSERT INTO comptage.na_2010_2017_p (id_comptag,dep, route, pr, abs, reseau, gestionnai, concession,type_poste, tmja_2015, pc_pl_2015, obs_2015) VALUES ('17-'||%s||'-'||%s||'+'||%s,'17', %s, %s,%s,'RD','CD17','N','ponctuel',%s,%s,'nouveau point,'||%s||',v85_tv '||%s)", (voie[i], pr[i], abscisse[i], voie[i], pr[i], abscisse[i], tmj[i], pc_pl[i], periode[i], v85[i]))
                c.connexionPsy.commit()
