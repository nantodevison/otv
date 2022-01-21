# -*- coding: utf-8 -*-
'''
Created on 7 janv. 2022

@author: martin.schoreisz
Stockage des parametres ou donnees de la Bdd OTV : liste d'attributs, noms de tables, connexion.... 
'''
nomConnBddOtv='local_otv_boulot'
attBddCompteur = ['geometrie', 'id_comptag', 'route', 'reseau', 'dep', 'gestionnai','concession', 'type_poste', 'src_geo', 'obs_geo', 'x_l93', 'y_l93',
                   'fictif', 'src_cpt', 'convention','obs_supl','sens_cpt']
attrComptageAssoc = ['id_comptag_ref', 'rang', 'periode', 'type_veh', 'src', 'obs', 'id_cpteur_asso']
attrCompteurAssoc = ['id_cpteur_asso', 'geom', 'route', 'pr', 'abs', 'type_poste', 'techno', 'src_geo', 
                     'obs_geo', 'obs_supl', 'src_cpt', 'convention', 'sens_cpt']
schemaComptage = 'comptage'
schemaComptageAssoc = 'comptage_assoc'
tableComptage = 'comptage'
tableCompteur = 'compteur'
tableIndicAgrege = 'indic_agrege'
tableIndicMensuel = 'indic_mensuel'
tableIndicHoraire = 'indic_horaire'
tableCorrespIdComptag = 'corresp_id_comptag'
tableEnumTypeVeh = 'enum_type_veh'

