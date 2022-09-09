# -*- coding: utf-8 -*-
'''
Created on 7 janv. 2022

@author: martin.schoreisz
Stockage des parametres ou donnees de la Bdd OTV : liste d'attributs, noms de tables, connexion.... 
'''
import Connexion_Transfert as ct
import pandas as pd

nomConnBddOtv='local_otv_boulot'
attBddCompteur = ['geom', 'id_comptag', 'pr', 'abs', 'route', 'reseau', 'dep', 'gestionnai','concession', 'type_poste', 'techno', 'src_geo', 
                  'obs_geo', 'x_l93', 'y_l93', 'fictif', 'id_cpt', 'id_sect', 'last_ann_sect', 'src_cpt', 'convention','obs_supl','sens_cpt']
attBddCompteurNonNull = ['id_comptag', 'reseau', 'dep', 'gestionnai','concession', 'type_poste', 'fictif', 'src_cpt', 'convention','sens_cpt']
attrComptage = ['id_comptag', 'annee', 'periode', 'src', 'obs', 'type_veh']
attrComptageNonNull = ['id_comptag', 'annee', 'type_veh']
attrComptageMano = ['id_comptag', 'src', 'periode']
attrIndicAgrege = ['id_comptag_uniq', 'fichier', 'valeur', 'indicateur']
attrCompteurValeurMano = ['id_comptag', 'type_poste', 'src_geo', 'periode', 'pr', 'abs', 'route', 'src_cpt', 'convention', 'sens_cpt']
attrComptageAssoc = ['id_cptag_ref', 'rang', 'periode', 'type_veh', 'src', 'obs', 'id_cpteur_asso']
attrCompteurAssoc = ['id_cpteur_asso', 'geom', 'route', 'pr', 'abs', 'type_poste', 'techno', 'src_geo', 
                     'obs_geo', 'obs_supl', 'src_cpt', 'convention', 'sens_cpt', 'id_cpt', 'id_sect', 'id_cpteur_ref']
attrIndicHoraireAssoc = ['jour', 'indicateur', 'h0_1', 'h1_2', 'h2_3', 'h3_4', 'h4_5', 'h5_6','h6_7', 'h7_8', 'h8_9', 'h9_10', 
                         'h10_11', 'h11_12', 'h12_13', 'h13_14','h14_15', 'h15_16', 'h16_17', 'h17_18', 'h18_19', 'h19_20',
                         'h20_21','h21_22', 'h22_23', 'h23_24', 'fichier', 'id_comptag_uniq']
attrIndicHoraire = ['jour', 'id_comptag_uniq', 'indicateur', 'h0_1', 'h1_2', 'h2_3', 'h3_4', 'h4_5', 'h5_6','h6_7', 'h7_8', 'h8_9', 'h9_10', 
                         'h10_11', 'h11_12', 'h12_13', 'h13_14','h14_15', 'h15_16', 'h16_17', 'h17_18', 'h18_19', 'h19_20',
                         'h20_21','h21_22', 'h22_23', 'h23_24', 'fichier']
attrIndicAgregeAssoc = ['id_comptag_uniq', 'indicateur', 'valeur', 'fichier']
schemaComptage = 'comptage'
schemaComptageAssoc = 'comptage_assoc'
tableComptage = 'comptage'
tableCompteur = 'compteur'
tableIndicAgrege = 'indic_agrege'
tableIndicMensuel = 'indic_mensuel'
tableIndicHoraire = 'indic_horaire'
tableCorrespIdComptag = 'corresp_id_comptag'
tableEnumTypeVeh = 'enum_type_veh'
tableEnumTypePoste = 'enum_type_poste'
tableEnumSensCpt = 'enum_nb_sens_cpt'
tableEnumIndicateur = 'enum_indicateur'
vueLastAnnKnow = 'vue_compteur_last_annee_know_tmja_pc_pl'
with ct.ConnexionBdd(nomConnBddOtv) as c:
    enumTypePoste = pd.read_sql(f"select code from {schemaComptage}.{tableEnumTypePoste}", c.sqlAlchemyConn).code.tolist()
    enumSensCpt = pd.read_sql(f"select code from {schemaComptage}.{tableEnumSensCpt}", c.sqlAlchemyConn).code.tolist()
    enumIndicateur = pd.read_sql(f"select code from {schemaComptage}.{tableEnumIndicateur}", c.sqlAlchemyConn).code.tolist()