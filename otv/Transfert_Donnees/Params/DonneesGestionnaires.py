# -*- coding: utf-8 -*-
'''
Created on 9 ao�t 2022

@author: martin.schoreisz
parametres liés aux gestionniaires
'''

##########
# GENERAL
##########

denominationSens = ('sens1', 'sens2', 'deportsens1', 'deportsens2', 'sensexter', 'sensinter', 'sens1n137'
                    , 'sens2n137', 'sens1n11', 'sens2n11', 'sens1n10', 'sens2n10', 'sens1n141', 'sens2n141')


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


#########
# CD79
#########

cd79_dicoCorrespMaterielTechno = {'Cigale': 'tube', 'Phoenix': 'boucle_electromagnetique', 'Tubes': 'tube', 'Plaques': 'plaquette',
             'Major': 'boucle_electromagnetique'}


#########
# CD79
#########

cd33_dicoAttrPermExcel = {'Tronçon': 'troncon', 'Data': 'route', 'MJA TV JO': 'tmjo', 'MJA PPL JO': 'pc_pl_o',
                     'MJE TV TCJ': 'tmje', 'MJE PPL TCJ': 'pc_pl_e', 'MJA TV TCJ': 'tmja', 'MJA PPL TCJ': 'pc_pl',
                     'MJM TV TCJ 01': 'janv', 'MJM TV TCJ 02': 'fevr', 'MJM TV TCJ 03': 'mars', 'MJM TV TCJ 04': 'avri',
                     'MJM TV TCJ 05': 'mai', 'MJM TV TCJ 06': 'juin', 'MJM TV TCJ 07': 'juil', 'MJM TV TCJ 08': 'aout',
                     'MJM TV TCJ 09': 'sept', 'MJM TV TCJ 10': 'octo', 'MJM TV TCJ 11': 'nove', 'MJM TV TCJ 12': 'dece',
                     'Année': 'annee', 'remarque': 'obs', 'Sens': 'sens'}
cd33_dicoAttrTournExcel = {'Tronçon': 'troncon', 'mje_tv_tcj': 'tmje', 'mja_tv_tcj': 'tmja', 'mja_ppl_tcj': 'pc_pl', 'ddpériode1': 'debut_periode1',
                           'dfpériode1': 'fin_periode1', 'mjptv1': 'tmja_periode1', 'mjpppl1': 'pc_pl_periode1',
                           'MJPVMTV1': 'vmoy_periode1', 'MJPV85TV1': 'v85_periode1', 'ddpériode2': 'debut_periode2',
                           'dfpériode2': 'fin_periode2', 'mjptv2': 'tmja_periode2', 'mjpppl2': 'pc_pl_periode2',
                           'MJPVMTV2': 'vmoy_periode2', 'MJPV85TV2': 'v85_periode2', 'ddpériode3': 'debut_periode3',
                           'dfpériode3': 'fin_periode3', 'mjptv3': 'tmja_periode3', 'mjpppl3': 'pc_pl_periode3',
                           'MJPVMTV3': 'vmoy_periode3', 'MJPV85TV3': 'v85_periode3', 'ddpériode4': 'debut_periode4',
                           'dfpériode4': 'fin_periode4', 'mjptv4': 'tmja_periode4', 'mjpppl4': 'pc_pl_periode4',
                           'MJPVMTV4': 'vmoy_periode4', 'MJPV85TV4': 'v85_periode4', 'annee_comptee': 'annee', 'sens': 'sens'}
cd33_dicoAttrPermTournShape = {'tronçon': 'troncon', 'type': 'type_poste', 'rattacheme': 'rattacheme',
                           'libelle': 'libelle', 'libellé': 'libelle', 'capteur': 'techno', 'prc': 'pr', 'abc': 'abs', 
                           'vitesse_ma': 'vma', 'geometry': 'geometry', 'data': 'route', 'identifian': 'id_cpt', 'sens': 'sens'}
cd33_dicoAttrEnqueteShape = {'ddperiode': 'debut_periode', 'dfperiode': 'fin_periode', 'mjptv1': 'tmja', 'mjpppl1': 'pc_pl',
                             'mjpv85tv1': 'v85', 'mjpv85vl1': 'v85_vl', 'mjpv85pl1': 'v85_pl', 'annee': 'annee', 'remarque': 'obs',
                             'compteur': 'techno'}
cd33_dicoCorrespTechno = {'Radars': 'radar', 'Radar': 'radar', 'Tubes': 'tube', 'Boucles': 'boucle_electromagnetique', 
                          'Tubes Mode3': 'tube', 'Tubes Mode4': 'tube'}


#########
# CD17
#########

cd17_permCsvDropligneDebut = range(3)
cd17_permCsvTmjmPlColumns = ['localisation', 'route', 'pr', 'abs', 'latitude', 'longitude', 'tmja', 'pc_pl', 'janv_tmja', 'janv_pc_pl', 
                             'fevr_tmja', 'fevr_pc_pl', 'mars_tmja', 'mars_pc_pl', 'avri_tmja', 'avri_pc_pl', 'mai_tmja', 'mai_pc_pl',
                             'juin_tmja', 'juin_pc_pl', 'juil_tmja', 'juil_pc_pl', 'aout_tmja', 'aout_pc_pl', 'sept_tmja', 'sept_pc_pl',
                             'octo_tmja', 'octo_pc_pl', 'nove_tmja', 'nove_pc_pl', 'dece_tmja', 'dece_pc_pl']


#########
# CD19
#########

cd19_dicoTypePoste = {'permanent': ('19-D9-34+570', '19-D979-69+40', '19-D982-1+855', '19-D1120-34+225', '19-D1120-51+250', '19-D1120-55+150',
                                    '19-D9-29+80', '19-D170-22+550', '19-D940-9+940', '19-D979-48+500', '19-D982-15+125')}

#########
# NIORT
#########
niort_formatFichierAccepte = ('xls', 'csv', 'mdb')
niort_vmoyHoraireVlStartCpev = 162
niort_vmoyHoraireVlPasCpev = 104
niort_vmoyHorairePlPasCpev = 52
niort_nbJoursHoraireCpev = 7
niort_ligneDebutDebitHoraireTv = 78
niort_ligneDebutDebitHorairePl = 68
niort_colonneDebutDebitHoraire = 1
niort_colonneFinDebitHoraire = 26
niort_colonneVmoyHoraire = 16
niort_colonneVmoyHoraireJour = 1