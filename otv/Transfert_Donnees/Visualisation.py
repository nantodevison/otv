# -*- coding: utf-8 -*-
'''
Created on 1 sept. 2021

@author: martin.schoreisz
Module de regroupement des fonctions de visualisation
'''

from Donnees_horaires import calculJourneeType 
import pandas as pd
import altair as alt
import numpy as np
from Connexions import Connexion_Transfert as ct
from prompt_toolkit.utils import to_int

def prepGraphJourneeType(dfTMJA,dfTMJO):
    """
    preparer une df pour realiser un graph avec Altair. le graph comprend les courbes TMJA et TMJO d'unpoint de comptage.
    in : 
        dfTMJA : df issue de calculJourneeType()
        dfTMJO : df issue de calculJourneeType()
    """
    dfTMJAStack=dfTMJA[[f'h{i}_{i+1}' for i in range (24)]].stack().reset_index().assign(type_jour='tmja')
    dfTMJOStack=dfTMJO[[f'h{i}_{i+1}' for i in range (24)]].stack().reset_index().assign(type_jour='tmjo')
    dfTMJAStack.columns, dfTMJOStack.columns=['id_comptag','heure','nb_veh','type_jour'],['id_comptag','heure','nb_veh','type_jour']
    dfVisu=pd.concat([dfTMJAStack,dfTMJOStack], axis=0, sort=False)
    dfVisu.heure=dfVisu.heure.apply(lambda x : pd.to_datetime(f"2019-01-01 {x.split('_')[0][1:]}:00:00"))
    return dfVisu

def GraphJourneeType(df,id_comptag):
    dfTMJA,dfTMJO=calculJourneeType(df)
    dfVisu=prepGraphJourneeType(dfTMJA,dfTMJO)
    return alt.Chart(dfVisu.loc[dfVisu['id_comptag']==id_comptag], title=id_comptag).mark_line().encode(x='heure', y='nb_veh', color='type_jour')
 
def graph2SensParJour(dfComp): 
    """
    a partir de la Df dfComp fournie par la classe d'erreur SensAssymetriqueError ou la fonction comparer2Sens,
    preparer une df et visu sur un graph la somme par jour pour cahque sens
    """
    dfGraph2sens=pd.concat([dfComp[['jour','type_veh','id_comptag','total_x']].rename(columns={'total_x':'nb_veh'}).assign(
        sens='sens1'),dfComp[['jour','type_veh','id_comptag','total_y']].rename(columns={'total_y':'nb_veh'}).assign(
            sens='sens2')], axis=0)
    return alt.Chart(dfGraph2sens).mark_line().encode(x='jour', y='nb_veh', color='sens')

class IdComptage(object):
    """ 
    classe permettant de regrouepr tout les infos liées à un id_comptag dans la bdd version 2021 (millesime 2020)
    """
    def __init__(self, id_comptag, bdd='local_otv_boulot', qualite=False):
        """
        attributs : 
            id_comptag: text : identifiant du comptage
            bdd : text : valeur de connexion à la bdd, default 'local_otv_boulot'
            qualite : boolean : traduit si l'indicateur qualite des comptages du compteur doit etre rappatrié ou non
            dfIdComptagBdd : dataframe recensensant tout les indicateurs agreges de toute les annees
            chartTrafic : cahart Altair contenant soit que le TMJA ou TMJA + Pc_pl si dispo
            anneeMinConnue : integer : annee de comptage la plus ancienne
            anneeMaxConnue : integer : annee de comptage la plus recente
        """
        self.id_comptag=id_comptag
        self.bdd=bdd
        self.qualite=qualite
        self.recup_indic_agreges()
        self.caracDonnees()
        
    def recup_indic_agreges(self):
        """
        fonction de récupérartion des données d'indicateur agrege
        in : 
            
        """
        with ct.ConnexionBdd(self.bdd) as c :
            if not self.qualite :
                rqt=f"select * from comptage_new.vue_indic_agrege_info_cpt where id_comptag='{self.id_comptag}'"
            else : 
                rqt=f"""select c.*, ca.txt_qualite
                     from comptage_new.vue_indic_agrege_info_cpt c join qualite.vue_qualite_cptag ca on ca.id_unique_cpt=c.id_comptag_uniq
                     where c.id_comptag='{self.id_comptag}'"""
            self.dfIdComptagBdd=pd.read_sql(rqt,c.sqlAlchemyConn)
        if self.dfIdComptagBdd.empty : 
            raise IdComptagInconnuError(self.id_comptag)
        
    def caracDonnees(self):
        """
        recuperer les caracteristique des donnees : annee de comptage min, annee de comptage max, liste des indicateurs de comptage
        """
        self.anneeMinConnue=self.dfIdComptagBdd['annee'].min()
        self.anneeMaxConnue=self.dfIdComptagBdd['annee'].max()
        self.listIndic=tuple(np.sort(self.dfIdComptagBdd.indicateur.unique()))
                
    def graphTrafic(self, anneeMaxExclue=None):
        """
        fonction qui retourne un graph avec Pc_pl et TMJA, si pc_pl est présent
        in : 
            anneeMaxExclue : integer : annee max non prise en compte dans la droite de regression : default : toute les annees prises
        """
        if  not anneeMaxExclue :
            dfIdComptagBddTmja=self.dfIdComptagBdd.loc[self.dfIdComptagBdd['indicateur']=='tmja']
            dfIdComptagBddPcpl=self.dfIdComptagBdd.loc[self.dfIdComptagBdd['indicateur']=='pc_pl']
        elif anneeMaxExclue<int(self.anneeMinConnue) : 
            raise ValueError(f'annee max de prise en compte : {anneeMaxExclue} inferieure a annee min connue : {self.anneeMinConnue}')
        elif anneeMaxExclue>int(self.anneeMaxConnue) : 
            raise UserWarning(f'annee max de prise en compte : {anneeMaxExclue} ne filtre pas par rapport a annee max connue {self.anneeMaxConnue}')
        else : 
            dfIdComptagBddTmja=self.dfIdComptagBdd.loc[(self.dfIdComptagBdd['indicateur']=='tmja') & 
                                                       (self.dfIdComptagBdd.annee.astype(int)<anneeMaxExclue)]
            dfIdComptagBddPcpl=self.dfIdComptagBdd.loc[(self.dfIdComptagBdd['indicateur']=='pc_pl') & 
                                                       (self.dfIdComptagBdd.annee.astype(int)<anneeMaxExclue)]
        chartTmja=alt.Chart(dfIdComptagBddTmja, 
                    title=f'{self.id_comptag} : {self.dfIdComptagBdd.type_poste.unique()[0]}',
                    width=dfIdComptagBddTmja.annee.nunique()*50).mark_bar().encode(
                    x='annee:O',
                    y=alt.Y('valeur:Q', axis=alt.Axis(title='TMJA')))
        if self.qualite : 
            chartTmja=chartTmja.encode(color=alt.Color('txt_qualite:N', scale=alt.Scale(domain=['bonne', 'moyenne', 'faible'],range=['green', 'blue','red'])))
        if dfIdComptagBddPcpl.empty : 
            self.chartTrafic=chartTmja
        else : 
            nbAnneePcpl=dfIdComptagBddPcpl.annee.nunique()
            chartPcpl=alt.Chart(dfIdComptagBddPcpl, 
                     title=f'{self.id_comptag} : {self.dfIdComptagBdd.type_poste.unique()[0]}',
                     width=nbAnneePcpl*50).encode(
                         x='annee:O',
                         y=alt.Y('valeur:Q', axis=alt.Axis(title='% PL'),
                                scale=alt.Scale(domain=(0,dfIdComptagBddPcpl.valeur.max()*2 ))))
            if nbAnneePcpl==1 : 
                self.chartTrafic=(chartTmja+chartPcpl.mark_point(color='red')).resolve_scale(y='independent')
            else :
                self.chartTrafic=(chartTmja+chartPcpl.mark_line(color='red')).resolve_scale(y='independent')
            
    def regressionTmja(self, anneeMaxExclue=None):
        """
        fonction qui retrourne une chart altair avec TMJA et droite de regression
        in : 
            anneeMaxExclue : integer : annee max non prise en compte dans la droite de regression : default : toute les annees prises
        """
        if  not anneeMaxExclue :
            dfIdComptagBddTmja=self.dfIdComptagBdd.loc[self.dfIdComptagBdd['indicateur']=='tmja']
        elif anneeMaxExclue<int(self.anneeMinConnue) : 
            raise ValueError(f'annee max de prise en compte : {anneeMaxExclue} inferieure a annee min connue : {self.anneeMinConnue}')
        elif anneeMaxExclue>int(self.anneeMaxConnue) : 
            raise UserWarning(f'annee max de prise en compte : {anneeMaxExclue} ne filtre pas par rapport a annee max connue {self.anneeMaxConnue}')
        else : 
            dfIdComptagBddTmja=self.dfIdComptagBdd.loc[(self.dfIdComptagBdd['indicateur']=='tmja') & 
                                                       (self.dfIdComptagBdd.annee.astype(int)<anneeMaxExclue)]
        chartTmja=alt.Chart(dfIdComptagBddTmja, 
                    title=f'{self.id_comptag} : {self.dfIdComptagBdd.type_poste.unique()[0]}',
                    width=dfIdComptagBddTmja.annee.nunique()*50).mark_bar().encode(
                    x='annee:O',
                    y=alt.Y('valeur:Q', axis=alt.Axis(title='TMJA')))
        self.chartRegressTmja=chartTmja+chartTmja.transform_regression('annee', 'valeur').mark_line(color='black')

class Otv(object):
    """
    production des graphs liés à l'OTV en général
    """        
    
    def __init__(self, bdd='local_otv_boulot'):
        """
        attributs : 
            bdd : text : chaine de caracteres de connexion a la bdd selon les identifiants 
            dicoCorresp : dico décrivantles paramètres des graphs, selon key=typeGraph, values=) dico avec en key : rqt, title, note
        """
        self.bdd=bdd
        self.dicoCorresp={'exhaust_comptage':{'rqt':"select * from qualite.vue_exaust_cptag",
                                     'title':['Répartition des comptages selon l\'exhaustivité globale','et le type de poste'],
                                     'note':'note_exhaust'},
                          'coherence_affine_comptage':{'rqt':"select * from qualite.vue_coherence_fonction_affine_cptag",
                                     'title':['Cohérence des comptages selon une fonction affine','et le type de poste'],
                                     'note':'note_coherence_fonction_affine_cptage'},
                          'coherence_evol_comptage':{'rqt':"select * from qualite.vue_coherence_evolution_cptag",
                                     'title':['Cohérence des comptages selon l\'évolution annuelle','et le type de poste'],
                                     'note':'note_coherence_evolutions_cptage'},
                          'coherence_comptage':{'rqt':"select * from qualite.vue_coherence_cptag",
                                     'title':['Cohérence des comptages selon le type de poste'],
                                     'note':'note_coherence_cptag'},
                          'periode_comptage' :{'rqt':"select * from qualite.vue_represent_typoste_periode_cptag",
                                     'title':['Représentativité des comptages selon la ou les période(s)', 'de mesure et le type de poste'],
                                     'note':'note_represent_typoste_periode'}, 
                          'densite_compteur' :{'rqt':"select * from qualite.vue_densite_annee_before_cpteur",
                                     'title':['Densité de comptage en fonction','du type de poste'],
                                     'note':'note_densite_cptg_cpteur'},
                          'fiabilite_geo_compteur' :{'rqt':"select * from qualite.vue_fiabilite_src_geo_cpteur",
                                     'title':['fiabilité de la géolocalisation du compteur','selon le type de poste'],
                                     'note':'note_src_geo'},
                          'coherence_compteur' :{'rqt':"select * from qualite.vue_coherence_globale_cpteur",
                                     'title':['cohérence de l\'ensemble des comptages du compteur','selon le type de poste'],
                                     'note':'note_coherence_cpteur'},
                          'qualite_comptage' :{'rqt':"select * from qualite.vue_qualite_cptag",
                                     'title':['Répartition des comptages selon la qualité globale','et le type de poste'],
                                     'note':'note_comptag_final'}}
         
    
    def tableauEcartCoherencesComptages(self):
        """
        produire le tableau de recenesement des comptages selon leur classmeent de qualite par methode affine ou evolution
        out : 
            
        """
        with ct.ConnexionBdd('local_otv_boulot') as c:
            df=pd.read_sql("select * from qualite.vue_coherence_cptag",c.sqlAlchemyConn)
            df['txt_fonction_affine']=pd.Categorical(df['txt_fonction_affine'], ["NC", "faible", "moyenne", "bonne"])
            df['txt_evolution']=pd.Categorical(df['txt_evolution'], ["NC", "faible", "moyenne", "bonne"])
        return pd.pivot_table(df, values='id_comptag', index='txt_fonction_affine', columns='txt_evolution', aggfunc=lambda x : x.count()).fillna(0)
        
    
    def donneesQualiteComptages(self, typeGraph):
        """
        production de graph selon les différentes vue qualité de l'OTV : 
        in : 
            typeGraph : text parmi (inserer un test sur ces valeurs)
        out : 
            grp : dataframe groupee par txt_qualite, note, type_poste
        """
        if typeGraph not in self.dicoCorresp.keys() : 
            raise ValueError (f"le type de graph doit etre parmi {','.join(self.dicoCorresp.keys())}")
        with ct.ConnexionBdd(self.bdd) as c:
            df=pd.read_sql(self.dicoCorresp[typeGraph]['rqt'],c.sqlAlchemyConn)
        grp=df.groupby(['txt_qualite',self.dicoCorresp[typeGraph]['note'], 'type_poste']).id_comptag.count().reset_index()
        return grp
    
    def graphQualiteComptages(self, typeGraph):
        """
        produire le graph de qualite de comptage a partir des donnees fournies par donneesQualiteComptages
        in : 
            typeGraph : text parmi (inserer un test sur ces valeurs)
        out : 
            chart : altair hconcatChart avec à gauche chart1 , a droite chart 2
            chart1 : altair chart avec le type de poste en abscisse et la qualite en couleur, normalisee
            chart2 : altair chart avec la qualite en absicce et le type de poste en couleur, en nombre
        """
        grp = self.donneesQualiteComptages(typeGraph)
        chart1=alt.Chart(grp, width=300).mark_bar(width=40).encode(
            x=alt.X('type_poste:N', axis=alt.Axis(title='type de poste', labelAngle=315), sort=['ponctuel', 'tournant', 'permanent']), 
            y=alt.Y('id_comptag:Q',axis=alt.Axis(title='Nb de comptage', format='%'), stack='normalize'),
            color=alt.Color('txt_qualite:N',legend=alt.Legend(title='qualité'), sort=['faible', 'moyenne', 'bonne']),
            order =alt.Order(f"{self.dicoCorresp[typeGraph]['note']}:Q", sort='ascending'))
        chart2=alt.Chart(grp, width=300).mark_bar(width=40).encode(
            x=alt.X('txt_qualite:N', axis=alt.Axis(title='qualité', labelAngle=315), sort=['NC','faible', 'moyenne', 'bonne']), 
            y=alt.Y('id_comptag:Q',axis=alt.Axis(title='Nb de comptage')),
            color=alt.Color('type_poste:N',legend=alt.Legend(title='type de poste'), sort=['ponctuel', 'tournant', 'permanent']),
            order=alt.Order(f"{self.dicoCorresp[typeGraph]['note']}:Q", sort='ascending'))
        chart=alt.HConcatChart(hconcat=[chart1,chart2], 
                title=alt.TitleParams(self.dicoCorresp[typeGraph]['title'],
                 anchor='middle')).resolve_legend('independent').resolve_scale('independent').resolve_scale(color='independent')
        chart1.title=self.dicoCorresp[typeGraph]['title']
        chart2.title=self.dicoCorresp[typeGraph]['title']
        return chart, chart1, chart2
        
        

class IdComptagInconnuError(Exception):
    """
    Exception levee si un id_comptag n'est pas présent dans la base de données
    """     
    def __init__(self, id_comptag):
        self.id_comptag=id_comptag
        Exception.__init__(self,f'le compteur {self.id_comptag} n\'est pas référencé dans la base')
