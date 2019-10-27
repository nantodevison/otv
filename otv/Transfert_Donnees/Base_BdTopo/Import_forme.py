# -*- coding: utf-8 -*-
'''
Created on 29 janv. 2019
@author: martin.schoreisz

Module de creation d'importataion des donnees de base seravnt a l'aregation

'''
import matplotlib
import geopandas as gp
import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
import Connexion_Transfert as ct
from shapely.wkt import loads
from shapely.ops import polygonize, linemerge, unary_union
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select
import Outils

def import_donnes_base(bdd, schema, table_graph,table_vertex ):
    """
    OUvrir une connexion vers le servuer de reference et recuperer les donn�es
    en entree : 
       bdd : string de reference de la connexion, selon le midule Outils , fichier Id_connexions, et module Connexion_transferts
       schema : schema contenant les tables de graph et de vertex
       table_graph : table contennat le referentiel (cf pgr_createtopology)
       table_vertex : table contenant la ecsription des vertex (cf pgr_analyzegraph)
    en sortie : 
        df : dataframe telle que telcharg�es depuis la bdd
    """
    with ct.ConnexionBdd(bdd) as c : 
        requete1=f"""with jointure as (
            select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom 
             from {schema}.{table_graph} t 
            left join {schema}.{table_vertex} v1 on t.source=v1.id 
            left join {schema}.{table_vertex} v2  on t.target=v2.id
            )
            select j.* from jointure j, zone_test_agreg z
            where st_intersects(z.geom, j.geom)"""
        requete2=f"""select t.*, v1.cnt nb_intrsct_src, st_astext(v1.the_geom) as src_geom, v2.cnt as nb_intrsct_tgt, st_astext(v2.the_geom) as tgt_geom 
             from {schema}.{table_graph} t 
            left join {schema}.{table_vertex} v1 on t.source=v1.id 
            left join {schema}.{table_vertex} v2  on t.target=v2.id"""
        df = gp.read_postgis(requete2, c.connexionPsy)
        return df