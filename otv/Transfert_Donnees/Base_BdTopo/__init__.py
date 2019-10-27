# -*- coding: utf-8 -*-


"""
Package de base d'agregation des troncons de la BdTopo
"""

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