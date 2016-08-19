#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""data.py:"""


from __future__ import unicode_literals

from glob import glob

import dslw


YEAR = 2015
DB_NAME = "UFDA_{}.sqlite".format(YEAR)

# Database Connection
conn = dslw.SpatialDB(DB_NAME)
cur = conn.cursor()


import arcpy

analysis_queries = {
    "length_mi":   "SELECT SUM(ST_Length(Shape))/5280 FROM {table};"}


# Allows paths in 'data' to be shortened
base = {
    "SDE":          r"Database Connections\Features.sde\SDEFeatures.GIS.{}",
    "ENGDATA":      r"\\CityFiles\DEVServices\ENGINEERING\ENGDATA{}"
    }

# List of tuples of insert values (name, desc, path, query)
data_sources = [
    ("chip_sealed_roads",
     "roads of recent chip seal projects (POLYLINE)",
     glob(base["ENGDATA"].format(
        (r"\CITYMAPS\Streets\Reclaimite Chip & Seal"
         r"\{}\*Chip_Seal*.shp").format(YEAR)))[0],
     ""),
    
    ("resurfaced_roads",
     r"roads recently repaved (email Ashley Strayer?)",
     "",
     ""),
     
    ("reconstructed_roads",
     "roads torn out and completely rebuilt (email Ashley Strayer?)",
     "",
     ""),
    
    ("sidewalks",
     "new sidewalk projects (POLYLINE)",
     base["SDE"].format(r"City_Roads\SDEFeatures.GIS.Sidewalks"),
     "YRBUILT={}".format(YEAR)),
    
    ("sewer_mains",
     "new sewer mains (POLYLINE)",
     base["SDE"].format(r"Sewer_Network\SDEFeatures.GIS.ssMainLine"),
     "YRBUILT={}".format(YEAR)),
     
    ("annexations",
     "new land annexed into the city limits (MULTIPOLYGON)",
     base["SDE"].format(r"Resolutions\SDEFeatures.GIS.Annex_Resolutions"),
     "YEAR_={}".format(YEAR)),
    
    ("deannexations",
     "land de-annexed from the city limits (MULTIPOLYGON)",
     base["SDE"].format(r"Resolutions\SDEFeatures.GIS.UnAnnexed_Resolutions"),
     "YEAR={}".format(YEAR)),

    ("trails",
     "city-county trails (temp location)",
     base["SDE"].format("City_County_Trails"),
     "build_year={}".format(YEAR))

    ]

def create_data_sources(conn, data_sources):
    """Creates data_sources table."""
    _c = conn.cursor()
    print("Creating data_sources table...")
    # SQL Statements
    create_sql = "CREATE TABLE data_sources (name, desc, path, query);"
    insert_sql = "INSERT INTO data_sources VALUES (?, ?, ?, ?);"
    # Execution
    _c.execute(create_sql)
    for row in data_sources:
        _c.execute(insert_sql, row)
    return


def load_data(conn):
    _c = conn.cursor()
    select_sql = "SELECT path, name, query FROM data_sources;"
    rows = _c.execute(select_sql).fetchall()
    for row in rows:
        print("Inserting {}...".format(row[1]))
        if not row[0]:
            continue
        elif row[0].endswith(".shp"):
            _c.execute(dslw.utils.ImportSHP(
                row[0][:-4], row[1], "UTF-8", 102700, "Shape"))
        elif "trails" in row[1]:
            continue
        else:
            arcpy.FeatureClassToFeatureClass_conversion(
                row[0], DB_NAME, row[1], row[2])
    return


def fix_srids(conn):
    _c = conn.cursor()
    sql = "SELECT f_table_name, srid FROM geometry_columns;"
    rows = _c.execute(sql).fetchall()
    for row in rows:
        srid = dslw.utils.SB_UpdateGeometrySRID(row[0], "Shape", 102700)
        if row[1] != 102700:
            _c.execute(srid)
    return


create_data_sources(conn, data_sources)
load_data(conn)
fix_srids(conn)


print(cur.execute(
    analysis_queries["length_mi"].format(table="sidewalks")).fetchone())


