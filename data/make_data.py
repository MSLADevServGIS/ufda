#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 10 08:51:56 2016

@author: wallyg
"""

import os

import yaml
import dslw
from yamlformatter import YAMLFormatter

os.chdir(r"\\cityfiles\DEVServices\WallyG\projects\UFDA\data")

conn = dslw.SpatialDB("ufda_20152.sqlite")
conn.insert_srid(102700)

data_sources = yaml.load(YAMLFormatter(
    r"\\cityfiles\DEVServices\WallyG\projects\UFDA\data\config.yaml",
    filetype=".shp", year=2015,
    out_path=os.path.abspath("raw"))())

import arcpy

os.chdir(r"\\cityfiles\DEVServices\WallyG\projects\UFDA\data")

for feature in data_sources.keys():
    name = data_sources[feature]["out_name"]
    print(name)
    arcpy.FeatureClassToFeatureClass_conversion(**data_sources[feature])
    #dslw.utils.normalize_table(conn, name, 102700)
