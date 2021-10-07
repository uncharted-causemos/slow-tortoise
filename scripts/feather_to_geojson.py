#!/usr/bin/python

import sys
import pandas
import geopandas as pd
import geofeather as gf

file = sys.argv[1]
gdf = gf.from_geofeather(file)
print(gdf.columns.to_list())
print(gdf.dtypes)
gdf.to_file(f"{file}.geojson", driver="GeoJSON")
