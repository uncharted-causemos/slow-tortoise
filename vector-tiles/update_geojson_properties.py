#!/usr/bin/python

import json
import sys

# This script updates the feature properties of the provided geojson file
# Usage: python update_geojson_properties.py {geojson file}
file = sys.argv[1]
with open(file) as geojson_file:
  data = json.load(geojson_file)
  # Add unique id field. id is constructed as {adm0 region name}-{adm1 region name}-{adm2 region name}-{adm3 region name}
  for f in data['features']:
    properties = f['properties']
    region_id = properties['NAME_0']
    for level in range(1, 3): 
      name_field = 'NAME_' + str(level)
      if name_field in properties:
        region_id = region_id + '-' + properties[name_field]
    properties['id'] = region_id
  
# Write back to the same file
with open(file, 'w') as geojson_file:
  json.dump(data, geojson_file)
  