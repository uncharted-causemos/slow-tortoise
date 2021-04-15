#!/usr/bin/python

import json
import sys

# This script updates the feature properties of the provided geojson file
# Usage: python update_geojson_properties.py {geojson file}
file = sys.argv[1]
with open(file) as geojson_file:
  data = json.load(geojson_file)
  # Add unique id field. id is constructed as {adm0 region name}_{adm1 region name}_{adm2 region name}_{adm3 region name}
  for f in data['features']:
    properties = f['properties']
    region_id = properties['NAME_0']
    if '_' in region_id:
      raise ValueError(f'Region name, {region_id} contains "_".\n Can not create an unique region id using region names joined with "_"')
    for level in range(1, 3): 
      name_field = 'NAME_' + str(level)
      if name_field in properties:
        region_name = properties[name_field] 
        if '_' in region_name:
          raise ValueError(f'Region name, {region_name} contains "_".\n Can not create an unique region id using region names joined with "_"')
        region_id = region_id + '_' + region_name
    properties['id'] = region_id
  
# Write back to the same file
with open(file, 'w') as geojson_file:
  json.dump(data, geojson_file)
  