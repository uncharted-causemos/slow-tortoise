#!/usr/bin/python

import json
import numpy as np
import geojson
import glob
import os

def get_bounding_box(geometry):
    coords = np.array(list(geojson.utils.coords(geometry)))
    return coords[:,0].min(), coords[:,0].max(), coords[:,1].min(), coords[:,1].max()

fileList = [f for f in glob.glob("*.geojson")]
outputFilePath = "test.json"
if os.path.isfile(outputFilePath):
    os.remove(outputFilePath)
with open(outputFilePath, "w") as outputFile:
    jsonOutputData = {}
    allBBoxObjs = []
    # loop through all shape files and extract bbox for each
    for file in fileList:
        print("Updating feature properties for", file, "...")
        bbox = []
        geo_code = ''
        with open(file) as geojson_file:
            data = json.load(geojson_file)
            # Add bbox field constructed as an array holding the bounding box rectangle of each region
            for f in data["features"]:
                properties = f["properties"]
                geometry = f["geometry"]
                # bbox [lng1, lat2, lng2, lat1]
                bbox = get_bounding_box(geometry)
                region_code = properties["GID_0"]
                for level in range(1, 4):
                    code_field = f"GID_{str(level)}"
                    if code_field in properties:
                        region_code = properties[code_field]
                # properties["bbox"] = bbox

                allBBoxObjs.append({
                    'code': region_code,
                    'bbox': bbox
                })

    jsonOutputData['geo'] = allBBoxObjs

    # write the bbox to the output file
    json.dump(jsonOutputData, outputFile)