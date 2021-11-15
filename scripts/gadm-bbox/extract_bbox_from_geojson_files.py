#!/usr/bin/python

import json
import numpy as np
import geojson
import glob
import os
import pathlib

dir_path = str(pathlib.Path(__file__).parent.resolve()) + '/.tmp/'

def get_bounding_box(geometry):
    coords = np.array(list(geojson.utils.coords(geometry)))
    return coords[:,0].min(), coords[:,0].max(), coords[:,1].min(), coords[:,1].max()

# a list of all geojson files from all countries and all admin levels
fileList = [f for f in glob.glob(dir_path + "countries/*.txt")]
# transform into an array of arrays by grouping each country as one array
fileListGrouped = []
for file in fileList:
    country = os.path.basename(file).rsplit( ".", 1 )[ 0 ]
    country_geojson_files = []
    for level in range(0, 4):
        fileName = 'gadm36_' + country + '_' + str(level) + '.geojson'
        filePath = dir_path + fileName
        if os.path.isfile(filePath):
            country_geojson_files.append(filePath)
    fileListGrouped.append(country_geojson_files)

for filelist in fileListGrouped:
    # loop through all geojson files of each country and extract bbox
    jsonOutputData = {}
    allBBoxObjs = []
    for file in filelist:
        print("Adding bounding-box for", file, "...")
        with open(file, 'r') as geojson_file:
            data = json.load(geojson_file)
            # Add bbox field constructed as an array holding the bounding box rectangle of each region
            for f in data["features"]:
                properties = f["properties"]
                geometry = f["geometry"]
                bbox = get_bounding_box(geometry)
                region_code = properties["GID_0"]
                for level in range(1, 4):
                    code_field = f"GID_{str(level)}"
                    if code_field in properties:
                        region_code = properties[code_field]

                allBBoxObjs.append({
                    "code": region_code,
                    "bbox": {
                        "type" : "envelope",
                        "coordinates" : [ [bbox[0], bbox[3]], [bbox[1], bbox[2]] ]
                    }
                })

    jsonOutputData['geo'] = allBBoxObjs
    if len(filelist) > 0:
        fileName = os.path.basename(filelist[0]).rsplit( ".", 1 )[0]
        fileName = fileName.rsplit( "_", 1 )[0]
        # fileName will be for each country something like "gadm36_ETH"
        outputFilePath = dir_path + fileName + ".json"
        print("Writing to", outputFilePath, "...")
        if os.path.isfile(outputFilePath):
            os.remove(outputFilePath)
        with open(outputFilePath, "w") as outputFile:
            json.dump(jsonOutputData, outputFile)


