#!/usr/bin/env bash

# Exit the script as soon as one of the commands failed
set -e

# Downlaod and process GADM shape files and generate bbox for each region

# Currently Supported Countries
# Djibouti: DJI
# Eritrea: ERI
# Somalia: SOM
# Kenya: KEN
# Uganda: UGA
# South Sudan: SSD
# Sudan: SDN
# Ethiopia: ETH
# Egypt: EGY
COUNTRIES=("ETH" "SSD" "EGY")

# Ensure there's a folder to store shape files
mkdir -p .tmp dist
cd ./.tmp

# Clean up previous files
rm -rf ./gadm*
rm -rf ../dist/*

# Download shape files for all countries
for country in ${COUNTRIES[@]}; do
  fname="gadm36_${country}_shp.zip"
  echo "Downloading shape files for ${country} ..."
  curl -LO "https://biogeo.ucdavis.edu/data/gadm3.6/shp/${fname}"

  echo "Extracting ${fname} ..."
  unzip ${fname} -d "gadm36_${country}_shp" 
done

# Convert shape files to geojson files
echo "Converting all shape files to geojson ..."
sfiles=($(ls */*.shp))
for f in ${sfiles[@]}; do
  # Drop directory and file extension, eg. gadm36_SOM_shp/gadm36_SOM_2.shp -> gadm36_SOM_2 
  fname=$(echo "$f" | cut -f 1 -d '.' | cut -f 2 -d '/')
  ogr2ogr -f GeoJSON "${fname}.geojson" ${f}
  echo "Converted ${f} to ${fname}.geojson"
done

# Extract bbox from geojson shape files for all countries/regions
python ../extract_bbox_from_shapefiles.py

# Update ES with bbox for all geo regions
python ../update_gdam_index_with_bbox.py

