#!/usr/bin/env bash

# Downlaod and process GADM shape files and generate bbox for each region

# Exit the script as soon as one of the commands failed
set -e

# Ensure there's a folder to store shape files
mkdir -p .tmp
cd ./.tmp

# Clean up previous files
rm -rf ./gadm*
rm -rf ./countries

# create a dir to store all country codes/names
mkdir countries

# download the list of gadm countries and create file for each
python ../download_and_create_country_files.py

# process each country
cfiles=($(ls countries/*))
for f in ${cfiles[@]}; do
  country=$(echo "$f" | cut -f 1 -d '.' | cut -f 2 -d '/')
  fname="gadm36_${country}_shp.zip"
  echo "Downloading shape files for ${country} ..."
  # Local cache, to download from GADM use URL below
  curl -LO "http://10.64.16.209:4005/gadm/countries/${fname}"
  # curl -LO "https://biogeo.ucdavis.edu/data/gadm3.6/shp/${fname}"
  echo "Extracting ${fname} ..."
  unzip ${fname} -d "gadm36_${country}_shp" 
done

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
#COUNTRIES=("ETH")
#for country in ${COUNTRIES[@]}; do
#  fname="gadm36_${country}_shp.zip"
#  echo "Downloading shape files for ${country} ..."
#  curl -LO "https://biogeo.ucdavis.edu/data/gadm3.6/shp/${fname}"
#  echo "Extracting ${fname} ..."
#  unzip ${fname} -d "gadm36_${country}_shp" 
#done

#echo "Downloading shape files for all countries ..."
#curl -LO "https://biogeo.ucdavis.edu/data/gadm3.6/gadm36_shp.zip"
## https://biogeo.ucdavis.edu/data/gadm3.6/gadm36_levels_shp.zip ## all levels of all countries
#echo "Extracting gadm36_shp.zip ..."
#unzip gadm36_shp.zip -d "gadm36_shp" 

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
python ../extract_bbox_from_geojson_files.py

# Update ES with bbox for all geo regions
python ../update_gadm_index_with_bbox.py 
