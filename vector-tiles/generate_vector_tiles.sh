#!/usr/bin/env bash

# Downlaod and process GADM shape files and generate vector tile sets

# Currently Supported Countries
# Djibouti: DJI
# Eritrea: ERI
# Somalia: SOM
# Kenya: KEN
# Uganda: UGA
# South Sudan: SSD
# Sudan: SDN
# Ethiopia: ETH
COUNTRIES=("DJI" "ERI" "SOM" "KEN" "UGA" "SSD" "SDN" "ETH")

# Ensure there's a folder to store shape files
mkdir -p .tmp dist
cd ./.tmp

# Downloading mbutil dependency (if not exist)
if [ ! -d "mbutil" ]
then
  git clone git://github.com/mapbox/mbutil.git
  mbutil/mb-util -h
fi

# Clean up previous files
rm -rf ./gadm* ./*.mbtiles ./cm-boundaries*

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

#TODO: update geojson properties

# Generate mbtiles from geojson files and export them to folders
LEVELS=(3 2 1 0)
for l in ${LEVELS[@]}; do
  outputName="cm-boundaries-adm${l}"
  mbtilesFile="$outputName.mbtiles"
  layerName="boundaries-adm${l}"

  echo "Creating admin $l vector tile set, '$mbtilesFile' ..."
  tippecanoe -zg -o $mbtilesFile -l $layerName --coalesce-densest-as-needed --extend-zooms-if-still-dropping $(ls *_$l.geojson)

  echo "Extracting $mbtilesFile to folder ..."
  ./mbutil/mb-util --image_format=pbf $mbtilesFile $outputName

  # Move the folder to dist
  mv $outputName ../dist/$outputNam
done
