## GADM Bounding Box Generation
These scripts will update the `gadm-name` index in Elasticsearch with the coordinates of a bounding box that encompases each GADM region.
The bounding box is computed from GADM shape files.

This script assumes that the index already contains a document for every GADM region. This can be done with the instructions [here](../gadm/README.md). 

### Prerequisite

The following dependencies need to be installed
- Python >= 2.6
- gdal 
- geojson

Installation on Mac:
```
brew install gdal
pip install geojson
```

### Update ES index

```
ES_URL=... ES_USER=... ES_PWD=... ./generate_bounding_boxes.sh
```
