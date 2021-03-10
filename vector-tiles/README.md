## Causemos Vector Tiles
This repo contains tools/scripts to process GADM shape files and create vector tile sets for Causemos

### Prerequisite

The following dependencies need to be installed
- Python >= 2.6
- gdal 
- tippecanoe

Installation on Mac:
```
brew install gdal tippecanoe
```

### Generate vector tile set

```
sh generate_vector_tiles.sh
```