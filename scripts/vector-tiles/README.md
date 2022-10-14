## Causemos Vector Tiles
This repo contains tools/scripts to process GADM shape files and create vector tile sets for Causemos

### Prerequisite

The following dependencies need to be installed
- Python >= 2.6
- gdal 
- tippecanoe
- mc (Minio client for uploading vector tiles to a bucket)

Installation on Mac:
```
brew install gdal tippecanoe minio/stable/mc
```

### Generate vector tile set

```
./generate_vector_tiles.sh
```

Vector tile sets will be generated in `dist` folder

### Upload vector tiles to minio/s3
```
mc alias set cm <TARGET-S3-ENDPOINT> <YOUR-ACCESS-KEY> <YOUR-SECRET-KEY>
mc mb cm/vector-tiles
mc cp -r dist/* cm/vector-tiles/
```
