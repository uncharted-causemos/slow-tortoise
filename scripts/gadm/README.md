## Populate gadm-name ES index
This folder contains tools/scripts to process GADM csv files and populate the `gadm-name` ES index. A document is created for each GADM region at each admin level.  

After the documents are generated, another set of scripts [here](../gadm-bbox/README.md) can update the documents with a bounding box for each region.

### Prerequisite

The following dependencies need to be installed
- Python >= 2.6
- Dependencies: elasticsearch, requests

```
pip install elasticsearch requests
```

This script assumes the `gadm-name` index has already been created using the correct mappings.

### Populate ES

```
ES_URL=... ES_USER=... ES_PWD=... python ingest_gadm_data.py
```
