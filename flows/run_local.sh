#!/bin/bash

export WM_DASK_SCHEDULER="" # spawn local cluster
export WM_LOCAL=true # run flow locally
export WM_ELASTIC_URL="" # don't write elastic
export WM_S3_DEST_URL=http://10.65.18.73:9000 # dev s3 cluster
export WM_S3_DEFAULT_INDICATOR_BUCKET=test-indicators
export WM_S3_DEFAULT_MODEL_BUCKET=test-models

python3 tile-v0.py
