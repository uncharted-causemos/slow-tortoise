#!/bin/bash

export WM_DASK_SCHEDULER="" # spawn local cluster
export WM_LOCAL=true # run flow locally
export WM_PUSH_IMAGE=false # don't push to registry
export WM_S3_DEST_URL=http://10.65.18.73:9000 # dev s3 cluster
export WM_DEST_TYPE=s3
export WM_S3_DEFAULT_INDICATOR_BUCKET=test-indicators
export WM_S3_DEFAULT_MODEL_BUCKET=test-models
export WM_DATA_PIPELINE_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline:latest

python3 data_pipeline.py
