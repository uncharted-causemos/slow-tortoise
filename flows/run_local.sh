#!/bin/bash
export WM_DASK_SCHEDULER="" # spawn local cluster eg. tcp://localhost:8786. Local dask cluster can be set up by running `dask scheduler` in a terminal and `dask worker tcp://127.0.0.1:8786` in a second terminal
export WM_LOCAL=true # run flow locally
export WM_S3_DEST_URL=http://10.65.18.73:9000 # dev s3 cluster
export WM_S3_DEST_KEY="foobar"
export WM_S3_DEST_SECRET="foobarbaz" 
export WM_DEST_TYPE=file # set to 's3' to write to WM_S3_DEST_URL
export WM_DEBUG_TILE=true # save tile to csv format for debugging if true
export WM_S3_DEFAULT_INDICATOR_BUCKET=test-indicators
export WM_S3_DEFAULT_MODEL_BUCKET=test-models
export WM_DATA_PIPELINE_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline:latest

python3 data_pipeline.py
