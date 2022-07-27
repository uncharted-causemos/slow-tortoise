#!/bin/bash

# registration process reads this env var to find the prefect server
# export PREFECT__SERVER__HOST=http://10.65.18.52
# export WM_DASK_SCHEDULER=10.65.18.58:8786
export PREFECT__SERVER__HOST=http://10.65.18.97
export WM_DASK_SCHEDULER=10.65.18.53:8786

# set this to true if images should be pushed to the docker registry as part of the
# registration process - not necessary if testing locally
export WM_PUSH_IMAGE=true

# export WM_DATA_PIPELINE_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline:latest
export WM_DATA_PIPELINE_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline-dev:latest


# Docker used by the agent, built on top of WM_DATA_PIPELINE_IMAGE
export DOCKER_REGISTRY_URL=docker.uncharted.software

# export DOCKER_RUN_IMAGE=worldmodeler/wm-data-pipeline/prefect-datacube-ingest
export DOCKER_RUN_IMAGE=worldmodeler/wm-data-pipeline/prefect-datacube-ingest-dev

PROJECT="Production"

# add calls to register flows here
prefect register --project="$PROJECT" --label wm-prefect-server.openstack.uncharted.software --label docker --path ../flows/data_pipeline.py
