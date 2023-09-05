#!/bin/bash

# Environment variables needed when registering prefect flow. These variable are used to configure flow.storage and flow.run_config
# Note: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY can be ommited if aws s3 default credentials are in ~/.aws/credentials
#
#   PREFECT__SERVER__HOST - Prefect server host. Tells prefect client where to connect to
#
#   WM_FLOW_STORAGE_S3_BUCKET_NAME - s3 bucket where the flow code will be stored in
#   WM_RUN_CONFIG_TYPE - defines which flow run configuration will be used. Available values are docker, local, and kubernetes 
#   WM_DATA_PIPELINE_IMAGE - The flow will be running in a container with this image
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY 
#

# Provide following variable if aws crenentials are not in ~/.aws/credentials
# export AWS_ACCESS_KEY_ID=
# export AWS_SECRET_ACCESS_KEY=

export WM_RUN_CONFIG_TYPE=kubernetes # docker, local, or kubernetes

export PREFECT__SERVER__HOST=https://apollo.causemos.ai
export PREFECT__SERVER__PORT=443

export DOCKER_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline
export DOCKER_IMAGE_VERSION=latest
export WM_DATA_PIPELINE_IMAGE=$DOCKER_IMAGE:$DOCKER_IMAGE_VERSION

export WM_FLOW_STORAGE_S3_BUCKET_NAME=causemos-prod-prefect-flows

PROJECT="Production"
prefect register --project="$PROJECT" --label $WM_RUN_CONFIG_TYPE --path ../../flows/data_pipeline.py