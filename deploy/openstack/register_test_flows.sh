#!/bin/bash
source ./prod.env
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

export PREFECT__SERVER__HOST=http://10.65.18.57
export WM_FLOW_STORAGE_S3_BUCKET_NAME=causemos-prod-prefect-flows-dev
export WM_RUN_CONFIG_TYPE=docker
export WM_DATA_PIPELINE_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline:latest

PROJECT="Tests"
prefect register --project="$PROJECT" --label wm-prefect-server.openstack.uncharted.software --label $WM_RUN_CONFIG_TYPE --path ../../flows/test/flow_test.py
prefect register --project="$PROJECT" --label wm-prefect-server.openstack.uncharted.software --label $WM_RUN_CONFIG_TYPE --path ../../flows/test/dask_flow_test.py

