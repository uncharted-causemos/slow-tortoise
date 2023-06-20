#!/bin/bash

# agent reads this env var to find the prefect server
export WM_DATA_PIPELINE_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline:latest
export PREFECT__SERVER__HOST=http://10.65.18.57

PROJECT="Tests"
prefect register --project="$PROJECT" --label wm-prefect-server.openstack.uncharted.software --label docker --path ../flows/test/flow_test.py
prefect register --project="$PROJECT" --label wm-prefect-server.openstack.uncharted.software --label docker --path ../flows/test/dask_flow_test.py

