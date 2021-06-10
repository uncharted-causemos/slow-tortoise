#!/bin/bash

# agent reads this env var to find the prefect server
export PREFECT__SERVER__HOST=http://10.65.18.52

# set this to true if images should be pushed to the docker registry as part of the
# registration process - not necessary if testing locally
export WM_PUSH_IMAGE=true

PROJECT="project"

# add calls to register flows here
prefect register --project="$PROJECT" --label wm-prefect-server.openstack.uncharted.software --path ../../flows/tile-v0.py
