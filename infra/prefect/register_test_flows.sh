#!/bin/bash

# needed to point registration process to external apollo server
# export PREFECT__SERVER__HOST=http://10.65.18.52

prefect register --project="test" --path ../../flows/flow_test.py
prefect register --project="test" --path ../../flows/dask_flow_test.py

