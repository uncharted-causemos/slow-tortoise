#!/bin/bash
export PREFECT__SERVER__HOST=http://10.65.18.52
prefect register --project="project" --path ../../flows/flow_test.py

