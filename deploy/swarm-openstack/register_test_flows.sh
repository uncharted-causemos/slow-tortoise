#!/bin/bash
source ./dev.env
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

# Create a docker container with the data pipeline image and run prefect register command with the flow codes inside the container.
cid=$(docker run -itd -e PREFECT__SERVER__HOST -e WM_DATA_PIPELINE_IMAGE -e WM_FLOW_STORAGE_S3_BUCKET_NAME -e WM_RUN_CONFIG_TYPE -e WM_S3_DEST_URL -e WM_S3_DEST_KEY -e WM_S3_DEST_SECRET $WM_DATA_PIPELINE_IMAGE /bin/sh)

docker cp ~/.aws $cid:/root/.aws # copy aws credetial to the container
docker exec $cid prefect create project --skip-if-exists Tests
docker exec $cid prefect register --project="Tests" --label wm-prefect-server.openstack.uncharted.software --label $WM_RUN_CONFIG_TYPE --path ./flows/data_pipeline.py
docker exec $cid prefect register --project="Tests" --label wm-prefect-server.openstack.uncharted.software --label $WM_RUN_CONFIG_TYPE --path ./flows/test/flow_test.py
docker exec $cid prefect register --project="Tests" --label wm-prefect-server.openstack.uncharted.software --label $WM_RUN_CONFIG_TYPE --path ./flows/test/dask_flow_test.py

# Remove the container
docker stop $cid && docker rm $cid

