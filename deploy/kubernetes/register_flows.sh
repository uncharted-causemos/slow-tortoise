#!/bin/bash
if [ "$1" == "-p" ]; then
  source ./prod.env
  echo "Registering flows for proudction environmnet..."
elif [ "$1" == "-s" ]; then
  source ./staging.env
  echo "Registering flows for staging environmnet..."
else
  source ./local.env
  echo "Registering flows for local environmnet..."
fi

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
cid=$(docker run -itd -e PREFECT__SERVER__HOST -e PREFECT__SERVER__PORT -e WM_DATA_PIPELINE_IMAGE -e WM_FLOW_STORAGE_S3_BUCKET_NAME -e WM_RUN_CONFIG_TYPE -e WM_S3_DEST_URL -e WM_S3_DEST_KEY -e WM_S3_DEST_SECRET $WM_DATA_PIPELINE_IMAGE /bin/sh)
# copy aws credetial to the container
docker cp ~/.aws $cid:/root/.aws

docker exec $cid prefect create project --skip-if-exists $PROJECT
docker exec $cid prefect register --project="$PROJECT" --label $WM_RUN_CONFIG_TYPE --path ./flows/data_pipeline.py

# Remove the container
docker stop $cid && docker rm $cid