PROJECT="Production"

# Variables needed for prefect registration
#   DOCKER_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline
#   DOCKER_IMAGE_VERSION=latest
#   WM_DATA_PIPELINE_IMAGE=$DOCKER_IMAGE:$DOCKER_IMAGE_VERSION

# add calls to register flows here
prefect register --project="$PROJECT" --label wm-prefect-server.openstack.uncharted.software --label docker --path ../flows/data_pipeline.py
