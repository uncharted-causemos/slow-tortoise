PROJECT="Production"

# Variables needed for prefect registration
#   DOCKER_REGISTRY_URL=docker.uncharted.software
#   DOCKER_IMAGE=docker.uncharted.software/worldmodeler/wm-data-pipeline
#   DOCKER_IMAGE_VERSION=latest
#   WM_DATA_PIPELINE_IMAGE=$DOCKER_IMAGE:$DOCKER_IMAGE_VERSION
#   DOCKER_RUN_IMAGE=worldmodeler/wm-data-pipeline/prefect-datacube-ingest-dev
#   WM_PUSH_IMAGE=true

# add calls to register flows here
prefect register --project="$PROJECT" --label wm-prefect-server.openstack.uncharted.software --label docker --path ../flows/data_pipeline.py
