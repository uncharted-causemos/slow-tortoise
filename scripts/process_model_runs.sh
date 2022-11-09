#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Usage: $0 {env_file_path} {model run id} [...{model run id}]"
  echo "Usage with multiple model run ids:" 
  echo "$0 ./env/modeler.env 01ad71a7-7c66-4738-92d9-28581770c73a 5b611560-7873-4e7b-94da-ae199d788167"
  echo "Usage with a file, runids.txt containing list of model run ids"
  echo "$0 ./env/modeler.env" '$(< runids.txt)'
  exit 1
fi

source $1 
echo "Running Configuration"
echo "DOJO_URL: $DOJO_URL"
echo "CAUSEMOS_URL: $CAUSEMOS_URL\n"

shift # shift the first argument
# iterate from the second argument
for i
do 
  echo $i
  python3 prefect_reprocess_model_run.py $i
done
