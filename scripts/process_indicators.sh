#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Usage: $0 {env_file_path} {dataset id} [...{dataset id}]"
  echo "Usage with multiple dataset ids:" 
  echo "$0 ./env/analyst.env 7ae6001f-2227-4e1b-8ac1-10bd012325e2 b9232bf2-b8a3-4718-bb94-08f0254415d9"
  echo "Usage with a file, datasetids.txt containing list of dataset ids"
  echo "$0 ./env/analyst.env" '$(< datasetids.txt)'
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
  python3 prefect_reprocess_indicator.py $i
done
