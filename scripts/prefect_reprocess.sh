#!/bin/bash

print_usage() {
  echo "Usage: $0 [-m] [-t OUTPUT_TASKS] ENV_FILE_PATH DATA_ID [DATA_ID ...]"
  echo
  echo "Re-process the ouput data for provided DATA_ID. DATA_ID is either dataset id for indicator dataset or model run id for model run."
  echo 
  echo "Options:"
  echo "  -t OUTPUT_TASKS     Set selected output tasks to be ran. OUTPUT_TASKS is a comma seperated list of output tasks. e.g. 'compute_regional_stats,compute_regional_timeseries' "
  echo "  -m                  Indicates if reprocessing model runs and provided DATA_ID is model run id from Causemos ES. If the flag is not provided, by default, It reprocesses the indicator dataset with metadata fetched from DOJO"
  echo "  -h                  Print usage"
  echo
  echo "Examples:"
  echo "  $0 ./env/analyst.env 7ae6001f-2227-4e1b-8ac1-10bd012325e2 b9232bf2-b8a3-4718-bb94-08f0254415d9" # 
  echo "    Run with multiple dataset ids"
  echo
  echo "  $0 ./env/analyst.env \$(< datasetids.txt)"
  echo "    Run with a file, datasetids.txt containing list of dataset ids"
  echo
  echo "  $0 -t compute_regional_stats,compute_regional_timeseries ./env/analyst.env 7ae6001f-2227-4e1b-8ac1-10bd012325e2" # 
  echo "    Run with selected output tasks"
  echo 
  echo "  $0 -m -t compute_regional_stats,compute_regional_timeseries ./env/analyst.env 01ad71a7-7c66-4738-92d9-28581770c73a" # 
  echo "    Reprocess a model run with model run id"
}

data_output_type="indicators"
run_script="prefect_reprocess_indicator.py"
selected_output_tasks=""

# Parse command-line arguments
while getopts ":t:mh" opt; do
  case $opt in
    t)
      selected_output_tasks=$OPTARG
      ;;
    m)
      data_output_type="model runs"
      run_script=prefect_reprocess_model_run.py
      ;;
    h)
      print_usage
      exit 0
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      print_usage
      exit 1
      ;;
  esac
done
# Shift the option arguments to skip past them. Now $1 refers to the first arugmet, ENV_FILE_PATH
shift "$((OPTIND - 1))"

if [ $# -lt 2 ]; then
  echo "Error: Required arguments missing."
  print_usage
  exit 1
fi

source $1 
echo "Running Configuration"
echo "Note: ES_URL is needed for fetching model run metadata and DOJO_URL is for fetching indicators metadata"
echo "CAUSEMOS_URL: $CAUSEMOS_URL"
echo "DOJO_URL: $DOJO_URL"
echo "ES_URL: $ES_URL"
echo
echo "Request processing $data_output_type ..."

# iterate from the second argument
shift
for i
do 
  echo $i
  python3 $run_script $i $selected_output_tasks
done
