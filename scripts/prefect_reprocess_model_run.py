import os
import sys
import common

# Reprocess a single model run
# Usage: python prefect_reprocess_model_run.py MODEL_RUN_ID [SELECTED_OUTPUT_TASKS]

ES_CONFIG = {
    "url": os.getenv("ES_URL", "http://localhost:9200"),
    "user": os.getenv("ES_USER", ""),
    "pwd": os.getenv("ES_PWD", ""),
}

CAUSEMOS_CONFIG = {
    "url": os.getenv("CAUSEMOS_URL", "http://localhost:3000"),
    "user": os.getenv("CAUSEMOS_USER", ""),
    "pwd": os.getenv("CAUSEMOS_PWD", ""),
}


def reprocess_model_run(run_id, selected_output_tasks=[]):
    try:
        metadata = common.get_model_run_from_es(run_id, config=ES_CONFIG)
        common.process_model_run(
            metadata,
            selected_output_tasks=selected_output_tasks,
            causemos_api_config=CAUSEMOS_CONFIG,
        )
    except Exception as exc:
        print(">> ERROR reprocessing model run")
        raise


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python prefect_reprocess_model_run.py MODEL_RUN_ID [SELECTED_OUTPUT_TASKS]")
        sys.exit(1)

    run_id = sys.argv[1]

    selected_output_tasks = []
    if len(sys.argv) > 2:
        selected_output_tasks = sys.argv[2].split(",")

    reprocess_model_run(run_id, selected_output_tasks)
