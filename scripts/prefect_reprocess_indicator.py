import os
import sys
import common

# Reprocess a single indicator
# Usage: python prefect_reprocess_indicator.py INDICATOR_ID [SELECTED_OUTPUT_TASKS]

DOJO_CONFIG = {
    "url": os.getenv("ES_URL", "https://dojo-test.com"),
    "usr": os.getenv("DOJO_USER", ""),
    "pwd": os.getenv("DOJO_PWD", ""),
}

CAUSEMOS_CONFIG = {
    "url": os.getenv("CAUSEMOS_URL", "http://localhost:3000"),
    "usr": os.getenv("CAUSEMOS_USER", ""),
    "pwd": os.getenv("CAUSEMOS_PWD", ""),
}


def reprocess_indicator(indicator_id, selected_output_tasks=[]):
    try:
        metadata = common.get_indicator_metadata_from_dojo(indicator_id)
        common.process_indicator(
            metadata,
            selected_output_tasks=selected_output_tasks,
            causemos_api_config=CAUSEMOS_CONFIG,
        )
    except Exception as exc:
        print(">> ERROR reprocessing indicator")
        raise


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python prefect_reprocess_indicator.py INDICATOR_ID [SELECTED_OUTPUT_TASKS]")
        sys.exit(1)

    indicator_id = sys.argv[1]

    selected_output_tasks = []
    if len(sys.argv) > 2:
        selected_output_tasks = sys.argv[2].split(",")

    reprocess_indicator(indicator_id, selected_output_tasks)
