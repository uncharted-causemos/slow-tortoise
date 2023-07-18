import sys
import common

# Reprocess a single indicator
# Usage: python prefect_reprocess_indicator.py INDICATOR_ID [SELECTED_OUTPUT_TASKS]


def reprocess_indicator(indicator_id, selected_output_tasks=[]):
    try:
        metadata = common.get_indicator_metadata_from_dojo(indicator_id)
        common.reprocess_indicator(metadata, selected_output_tasks=selected_output_tasks)
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
        selected_output_tasks = sys.argv[2].split(',')

    reprocess_indicator(indicator_id, selected_output_tasks)
