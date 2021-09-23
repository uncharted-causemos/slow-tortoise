import sys
import common

# Usage python prefect_reprocess_model_run.py {run id}

def reprocess_model_run(run_id):
  try:
    metadata = common.get_model_run_from_es(run_id)
    common.process_model_run(metadata)
  except Exception as exc:
    print(exc)
    sys.exit(1)

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print('Usage: python prefect_reprocess_model_run.py {run id}')
    sys.exit(1)

  run_id = sys.argv[1]
  reprocess_model_run(run_id)
