import sys
import common

# Usage python prefect_reprocess_indicator.py {indicator id}

def reprocess_indicator(indicator_id):
  try:
    metadata = common.get_indicator_metadata_from_dojo(indicator_id)
    common.delete_indicator_from_es(indicator_id)
    common.reprocess_indicator(metadata)
  except Exception as exc:
    print(exc)
    sys.exit(1)

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print('Usage: python prefect_reprocess_indicator.py {indicator id}')
    sys.exit(1)

  indicator_id = sys.argv[1]

  reprocess_indicator(indicator_id)
