import json
import sys
import time
import requests

CAUSEMOS_URL = 'http://localhost:3000'

# Usage python prefect_indicator_ingest.py all-indicators-once.json datamart.txt (start (end))

total_start_time = time.time()

with open(sys.argv[1]) as indicators_file:
    indicators = json.loads(indicators_file.read())
num_indicators = len(indicators)

with open(sys.argv[2]) as datamart_file:
    datamart_list = datamart_file.readlines()
num_datamart = len(datamart_list)

if len(sys.argv) > 3:
    start_num = int(sys.argv[3])
else:
    start_num = 0

if len(sys.argv) > 4:
    end_num = int(sys.argv[4])
else:
    end_num = num_indicators

print(f'>> Read {num_indicators} indicators, {num_datamart} datamart IDs.')
for index, indicator_id in enumerate(datamart_list):
    if index < start_num:
        continue
    if index >= end_num:
        break

    indicator_id = indicator_id.rstrip()
    print(f'>> Progess {index}/{num_datamart}. Started {int((time.time() - total_start_time) / 60)} minutes ago.')
    print(f'>> Processing {indicator_id}')
    start_time = time.time()
    try:
        metadata = None
        for x in indicators:
            if x['id'] == indicator_id:
                metadata = x
                break
        if metadata is None:
            print(f'>> Cannot find metadata for {indicator_id}')
            continue

        tags = ['DATAMART']
        prefix = indicator_id[:3]
        if prefix == "UAZ" or prefix == "WDI" or prefix == "FAO":
            metadata['family_name'] = prefix
            tags.append(prefix)
        metadata['tags'] = tags

        r = requests.post(f'{CAUSEMOS_URL}/api/maas/indicators/post-process', json=metadata)
        r.raise_for_status()

        print(f'>> Submitted {indicator_id}')
    except Exception as exc:
        print(f'>> Error processing {indicator_id}')
        print(exc)
    print('>> Finished in %.2f seconds' % (time.time() - start_time))

print('## All completed in %.2f seconds' % (time.time() - total_start_time))
