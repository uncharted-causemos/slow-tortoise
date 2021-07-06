import json
import sys
import os
import time
import requests
from dask.distributed import Client
from prefect.executors import DaskExecutor, LocalDaskExecutor
from prefect.utilities.debug import raise_on_exception

tile_v0 = __import__('tile-v0')

DASK_SCHEDULER = os.getenv('WM_DASK_SCHEDULER', '10.65.18.58:8786')  # Dask Dashboard: http://10.65.18.58:8787/status
ELASTIC_URL = os.getenv('WM_ELASTIC_URL', 'http://10.64.18.99:9200')

total_start_time = time.time()
with raise_on_exception():
    if not DASK_SCHEDULER:
        executor = LocalDaskExecutor()
    else:
        executor = DaskExecutor(address=DASK_SCHEDULER)
        client = Client(DASK_SCHEDULER)
        client.upload_file('tiles_pb2.py')
        client.upload_file('common.py')

    jsons_dir = f'{os.getcwd()}/s3_jsons/'
    indicator_metadata_files = os.listdir(jsons_dir)
    indicator_metadata_files.sort()
    num_indicators = len(indicator_metadata_files)

    for index, file_name in enumerate(indicator_metadata_files):
        print(f'>> Progess {index}/{num_indicators}. Started {int((time.time() - total_start_time) / 60)} minutes ago.')
        print(f'>> Processing {file_name}')
        start_time = time.time()
        try:
            with open(f'{jsons_dir}{file_name}') as f:
                metadata = json.loads(f.read())
                state = tile_v0.flow.run(
                    executor=executor,
                    parameters=dict(
                        is_indicator=True,
                        model_id=metadata['id'],
                        run_id='indicator',
                        data_paths=metadata['data_paths'],
                    ),
                )
                if state.is_successful():
                    print(f'>> Successfully ingested {metadata["id"]}')

                    metadata['data_id'] = metadata['id']
                    metadata['family_name'] = metadata['name']
                    metadata['default_feature'] = metadata['outputs'][0]['name']
                    metadata['type'] = 'indicator'
                    metadata['status'] = 'READY'

                    tags = ['DATAMART']
                    prefix = file_name[:3]
                    if prefix == "UAZ" or prefix == "WDI":
                        tags.append(prefix)
                    metadata['tags'] = tags

                    r = requests.post(f'{ELASTIC_URL}/data-datacube/_create/{metadata["id"]}', json=metadata)
                    r.raise_for_status()
                    print(r.text)
                else:
                    print(f'>> Ingest failed for {metadata["id"]}')
        except Exception as exc:
            print(f'>> Error processing {metadata["id"]}')
            print(exc)
        print('>> Finished in %.2f seconds' % (time.time() - start_time))

print('## All completed in %.2f seconds' % (time.time() - total_start_time))
