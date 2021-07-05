import json
import os
import time
import requests
from dask.distributed import Client
from prefect.executors import DaskExecutor
from prefect.utilities.debug import raise_on_exception

tile_v0 = __import__('tile-v0')

total_start_time = time.time()
with raise_on_exception():
    executor = DaskExecutor(address='tcp://10.65.18.58:8786') # Dask Dashboard: http://10.65.18.58:8787/status
    client = Client('10.65.18.58:8786')
    client.upload_file('tiles_pb2.py')
    client.upload_file('common.py')

    root = os.getcwd()
    jsons_dir = root + '/s3_jsons/'
    indicator_metadata_files = os.listdir(jsons_dir)
    indicator_metadata_files.sort()
    index = 0

    for file_name in indicator_metadata_files:
        index += 1
        print(f'>> Processing {file_name}')
        print(f'>> Progess {index}/{len(indicator_metadata_files)}. Started {int((time.time() - total_start_time) / 60)} minutes ago.')
        start_time = time.time()
        try:
            with open(jsons_dir + file_name) as f:
                metadata = json.loads(f.read())
                state = tile_v0.flow.run(executor=executor, parameters=dict(is_indicator=True, model_id=metadata['id'], run_id='indicator', data_paths=metadata['data_paths']))
                if state.is_successful():
                    print(f'>> Successfully ingested {metadata["id"]}')

                    metadata['data_id'] = metadata['id']
                    metadata['family_name'] = metadata['name']
                    metadata['default_feature'] = metadata['outputs'][0]['name']
                    metadata['type'] = 'indicator'
                    metadata['status'] = 'READY'

                    tags = ['DATAMART']
                    if file_name[:3] == "UAZ" or file_name[:3] == "WDI":
                        tags.append(file_name[:3])
                    metadata['tags'] = tags

                    r = requests.post(f'http://10.64.18.99:9200/data-datacube/_create/{metadata["id"]}', json = metadata)
                    r.raise_for_status()
                    print(r.text)
                else:
                    print(f'>> Ingest failed for {metadata["id"]}')
        except Exception as exc:
            print(f'>> Error processing {metadata["id"]}')
            print(exc)
        print('>> Finished in %.2f seconds' % (time.time() - start_time))

print('## All completed in %.2f seconds' % (time.time() - total_start_time))
