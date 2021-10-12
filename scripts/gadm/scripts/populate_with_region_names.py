from elasticsearch import Elasticsearch, helpers
import uuid
import csv

# This script is used for processing documents that can be found at the following url pattern:
# https://data.apps.fao.org/catalog/dataset/code-list-gadm36-global-admin-{x}
# Please run `download_gadm_files.py` to download these csv files first

from scripts.gadm.scripts.common_gadm import MAX_GADM_INDEX, get_csv_filename

def populate_es_with_gadm():
    elastic = Elasticsearch("10.65.18.69")

    files = [get_csv_filename(i) for i in range(MAX_GADM_INDEX)]
    columns_to_region = {
        "name_0": "country",
        "name_1": "admin1",
        "name_2": "admin2",
        "name_3": "admin3"
    }

    def generate_id(filtered_row):
        sorted_row = sorted([(key, filtered_row[key]) for key in filtered_row])
        row_as_string = ":".join([f"{key[0]},{key[1]}" for key in sorted_row])
        return str(uuid.uuid3(uuid.NAMESPACE_DNS, row_as_string))

    for file in files:
        with open(file) as f:
            list_of_rows = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]
            filtered_rows = []
            ids = set()
            for row in list_of_rows:
                new_row = {}
                for column in columns_to_region:
                    if column in row:
                        new_row[columns_to_region[column]] = row[column]
                generated_id = generate_id(new_row)
                ids.add(generated_id)
                new_row["_id"] = generated_id
                filtered_rows.append(new_row)
            if len(ids) != len(filtered_rows):
                print("The number of unique ids generated do not match the number of documents inserted.")
            try:
                # make the bulk call, and get a response
                response = helpers.bulk(elastic, filtered_rows, index="gadm-name")
                print("\nRESPONSE:", response)
            except Exception as e:
                print("\nERROR:", e)
                print(filtered_rows)