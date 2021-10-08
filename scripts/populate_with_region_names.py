from elasticsearch import Elasticsearch, helpers
import uuid
import csv

elastic = Elasticsearch("10.65.18.69")

total_names = 4
files = [f"./gadm/gadm36_{i}.csv" for i in range(total_names)]
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
        for row in list_of_rows:
            new_row = {}
            for column in columns_to_region:
                if column in row:
                    new_row[columns_to_region[column]] = row[column]
            filtered_rows.append(new_row)
        for filtered_row in filtered_rows:
            filtered_row["_id"] = generate_id(filtered_row)
        try:
            # make the bulk call, and get a response
            response = helpers.bulk(elastic, filtered_rows, index="gadm-name")
            print ("\nRESPONSE:", response)
        except Exception as e:
            print("\nERROR:", e)
            print(filtered_rows)