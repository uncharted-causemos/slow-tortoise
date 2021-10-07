import requests
import json

# elastic search output URL
ELASTIC_URL = "http://10.65.18.69:9200"
CAUSEMOS_URL = "http://localhost:3000"

INDICATOR_IDS = [
    "39f7959d-a63e-4db4-a54d-24c66184cf82",
    "db695e0b-69c6-4914-a25d-190d6283a82a",
    "2371b4a4-d664-4cfb-bebe-7b6937398b7d",
    "8a17ba05-b1f5-4fc6-8ce5-bc1642caeffb",
    "32bb31e1-94ee-4f26-84bc-548ce516a520",
    "1de00996-ddd1-4f26-804f-6b5050574997",
    "dbabd6f7-982a-44e9-8001-06188e367e35",
    "c6c56d3f-c22e-4b43-b1bc-83585704e6be",
    "70c673d2-6f01-4941-969e-e579d8712b2a",
    "b656be17-51e1-4c83-b6b9-41e34d014984",
    "8077f0b2-c15c-4b0d-af81-91090d82e5f4",
    "e1d79bb5-eace-4407-9184-35b040b099a8",
    "19133c16-9b1a-43dc-84b8-fc372c50d34a",
    "bf8a23c8-c2f9-4523-93c2-b645d5a2c7d5",
    "928ddd70-0266-49d7-ba10-704389eaf1b0",
    "9908bbfb-5ccc-4d29-afe0-aaa9b1c873aa",
    "e8f6aa91-2b52-4daa-b15b-ac1deac63940",
    "9437b74a-3641-4d6e-a14d-1188cd660393",
    "09ab9d43-a0bb-4985-9461-54183c05cabf",
    "d25d510c-4ae5-4393-a952-d6cd6669f917",
    "08da0476-6749-40b0-89e1-bd9dd11e2fd2",
    "709abbb4-9219-497d-9d31-e12bab1043c5",
    "101e8e84-d759-4d14-92c3-cd430bade617",
    "126560a1-0e51-43c5-939e-179501abb9bb",
    "7cf8d681-b941-4d58-aac8-53d35d65cb70",
    "e15d0e45-f203-434e-80b8-d2c16aae9e62",
    "7de5f6a3-0bd7-4bed-856a-7073f258f5fd",
    "33f37bd8-5037-4f08-9d28-2f7b909147a3",
    "9363942e-bdc2-40cb-9026-fca29bf581fa",
    "1981ffb6-0e07-4c46-acba-2e6e35758438",
    "9758b79e-6800-4595-b075-e0b8cb5c108a",
    "633230aa-450b-4ca2-96eb-a0dc3262c3d1",
    "4494cd84-e8dd-48e2-8b06-6b8a22a4cc64",
    "93fce4ce-1e7e-4516-9c7b-7e7e7ef9f2ac",
    "9e147b4b-495b-4451-ade7-3329d7f43e7e",
    "9c2288ce-252d-412d-8963-add81c8e1aef",
    "45088141-4601-4220-b1de-498911f41096",
    "4071153e-9257-4b72-918f-3703f1e3d286",
    "afcf4673-60ab-4409-aea8-beac76023aff",
    "07cc31fc-09db-41a2-aa91-aed193970a10",
    "7dbba64c-d0d2-44a9-aed5-d3c746b78605",
]

# broken indicators
INDICATOR_IDS2 = [
    #'c0a5964c-fb0f-48c1-8031-83b7ec007cc8',
    #'3b53b2a1-385b-43f7-9689-9aabaf35643e'
]

TEST = ["7dbba64c-d0d2-44a9-aed5-d3c746b78605"]

qualifiers_to_ignore_set = {
    "timestamp",
    "lat",
    "lng",
    "feature",
    "value",
    "country",
    "admin1",
    "admin2",
    "admin3",
    "admin4",
}

headers = {"Accept": "application/json", "Content-type": "application/json"}


def get_qualifier_values(data_id, feature_name, qualifier_name, timestamp):
    url = (
        CAUSEMOS_URL
        + "/api/maas/output/qualifier-data?data_id="
        + data_id
        + "&run_id=indicator&feature="
        + feature_name
        + "&resolution=month&temporal_agg=mean&spatial_agg=mean&timestamp="
        + str(timestamp)
        + "&qlf[]="
        + qualifier_name
    )
    response = requests.get(url)

    if response:
        results = response.json()
        if results[0] and results[0]["options"]:
            options = results[0]["options"]
            options_values = [option["name"] for option in options]
            return options_values
    return []


def getDefaultTimestamp(data_id, feature_name):
    url = (
        CAUSEMOS_URL
        + "/api/maas/output/timeseries?data_id="
        + data_id
        + "&run_id=indicator&feature="
        + feature_name
        + "&resolution=month&temporal_agg=mean&spatial_agg=mean"
    )
    response = requests.get(url)
    # print(url)
    results = response.json()
    return results[0]["timestamp"]


def update_metadata(doc_id, indx):
    url = ELASTIC_URL + "/data-datacube/_update_by_query"
    print(doc_id)
    query = json.dumps(
        {
            "script": {
                "source": f"ctx._source.qualifier_outputs[{indx}].is_visible = true",
                "lang": "painless",
            },
            "query": {"bool": {"must": [{"match": {"id": doc_id}}]}},
        }
    )
    res = requests.post(url, data=query, headers=headers)
    print(res.json())


############################################
## replace TEST with INDICATOR_IDS to update the list of all indicators
############################################
for doc_id in TEST:
    print("----- fetching for:", doc_id, "------")
    query = json.dumps({"query": {"match": {"data_id": doc_id}}})
    response = requests.get(ELASTIC_URL + "/data-datacube/_search", data=query, headers=headers)
    results = response.json()
    indicator_list = results["hits"]["hits"]
    for ind in indicator_list:
        qualifier_outputs = ind["_source"]["qualifier_outputs"]
        qualifier_id = ind["_source"]["id"]
        if qualifier_outputs:
            feature_name = ind["_source"]["default_feature"]
            valid_qualifier_outputs = list(
                filter(lambda x: x["name"] not in qualifiers_to_ignore_set, qualifier_outputs)
            )
            if len(valid_qualifier_outputs) > 0:
                timestamp = getDefaultTimestamp(doc_id, feature_name)
                for qualifer_output in valid_qualifier_outputs:
                    qualifier_name = qualifer_output["name"]
                    qualifier_index = -1
                    qualifier_output_names = list(map(lambda x: x["name"], qualifier_outputs))
                    if qualifier_name in qualifier_output_names:
                        qualifier_index = qualifier_output_names.index(qualifier_name)
                    qualifier_values = get_qualifier_values(
                        doc_id, feature_name, qualifier_name, timestamp
                    )
                    qualifier_values_count = len(qualifier_values)
                    if qualifier_values_count <= 20:
                        # simple heuristic: if a qualifier has less than 20 choices, then make it visible by default
                        print(
                            "updating: "
                            + qualifier_id
                            + " for qualifier: "
                            + qualifier_name
                            + " indexed at "
                            + str(qualifier_index)
                        )
                        update_metadata(qualifier_id, qualifier_index)

    print("----- end -----")
