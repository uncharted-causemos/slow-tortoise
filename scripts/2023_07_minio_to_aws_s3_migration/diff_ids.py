#!/usr/bin/env python3

import boto3

# This script prints out the difference in data set ids between minio buckets in openstack and the corresponding buckets in aws deployment.
# It expects aws credentials to be in ~/.aws/credentials file.

minio_config = {
    "endpoint_url": "http://10.65.18.29:9000",
    "region_name": "us-east-1",
    "key": "foobar",
    "secret": "foobarbaz",
}

butcket_info = {
    "Analyst": {
        "indicators": {"minio": "analyst-indicators", "aws": "causemos-prod-analyst-indicators"},
        "models": {"minio": "analyst-models", "aws": "causemos-prod-analyst-models"},
    },
    "Modeler": {
        "indicators": {"minio": "modeler-indicators", "aws": "causemos-prod-modeler-indicators"},
        "models": {"minio": "modeler-models", "aws": "causemos-prod-modeler-models"},
    },
}

s3 = boto3.client("s3")

# Print out bucket names
print("====== AWS S3 Buckets =====")
for bucket in s3.list_buckets()["Buckets"]:
    print(bucket["Name"])

minio = boto3.client(
    "s3",
    endpoint_url=minio_config.get("endpoint_url"),
    region_name=minio_config.get("region_name"),
    aws_access_key_id=minio_config.get("key"),
    aws_secret_access_key=minio_config.get("secret"),
)

print("\n====== Minio Buckets =====")
# Print out bucket names
for bucket in minio.list_buckets()["Buckets"]:
    print(bucket["Name"])

for instance in butcket_info.keys():
    print(f"\n====== {instance} Instance =====")
    bucket = butcket_info[instance]
    for data_type in bucket.keys():
        print(f"\n- For {data_type} dataset")
        # print(bucket[data_type])
        minio_bucket_name = bucket[data_type]["minio"]
        aws_bucket_name = bucket[data_type]["aws"]

        minio_ids = []
        aws_ids = []

        minio_res = minio.list_objects_v2(Bucket=minio_bucket_name, Delimiter="/")
        if "CommonPrefixes" in minio_res:
            for prefix in minio_res["CommonPrefixes"]:
                minio_ids.append(prefix["Prefix"].split("/")[0])

        aws_res = s3.list_objects_v2(Bucket=aws_bucket_name, Delimiter="/")
        if "CommonPrefixes" in aws_res:
            for prefix in aws_res["CommonPrefixes"]:
                aws_ids.append(prefix["Prefix"].split("/")[0])

        print(f"# of top level folders in minio bucket, '{minio_bucket_name}': {len(minio_ids)}")
        print(f"# of top level folders in aws bucket, '{aws_bucket_name}': {len(aws_ids)}")
        diff = set(minio_ids).difference(set(aws_ids))
        print(f"# of top level folders missing in aws bucket: {len(diff)}")
        print("Missing items: ")
        print("\n".join(list(diff)))

        print(
            f"\n(Folders only exist in aws bucket but not in minio: {list(set(aws_ids).difference(set(minio_ids)))})\n"
        )
