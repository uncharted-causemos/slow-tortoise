#!/usr/bin/env python3

import boto3

# It finds out and grabs all data paths of the newly generated stats files in Minio buckets and copy them over to corresponding existing aws buckets.
# It ignores the files of the dataset that only exist in minio buckets to prevent generating new directory with partial files in aws buckets.
# Aws credentials are expected to be in ~/.aws/credentials file.

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

s3_client = boto3.client("s3")

minio_client = boto3.client(
    "s3",
    endpoint_url=minio_config.get("endpoint_url"),
    region_name=minio_config.get("region_name"),
    aws_access_key_id=minio_config.get("key"),
    aws_secret_access_key=minio_config.get("secret"),
)


def list_paths(client, bucket, prefix=""):
    res = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    items = []
    if "CommonPrefixes" in res:
        for prefix in res["CommonPrefixes"]:
            prefix = prefix["Prefix"]
            items.append(prefix)
    return items


def copy_file(source_client, source_bucket, dest_client, dest_bucket, file_path):
    try:
        print(f"Copying '{file_path}' from '{source_bucket}' to '{dest_bucket}' bucket ...")
        res = source_client.get_object(Bucket=source_bucket, Key=file_path)
        content = res["Body"].read()
        dest_client.put_object(Bucket=dest_bucket, Key=file_path, Body=content)
        return 1
    except Exception as e:
        print(f"Error copying '{file_path}' from bucket '{source_bucket}' to '{dest_bucket}':\n", e)
        return 0


count = 0
for instance in butcket_info.keys():
    print(f"\n====== {instance} Instance =====")
    bucket = butcket_info[instance]
    for data_type in bucket.keys():
        print(f"\n- For {data_type} dataset")

        minio_bucket_name = bucket[data_type]["minio"]
        aws_bucket_name = bucket[data_type]["aws"]

        # Get data id paths that exist in both minio and aws. We don't want to create new top level directories (with only stat files) in aws buckets.
        data_id_paths_minio = list_paths(minio_client, minio_bucket_name)
        data_id_paths_aws = list_paths(s3_client, aws_bucket_name)
        data_id_paths = list(set(data_id_paths_minio).intersection(set(data_id_paths_aws)))
        for data_id_path in data_id_paths:
            run_id_paths = list_paths(minio_client, minio_bucket_name, prefix=data_id_path)
            for run_id_path in run_id_paths:
                for tmp_res in ["month", "year"]:
                    feature_paths = list_paths(
                        minio_client, minio_bucket_name, prefix=run_id_path + tmp_res + "/"
                    )
                    for feature_path in feature_paths:
                        admin_level_paths = list_paths(
                            minio_client, minio_bucket_name, prefix=feature_path + "regional/"
                        )
                        for admin_level_path in admin_level_paths:
                            stat_file_path = admin_level_path + "stats/default/extrema.json"
                            count += copy_file(
                                minio_client,
                                minio_bucket_name,
                                s3_client,
                                aws_bucket_name,
                                stat_file_path,
                            )

print(f"{count} files has been successfully copied over to aws")
