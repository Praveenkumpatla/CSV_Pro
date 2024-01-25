import json
import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
from io import StringIO


def create_file(event):
    # TODO implement
    def read_parquet_files_from_s3(bucket_name, folder_path, keyword, start_date, end_date, destination):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucket_name)
        files = [
            obj.key
            for obj in bucket.objects.filter(Prefix=folder_path)
            if obj.key.endswith(".parquet")
        ]
        # and start_date <= obj.last_modified <= end_date
        # print(event["key1"])
        dfs = []
        for file in files:
            obj = s3.Object(bucket_name, file)
            buffer = io.BytesIO(obj.get()["Body"].read())
            table = pq.read_table(buffer)
            df = table.to_pandas()
            df = df[df["tagname"].str.contains(keyword)]
            df = df[df["date"].between(start_date, end_date)]
            dfs.append(df)
        result = pd.concat(dfs, ignore_index=True)
        # print(result)

        csv_buffer = StringIO()
        result.to_csv(csv_buffer)
        s3_resource = boto3.resource("s3")
        s3_resource.Object(destination, "Result.csv").put(Body=csv_buffer.getvalue())

    # Replace the following variables with your own values
    bucket_name = event["Source"]
    folder_path = event["path"]
    keyword = event["keyword"]
    output_bucket_name = event["destination"]
    start_date = event["startdate"]
    end_date = event["enddate"]

    read_parquet_files_from_s3(
        bucket_name, folder_path, keyword, start_date, end_date, output_bucket_name
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Covert to CSV and stored in destination bucket"),
    }


ins = {
    "Source": "s3-source-bowl",
    "path": "assetid=12345/",
    "keyword": "HRTBT_LHIST",
    "destination": "s3-destination-bowl",
    "startdate": "01.12.2023",
    "enddate": "31.12.2023",
}
create_file(ins)
