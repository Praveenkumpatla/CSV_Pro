import json
import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from io import StringIO


def create_file(event):
    # TODO implement
    def read_parquet_files_from_s3(bucket_name, folder_path, keyword, start_date, end_date, destination,to):
        s3 = boto3.resource("s3",aws_access_key_id='AKIARI6IVDTZZFYHTHNQ', aws_secret_access_key='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')
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
            #df = df[df["date"].between(start_date, end_date)]
            dfs.append(df)
        result = pd.concat(dfs, ignore_index=True)
        #print(result)

        csv_buffer = StringIO()
        result.to_csv(csv_buffer)
        s3_resource = boto3.resource("s3",aws_access_key_id='AKIARI6IVDTZZFYHTHNQ', aws_secret_access_key='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')
        s3_resource.Object(destination, "Result.csv").put(Body=csv_buffer.getvalue())

        # Create a multipart message
        message = MIMEMultipart()
        message['Subject'] = 'Parquet file data'
        message['From'] = 'kpraveen.tech@gmail.com'
        message['To'] = ', '.join(to)

        message.attach(MIMEText('Dear User,\n\nPlease find the attached CSV file.\n\nThis is a system generated e-mail.', 'plain'))
        # Convert the buffer to a string
        csv_string = csv_buffer.getvalue()

        part = MIMEApplication(csv_string.encode('utf-8'), Name='Data.csv')
        part['Content-Disposition'] = f'attachment; filename="Data.csv"'
        message.attach(part)

        # Create SMTP object
        smtp_obj = smtplib.SMTP('smtp.gmail.com', 587)
        # Start TLS for security
        smtp_obj.starttls()
        # Login to the server
        smtp_obj.login('kpraveen.tech@gmail.com', "wvqz joxm uxqs thxq")
        # Convert the message to a string and send it
        smtp_obj.sendmail(message['From'], to, message.as_string())
        # Close the SMTP object
        smtp_obj.quit()

    # Replace the following variables with your own values
    bucket_name = event["Source"]
    folder_path = event["path"]
    keyword = event["keyword"]
    output_bucket_name = event["destination"]
    start_date = event["startdate"]
    end_date = event["enddate"]
    receiver = event["receipt"]

    read_parquet_files_from_s3(
        bucket_name, folder_path, keyword, start_date, end_date, output_bucket_name,receiver
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
    "receipt":["praveen100108@gmail.com","kumpatlas@icloud.com"]
}
create_file(ins)
