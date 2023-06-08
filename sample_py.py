import boto3
import requests
import pytz
import datetime
import json
import base64
import hmac
import hashlib
from collections import OrderedDict

s3_bucket_name = "sph-s3-comp-bucket"
minio_bucket_name = "sph-s3-comp-bucket"

object_name = "1.txt"
local_object_name = "dl-1.txt"


s3_client = boto3.client('s3',
                      aws_access_key_id='TBD',
                      aws_secret_access_key='TBD',
                      config=boto3.session.Config(signature_version='s3v4'),
                      verify=True
                      )

minio_client = boto3.client('s3',
                         endpoint_url='http://localhost:22000',
                         aws_access_key_id='minio',
                         aws_secret_access_key='minio123',
                         config=boto3.session.Config(signature_version='s3v4'),
                         verify=False
                         )


# s3_client.download_file(s3_bucket_name, object_name, local_object_name)
# minio_client.download_file(s3_bucket_name, object_name, local_object_name)


s3_head_res = s3_client.head_object(Bucket=s3_bucket_name, Key=object_name)
print("S3", s3_head_res)

minio_head_res = minio_client.head_object(Bucket=s3_bucket_name, Key=object_name)
print("MinIO", minio_head_res)