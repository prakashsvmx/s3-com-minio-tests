import base64
import datetime
import hashlib
import hmac
import json
import random
import string
from collections import OrderedDict

import pytz as pytz

from s3_clients import get_s3_client, get_minio_client, S3_BUCKET_NAME, OBJECT_NAME, LOCAL_OBJECT_NAME

s3_client = get_s3_client()
minio_client = get_minio_client()
bucket_name = S3_BUCKET_NAME
object_name = OBJECT_NAME
local_object_name = LOCAL_OBJECT_NAME

# s3_client.download_file(bucket_name, object_name, local_object_name)
# minio_client.download_file(bucket_name, object_name, local_object_name)

def _create_simple_tagset(count):
    tagset = []
    for i in range(count):
        tagset.append({'Key': str(i), 'Value': str(i)})

    return {'TagSet': tagset}

def _make_random_string(size):
    return ''.join(random.choice(string.ascii_letters) for _ in range(size))


def test_put_max_tags(client, max_tags):
    key = bucket_name

    input_tagset = _create_simple_tagset(max_tags)

    client.put_object(Bucket=bucket_name, Key=key, Body='tom_body')
    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    put_tag_res = response['ResponseMetadata']['HTTPStatusCode']
    print("PUT Status::", put_tag_res)

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    print("Status", put_tag_res,"", response['TagSet'])
    # print(input_tagset['TagSet'])


# print("MinIO", test_put_max_tags(minio_client, 10))
# print("S3", test_put_max_tags(s3_client, 10))

# print("MinIO", test_put_max_tags(minio_client, 11))
# botocore.exceptions.ClientError: An error occurred (BadRequest) when calling the PutObjectTagging operation: Tags cannot be more than 10

print("S3", test_put_max_tags(s3_client, 11))
#botocore.exceptions.ClientError: An error occurred (BadRequest) when calling the PutObjectTagging operation: Object tags cannot be greater than 10

