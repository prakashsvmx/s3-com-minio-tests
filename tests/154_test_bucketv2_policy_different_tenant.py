import json

import botocore as botocore

from s3_clients import get_s3_client, get_minio_client, S3_BUCKET_NAME, OBJECT_NAME, LOCAL_OBJECT_NAME

s3_client = get_s3_client()
minio_client = get_minio_client()
bucket_name = S3_BUCKET_NAME
object_name = OBJECT_NAME
local_object_name = LOCAL_OBJECT_NAME


# s3_client.download_file(bucket_name, object_name, local_object_name)
# minio_client.download_file(bucket_name, object_name, local_object_name)

def test_bucketv2_policy_different_tenant(client, test_name):
    key = 'asdf'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    # resource1 = "arn:aws:s3::*:" + bucket_name
    # resource2 = "arn:aws:s3::*:" + bucket_name + "/*"

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"

    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:ListBucket",
                "Resource": [
                    "{}".format(resource1),
                    "{}".format(resource2)
                ]
            }]
        })

    response = client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, response_code)


def test_bucket_policy_different_tenant(client, test_name):
    key = 'asdf'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    resource1 = "arn:aws:s3::*:" + bucket_name
    resource2 = "arn:aws:s3::*:" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:ListBucket",
                "Resource": [
                    "{}".format(resource1),
                    "{}".format(resource2)
                ]
            }]
        })

    response = client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, response_code)


def test_object_set_get_unicode_metadata(client, test_name):
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata={
        # "x-amz-meta-meta1": u"Hello World\xe9",
        # "x-amz-meta-nonascii": "ÄMÄZÕÑ S3",
        "x-amz-meta-ascii-1": "=?UTF-8?B?w4PChE3Dg8KEWsODwpXDg8KRIFMz?=",
        "x-amz-meta-ascii": "AMAZONS3"
    })

    response = client.get_object(Bucket=bucket_name, Key='foo')
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    metadata_res = response['Metadata']

    print(test_name, response_code, metadata_res, ":Actual", u"Hello World\xe9")


def test_lifecycle_expiration_date(client, test_name):
    keys = ['past/foo', 'future/bar']
    for key in keys:
        obj = client.put_object(Bucket=bucket_name, Body=key, Key=key)

    rules = [{'ID': 'rule1', 'Expiration': {'Date': '2015-01-01'}, 'Prefix': 'past/', 'Status': 'Enabled'},
             {'ID': 'rule2', 'Expiration': {'Date': '2030-01-01'}, 'Prefix': 'future/', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}

    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, response_code, response)

    response = client.list_objects(Bucket=bucket_name)
    expire_objects = response['Contents']

    print(test_name, expire_objects, )


def test_lifecycle_noncur_expiration(client, test_name):
    # not checking the object contents on the second run, because the function doesn't support multiple checks

    rules = [
        {'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 2}, 'Prefix': 'test1/', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, response_code, )


def test_lifecyclev2_expiration(client, test_name):
    rules = [{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'expire1/', 'Status': 'Enabled'},
             {'ID': 'rule2', 'Expiration': {'Days': 5}, 'Prefix': 'expire3/', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, response_code)


def test_bucket_policy_set_condition_operator_end_with_IfExists(client, test_name):
    key = 'foo'
    client.put_object(Bucket=bucket_name, Key=key)
    policy = '''{
      "Version":"2012-10-17",
      "Statement": [{
        "Sid": "Allow Public Access to All Objects",
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Condition": {
                    "StringLike": {
                        "aws:Referer": "http://www.example.com/*"
                    }
                },
        "Resource": "arn:aws:s3:::%s/*"
      }
     ]
    }''' % bucket_name
    # boto3.set_stream_logger(name='botocore')
    response = client.put_bucket_policy(Bucket=bucket_name, Policy=policy)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, response_code)



def test_lifecycle_expiration_days0(client, test_name):
    rules=[{'Expiration': {'Days': 0}, 'ID': 'rule1', 'Prefix': 'days0/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    print(test_name, "Start",)

    # days: 0 is legal in a transition rule, but not legal in an
    # expiration rule
    response_code = ""
    try:
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        print(test_name, response_code)
    except botocore.exceptions.ClientError as e:
        response_code = e.response['Error']['Code']
        print(test_name, response_code)


def test_lifecycle_expiration_header_tags_head(client, test_name):
    lifecycle={
        "Rules": [
            {
                "Filter": {
                    "Tag": {"Key": "key1", "Value": "tag1"}
                },
                "Status": "Enabled",
                "Expiration": {
                    "Days": 1
                },
                "ID": "rule1"
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    print(test_name, "1", response['ResponseMetadata']['HTTPStatusCode'])

    key1 = "obj_key1"
    body1 = "obj_key1_body"
    tags1={'TagSet': [{'Key': 'key1', 'Value': 'tag1'},
                      {'Key': 'key5','Value': 'tag5'}]}
    response = client.put_object(Bucket=bucket_name, Key=key1, Body=body1)
    print(test_name, "2", response['ResponseMetadata']['HTTPStatusCode'])

    response = client.put_object_tagging(Bucket=bucket_name, Key=key1,Tagging=tags1)
    print(test_name, "3", response['ResponseMetadata']['HTTPStatusCode'])

    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    print(test_name, "4", response['ResponseMetadata']['HTTPStatusCode'])

    # test that header is not returning when it should not
    lifecycle={
        "Rules": [
            {
                "Filter": {
                    "Tag": {"Key": "key2", "Value": "tag1"}
                },
                "Status": "Enabled",
                "Expiration": {
                    "Days": 1
                },
                "ID": "rule1"
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    print(test_name, "5", response['ResponseMetadata']['HTTPStatusCode'])

# stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    print(test_name, "6", response['ResponseMetadata']['HTTPStatusCode'])
    print(test_name, "Done",)




def test_lifecycle_expiration_header_and_tags_head(client, test_name):
    print("Start", test_name)
    lifecycle={
        "Rules": [
            {
                "Filter": {
                    "And": {
                        "Tags": [
                            {
                                "Key": "key1",
                                "Value": "tag1"
                            },
                            {
                                "Key": "key5",
                                "Value": "tag6"
                            }
                        ]
                    }
                },
                "Status": "Enabled",
                "Expiration": {
                    "Days": 1
                },
                "ID": "rule1"
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name,"1", response_code)

    key1 = "obj_key1"
    body1 = "obj_key1_body"
    tags1={'TagSet': [{'Key': 'key1', 'Value': 'tag1'},
                      {'Key': 'key5','Value': 'tag5'}]}
    response = client.put_object(Bucket=bucket_name, Key=key1, Body=body1)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, "2", response_code)

    response = client.put_object_tagging(Bucket=bucket_name, Key=key1,Tagging=tags1)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, "3", response_code)

    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, "4", response_code)

    response_code = response['ResponseMetadata']['HTTPStatusCode']
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, "5", response_code)
    print(test_name, "Done")



# test_bucketv2_policy_different_tenant(minio_client, "MinIO:")
# botocore.exceptions.ClientError: An error occurred (MalformedPolicy) when calling the PutBucketPolicy operation: invalid resource 'arn:aws:s3::*:sph-s3-comp-bucket'

# test_bucketv2_policy_different_tenant(s3_client, "S3:")
# botocore.exceptions.ClientError: An error occurred (MalformedPolicy) when calling the PutBucketPolicy operation:
# Policy has invalid resource


# test_bucket_policy_different_tenant(minio_client, "MinIO:")
# test_bucket_policy_different_tenant(s3_client, "S3:")


# test_object_set_get_unicode_metadata(s3_client, "S3")
# (minio_client, "MinIO")

# test_lifecycle_expiration_date(minio_client, "MinIO")
# test_lifecycle_expiration_date(s3_client, "S3")

# test_lifecycle_noncur_expiration(s3_client, "S3")
# test_lifecycle_noncur_expiration(minio_client, "MinIO")

# test_lifecyclev2_expiration(s3_client, "S3")
# test_lifecyclev2_expiration(minio_client, "MinIO")


# test_bucket_policy_set_condition_operator_end_with_IfExists(s3_client, "S3")
# botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the PutBucketPolicy operation:
# Access Denied
# test_bucket_policy_set_condition_operator_end_with_IfExists(minio_client, "MinIO")

# test_lifecycle_expiration_days0(s3_client, "S3")
# test_lifecycle_expiration_days0(minio_client, "MinIO")

#test_lifecycle_expiration_header_tags_head(s3_client, "S3")
# test_lifecycle_expiration_header_tags_head(s3_client, "MinIO")

# test_lifecycle_expiration_header_and_tags_head(s3_client, "S3")
test_lifecycle_expiration_header_and_tags_head(minio_client, "MinIO")

