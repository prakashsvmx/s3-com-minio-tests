from s3_clients import get_s3_client, get_minio_client, S3_BUCKET_NAME, OBJECT_NAME, LOCAL_OBJECT_NAME, get_new_bucket

s3_client = get_s3_client()
minio_client = get_minio_client()
object_name = OBJECT_NAME
local_object_name = LOCAL_OBJECT_NAME


# s3_client.download_file(bucket_name, object_name, local_object_name)
# minio_client.download_file(bucket_name, object_name, local_object_name)

def setup_lifecycle_tags2(client, bucket_name):
    tom_key = 'days1/tom'
    tom_tagset = {'TagSet':
                      [{'Key': 'tom', 'Value': 'sawyer'}]}

    client.put_object(Bucket=bucket_name, Key=tom_key, Body='tom_body')

    response = client.put_object_tagging(Bucket=bucket_name, Key=tom_key,
                                         Tagging=tom_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    huck_key = 'days1/huck'
    huck_tagset = {
        'TagSet':
            [{'Key': 'tom', 'Value': 'sawyer'},
             {'Key': 'huck', 'Value': 'finn'}]}

    client.put_object(Bucket=bucket_name, Key=huck_key, Body='huck_body')

    response = client.put_object_tagging(Bucket=bucket_name, Key=huck_key,
                                         Tagging=huck_tagset)

    lifecycle_config = {
        'Rules': [
            {
                'Expiration': {
                    'Days': 1,
                },
                'ID': 'rule_tag1',
                'Filter': {
                    'Prefix': 'days1/',
                    'Tag': {
                        'Key': 'tom',
                        'Value': 'sawyer'
                    },
                    'And': {
                        'Prefix': 'days1',
                        'Tags': [
                            {
                                'Key': 'huck',
                                'Value': 'finn'
                            },
                        ]
                    }
                },
                'Status': 'Enabled',
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    return response['ResponseMetadata']['HTTPStatusCode'] == 200



def test_lifecycle_expiration_tags1(client, bucket_name):

    tom_key = 'days1/tom'
    tom_tagset = {'TagSet':
                      [{'Key': 'tom', 'Value': 'sawyer'}]}

    client.put_object(Bucket=bucket_name, Key=tom_key, Body='tom_body')

    response = client.put_object_tagging(Bucket=bucket_name, Key=tom_key,
                                         Tagging=tom_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lifecycle_config = {
        'Rules': [
            {
                'Expiration': {
                    'Days': 1,
                },
                'ID': 'rule_tag1',
                'Filter': {
                    'Prefix': 'days1/',
                    'Tag': {
                        'Key': 'tom',
                        'Value': 'sawyer'
                    },
                },
                'Status': 'Enabled',
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    return response['ResponseMetadata']['HTTPStatusCode']


def check_s3():
    #bucket_name = get_new_bucket(s3_client, S3_BUCKET_NAME)
    bucket_name = S3_BUCKET_NAME
    s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'})
    # return setup_lifecycle_tags2(s3_client, bucket_name)
    return test_lifecycle_expiration_tags1(s3_client, bucket_name)
#botocore.exceptions.ClientError: An error occurred (MalformedXML) when calling the PutBucketLifecycleConfiguration operation: The XML you provided was not well-formed or did not validate against our published schema

def check_minio():
   # bucket_name = get_new_bucket(minio_client, S3_BUCKET_NAME)
    bucket_name = S3_BUCKET_NAME
    minio_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'})
    return test_lifecycle_expiration_tags1(minio_client, bucket_name)
# return setup_lifecycle_tags2(minio_client, bucket_name)

# botocore.exceptions.ClientError: An error occurred (InvalidRequest) when calling the PutBucketLifecycleConfiguration operation: Filter must have exactly one of Prefix, Tag, or And specified


# print("S3", check_s3())
#print("MinIO", check_minio())

