from s3_clients import get_s3_client, get_minio_client, S3_BUCKET_NAME, OBJECT_NAME, LOCAL_OBJECT_NAME

s3_client = get_s3_client()
minio_client = get_minio_client()
bucket_name = S3_BUCKET_NAME
object_name = OBJECT_NAME
local_object_name = LOCAL_OBJECT_NAME

# s3_client.download_file(bucket_name, object_name, local_object_name)
# minio_client.download_file(bucket_name, object_name, local_object_name)

rules = [
    {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled',
     'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 2}},
    {'ID': 'rule2', 'Prefix': 'test2/', 'Status': 'Disabled',
     'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 3}}
]
lifecycle = {'Rules': rules}

s3Response = s3_client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
s3ResponseCode = s3Response['ResponseMetadata']['HTTPStatusCode']
print("S3", s3ResponseCode)


minioResponse = minio_client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
minioResponseCode = minioResponse['ResponseMetadata']['HTTPStatusCode']
print("MinIO", minioResponseCode)
