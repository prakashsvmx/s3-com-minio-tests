from s3_clients import get_s3_client, get_minio_client, S3_BUCKET_NAME, OBJECT_NAME, LOCAL_OBJECT_NAME

s3_client = get_s3_client()
minio_client = get_minio_client()
bucket_name = S3_BUCKET_NAME
object_name = OBJECT_NAME
local_object_name = LOCAL_OBJECT_NAME

# s3_client.download_file(bucket_name, object_name, local_object_name)
# minio_client.download_file(bucket_name, object_name, local_object_name)

s3_head_res = s3_client.head_object(Bucket=bucket_name, Key=object_name)
print("S3", s3_head_res)

minio_head_res = minio_client.head_object(Bucket=bucket_name, Key=object_name)
print("MinIO", minio_head_res)
