import boto3

S3_BUCKET_NAME = "sph-s3-comp-bucket"
MINIO_BUCKET_NAME = "sph-s3-comp-bucket"

OBJECT_NAME = "1.txt"
LOCAL_OBJECT_NAME = "dl-1.txt"


def get_s3_client():
    s3_client = boto3.client('s3',
                             aws_access_key_id='TBD',
                             aws_secret_access_key='TBD',
                             config=boto3.session.Config(signature_version='s3v4'),
                             verify=True
                             )
    return s3_client


def get_minio_client():
    minio_client = boto3.client('s3',
                                endpoint_url='http://localhost:22000',
                                aws_access_key_id='minio',
                                aws_secret_access_key='minio123',
                                config=boto3.session.Config(signature_version='s3v4'),
                                verify=False
                                )
    return minio_client


def get_new_bucket(client=None, name=None):
    if client is None:
        client = get_minio_client()
    client.create_bucket(Bucket=name)
    return name


def get_res_body(response):
    body = response['Body']
    got = body.read()
    if type(got) is bytes:
        got = got.decode()
    return got
