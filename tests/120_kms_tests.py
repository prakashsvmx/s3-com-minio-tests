import base64
import datetime
import hashlib
import hmac
import json
import random
import string

from collections import OrderedDict

import pytz
import requests
import requests as requests

from s3_clients import get_s3_client, get_minio_client, get_res_body, S3_BUCKET_NAME, OBJECT_NAME, LOCAL_OBJECT_NAME

s3_client = get_s3_client()
minio_client = get_minio_client()
bucket_name = S3_BUCKET_NAME
object_name = OBJECT_NAME
local_object_name = LOCAL_OBJECT_NAME

kms_keyid = "my-minio-key"

# s3_client.download_file(bucket_name, object_name, local_object_name)
# minio_client.download_file(bucket_name, object_name, local_object_name)


def test_sse_kms_method_head(client, test_name):
    #
    # data = 'A'*1000
    # key = 'testobj'
    #
    # client.put_object(Bucket=bucket_name, Key=key, Body=data, Metadata={
    #     "SSEKMSKeyId":kms_keyid,
    #     "x-amz-server-side-encryption": 'aws:kms'
    # })

    response = client.head_object(Bucket=bucket_name, Key='1.png')
    encryption_conf = response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption']
    is_kms_as_expected = encryption_conf == 'aws:kms'
    kms_key_id_conf = response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption-aws-kms-key-id']
    is_key_as_expected = kms_key_id_conf == "arn:aws:kms:" + kms_keyid
    print(test_name, encryption_conf, kms_key_id_conf, is_kms_as_expected, is_key_as_expected)


def test_sse_kms_present(client, test_name):
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid
    }
    data = 'A' * 100
    key = 'testobj'

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = client.get_object(Bucket=bucket_name, Key=key)
    # body = get_res_body(response)

    response = client.head_object(Bucket=bucket_name, Key=key)
    print(response)


def test_lifecycle_deletemarker_expiration(client, test_name):
    rules = [{'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 1},
              'Expiration': {'ExpiredObjectDeleteMarker': True}, 'Prefix': 'test1/', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)


def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size // chunk):
            s = s + strpart
        s = s + strpart[:(this_part_size % chunk)]
        yield s
        if (x == size):
            return

def _multipart_upload_enc(client, bucket_name, key, size, part_size, init_headers, part_headers, metadata, resend_parts):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """

    lf = (lambda **kwargs: kwargs['params']['headers'].update(init_headers))
    client.meta.events.register('before-call.s3.CreateMultipartUpload', lf)
    if metadata == None:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    else:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Metadata=metadata)

    upload_id = response['UploadId']
    s = ''
    parts = []
    for i, part in enumerate(generate_random(size, part_size)):
        # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
        part_num = i+1
        s += part
        lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
        client.meta.events.register('before-call.s3.UploadPart', lf)
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num})
        print("Part Res:", response)

    if i in resend_parts:
            lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
            client.meta.events.register('before-call.s3.UploadPart', lf)
            part_res = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)
            print("Part Res:", part_res)

    return (upload_id, s, parts)



def _check_content_using_range(client, key, bucket_name, data, step):
    response = client.get_object(Bucket=bucket_name, Key=key)
    size = response['ContentLength']

    for ofs in range(0, size, step):
        toread = size - ofs
        if toread > step:
            toread = step
        end = ofs + toread - 1
        r = 'bytes={s}-{e}'.format(s=ofs, e=end)
        response = client.get_object(Bucket=bucket_name, Key=key, Range=r)
        body = get_res_body(response)
        return body == data[ofs:end+1]

def test_sse_kms_multipart_upload(client, test_name):
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    enc_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
        'Content-Type': content_type
    }
    resend_parts = []
    part_headers={}
    (upload_id, data, parts) = _multipart_upload_enc(client, bucket_name, key, objlen,
                                                     part_size=5*1024*1024, init_headers=enc_headers, part_headers=enc_headers, metadata=metadata, resend_parts=resend_parts)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
    client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.head_bucket(Bucket=bucket_name)
    rgw_object_count = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-object-count', 1))
    rgw_bytes_used = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-bytes-used', objlen))

    lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
    client.meta.events.register('before-call.s3.UploadPart', lf)

    response = client.get_object(Bucket=bucket_name, Key=key)

    print( response['Metadata'] )
    print( response['ResponseMetadata']['HTTPHeaders']['content-type'] )

    body = get_res_body(response)
    size = response['ContentLength']

    print("Content Check Status", test_name,  _check_content_using_range(client, key, bucket_name, data, 1000000))
    print("Content Check Status",test_name, _check_content_using_range(client,key, bucket_name, data, 10000000))



def test_sse_kms_multipart_invalid_chunks_1(client, test_name):
    kms_keyid2 = kms_keyid
    key = "multipart_enc"
    content_type = 'text/bla'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    init_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid2
    }
    resend_parts = []

    _multipart_upload_enc(client, bucket_name, key, objlen, part_size=5*1024*1024,
                          init_headers=init_headers, part_headers=part_headers, metadata=metadata,
                          resend_parts=resend_parts)
    print(test_name, "Done")



def test_sse_kms_multipart_invalid_chunks_2(client, test_name):
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    init_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-not-present'
    }
    resend_parts = []

    _multipart_upload_enc(client, bucket_name, key, objlen, part_size=5*1024*1024,
                          init_headers=init_headers, part_headers=part_headers, metadata=metadata,
                          resend_parts=resend_parts)
    print(test_name, "Done")



def test_sse_kms_multipart_invalid_chunks_2(client, test_name):
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    init_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-not-present'
    }
    resend_parts = []

    _multipart_upload_enc(client, bucket_name, key, objlen, part_size=5*1024*1024,
                          init_headers=init_headers, part_headers=part_headers, metadata=metadata,
                          resend_parts=resend_parts)
    print(test_name, "Done")



def test_encryption_sse_c_multipart_bad_download(client, test_name):
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}


    put_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    get_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
        'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(client, bucket_name, key, objlen,
                                                     part_size=5*1024*1024, init_headers=put_headers, part_headers=put_headers, metadata=metadata, resend_parts=resend_parts)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(put_headers))
    client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.head_bucket(Bucket=bucket_name)
    rgw_object_count = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-object-count', 1))
    print(test_name ,"Object count", rgw_object_count)
    rgw_bytes_used = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-bytes-used', objlen))
    print(test_name ,"Object count", rgw_bytes_used)


    lf = (lambda **kwargs: kwargs['params']['headers'].update(put_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)
    response = client.get_object(Bucket=bucket_name, Key=key)
    print(test_name ,"Metadata", response['Metadata'])
    print(test_name ,"Metadata", response['ResponseMetadata']['HTTPHeaders']['content-type'])
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name ,"Metadata", response_code)


    lf = (lambda **kwargs: kwargs['params']['headers'].update(get_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)
    print(test_name,"status final")



def test_encryption_sse_c_other_key(client, test_name):
    data = 'A'*100
    key = 'testobj'
    sse_client_headers_A = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    sse_client_headers_B = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
        'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers_A))
    client.meta.events.register('before-call.s3.PutObject', lf)
    response = client.put_object(Bucket=bucket_name, Key=key, Body=data)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers_B))
    client.meta.events.register('before-call.s3.GetObject', lf)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(test_name, "Status Code", response_code, "")


def _get_post_url(bucket_name):
    endpoint =  "%s://%s:%d" % ("http", "localhost", 22000)
    return '{endpoint}/{bucket_name}'.format(endpoint=endpoint, bucket_name=bucket_name)


def test_post_object_tags_authenticated_request(client, aKey, sKey, test_name):

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"), \
                       "conditions": [
                           {"bucket": bucket_name},
                           ["starts-with", "$key", "foo"],
                           {"acl": "private"},
                           ["starts-with", "$Content-Type", "text/plain"],
                           ["content-length-range", 0, 1024],
                           ["starts-with", "$tagging", ""]
                       ]}
    # xml_input_tagset is the same as `input_tagset = _create_simple_tagset(2)` in xml
    # There is not a simple way to change input_tagset to xml like there is in the boto2 tetss
    xml_input_tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"
    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = sKey
    aws_access_key_id = aKey
    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())
    payload = OrderedDict([
        ("key" , "foo.txt"),
        #("AWSAccessKeyId" , aws_access_key_id),
        #("acl" , "private"),("signature" , signature),("policy" , policy),
        ("tagging", xml_input_tagset),
        ("Content-Type" , "text/plain"),
        ('file', ('bar'))])
    r = requests.post(_get_post_url(bucket_name), files=payload, verify=False)
    print(test_name, "Status Code, ", r.status_code, "Expected, ", 204)
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = get_res_body(response)
    print(test_name, "body ", body)




def _create_simple_tagset(count):
    tagset = []
    for i in range(count):
        tagset.append({'Key': str(i), 'Value': str(i)})

    return {'TagSet': tagset}


def test_post_object_tags_anonymous_request(client, test_name):
    url = _get_post_url(bucket_name)
    # client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    key_name = "foo.txt"
    input_tagset = _create_simple_tagset(2)
    # xml_input_tagset is the same as input_tagset in xml.
    # There is not a simple way to change input_tagset to xml like there is in the boto2 tetss
    xml_input_tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"


    payload = OrderedDict([
        ("key" , key_name),
        # ("acl" , "public-read"),
        ("Content-Type" , "text/plain"),
        ("tagging", xml_input_tagset),
        ('file', ('bar')),
    ])

    r = requests.post(url, files=payload, verify=False)
    print(test_name, "body ", r.status_code, "Expected,", '204')

    response = client.get_object(Bucket=bucket_name, Key=key_name)
    body = get_res_body(response)
    print(test_name, "body ", body, "Expected,", 'bar')

    response = client.get_object_tagging(Bucket=bucket_name, Key=key_name)
    print(test_name, "Tags ", response['TagSet'] , "Expected,",input_tagset['TagSet'])


# print(test_sse_kms_method_head(s3_client, "S3"))
# print(test_sse_kms_method_head(minio_client, "MinIO"))


# print(test_sse_kms_present(s3_client, "S3"))
# print(test_sse_kms_present(minio_client, "MinIo"))

# print(test_lifecycle_deletemarker_expiration(s3_client, "S3"))
# print(test_lifecycle_deletemarker_expiration(minio_client, "MinIO"))


# test_sse_kms_multipart_upload(s3_client, "S3")
# test_sse_kms_multipart_upload(minio_client, "MinIO")

# test_sse_kms_multipart_invalid_chunks_1(minio_client, "MinIO")

# test_sse_kms_multipart_invalid_chunks_2(minio_client, "MinIO - test_sse_kms_multipart_invalid_chunks_2")

# test_encryption_sse_c_multipart_bad_download(minio_client, "MinIO")

# test_encryption_sse_c_other_key(minio_client, "MinIO")

# test_post_object_tags_authenticated_request(minio_client, "minio", "minio123", "MinIO")
# test_post_object_tags_anonymous_request(minio_client, "MinIO")
