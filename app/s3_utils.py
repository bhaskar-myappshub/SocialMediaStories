import os
import json
import tempfile
import subprocess
import boto3
from botocore.exceptions import ClientError
from app.config import AWS_REGION, GET_PRESIGN_EXPIRES, VIEW_PRESIGN_EXPIRES

try:
    s3_client = boto3.client("s3", region_name=AWS_REGION)
except ClientError:
    raise 

def generate_presigned_post(bucket: str, key: str, content_type: str, expires_in: int = GET_PRESIGN_EXPIRES):
    try:
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                "ContentType": content_type
            },
            ExpiresIn=expires_in  # URL valid for 1 hour
        )
        return {
            "statusCode": 200,
            "body": {"url": presigned_url}
        }
    except ClientError:
        raise

def generate_presigned_get(bucket: str, key: str, expires_in: int = VIEW_PRESIGN_EXPIRES):
    return s3_client.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires_in)

def head_object(bucket: str, key: str):
    return s3_client.head_object(Bucket=bucket, Key=key)

def delete_object(bucket: str, key: str):
    return s3_client.delete_object(Bucket=bucket, Key=key)

def download_file(bucket: str, key: str, local_path: str):
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=local_path
    )

def get_video_duration_from_s3(bucket: str, key: str):
    """Download the video temporarily and check duration using ffprobe."""
    tmp_path = os.path.join(tempfile.gettempdir(), key.split("/")[-1])
    download_file(bucket, key, tmp_path)

    try:
        cmd = [
            "/opt/bin/ffprobe",  # path for AWS Lambda Layer
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "json",
            tmp_path,
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        info = json.loads(result.stdout)
        duration = float(info["format"]["duration"])
    finally:
        # Always remove temporary file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return duration

def copy_object(bucket: str, source_key: str, dest_key: str, content_type: str = None):
    copy_source = {"Bucket": bucket, "Key": source_key}
    params = {
        "Bucket": bucket,
        "CopySource": copy_source,
        "Key": dest_key,
        "MetadataDirective": "REPLACE",
    }
    if content_type:
        params["ContentType"] = content_type
    return s3_client.copy_object(**params)

def upload_file(local_path: str, bucket: str, dest_key: str, content_type: str = None):
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    s3_client.upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=dest_key,
        ExtraArgs=extra_args if extra_args else None,
    )