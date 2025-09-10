import boto3
from botocore.exceptions import ClientError
from app.config import AWS_REGION, PRESIGN_EXPIRES

s3_client = boto3.client("s3", region_name=AWS_REGION)


def generate_presigned_post(bucket: str, key: str, content_type: str, max_bytes: int, expires_in: int = PRESIGN_EXPIRES):
    """Generate a presigned POST dict (url + fields) with content-length-range condition."""
    fields = {"Content-Type": content_type, "acl": "private"}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, max_bytes],
        {"acl": "private"},
    ]

    try:
        presigned = s3_client.generate_presigned_post(
            Bucket=bucket,
            Key=key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=expires_in,
        )
        return presigned
    except ClientError as e:
        raise


def generate_presigned_get(bucket: str, key: str, expires_in: int = 3600):
    return s3_client.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires_in)


def head_object(bucket: str, key: str):
    return s3_client.head_object(Bucket=bucket, Key=key)


def delete_object(bucket: str, key: str):
    return s3_client.delete_object(Bucket=bucket, Key=key)
