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
