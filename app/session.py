import boto3
from app.config import S3_BUCKET, API_ID, API_HASH, SESSION_KEY


s3 = boto3.client("s3")

SESSION_FILE = "/tmp/telegram_session.session"

def download_session():
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=SESSION_KEY)
        import logging
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        with open(SESSION_FILE, "wb") as f:
            f.write(resp["Body"].read())
        return SESSION_FILE
    except s3.exceptions.NoSuchKey:
        return None

def upload_session():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE, "rb") as f:
            import logging
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            logger.info("haiu")
            s3.put_object(Bucket=S3_BUCKET, Key=SESSION_KEY, Body=f.read())

