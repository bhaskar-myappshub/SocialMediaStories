import os
from pathlib import Path

DATABASE_URL = os.getenv("DATABASE_URL")
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Limits
IMAGE_MAX_BYTES = 2 * 1024 * 1024    # 2 MB
VIDEO_MAX_BYTES = 10 * 1024 * 1024   # 10 MB

# Presigned URL expiry
PRESIGN_EXPIRES = 3600  # seconds

# Timezone-aware datetimes will use UTC
