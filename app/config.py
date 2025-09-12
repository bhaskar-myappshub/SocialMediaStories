import os
from pathlib import Path

DATABASE_URL = ""
S3_BUCKET = ""
AWS_REGION = ""

# Limits
IMAGE_MAX_BYTES = 5 * 1024 * 1024    # 5 MB
VIDEO_MAX_BYTES = 40 * 1024 * 1024   # 40 MB

# Presigned URL expiry
GET_PRESIGN_EXPIRES = 120  # seconds
VIEW_PRESIGN_EXPIRES = 60 # seconds

# Timezone-aware datetimes will use UTC
