
DATABASE_URL = "postgresql+psycopg2-binary://core_payment_user:HkdjHjijowejT@43q2@52.207.245.55:5432/core_payment_db"
S3_BUCKET = "status-bucket-sway"
AWS_REGION = "us-east-1"

# Limits
IMAGE_MAX_BYTES = 5 * 1024 * 1024    # 10 MB
VIDEO_MAX_BYTES = 40 * 1024 * 1024   # 100 MB

# Presigned URL expiry
GET_PRESIGN_EXPIRES = 120  # seconds
VIEW_PRESIGN_EXPIRES = 60 # seconds

# Timezone-aware datetimes will use UTC
