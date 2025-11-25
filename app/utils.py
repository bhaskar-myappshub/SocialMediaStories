import json
import boto3
from urllib.parse import parse_qs
from datetime import datetime, timezone
from typing import List, Optional

s3_client = boto3.client("s3")

def forbidden(message):
    return {
        "statusCode": 403,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }

def bad_response(error):
    return {
        "statusCode": 502,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "error": str(error),
            "type": error.__class__.__name__
        })
    }

def response_json(body, status=200):
    return {"statusCode": status, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}

def bad_request(msg):
    return response_json({"error": msg}, status=400)

def not_found(msg="not found"):
    return response_json({"error": msg}, status=404)

def parse_body(event):
    body = event.get("body")
    if body is None:
        return {}
    if event.get("isBase64Encoded"):
        # For our use, body should be text JSON; base64 unlikely
        import base64
        body = base64.b64decode(body).decode("utf-8")

    try:
        return json.loads(body)
    except Exception:
        # maybe form-encoded?
        try:
            return {k: v[0] for k, v in parse_qs(body).items()}
        except Exception:
            return {}

def serialize_comment(comment):
    """Recursively serialize a comment with nested replies"""
    return {
        "id": comment.id,
        "user_id": str(comment.user_id),
        "username": comment.user.username if comment.user else None,
        "text": comment.text,
        "gif_url": comment.gif_url,
        "created_at": comment.created_at.isoformat(),
        "replies": [serialize_comment(reply) for reply in comment.replies]
    }

def parse_cursor(cursor: str) -> datetime:
    """Parse ISO string cursor into datetime safely."""
    cursor = cursor.strip()
    if cursor.endswith("Z"):
        cursor = cursor[:-1] + "+00:00"
    if " " in cursor[-6:]:
        cursor = cursor[:-6] + "+" + cursor[-5:]
    return datetime.strptime(cursor, "%Y-%m-%dT%H:%M:%S.%f%z")

def split_path(path):
    if not path:
        return []
    p = path
    if p.startswith("/"):
        p = p[1:]
    if p.endswith("/"):
        p = p[:-1]
    return p.split("/") if p else []

def parse_int_list(csv: Optional[str]) -> List[int]:
    if not csv:
        return []
    try:
        return [int(x.strip()) for x in csv.split(",") if x.strip()]
    except ValueError:
        return []

def parse_iso_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # Expect ISO-like or postgres-format string: "2025-10-05 21:37:21.633811+00"
    try:
        # datetime.fromisoformat handles both with and without tz
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        try:
            # fallback to parsing common postgres format
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f%z")
        except Exception:
            return None
