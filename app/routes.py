import json
import uuid
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote, parse_qs
from sqlalchemy import or_, and_

from app.db import SessionLocal
from app.models import User, Story

from app.config import IMAGE_MAX_BYTES, VIDEO_MAX_BYTES, S3_BUCKET
from app.s3_utils import generate_presigned_post, head_object, generate_presigned_get, delete_object


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

# Endpoint implementations
def presign(event):
    data = parse_body(event)
    user_id = data.get("user_id")
    filename = data.get("filename")
    content_type = data.get("content_type")
    media_type = data.get("media_type")
    valid_mime_types = {"image/jpeg", "image/png", "image/jpg", "video/mp4", "video/mpeg"}

    if not (user_id and filename and content_type and media_type):
        return bad_request("user_id, filename, content_type and media_type are required")
    if media_type not in ("image", "video"):
        return bad_request("media_type must be 'image' or 'video'")
    if content_type not in valid_mime_types:
        return bad_request("unsupported file type")

    try:
        uid = uuid.UUID(user_id)
    except Exception:
        return bad_request("invalid user_id")

    try:
        s = SessionLocal()
        user = s.query(User).get(uid)
        if not user:
            return not_found("user not found")
    finally:
        s.close()

    max_bytes = IMAGE_MAX_BYTES if media_type == "image" else VIDEO_MAX_BYTES
    safe_fn = filename.replace("/", "_")
    key = f"stories/{user_id}/{uuid.uuid4().hex}_{safe_fn}"

    try:
        presigned = generate_presigned_post(bucket=S3_BUCKET, key=key, content_type=content_type)
    except Exception as e:
        return bad_response(e)

    return response_json({"upload": presigned, "s3_key": key, "max_bytes": max_bytes})

def confirm_story(event):
    data = parse_body(event)
    user_id = data.get("user_id")
    s3_key = data.get("s3_key")

    if not (user_id and s3_key):
        return bad_request("user_id and s3_key are required")

    try:
        uid = uuid.UUID(user_id)
    except Exception:
        return bad_request("invalid user_id")

    try:
        s = SessionLocal()
        user = s.query(User).get(uid)

        if not user:
            return not_found("user not found")

        try:
            obj = head_object(S3_BUCKET, s3_key)
        except Exception as e:
            return bad_request(f"s3 object not found or inaccessible: {str(e)}")

        size = obj.get("ContentLength")
        content_type = obj.get("ContentType") or "application/octet-stream"

        if content_type.startswith("image/"):
            media_type = "image"
        elif content_type.startswith("video/"):
            media_type = "video"
        else:
            return bad_request("unsupported content_type")

        max_bytes = IMAGE_MAX_BYTES if media_type == "image" else VIDEO_MAX_BYTES

        if size > max_bytes:
            delete_object(S3_BUCKET, s3_key)
            return bad_request(f"file too large (size={size}, max={max_bytes})")

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=24)
        story = Story(user_id=user.id, s3_key=s3_key, filename=s3_key.split("/")[-1],
                      content_type=content_type, size=size, media_type=media_type,
                      created_at=now, expires_at=expires_at)
        try:
            s.add(story)
            s.commit()
        except Exception as e:
            delete_object(S3_BUCKET, s3_key)
            s.rollback()
            return bad_response(e)

        try:
            signed = generate_presigned_get(S3_BUCKET, s3_key)
        except Exception:
            signed = ""
            
        return response_json({"id": str(story.id), "s3_key": s3_key, "view_url": signed, "expires_at": story.expires_at.isoformat()}, status=201)
    finally:
        s.close()

def list_user_stories(event, user_id):
    data = parse_body(event)

    try:
        viewer_id = data.get("viewer_id")
        uid = uuid.UUID(unquote(user_id))
        vid = uuid.UUID(unquote(viewer_id))
    except Exception:
        return bad_request("invalid user_id")

    try:
        s = SessionLocal()
        user = s.query(User).get(uid)

        if not user:
            return not_found()

        now = datetime.now(timezone.utc)

        stories = (
            s.query(Story)
            .join(User, Story.user_id == User.id)  # explicit join with User
            .filter(
                Story.user_id == uid,
                Story.expires_at > now,
                or_(
                    Story.viewership == "public",
                    or_(
                        and_(
                            Story.viewership == "followers",
                            User.followers.contains([str(vid)])
                        ),
                        Story.user_id == vid
                    )
                )
            )
            .order_by(Story.created_at.desc())
            .all()
        )

        out = []
        if not stories:
            return response_json({})
        else:
            filtered_stories = [
                st for st in stories
                if viewer_id not in (st.viewers or [])
            ]
            if filtered_stories and uid != vid:
                for st in filtered_stories:
                    # return response_json({"m":st.viewers})
                    try:
                        url = generate_presigned_get(S3_BUCKET, st.s3_key)
                    except Exception as e:
                        return bad_response(e)

                    out.append({
                        "id": str(st.id),
                        "s3_key": st.s3_key,
                        "filename": st.filename,
                        "media_type": st.media_type,
                        "size": st.size,
                        "view_url": url,
                        "expires_at": st.expires_at.isoformat()
                    })
                    v = list(st.viewers or [])
                    v.append(viewer_id)
                    st.viewers = v
                    s.add(st)

                s.commit()
            else:
                for st in stories:
                    try:
                        url = generate_presigned_get(S3_BUCKET, st.s3_key)
                    except Exception as e:
                        return bad_response(e)

                    if vid == uid:
                        viewer_ids = [uuid.UUID(v) for v in (st.viewers or [])]
                        # Fetch users directly from DB
                        viewer_users = (
                            s.query(User)
                            .filter(User.id.in_(viewer_ids))
                            .all()
                        )
                        viewer_names = [u.username for u in viewer_users]

                        out.append({
                            "id": str(st.id),
                            "s3_key": st.s3_key,
                            "filename": st.filename,
                            "media_type": st.media_type,
                            "size": st.size,
                            "view_url": url,
                            "expires_at": st.expires_at.isoformat(),
                            "views": len(viewer_ids),
                            "viewers": viewer_names
                        })
                    else:
                        out.append({
                            "id": str(st.id),
                            "s3_key": st.s3_key,
                            "filename": st.filename,
                            "media_type": st.media_type,
                            "size": st.size,
                            "view_url": url,
                            "expires_at": st.expires_at.isoformat()
                        })
            return response_json(out)
    finally:
        s.close()