import uuid
from datetime import datetime, timezone, timedelta
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename

from app.config import IMAGE_MAX_BYTES, VIDEO_MAX_BYTES, S3_BUCKET
from app.db import SessionLocal
from app.models import User, Story
from app.s3_utils import generate_presigned_post, head_object, generate_presigned_get, delete_object

bp = Blueprint("api", __name__)


@bp.route("/users", methods=["POST"])
def create_user():
    data = request.get_json() or {}
    username = (data.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username is required"}), 400

    s = SessionLocal()
    try:
        exists = s.query(User).filter(User.username == username).first()
        if exists:
            return jsonify({"error": "username already exists"}), 400
        user = User(username=username)
        s.add(user)
        s.commit()
        s.refresh(user)
        return jsonify({"id": str(user.id), "username": user.username, "followers": user.followers}), 201
    finally:
        s.close()


@bp.route("/users/<user_id>", methods=["GET"])
def get_user(user_id):
    s = SessionLocal()
    try:
        user = s.query(User).get(uuid.UUID(user_id))
        if not user:
            return jsonify({"error": "not found"}), 404
        return jsonify({"id": str(user.id), "username": user.username, "followers": user.followers})
    finally:
        s.close()


@bp.route("/users/<user_id>/followers", methods=["POST"])
def add_follower(user_id):
    data = request.get_json() or {}
    follower_id = data.get("follower_id")
    if not follower_id:
        return jsonify({"error": "follower_id is required"}), 400

    s = SessionLocal()
    try:
        user = s.query(User).get(uuid.UUID(user_id))
        follower = s.query(User).get(uuid.UUID(follower_id))
        if not user or not follower:
            return jsonify({"error": "user or follower not found"}), 404
        followers = user.followers or []
        if str(follower.id) in followers:
            return jsonify({"message": "already following"}), 200
        followers.append(str(follower.id))
        user.followers = followers
        s.add(user)
        s.commit()
        return jsonify({"message": "ok", "followers": user.followers}), 200
    finally:
        s.close()


@bp.route("/presign", methods=["POST"])
def presign():
    """Request body: { user_id, filename, content_type, media_type }
    media_type: 'image' or 'video'
    Returns: presigned POST dict {url, fields}
    """
    data = request.get_json() or {}
    user_id = data.get("user_id")
    filename = data.get("filename")
    content_type = data.get("content_type")
    media_type = data.get("media_type")

    if not (user_id and filename and content_type and media_type):
        return jsonify({"error": "user_id, filename, content_type and media_type are required"}), 400

    if media_type not in ("image", "video"):
        return jsonify({"error": "media_type must be 'image' or 'video'"}), 400

    s = SessionLocal()
    try:
        user = s.query(User).get(uuid.UUID(user_id))
        if not user:
            return jsonify({"error": "user not found"}), 404
    finally:
        s.close()

    # size limit
    max_bytes = IMAGE_MAX_BYTES if media_type == "image" else VIDEO_MAX_BYTES

    # generate key
    safe_fn = secure_filename(filename)
    key = f"stories/{user_id}/{uuid.uuid4().hex}_{safe_fn}"

    presigned = generate_presigned_post(bucket=S3_BUCKET, key=key, content_type=content_type, max_bytes=max_bytes)

    return jsonify({"upload": presigned, "s3_key": key, "max_bytes": max_bytes})


@bp.route("/stories/confirm", methods=["POST"])
def confirm_story():
    """After client uploads to S3, call this endpoint with { user_id, s3_key }
    Backend will `head_object` to confirm and then record the story with a 24-hour expiry.
    """
    data = request.get_json() or {}
    user_id = data.get("user_id")
    s3_key = data.get("s3_key")

    if not (user_id and s3_key):
        return jsonify({"error": "user_id and s3_key are required"}), 400

    s = SessionLocal()
    try:
        user = s.query(User).get(uuid.UUID(user_id))
        if not user:
            return jsonify({"error": "user not found"}), 404

        # verify object exists and size
        try:
            obj = head_object(S3_BUCKET, s3_key)
        except Exception as e:
            return jsonify({"error": "s3 object not found or not accessible", "exception": str(e)}), 400

        size = obj.get("ContentLength")
        content_type = obj.get("ContentType") or "application/octet-stream"

        # determine media_type
        media_type = "image" if content_type.startswith("image/") else "video" if content_type.startswith("video/") else "other"
        if media_type == "other":
            return jsonify({"error": "unsupported content_type"}), 400

        max_bytes = IMAGE_MAX_BYTES if media_type == "image" else VIDEO_MAX_BYTES
        if size > max_bytes:
            return jsonify({"error": "file too large", "size": size, "max": max_bytes}), 400

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=24)

        story = Story(user_id=user.id, s3_key=s3_key, filename=s3_key.split('/')[-1], content_type=content_type, size=size, media_type=media_type, created_at=now, expires_at=expires_at)
        s.add(story)
        s.commit()
        s.refresh(story)

        # Optionally, return a presigned GET for immediate view (short expiry)
        signed = generate_presigned_get(S3_BUCKET, s3_key)
        return jsonify({"id": str(story.id), "s3_key": s3_key, "view_url": signed, "expires_at": story.expires_at.isoformat()}), 201

    finally:
        s.close()


@bp.route("/users/<user_id>/stories", methods=["GET"])
def list_user_stories(user_id):
    s = SessionLocal()
    try:
        user = s.query(User).get(uuid.UUID(user_id))
        if not user:
            return jsonify({"error": "not found"}), 404

        now = datetime.now(timezone.utc)
        stories = s.query(Story).filter(Story.user_id == user.id, Story.expires_at > now).order_by(Story.created_at.desc()).all()

        out = []
        for st in stories:
            out.append({
                "id": str(st.id),
                "s3_key": st.s3_key,
                "filename": st.filename,
                "media_type": st.media_type,
                "size": st.size,
                "view_url": generate_presigned_get(S3_BUCKET, st.s3_key),
                "expires_at": st.expires_at.isoformat(),
            })
        return jsonify(out)
    finally:
        s.close()
