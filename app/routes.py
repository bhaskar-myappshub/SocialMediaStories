import os
import tempfile
import subprocess
import json
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import or_, and_, func, asc, desc
from sqlalchemy.orm import aliased
from botocore.exceptions import ClientError

from app.db import SessionLocal
from app.models import UserDB, Story, StoryReaction, Reaction, VideoView, AccountHistory
from app.models import Video, Comment, StoryComment, CloseFriend, Follower, Highlight, Sticker, StickerResponse
from app.utils import forbidden, bad_response, response_json, bad_request, not_found, split_path
from app.utils import parse_body, parse_cursor
from app.utils import parse_int_list, parse_iso_datetime

from app.config import IMAGE_MAX_BYTES, VIDEO_MAX_BYTES, S3_BUCKET, VIDEO_MAX_DURATION
from app.s3_utils import generate_presigned_post, head_object, generate_presigned_get, delete_object
from app.s3_utils import get_video_duration_from_s3, copy_object, upload_file, download_file

#cleanup function
def run_cleanup():
    now = datetime.now(timezone.utc)

    s = SessionLocal()
    try:
        # Fetch expired stories (not yet archived)
        expired_stories = (
            s.query(Story)
            .join(UserDB, Story.user_id == UserDB.id)
            .filter(Story.expires_at <= now, Story.archive == False)
            .all()
        )

        for story in expired_stories:
            user = s.query(UserDB).get(story.user_id)

            # 1. Auto archive
            if user and user.auto_archive_stories:
                story.archive = True
                continue

            # 2. Recently deleted (keep for 30 days)
            if story.deleted_at and (now - story.deleted_at) < timedelta(days=30):
                continue

            # 3. Highlighted story (never delete)
            if story.highlight:
                continue

            
            # 🚫 Auto-archive disabled → delete story and S3 file
            try:
                delete_object(S3_BUCKET, story.s3_key)
                delete_object(S3_BUCKET, story.thumbnail_key)
            except Exception:
                continue

            s.delete(story)
        s.commit()

    finally:
        s.close()


# Endpoint implementations
def generate_presigned_puts(event):
    data = parse_body(event)
    files = data.get("files")
    user_id = data.get("user_id")

    if len(files) > 10:
        return bad_request("Maximum total quantity of 10 files can be uploaded")

    valid_mime_types = {"image/jpeg", "image/png", "image/jpg", "video/mp4", "video/mpeg"}

    presigned_urls = []
    for file in files:
        file_name = (file.get("file_name")).replace("_", "-")
        file_size = file.get("file_size")
        content_type = file.get("content_type")
        media_type = file.get("media_type")

        if not (file_name and file_size and content_type and media_type):
            return bad_request("file_name, file_size, content_type and media_type are required")
        if content_type not in valid_mime_types:
            return bad_request("unsupported file type")

        try:
            s = SessionLocal()
            user = s.query(UserDB).get(user_id)
            if not user:
                return not_found("user not found")
        finally:
            s.close()
        
        max_bytes = IMAGE_MAX_BYTES if media_type == "image" else VIDEO_MAX_BYTES

        if file_size > max_bytes:
            return bad_request(f"{file_name} file is too large: expected <{max_bytes}")

        safe_fn = file_name.replace("/", "_")
        key = f"stories/{user_id}/{uuid.uuid4().hex}_{safe_fn}"

        try:
            presigned = generate_presigned_post(bucket=S3_BUCKET, key=key, content_type=content_type)
        except Exception as e:
            return bad_response(e)

        presigned_urls.append({"file_name": file_name, "upload": presigned, "s3_key": key, "max_bytes": max_bytes})

    return response_json(presigned_urls)


def confirm_story(event):
    data = parse_body(event)
    user_id = int(data.get("user_id"))
    s3_key = data.get("s3_key")

    user_from_s3 = int(split_path(s3_key)[1])
    if user_from_s3 != user_id:
        delete_object(S3_BUCKET, s3_key)
        return bad_request("user_id did not match with s3 key")

    if not (user_id and s3_key):
        return bad_request("user_id and s3_key are required")

    s = SessionLocal()
    try:
        user = s.query(UserDB).get(user_id)
        if not user:
            return not_found("user not found")

        # Verify S3 object
        try:
            obj = head_object(S3_BUCKET, s3_key)
        except ClientError as e:
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

        # Check video duration
        if media_type == "video":
            try:
                duration = get_video_duration_from_s3(S3_BUCKET, s3_key)
            except Exception as e:
                delete_object(S3_BUCKET, s3_key)
                return bad_request(f"failed to check video duration: {str(e)}")
            if duration > VIDEO_MAX_DURATION:
                delete_object(S3_BUCKET, s3_key)
                return bad_request(f"video too long (duration={duration:.2f}s, max={VIDEO_MAX_DURATION}s)")

        # Validate countdown/link stickers
        countdown_count = 0
        link_count = 0
        stickers = data.get("stickers") or []
        for st in stickers:
            st_type = st.get("type")
            if st_type == "countdown":
                if not st.get("date") or not st.get("time") or not st.get("position"):
                    return bad_request("Missing date or time or position fields")
                countdown_count += 1
                if countdown_count > 1:
                    return bad_request("Multiple countdown stickers found")
            elif st_type == "link":
                if not st.get("link") or not st.get("position"):
                    return bad_request("Missing link or position fields")
                link_count += 1
                if link_count > 1:
                    return bad_request("Multiple link stickers found")
            else:
                return bad_request(f"Unexpected type found in stickers - {st_type}")

        # Create story thumbnail
        filename = s3_key.split("_")[-1]
        thumbnail_key = f"story_thumbnails/{user_id}/{uuid.uuid4().hex}_{filename}"

        if content_type.startswith("image/"):
            # 🖼️ Directly copy image to story_thumbnails
            copy_object(S3_BUCKET, s3_key, thumbnail_key, "image/jpeg")
        elif content_type.startswith("video/"):
            # 🎥 Process video: extract thumbnail
            tmp_input = os.path.join(tempfile.gettempdir(), filename)
            tmp_output = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.jpg")

            try:
                download_file(S3_BUCKET, s3_key, tmp_input)
            except Exception as e:
                bad_response(f"❌ Failed to download file: {e}")

            try:
                cmd = [
                    "/opt/bin/ffmpeg",  # Lambda layer path
                    "-y",
                    "-i", tmp_input,
                    "-ss", "00:00:00.5",  # Capture frame at 0.5s
                    "-vframes", "1",
                    "-vf", "scale=480:-1",
                    tmp_output,
                ]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                # Upload screenshot to S3
                upload_file(tmp_output, S3_BUCKET, thumbnail_key, "image/jpeg")
            except subprocess.CalledProcessError as e:
                bad_response(f"❌ FFmpeg error: {e.stderr.decode('utf-8', errors='ignore')}")
            finally:
                if os.path.exists(tmp_input):
                    os.remove(tmp_input)
                if os.path.exists(tmp_output):
                    os.remove(tmp_output)

        else:
            bad_request(f"⚠️ Unsupported content type: {content_type}")

        # Create story entry
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=24)
        story = Story(
            user_id=user.id,
            s3_key=s3_key,
            filename=s3_key.split("_")[-1],
            content_type=content_type,
            size=size,
            media_type=media_type,
            created_at=now,
            expires_at=expires_at,
            privacy=data.get("privacy", user.profile_visibility.lower()),
            caption=data.get("caption"),
            location=data.get("location"),
            mentions=data.get("mentions"),
            hashtags=data.get("hashtags"),
            music=data.get("music"),
            stickers=stickers,
            allow_replies=data.get("allow_replies"),
            allow_sharing=data.get("allow_sharing"),
            thumbnail_key=thumbnail_key
        )
        s.add(story)
        s.flush()  # ✅ story.id now available

        # Store poll, reaction_bar, quiz stickers
        if data.get("poll"):
            poll = data["poll"]
            s.add(Sticker(
                story_id=story.id,
                type="poll",
                question_text=poll.get("question"),
                options=json.dumps(poll.get("options")),
                position=poll.get("position") or {"x":0.5,"y":0.5},
            ))

        if data.get("reaction_bar"):
            rb = data["reaction_bar"]
            s.add(Sticker(
                story_id=story.id,
                type="slider",
                emoji_icon=rb.get("reaction_type"),
                position=rb.get("position") or {"x":0.5,"y":0.5},
            ))

        if data.get("quiz"):
            quiz = data["quiz"]
            s.add(Sticker(
                story_id=story.id,
                type="quiz",
                question_text=quiz.get("question"),
                options=json.dumps(quiz.get("options")),
                correct_option=quiz.get("correct_option"),
                position=quiz.get("position") or {"x":0.5,"y":0.5},
            ))

        # Presigned URL
        try:
            media_url = generate_presigned_get(S3_BUCKET, s3_key)
        except Exception:
            media_url = ""

        reaction_count = s.query(StoryReaction).filter(StoryReaction.story_id == story.id).count()
        view_count = len(story.viewers) if story.viewers else 0

        response = {
            "story_id": str(story.id),
            "user_id": story.user_id,
            "media_url": media_url,
            "created_at": story.created_at.isoformat(),
            "expires_at": story.expires_at.isoformat(),
            "privacy": story.privacy or "public",
            "view_count": view_count,
            "reaction_count": reaction_count,
        }

        try:
            s.commit()
        except Exception as e:
            delete_object(S3_BUCKET, s3_key)
            s.rollback()
            return bad_response(e)

        return response_json(response, status=201)

    finally:
        s.close()


def get_feed(event):
    query_params = event.get("queryStringParameters") or {}
    user_id = int(query_params.get("user_id"))
    limit = int(query_params.get("limit", 20))
    next_cursor = query_params.get("next_cursor")

    now = datetime.now(timezone.utc)

    s = SessionLocal()
    try:
        current_user = s.query(UserDB).get(user_id)
        if not current_user:
            return response_json({"error": "user not found"}, status=404)

        # Base query: users followed OR are close friends, with at least one non-archived story
        base_q = (
            s.query(
                UserDB.id,
                UserDB.username,
                UserDB.profile_image_key,
                func.max(Story.created_at).label("latest_story_time")
            )
            .outerjoin(Follower, (Follower.following_id == UserDB.id) & (Follower.follower_id == user_id))
            .outerjoin(CloseFriend, (CloseFriend.user_id == UserDB.id) & (CloseFriend.close_friend_id == user_id))
            .join(Story, Story.user_id == UserDB.id)
            .filter(
                Story.archive == False,     # exclude archived stories
                Story.expires_at > now,     # story not expired
                Story.deleted_at == None,
                or_(
                    and_(
                        Follower.status == "accepted",
                        Follower.blocked == False
                    ),
                    CloseFriend.id.isnot(None)
                )
            )
            .group_by(UserDB.id)
        )

        # Cursor pagination
        if next_cursor:
            next_dt = parse_cursor(next_cursor) - timedelta(microseconds=1)
            base_q = base_q.having(func.max(Story.created_at) < next_dt)
            base_q = base_q.order_by(desc("latest_story_time"))
        else:
            base_q = base_q.order_by(desc("latest_story_time"))

        feed_data = []

        if not next_cursor:
            latest_story = (
                s.query(Story)
                .filter(Story.user_id == user_id, Story.archive == False)
                .order_by(desc(Story.created_at))
            ).first()

            has_new_stories = user_id not in latest_story.viewers if latest_story else False

            feed_data.append({
                "user_id": current_user.id,
                "username": current_user.username,
                "profile_pic": generate_presigned_get("S3_BUCKET", current_user.profile_image_key) if current_user.profile_image_key else "",
                "last_story_time": latest_story.created_at.isoformat() if latest_story else None,
                "has_new_stories": has_new_stories,
                "privacy": latest_story.privacy if latest_story else None,
                "is_current_user": True
            })

        for r in base_q:

            if len(feed_data) == limit:
                break

            latest_story = (
                s.query(Story)
                .filter(Story.user_id == r.id)
                .order_by(Story.created_at.desc())
                .first()
            )
            if not latest_story:
                continue

            privacy = latest_story.privacy

            # Check if current user is a close friend
            is_close_friend = s.query(CloseFriend).filter(
                CloseFriend.user_id == r.id,
                CloseFriend.close_friend_id == user_id
            ).first() is not None

            # Only include profile if privacy allows
            if privacy == "close friends":
                if not is_close_friend:
                    continue

            has_new_stories = (
                s.query(Story)
                .filter(
                    Story.user_id == r.id,                     # stories by this user
                    Story.archive == False,                    # not archived
                    Story.expires_at > datetime.now(timezone.utc),             # not expired
                    ~Story.viewers.contains([user_id])         # viewers JSONB does NOT contain user_id
                )
                .first()
            ) is not None

            feed_data.append({
                "user_id": r.id,
                "username": r.username,
                "profile_pic": generate_presigned_get("S3_BUCKET", r.profile_image_key) if r.profile_image_key else "",
                "last_story_time": r.latest_story_time.isoformat() if r.latest_story_time else None,
                "has_new_stories": has_new_stories,
                "privacy": privacy,
                "is_current_user": False
            })

        next_cursor_val = feed_data[-1]["last_story_time"] if feed_data else None

        return response_json({
            "feed": feed_data,
            "limit": limit,
            "next_cursor": next_cursor_val,
            "has_more": len(feed_data) == limit
        }, status=200)

    finally:
        s.close()


def get_sticker_aggregates(s, story_id):
    stickers = s.query(Sticker).filter(Sticker.story_id == story_id).all()
    result = []

    for st in stickers:
        if st.type in ("poll", "quiz"):
            # Decode options
            options = []
            if st.options:
                try:
                    options = json.loads(st.options)
                except:
                    options = []

            if st.type == "quiz":
                result.append({
                    "sticker_id": str(st.id),
                    "type": st.type,
                    "question_text": st.question_text,
                    "options": options,
                    "correct_option": st.correct_option,
                    "position": st.position,
                })
            else:
                # Count votes for each option
                votes = [0] * len(options)
                for resp in st.responses:
                    if resp.selected_option is not None and 0 <= resp.selected_option < len(options):
                        votes[resp.selected_option] += 1

                result.append({
                    "sticker_id": str(st.id),
                    "type": st.type,
                    "question_text": st.question_text,
                    "options": options,
                    "votes": votes,
                    "position": st.position,
                })

        elif st.type == "slider":  # reaction_bar
            reaction_values = [resp.slider_value for resp in st.responses if resp.slider_value is not None]
            count = len(reaction_values)
            avg_percentage = round(sum(reaction_values) / count, 2) if count else 0

            result.append({
                "sticker_id": str(st.id),
                "type": st.type,
                "reaction_type": st.emoji_icon,
                "position": st.position,
                "average_percentage": avg_percentage,
                "reaction_count": count
            })

    return result

def list_user_stories(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    viewer_id = int(query_params.get("viewer_id"))

    now = datetime.now(timezone.utc)
    s = SessionLocal()
    try:
        # Load user info
        user = s.query(UserDB).get(user_id)
        if not user:
            return not_found("user not found")

        # Check if viewer is a follower
        is_follower = (
            s.query(Follower)
            .filter(
                Follower.follower_id == viewer_id,
                Follower.following_id == user_id,
                Follower.status == "accepted",
                Follower.blocked == False
            )
            .first()
            is not None
        )

        # Check if viewer is a close friend
        is_close_friend = (
            s.query(CloseFriend)
            .filter(
                CloseFriend.user_id == user_id,
                CloseFriend.close_friend_id == viewer_id
            )
            .first()
            is not None
        )

        # Filter stories by privacy
        stories = (
            s.query(Story)
            .filter(Story.user_id == user_id, now < Story.expires_at)
            .filter(
                or_(
                    user_id == viewer_id,
                    Story.privacy == "public",
                    (Story.privacy == "private") & is_follower,
                    (Story.privacy == "close friends") & is_close_friend
                )
            )
            .filter(Story.archive == False, Story.deleted_at == None)  # exclude archived stories
            .order_by(Story.created_at.desc())
            .all()
        )

        story_list = []
        has_unseen = False

        for story in stories:
            # Count reactions
            reaction_count = len(story.reactions) if story.reactions else 0

            # Count replies (story comments)
            reply_count = s.query(StoryComment).filter(StoryComment.story_id == story.id).count()

            viewer_ids = [v for v in story.viewers if v != user_id]

            viewers_with_reactions = (
                s.query(
                    UserDB.profile_image_key,
                    UserDB.cover_image_key,
                    UserDB.username,
                    UserDB.display_name,
                    StoryReaction.reaction_type,
                )
                .outerjoin(
                    StoryReaction,
                    (StoryReaction.user_id == UserDB.id) & (StoryReaction.story_id == story.id),
                )
                .filter(UserDB.id.in_(viewer_ids))
                .all()
            )

            viewer_list = [
                {
                    "profile_image_key": v.profile_image_key,
                    "cover_image_key": v.cover_image_key,
                    "username": v.username,
                    "display_name": v.display_name,
                    "reaction_type": v.reaction_type if v.reaction_type else None,
                }
                for v in viewers_with_reactions
            ]

            if user_id in story.viewers:
                view_count = len(story.viewers) - 1
            else:
                view_count = len(story.viewers)

            highlight_name = None
            if story.highlight:
                highlight = s.query(Highlight).filter(Highlight.story_id == story.id).first()
                highlight_name = highlight.name

            if user_id == viewer_id:
                if story.highlight:
                    story_list.append({
                        "story_id": str(story.id),
                        "media_url": generate_presigned_get(S3_BUCKET, story.s3_key),
                        "media_type": story.media_type,
                        "caption": getattr(story, "caption", ""),
                        "created_at": story.created_at.isoformat(),
                        "expires_at": story.expires_at.isoformat(),
                        "has_seen": viewer_id in (story.viewers or []) is not None,
                        "view_count": view_count,
                        "reaction_count": reaction_count,
                        "viewers": viewer_list,
                        "reply_count": reply_count,
                        "location": story.location or {},
                        "mentions": story.mentions or [],
                        "hashtags": story.hashtags or [],
                        "music": story.music or {},
                        "stickers": story.stickers or [],
                        "other_stickers": get_sticker_aggregates(s, story.id),
                        "allow_replies": story.allow_replies,
                        "allow_sharing": story.allow_sharing,
                        "privacy": story.privacy,
                        "is_highlight": story.highlight,
                        "highlight_name": highlight_name
                    })
                else:
                    story_list.append({
                        "story_id": str(story.id),
                        "media_url": generate_presigned_get(S3_BUCKET, story.s3_key),
                        "media_type": story.media_type,
                        "caption": getattr(story, "caption", ""),
                        "created_at": story.created_at.isoformat(),
                        "expires_at": story.expires_at.isoformat(),
                        "has_seen": viewer_id in (story.viewers or []) is not None,
                        "view_count": view_count,
                        "reaction_count": reaction_count,
                        "viewers": viewer_list,
                        "reply_count": reply_count,
                        "location": story.location or {},
                        "mentions": story.mentions or [],
                        "hashtags": story.hashtags or [],
                        "music": story.music or {},
                        "stickers": story.stickers or [],
                        "other_stickers": get_sticker_aggregates(s, story.id),
                        "allow_replies": story.allow_replies,
                        "allow_sharing": story.allow_sharing,
                        "privacy": story.privacy,
                        "is_highlight": story.highlight
                    })
            else:
                story_list.append({
                    "story_id": str(story.id),
                    "media_url": generate_presigned_get(S3_BUCKET, story.s3_key),
                    "media_type": story.media_type,
                    "caption": getattr(story, "caption", ""),
                    "created_at": story.created_at.isoformat(),
                    "expires_at": story.expires_at.isoformat(),
                    "has_seen": viewer_id in (story.viewers or []) is not None,
                    "location": story.location or {},
                    "mentions": story.mentions or [],
                    "hashtags": story.hashtags or [],
                    "music": story.music or {},
                    "stickers": story.stickers or [],
                    "other_stickers": get_sticker_aggregates(s, story.id),
                    "allow_replies": story.allow_replies,
                    "allow_sharing": story.allow_sharing,
                    "privacy": story.privacy
                })
            # Check if viewer has seen the story
            has_seen = viewer_id in (story.viewers or [])
            if not has_seen:
                story.viewers = (story.viewers or []) + [viewer_id]  # append viewer
                s.add(story)
                has_unseen = True

        response = {
            "user_id": user.id,
            "username": user.username,
            "profile_pic": getattr(user, "profile_image_key", ""),
            "has_unseen_stories": has_unseen,
            "total_stories": len(story_list),
            "stories": story_list
        }

        s.commit()
        return response_json(response)

    except Exception as e:
        s.rollback()
        return bad_response(e)

    finally:
        s.close()


def delete_story(event, story_id):
    query_params = event.get("queryStringParameters")
    user_id = int(query_params.get("user_id"))

    if not user_id:
        return bad_request("user_id is required")

    # Validate story_id
    try:
        sid = uuid.UUID(story_id)
    except Exception:
        return bad_request("invalid story_id")

    now = datetime.now(timezone.utc)
    s = SessionLocal()
    try:
        story = s.query(Story).get(sid)
        if not story:
            return not_found("story not found")

        # Check ownership
        if story.user_id != user_id:
            return forbidden("you are not the owner of this story")

        # ✅ Instead of deleting, mark as expired
        story.expires_at = now - timedelta(days=1)
        story.deleted_at = now
        s.commit()

        return response_json({
            "message": "Story deleted successfully",
            "story_id": str(story.id)
        })

    finally:
        s.close()


def archive_story(event):
    data = parse_body(event)
    user_id = int(data.get("user_id"))
    story_id = data.get("story_id")

    if not user_id or not story_id:
        return bad_request("user_id and story_id are required")

    try:
        sid = uuid.UUID(story_id)
    except ValueError:
        return bad_request("invalid story_id")

    s = SessionLocal()
    try:
        # Fetch the story
        story = s.get(Story, sid)
        if not story or story.archive:
            return bad_request("Story not found")

        # Allow only self-archive
        if not story.user_id == user_id:
            return bad_request("You are not the owner of the story")

        story.archive = True
        s.commit()

        return response_json({
            "message": "Story archived successfully",
            "story_id": str(story.id)
        })

    finally:
        s.close()


def react_to_story(event, story_id):
    data = parse_body(event)
    user_id = int(data.get("user_id"))
    reaction_type = data.get("reaction_type")

    if not user_id or not reaction_type:
        return bad_request("user_id and reaction_type are required")

    try:
        sid = uuid.UUID(story_id)
    except ValueError:
        return bad_request("invalid story_id")

    s = SessionLocal()
    try:
        # Fetch the story
        story = s.get(Story, sid)
        if not story or story.archive:
            return bad_request("Story not found")

        # Prevent self-reaction
        if story.user_id == user_id:
            return bad_request("You cannot react to your own story")

        # ----- 🔒 Privacy checks -----
        if story.privacy == "private":
            is_follower = (
                s.query(Follower)
                .filter_by(follower_id=user_id, following_id=story.user_id, status="accepted", blocked=False)
                .first()
            )
            if not is_follower:
                return bad_request("You must be a follower to react to this story")

        elif story.privacy == "close friends":
            is_close_friend = (
                s.query(CloseFriend)
                .filter_by(user_id=story.user_id, close_friend_id=user_id)
                .first()
            )
            if not is_close_friend:
                return bad_request("You must be a close friend to react to this story")

        # ----- 💬 Add or update reaction -----
        existing = (
            s.query(StoryReaction)
            .filter_by(story_id=sid, user_id=user_id)
            .first()
        )

        if existing:
            existing.reaction_type = reaction_type
            existing.reacted_at = datetime.now(timezone.utc)
            message = "Reaction updated successfully"
        else:
            reaction = StoryReaction(
                story_id=sid, user_id=user_id, reaction_type=reaction_type
            )
            s.add(reaction)
            message = "Reaction added successfully"

        s.commit()
        return response_json({"message": message}, 201)

    except Exception as e:
        s.rollback()
        return bad_response(e)
    finally:
        s.close()


def delete_story_reaction(event, story_id):
    query_params = event.get("queryStringParameters")
    user_id = int(query_params.get("user_id"))

    if not user_id:
        return bad_request("user_id is required")

    try:
        sid = uuid.UUID(story_id)
    except ValueError:
        return bad_request("invalid story_id")

    s = SessionLocal()
    try:
        # Fetch story
        story = s.get(Story, sid)
        if not story or story.archive:
            return bad_request("Story not found")

        # Prevent deleting reaction on own story (if not needed, you can remove this)
        if story.user_id == user_id:
            return bad_request("You cannot delete reactions of other users' stories")

        # Find existing reaction
        existing = (
            s.query(StoryReaction)
            .filter_by(story_id=sid, user_id=user_id)
            .first()
        )

        if not existing:
            return bad_request("No existing reaction to delete")

        # Delete reaction
        s.delete(existing)
        s.commit()

        return response_json({"message": "Reaction deleted successfully"})

    except Exception as e:
        s.rollback()
        return bad_response(e)
    finally:
        s.close()


def list_existing_highlights_folders(event):
    """
    Returns distinct highlight names and their S3 cover image URLs for a given user_id.
    """
    query_params = event.get("queryStringParameters")
    user_id = int(query_params.get("user_id"))

    s = SessionLocal()
    try:
        # Fetch unique (name, cover_image_key) pairs
        highlights = (
            s.query(Highlight.name, Highlight.cover_image_key)
            .join(Story, Highlight.story_id == Story.id)
            .filter(Story.user_id == user_id, Highlight.archive == False)
            .distinct(Highlight.name, Highlight.cover_image_key)
            .order_by(Highlight.name.asc())
            .all()
        )

        data = []
        seen_names = set()

        for h in highlights:
            # Skip duplicates by name only
            if h.name in seen_names:
                continue
            seen_names.add(h.name)

            # Generate S3 URL if key exists
            cover_image_url = generate_presigned_get(S3_BUCKET, h.cover_image_key)

            data.append({
                "name": h.name,
                "cover_image_key": h.cover_image_key,
                "cover_image_url": cover_image_url
            })

        return response_json({
            "highlights": data
        }, status=200)

    except Exception as e:
        import traceback
        print("Error listing highlights:", e)
        print(traceback.format_exc())
        return response_json({"error": "internal server error"}, status=500)

    finally:
        s.close()


def generate_presigned_put(event):
    data = parse_body(event)
    user_id = int(data.get("user_id"))
    file_name = data.get("file_name")
    file_size = data.get("file_size")
    content_type = data.get("content_type")
    existing_cover_key = data.get("existing_cover_key")

    valid_mime_types = {"image/jpeg", "image/png", "image/jpg"}

    if not (file_name and file_size and content_type):
        return bad_request("file_name, file_size, content_type are required")
    if content_type not in valid_mime_types:
        return bad_request("unsupported file type")

    try:
        s = SessionLocal()
        user = s.query(UserDB).get(user_id)
        if not user:
            return not_found("user not found")
    finally:
        s.close()
        
    max_bytes = IMAGE_MAX_BYTES

    if file_size > max_bytes:
        return bad_request(f"{file_name} file is too large: expected <{max_bytes}")

    safe_fn = file_name.replace("/", "_")

    if not existing_cover_key:
        return bad_request("'existing_cover_key' required")

    try:
        obj = head_object(S3_BUCKET, existing_cover_key)
    except ClientError as e:
        return bad_request(f"s3 object not found or inaccessible: {str(e)}")
        
    delete_object(S3_BUCKET, existing_cover_key)
    key = f"cover_images/{user_id}/{uuid.uuid4().hex}_{safe_fn}"

    try:
        presigned = generate_presigned_post(bucket=S3_BUCKET, key=key, content_type=content_type)
    except Exception as e:
        return bad_response(e)

    return response_json({"file_name": file_name, "upload": presigned, "s3_key": key, "max_bytes": max_bytes})


def get_updated_cover_url(event, user_id):
    query_params = event.get("queryStringParameters")
    s3_key = query_params.get("s3_key")
    
    uid = int(s3_key.split("/")[1])

    if not uid == user_id:
        return bad_request("s3_key doesn't belong to the user")

    try:
        url = generate_presigned_get(S3_BUCKET, s3_key)
    except Exception as e:
        return bad_response(str(e))

    return response_json({
        "user_id": user_id,
        "s3_key": s3_key,
        "url": url
    })


def add_to_highlights(event, user_id):
    data = parse_body(event)
    story_id = data.get("story_id")
    name = data.get("name")
    cover_image_key = data.get("cover_image_key")

    if not user_id or not story_id or not name:
        return bad_request("Missing required fields: user_id, story_id, name")

    try:
        sid = uuid.UUID(story_id)
    except ValueError:
        return bad_request("invalid story_id format")

    s = SessionLocal()
    try:
        # Get the story to be highlighted
        story = s.query(Story).filter_by(id=sid, user_id=user_id).first()
        if not story:
            return not_found("Story not found or does not belong to this user")

        if story.highlight:
            return bad_request("story already exists in highlights")

        existing = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(Story.user_id == user_id, Highlight.name == name, Highlight.archive == False)
            .order_by(desc(Highlight.order))
            .first()
        )

        if not existing:
            return response_json({"error": "Highlight with the given name doesn't exists"}, status=400)

        # Create highlight record
        highlight = Highlight(
            story_id=story.id,
            name=name,
            cover_image_key=cover_image_key,
            created_at=datetime.now(timezone.utc),
            order=existing.order + 1
        )

        story.highlight = not story.highlight

        s.add(story)
        s.add(highlight)
        s.commit()

        return response_json(
            {
                "message": "Story added to highlights successfully",
                "highlight": {
                    "id": str(highlight.id),
                    "name": highlight.name,
                    "cover_image_key": highlight.cover_image_key,
                    "created_at": highlight.created_at.isoformat()
                }
            },
            status=201
        )

    except Exception as e:
        s.rollback()
        return response_json({"error": str(e)}, status=500)
    finally:
        s.close()


def remove_story_from_highlights(event, user_id):
    query_params = event.get("queryStringParameters")
    story_id = query_params.get("story_id")

    if not story_id:
        return bad_request("story_id is required")

    s = SessionLocal()
    try:
        # Convert and validate story_id
        try:
            sid = uuid.UUID(story_id)
        except ValueError:
            return bad_request("invalid story_id format")

        story = s.query(Story).filter_by(id=sid).first()
        if not story:
            return response_json({"error": "Story not found"}, status=404)

        if not user_id == story.user_id:
            return bad_request("you are not the owner of the story")

        if not story.highlight:
            return response_json(
                {"error": "the story cant be found in highlights"},
                status=404
            )

        highlight_to_delete = s.query(Highlight).filter_by(story_id=story.id).first()

        post_highlights = (
            s.query(Highlight)
            .filter(
                Highlight.name == highlight_to_delete.name,
                Highlight.cover_image_key == highlight_to_delete.cover_image_key,
                Highlight.order > highlight_to_delete.order
            )
            .all()
        )

        for h in post_highlights:
            h.order = h.order - 1
            s.add(h)

        if not highlight_to_delete:
            return response_json({"error": "highlight not found"}, status=404)

        count = s.query(Highlight).filter_by(name=highlight_to_delete.name).count()
        if count == 1:
            try:
                delete_object(S3_BUCKET, highlight_to_delete.cover_image_key)
            except Exception as e:
                return bad_response(f"⚠️ Failed to delete highlight media from S3: {e}")

        story.highlight = not story.highlight
        s.add(story)
        s.delete(highlight_to_delete)
        s.commit()

        return response_json(
            {
                "message": "Highlight removed successfully",
                "deleted_highlight_id": str(highlight_to_delete.id),
                "story_id": story_id
            },
            status=200
        )

    except Exception as e:
        s.rollback()
        import traceback
        print(f"Error in remove_from_highlights: {str(e)}")
        print(traceback.format_exc())
        return response_json(
            {"error": "Internal server error", "message": str(e)},
            status=500
        )
    finally:
        s.close()


def comment_on_story(event, story_id):
    """Add a comment (or reply) to a story"""
    data = parse_body(event)
    user_id = int(data.get("posted_by_user_id"))
    text = data.get("text")

    # Validate input
    if not user_id or not text:
        return bad_request("posted_by_user_id and comment text are required")

    # Validate UUID
    try:
        sid = uuid.UUID(story_id)
    except ValueError:
        return bad_request("invalid story_id")

    s = SessionLocal()
    try:
        # Check if story exists
        story = s.get(Story, sid)
        if not story or story.archive:
            return not_found("Story not found")

        id = story.user_id

        if user_id == id:
            return bad_request("Cannot comment on your own story")

        # Privacy checks
        if story.privacy == "private":
            # Check if user_id follows story.user_id
            is_follower = (
                s.query(Follower)
                .filter_by(follower_id=user_id, following_id=id, status="accepted", blocked=False)
                .first()
            )
            if not is_follower:
                return bad_request("You are not allowed to comment on this private story")

        elif story.privacy == "close friends":
            # Check if user_id is in story user's close friends
            is_close_friend = (
                s.query(CloseFriend)
                .filter_by(user_id=id, close_friend_id=user_id)
                .first()
            )
            if not is_close_friend:
                return bad_request("You are not allowed to comment on this close friends story")

        # Create new comment
        new_comment = StoryComment(
            story_id=story.id,
            user_id=user_id,
            text=text,
            created_at=datetime.now(timezone.utc)
        )

        s.add(new_comment)
        s.commit()

        return response_json({
            "message": "Comment added successfully",
            "comment_id": str(new_comment.id),
            "story_id": str(story.id),
            "posted_by_user_id": user_id,
            "text": text
        }, status=201)

    except Exception as e:
        s.rollback()
        return bad_response(e)
    finally:
        s.close()


def vote_poll(event, story_id):
    data = parse_body(event)
    user_id = int(data.get("user_id"))
    vote_for = int(data.get("option"))

    # story_id must be UUID
    try:
        sid = uuid.UUID(story_id)
    except ValueError:
        return bad_request("Invalid story_id")

    s = SessionLocal()
    try:
        if not vote_for or not user_id:
            return bad_request("Missing 'option' or 'user_id' field")

        # Find the story
        story = (
            s.query(Story)
            .filter(Story.id == sid)
            .first()
        )
        if not story:
            return bad_request("Story not found")

        if story.user_id == user_id:
            return bad_request("You cannot vote for your own poll")

        # Find the corresponding POLL sticker for the story
        poll_sticker = (
            s.query(Sticker)
            .filter(
                Sticker.story_id == sid,
                Sticker.type == "poll",
            )
            .first()
        )
        if not poll_sticker:
            return bad_request("Poll sticker not found for story")

        # Parse options (stored as JSON string)
        try:
            sticker_options = json.loads(poll_sticker.options)
        except Exception:
            return bad_request("Sticker options are invalid JSON")

        # Validate selected option
        if vote_for < 0 or vote_for >= len(sticker_options):
            return bad_request("Invalid option index")

        # Prevent duplicate votes (recommended)
        existing_vote = (
            s.query(StickerResponse)
            .filter(
                StickerResponse.sticker_id == poll_sticker.id,
                StickerResponse.user_id == user_id,
            )
            .first()
        )
        if existing_vote:
            return bad_request("User already voted")

        # Store vote
        new_response = StickerResponse(
            sticker_id=poll_sticker.id,
            user_id=user_id,
            selected_option=vote_for,
        )
        s.add(new_response)
        s.commit()

        return response_json({
            "message": "Vote submitted",
            "selected_option": vote_for,
            "option_text": sticker_options[vote_for]
        }, 201)

    except Exception as e:
        s.rollback()
        return bad_response(e)
    finally:
        s.close()


def record_answer_for_quiz(event, story_id):
    data = parse_body(event)
    user_id = int(data.get("user_id"))
    option = int(data.get("option"))

    # story_id must be UUID
    try:
        sid = uuid.UUID(story_id)
    except ValueError:
        return bad_request("Invalid story_id")

    s = SessionLocal()
    try:
        if not option or not user_id:
            return bad_request("Missing 'option' or 'user_id' field")

        # Find the story
        story = (
            s.query(Story)
            .filter(Story.id == sid)
            .first()
        )
        if not story:
            return bad_request("Story not found")

        if story.user_id == user_id:
            return bad_request("You cannot answer for your own quiz")

        # Find the corresponding POLL sticker for the story
        quiz_sticker = (
            s.query(Sticker)
            .filter(
                Sticker.story_id == sid,
                Sticker.type == "quiz",
            )
            .first()
        )
        if not quiz_sticker:
            return bad_request("Quiz sticker not found for story")

        # Parse options (stored as JSON string)
        try:
            sticker_options = json.loads(quiz_sticker.options)
        except Exception:
            return bad_request("Sticker options are invalid JSON")

        # Validate selected option
        if option < 0 or option >= len(sticker_options):
            return bad_request("Invalid option index")

        # Prevent duplicate entries (recommended)
        existing_answer = (
            s.query(StickerResponse)
            .filter(
                StickerResponse.sticker_id == quiz_sticker.id,
                StickerResponse.user_id == user_id,
            )
            .first()
        )
        if existing_answer:
            return bad_request("User already answered")

        # Store answer
        new_response = StickerResponse(
            sticker_id=quiz_sticker.id,
            user_id=user_id,
            selected_option=option,
        )
        s.add(new_response)
        s.commit()

        return response_json({
            "message": "Succesfully answered",
            "selected_option": option,
            "option_text": sticker_options[option]
        }, 201)

    except Exception as e:
        s.rollback()
        return bad_response(e)
    finally:
        s.close()


def update_reaction_bar(event, story_id):
    try:
        sid = uuid.UUID(story_id)
    except ValueError:
        return bad_request("Invalid story_id")

    s = SessionLocal()
    try:
        data = parse_body(event)
        user_id = int(data.get("user_id"))
        slider_value = data.get("percentage")  # 0-100 slider value

        if user_id is None or slider_value is None:
            return bad_request("Missing 'user_id' or 'percentage' field")

        # Find the story
        story = s.query(Story).filter(Story.id == sid).first()
        if not story:
            return bad_request("Story not found")

        if story.user_id == user_id:
            return bad_request("You cannot react to your own slider")

        # Find reaction_bar sticker
        reaction_bar_sticker = (
            s.query(Sticker)
            .filter(Sticker.story_id == sid, Sticker.type == "slider")
            .first()
        )

        if not reaction_bar_sticker:
            return bad_request("reaction_bar sticker not found")

        # Check if user already reacted
        existing_resp = (
            s.query(StickerResponse)
            .filter(
                StickerResponse.sticker_id == reaction_bar_sticker.id,
                StickerResponse.user_id == user_id
            )
            .first()
        )

        if existing_resp:
            return bad_request("User already reacted")
        else:
            new_resp = StickerResponse(
                sticker_id=reaction_bar_sticker.id,
                user_id=user_id,
                slider_value=slider_value
            )
            s.add(new_resp)

        s.commit()

        return response_json({
            "message": "Reaction bar updated successfully",
        }, 201)

    except Exception as e:
        s.rollback()
        return response_json({"error": str(e)}, 500)
    finally:
        s.close()


def list_archived_stories_for_highlights(event, user_id):
    """List all archived stories for a given user"""
    s = SessionLocal()
    try:
        # Query archived stories
        archived_stories = (
            s.query(Story)
            .filter(Story.user_id == user_id, Story.archive == True, Story.deleted_at == None, Story.highlight == False)
            .order_by(Story.created_at.desc())
            .all()
        )

        # Format response
        result = []
        for story in archived_stories:
            result.append({
                "id": str(story.id),
                "s3_key": story.s3_key,
                "thumbnail_url": generate_presigned_get(S3_BUCKET, story.thumbnail_key),
                "created_at": story.created_at.isoformat() if story.created_at else None
            })

        return response_json({"user_id": user_id, "archived_stories": result})

    except Exception as e:
        print("Error listing archived stories:", e)
        return bad_response(str(e))
    finally:
        s.close()


def upload_default_cover_image(event):
    """
    Creates a default cover image for a user's story highlight.
    If the key is an image → copy as-is.
    If it's a video → take the first frame and upload that frame as image.
    """
    data = parse_body(event)
    user_id = int(data.get("user_id"))
    key = data.get("key")
    filename = key.split("_")[-1]
    uid = int(key.split("/")[1])

    if not user_id or not key:
        return bad_request("user_id and key are required")

    if not user_id == uid:
        return bad_request("s3 object doesn't belong to the user")

    # Check object exists
    try:
        obj = head_object(S3_BUCKET, key)
    except ClientError as e:
        return bad_request(f"s3 object not found: {str(e)}")

    content_type = obj.get("ContentType", "")
    cover_key = f"cover_images/{user_id}/{uuid.uuid4()}_{filename}"

    # Create temporary file paths
    tmp_input = os.path.join(tempfile.gettempdir(), os.path.basename(key))
    tmp_output = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}_{filename}.jpg")

    # Download the source file
    try:
        download_file(S3_BUCKET, key, tmp_input)
    except Exception as e:
        return bad_response(f"failed to download s3 file: {e}")

    try:
        if content_type.startswith("image/"):
            # ✅ Directly copy the image file
            copy_object(S3_BUCKET, key, cover_key, content_type)

        elif content_type.startswith("video/"):
            # 🎥 Extract first frame using ffmpeg
            cmd = [
                "/opt/bin/ffmpeg",  # Path in Lambda layer
                "-i", tmp_input,
                "-ss", "00:00:00.5",  # Capture a frame at 0.5s
                "-vframes", "1",
                "-vf", "scale=480:-1",  # Resize reasonably
                tmp_output,
            ]
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Upload screenshot to S3
            upload_file(tmp_output, S3_BUCKET, cover_key, "image/jpeg")
        else:
            return bad_request(f"Unsupported content type: {content_type}")

    except subprocess.CalledProcessError as e:
        return bad_response(f"Failed to process video: {e.stderr.decode('utf-8', errors='ignore')}")

    finally:
        # Clean up temporary files
        for f in (tmp_input, tmp_output):
            if os.path.exists(f):
                os.remove(f)

    # ✅ Return success response
    return response_json({
        "message": "Default cover image created successfully",
        "user_id": user_id,
        "original_key": key,
        "cover_image_key": cover_key,
        "cover_image_url": generate_presigned_get(S3_BUCKET, cover_key)
    }, status=201)


def create_highlight(event):
    """Create a new highlight for a user"""
    data = parse_body(event)
    user_id = int(data.get("user_id"))
    name = data.get("name")
    cover_image_key = data.get("cover_image_key")
    keys = data.get("keys")  # list of story S3 keys

    # Validate required fields
    if not user_id or not name or not cover_image_key or not keys:
        return bad_request("user_id, name, cover_image_key, and keys are required")

    try:
        obj = head_object(S3_BUCKET, cover_image_key)
    except ClientError as e:
        return bad_request(f"s3 object not found or inaccessible: {str(e)}")

    s = SessionLocal()
    try:
        # ✅ Ensure highlight name not duplicated for same user
        existing = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(Story.user_id == user_id, Highlight.name == name)
            .first()
        )

        if existing:
            return bad_request("Highlight with this name already exists")

        i = 1
        for key in keys:
            story = s.query(Story).filter(Story.s3_key == key, Story.user_id == user_id).first()

            if story.highlight:
                return bad_request(f"story already exists in highlights - {story.s3_key}")

            if not story:
                return bad_request("you are not the owner of the story")

            story.highlight = not story.highlight
            s.add(story)

            # ✅ Create new Highlight DB record
            highlight = Highlight(
                name=name,
                cover_image_key=cover_image_key,
                created_at=datetime.now(timezone.utc),
                story_id=story.id,
                order=i
            )
            s.add(highlight)
            i += 1

        s.commit()

        return response_json(
            {
                "message": "Highlight created successfully",
                "highlight_name": name
            },
            status=201,
        )

    except Exception as e:
        s.rollback()
        import traceback
        print(traceback.format_exc())
        return response_json({"error": "Internal server error", "message": str(e)}, status=500)

    finally:
        s.close()


def get_highlights_folders(event, user_id):
    """
    Retrieve highlight folders for a user.
    Each folder represents a group of highlights with the same name,
    showing only the folder name and one cover image URL.
    """
    query_params = event.get("queryStringParameters") or {}
    
    if not query_params.get("viewer_id"):
        return bad_request("'viewer_id' is required")

    viewer_id = int(query_params.get("viewer_id"))

    s = SessionLocal()
    try:
        user = s.query(UserDB).get(user_id)
        if not user:
            return not_found("User not found")

        # 🔍 Fetch all highlights of this user
        highlights = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(Story.user_id == user_id, Highlight.archive == False)
            .distinct(Highlight.name, Highlight.cover_image_key)
            .order_by(
                Highlight.name,
                Highlight.cover_image_key,
                Highlight.order.asc()
            )
            .all()
        )

        if not highlights:
            return response_json({"highlights_folders": []}, status=200)

        grouped = []

        for hl in highlights:
            grouped.append({"name": hl.name, "cover_image_key": hl.cover_image_key})

        # 🧩 Optional privacy check
        if viewer_id != user_id and user.profile_visibility.lower() == "private":
            is_follower = (
                s.query(Follower)
                .filter(
                    Follower.following_id == user_id,
                    Follower.follower_id == viewer_id,
                    Follower.status == "accepted",
                    Follower.blocked == False
                )
                .first()
                )
            if not is_follower:
                return forbidden("This user's highlights are private")

        # 🪶 Format output
        highlights_result = [
            {"name": item["name"], "cover_image_key": item["cover_image_key"], "cover_image_url": generate_presigned_get(S3_BUCKET, item["cover_image_key"])}
            for item in grouped
        ]

        return response_json({
            "user_id": user_id,
            "username": user.username,
            "highlights_folders": highlights_result,
            "is_user_the_viewer": user_id == viewer_id
        }, status=200)

    finally:
        s.close()


def get_highlights(event, user_id):
    """Retrieve all highlights for a given user_id and highlight name."""
    query_params = event.get("queryStringParameters") or {}
    name = query_params.get("name")

    if not query_params.get("viewer_id"):
        return bad_request("'viewer_id' is required")

    viewer_id = int(query_params.get("viewer_id"))

    if not name:
        return bad_request("highlight name is required")

    s = SessionLocal()
    try:
        user = s.query(UserDB).get(int(user_id))
        if not user:
            return not_found("user not found")

        if user.profile_visibility.lower() == "private":
            # Check if viewer is a follower of the user
            is_follower = (
                s.query(Follower)
                .filter(
                    Follower.follower_id == viewer_id,
                    Follower.following_id == user_id,
                    Follower.status == "accepted",
                    Follower.blocked == False
                )
                .first()
            )
            if not is_follower and not user_id == viewer_id:
                return forbidden("cannot view highlights")

        # Fetch all highlights with given name for that user
        highlights = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(Story.user_id == user_id, Highlight.name == name)
            .order_by(Highlight.order.asc())
            .all()
        )

        if not highlights:
            return not_found("no highlights found for this user and name")

        data = []
        # Prepare response data
        for h in highlights:
            if h.story.privacy == "close friends":
                # Check if current user is a close friend
                is_close_friend = s.query(CloseFriend).filter(
                    CloseFriend.user_id == user_id,
                    CloseFriend.close_friend_id == viewer_id
                ).first() is not None

                if not is_close_friend:
                    continue

            # Count reactions
            reaction_count = len(h.story.reactions) if h.story.reactions else 0

            # Count replies (story comments)
            reply_count = s.query(StoryComment).filter(StoryComment.story_id == h.story.id).count()

            viewer_ids = [v for v in h.story.viewers if v != user_id]

            viewers_with_reactions = (
                s.query(
                    UserDB.profile_image_key,
                    UserDB.cover_image_key,
                    UserDB.username,
                    UserDB.display_name,
                    StoryReaction.reaction_type,
                )
                .outerjoin(
                    StoryReaction,
                    (StoryReaction.user_id == UserDB.id) & (StoryReaction.story_id == h.story.id),
                )
                .filter(UserDB.id.in_(viewer_ids))
                .all()
            )

            viewer_list = [
                {
                    "profile_image_key": v.profile_image_key,
                    "cover_image_key": v.cover_image_key,
                    "username": v.username,
                    "display_name": v.display_name,
                    "reaction_type": v.reaction_type if v.reaction_type else None,
                }
                for v in viewers_with_reactions
            ]

            if user_id in h.story.viewers:
                view_count = len(h.story.viewers) - 1
            else:
                view_count = len(h.story.viewers)
            if user_id == viewer_id:
                data.append({
                    "id": str(h.id),
                    "highlight_thumbnail_url": generate_presigned_get(S3_BUCKET, h.story.thumbnail_key),
                    "media_url": generate_presigned_get(S3_BUCKET, h.story.s3_key),
                    "media_type": h.story.media_type,
                    "caption": getattr(h.story, "caption", ""),
                    "view_count": view_count,
                    "reaction_count": reaction_count,
                    "viewers": viewer_list,
                    "reply_count": reply_count,
                    "location": h.story.location or {},
                    "mentions": h.story.mentions or [],
                    "hashtags": h.story.hashtags or [],
                    "music": h.story.music or {},
                    "stickers": h.story.stickers or [],
                    "other_stickers": get_sticker_aggregates(s, h.story.id),
                    "allow_replies": h.story.allow_replies,
                    "allow_sharing": h.story.allow_sharing,
                    "privacy": h.story.privacy,
                    "created_at": h.created_at.isoformat() if h.created_at else None,
                })
            else:
                data.append({
                    "id": str(h.id),
                    "highlight_thumbnail_url": generate_presigned_get(S3_BUCKET, h.story.thumbnail_key),
                    "media_url": generate_presigned_get(S3_BUCKET, h.story.s3_key),
                    "media_type": h.story.media_type,
                    "caption": getattr(h.story, "caption", ""),
                    "location": h.story.location or {},
                    "mentions": h.story.mentions or [],
                    "hashtags": h.story.hashtags or [],
                    "music": h.story.music or {},
                    "stickers": h.story.stickers or [],
                    "other_stickers": get_sticker_aggregates(s, h.story.id),
                    "allow_replies": h.story.allow_replies,
                    "allow_sharing": h.story.allow_sharing,
                    "privacy": h.story.privacy,
                    "created_at": h.created_at.isoformat() if h.created_at else None,
                })
            # Check if viewer has seen the highlight
            has_seen = viewer_id in (h.story.viewers or [])
            if not has_seen:
                h.story.viewers = (h.story.viewers or []) + [viewer_id]  # append viewer
                s.add(h.story)

        response = {
            "user_id": user.id,
            "username": user.username,
            "profile_image_key": user.profile_image_key,
            "highlight_name": name,
            "cover_image_key": highlights[0].cover_image_key,
            "cover_image_url": generate_presigned_get(S3_BUCKET, highlights[0].cover_image_key),
            "total_highlights": len(data),
            "highlights": data
        }

        s.commit()
        return response_json(response)

    except Exception as e:
        s.rollback()
        return bad_response(str(e))
    finally:
        s.close()


def delete_highlight_folder(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    name = query_params.get("name")

    if not name:
        return bad_request("highlight name is required")

    s = SessionLocal()
    now = datetime.now(timezone.utc)
    try:
        # ✅ Fetch all highlights matching name and user_id
        highlights = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(Story.user_id == user_id, Highlight.name == name)
            .all()
        )

        if not highlights:
            return not_found("No highlights found")

        # 🧹 Delete highlight cover image from S3
        try:
            delete_object(S3_BUCKET, highlights[0].cover_image_key)
        except Exception as e:
            return bad_response(f"⚠️ Failed to delete cover image {highlights[0].cover_image_key}: {e}")

        for highlight in highlights:
            story = highlight.story

            # 🧠 Decide what to do with the story
            if story:
                if story.expires_at > now or story.archive:
                    # Story still active or archived → just unmark highlight
                    story.highlight = False
                elif story.deleted_at and (now - story.deleted_at) < timedelta(days=30):
                    # Story still actively deleted → just unmark highlight
                    story.highlight = False
                else:
                    # Story expired and not archived → delete from S3 + DB
                    try:
                        delete_object(S3_BUCKET, story.s3_key)
                        delete_object(S3_BUCKET, story.thumbnail_key)
                    except Exception as e:
                        return bad_response(f"⚠️ Failed to delete story media from S3: {e}")
                    s.delete(story)

            # Delete highlight entry from DB
            s.delete(highlight)

        s.commit()
        return response_json({"message": f"Highlight folder '{name}' deleted successfully"})

    finally:
        s.close()


def remove_highlight_from_highlights(event, highlight_id):
    query_params = event.get("queryStringParameters")

    if not query_params.get("user_id"):
        return bad_request("'user_id' is required")

    user_id = int(query_params.get("user_id"))

    try:
        hid = uuid.UUID(highlight_id)
    except ValueError:
        return bad_request("invalid highlight_id format")

    s = SessionLocal()
    now = datetime.now(timezone.utc)
    try:
        highlight = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(Story.user_id == user_id, Highlight.id == hid)
            .first()
        )

        if not highlight:
            return bad_request("highlight doesn't exists or belongs to the user")

        post_highlights = (
            s.query(Highlight)
            .filter(
                Highlight.name == highlight.name,
                Highlight.cover_image_key == highlight.cover_image_key,
                Highlight.order > highlight.order
            )
            .all()
        )

        for h in post_highlights:
            h.order = h.order - 1
            s.add(h)

        story = highlight.story

        # 🧠 Decide what to do with the story
        if story:
            if story.expires_at > now or story.archive:
                # Story still active or archived → just unmark highlight
                story.highlight = False
            elif story.deleted_at and (now - story.deleted_at) < timedelta(days=30):
                # Story still actively deleted → just unmark highlight
                story.highlight = False
            else:
                # Story expired and not archived → delete from S3 + DB
                try:
                    delete_object(S3_BUCKET, story.s3_key)
                    delete_object(S3_BUCKET, story.thumbnail_key)
                except Exception as e:
                    return bad_response(f"⚠️ Failed to delete story media from S3: {e}")
                s.delete(story)

        # Delete highlight entry from DB
        s.delete(highlight)

        s.commit()

        remaining_highlights = (
            s.query(Highlight)
            .filter(
                Highlight.name == highlight.name,
                Highlight.cover_image_key == highlight.cover_image_key
            )
            .all()
        )

        if not remaining_highlights:
            try:
                delete_object(S3_BUCKET, highlight.cover_image_key)
            except Exception as e:
                return bad_response(f"⚠️ Failed to delete cover image {highlight.cover_image_key}: {e}")

        return response_json(
            {
                "message": "removed from highlight successfully",
                "deleted_highlight_id": str(highlight.id),
                "story_id": str(story.id)
            },
            status=200
        )

    except Exception as e:
        s.rollback()
        import traceback
        print(f"Error in remove_from_highlights: {str(e)}")
        print(traceback.format_exc())
        return response_json(
            {"error": "Internal server error", "message": str(e)},
            status=500
        )
    finally:
        s.close()


def archive_highlight_folder(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    name = query_params.get("name")
    cover_image_key = query_params.get("cover_image_key")

    if not name or not cover_image_key:
        return bad_request("'name' and 'cover_image_key' are required")

    s = SessionLocal()
    try:
        # Fetch all highlights with the given name and cover image
        highlights = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(
                Highlight.name == name,
                Highlight.cover_image_key == cover_image_key,
                Highlight.archive == False
            )
            .all()
        )

        if not highlights:
            return not_found("No highlights found with the given name and cover_image_key")

        if not highlights[0].story.user_id == user_id:
            return forbidden("the user is not accessed to archive the highlights")

        # Mark all as archived
        for highlight in highlights:
            highlight.archive = True

        s.commit()

        return response_json({
            "message": f"All highlights under '{name}' archived successfully",
            "count": len(highlights)
        }, status=200)

    except Exception as e:
        s.rollback()
        return bad_response(f"Failed to archive highlights: {e}")

    finally:
        s.close()


def unarchive_highight_folder(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    name = query_params.get("name")
    cover_image_key = query_params.get("cover_image_key")

    if not name or not cover_image_key:
        return bad_request("'name' and 'cover_image_key' are required")

    s = SessionLocal()
    try:
        # Fetch all highlights with the given name and cover image
        highlights = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(
                Highlight.name == name,
                Highlight.cover_image_key == cover_image_key,
                Highlight.archive == True
            )
            .all()
        )

        if not highlights:
            return not_found("No highlights found with the given name and cover_image_key")

        if not highlights[0].story.user_id == user_id:
            return forbidden("the user is not accessed to unarchive the highlights")

        # Mark all as archived
        for highlight in highlights:
            highlight.archive = False

        s.commit()

        return response_json({
            "message": f"All highlights under '{name}' unarchived successfully",
            "count": len(highlights)
        }, status=200)

    except Exception as e:
        s.rollback()
        return bad_response(f"Failed to unarchive highlights: {e}")

    finally:
        s.close()


def get_selected_and_story_archives(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    name = query_params.get("name")
    cover_image_key = query_params.get("cover_image_key")

    if not name or not cover_image_key:
        return bad_request("'name' and 'cover_image_key' are required")

    s = SessionLocal()
    try:
        # ✅ Get highlight thumbnails
        highlights = (
            s.query(Highlight)
            .join(Story, Highlight.story_id == Story.id)
            .filter(
                Highlight.name == name,
                Highlight.cover_image_key == cover_image_key,
                Highlight.archive == False
            )
            .order_by(Highlight.order.asc())
            .all()
        )

        if not highlights:
            return bad_request("cannot edit non existing highlights")

        if not user_id == highlights[0].story.user_id:
            return bad_request("The user cannot view the highlights of another user")

        highlight_name = highlights[0].name
        cover_image_key = highlights[0].cover_image_key

        highlight_thumbnails = [
            {
                "story_id": str(h.story_id),
                "highlight_id": str(h.id),
                "highlight_thumbnail_url": generate_presigned_get(S3_BUCKET, h.story.thumbnail_key)
            }
            for h in highlights
        ]

        # ✅ Get archived stories for this user
        archived_stories = (
            s.query(Story)
            .filter(
                Story.user_id == user_id,
                Story.deleted_at == None,
                Story.archive == True,
                Story.highlight == False
            )
            .order_by(Story.created_at.desc())
            .all()
        )

        story_archives = []
        for st in archived_stories:
            is_highlight = False
            h = (
                s.query(Highlight)
                .filter(
                    Highlight.name == highlight_name,
                    Highlight.cover_image_key == cover_image_key,
                    Highlight.story_id == st.id
                )
                .first()
            )
            if h:
                is_highlight = True

            story_archives.append({
                    "story_id": str(st.id),
                    "archive_thumbnail_url": generate_presigned_get(S3_BUCKET, st.thumbnail_key),
                    "is_highlight": is_highlight,
                    "created_at": st.created_at.isoformat()
            })

        # ✅ Final response
        return response_json({
            "highlight_thumbnails": highlight_thumbnails,
            "story_archives": story_archives
        })

    except Exception as e:
        return bad_response(e)
    finally:
        s.close()


def edit_highlight_folder(event, user_id):
    data = parse_body(event)
    name = data.get("name")
    cover_image_key = data.get("cover_image_key")
    deselected_ids = data.get("deselected_ids") or []
    selected_ids = data.get("selected_ids") or []
    old_name = data.get("old_name")
    old_cover_image_key = data.get("old_cover_image_key")

    if not name or not cover_image_key:
        return bad_request("'name' and 'cover_image_key' are required")

    s = SessionLocal()
    try:
        remaining_highlights = s.query(Highlight).filter(Highlight.name == old_name, Highlight.cover_image_key == old_cover_image_key). all()
        for h in remaining_highlights:
            h.name = name
            h.cover_image_key = cover_image_key
            s.add(h)

        now = datetime.now(timezone.utc)
        i = 1
        # ✅ 1. Add selected story archives as highlights (if not already there)
        for story_id in selected_ids:

            try:
                sid = uuid.UUID(story_id)
            except ValueError:
                return bad_request("invalid story_id format")

            story = s.query(Story).get(sid)
            if not story.user_id == user_id:
                return bad_request(f"user_id doesn't have access to story with id: {story_id}")

            if not story:
                return bad_request("story archive doesn't exists")

            if story.highlight:
                highlight = s.query(Highlight).filter_by(story_id=story.id).first()

                if not highlight:
                    story.highlight = False
                    s.add(story)
                    s.commit()
                    return bad_response("highlight not found")

                highlight.order = i
                i += 1
                s.add(highlight)
                continue

            story.highlight = True

            new_highlight = Highlight(
                story_id=story.id,
                name=name,
                cover_image_key=cover_image_key,
                created_at=datetime.now(timezone.utc),
                order=i
            )
            i += 1
            s.add(new_highlight)
            s.add(story)

        # ✅ 2. Remove deselected story highlights
        for story_id in deselected_ids:
            try:
                sid = uuid.UUID(story_id)
            except ValueError:
                return bad_request("invalid story_id format")

            if sid in selected_ids:
                continue

            h = s.query(Highlight).join(Story, Highlight.story_id == Story.id).filter(Highlight.story_id == sid).first()

            story = h.story

            if not story.user_id == user_id:
                return bad_request(f"user_id doesn't have access to story with id: {story_id}")

            s.delete(h)

            if story.expires_at > now or story.archive:
                # Story still active or archived → just unmark highlight
                story.highlight = False
            elif story.deleted_at and (now - story.deleted_at) < timedelta(days=30):
                # Story still actively deleted → just unmark highlight
                story.highlight = False
            else:
                # Story expired and not archived → delete from S3 + DB
                try:
                    delete_object(S3_BUCKET, story.s3_key)
                    delete_object(S3_BUCKET, story.thumbnail_key)
                except Exception as e:
                    return bad_response(f"⚠️ Failed to delete story media from S3: {e}")
                s.delete(story)

        s.commit()
        return response_json({"message": "Highlight folder updated successfully"})

    except Exception as e:
        s.rollback()
        return bad_response(str(e))
    finally:
        s.close()


def list_archived_stories(event, user_id):
    """List all archived stories for a given user"""
    s = SessionLocal()
    try:
        # Query archived stories
        archived_stories = (
            s.query(Story)
            .filter(Story.user_id == user_id, Story.archive == True, Story.deleted_at == None)
            .order_by(Story.created_at.desc())
            .all()
        )

        # Format response
        result = []
        for story in archived_stories:
            result.append({
                "id": str(story.id),
                "s3_key": story.s3_key,
                "thumbnail_url": generate_presigned_get(S3_BUCKET, story.thumbnail_key),
                "created_at": story.created_at.isoformat() if story.created_at else None
            })

        return response_json({"user_id": user_id, "archived_stories": result})

    except Exception as e:
        print("Error listing archived stories:", e)
        return bad_response(str(e))
    finally:
        s.close()


def get_archived_highlight_folders(event, user_id):
    s = SessionLocal()
    try:
        # ✅ Fetch archived highlights linked to story highlights
        archived_highlights = (
            s.query(Highlight.name, Highlight.cover_image_key)
            .join(Story, Story.id == Highlight.story_id)
            .filter(
                Story.highlight == True,
                Highlight.archive == True,
                Story.user_id == user_id
            )
            # PostgreSQL requires DISTINCT ON fields appear first in ORDER BY
            .distinct(Highlight.name, Highlight.cover_image_key)
            .order_by(Highlight.name, Highlight.cover_image_key, Highlight.order.asc())
            .all()
        )

        # ✅ Generate URLs and format result
        folders = [
            {
                "name": h.name,
                "cover_image_key": h.cover_image_key,
                "cover_image_url": generate_presigned_get(S3_BUCKET, h.cover_image_key)
            }
            for h in archived_highlights
        ]

        return response_json({"archived_highlight_folders": folders})

    except Exception as e:
        return bad_response(str(e))
    finally:
        s.close()


def view_archived_story(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    story_id = query_params.get("id")

    if not story_id:
        return bad_request("'id' (story_id) is required")

    s = SessionLocal()
    try:
        # Validate story_id format
        try:
            sid = uuid.UUID(story_id)
        except Exception:
            return bad_request("invalid story_id")

        # Fetch the story
        story = (
            s.query(Story)
            .filter(
                Story.id == sid,
                Story.user_id == user_id,
                Story.archive == True,
                Story.deleted_at == None
            )
            .first()
        )

        if not story:
            return not_found("archived story not found or doesn't belong to the user")

        user = s.query(UserDB).filter(UserDB.id == story.user_id).first()

        # Generate presigned URL for the archived file
        story_url = generate_presigned_get(S3_BUCKET, story.s3_key)

        # Count reactions
        reaction_count = len(story.reactions) if story.reactions else 0

        # Count replies (story comments)
        reply_count = s.query(StoryComment).filter(StoryComment.story_id == story.id).count()

        viewer_ids = [v for v in story.viewers if v != user_id]

        viewers_with_reactions = (
            s.query(
                UserDB.profile_image_key,
                UserDB.cover_image_key,
                UserDB.username,
                UserDB.display_name,
                StoryReaction.reaction_type,
            )
            .outerjoin(
                StoryReaction,
                (StoryReaction.user_id == UserDB.id) & (StoryReaction.story_id == story.id),
            )
            .filter(UserDB.id.in_(viewer_ids))
            .all()
        )

        viewer_list = [
            {
                "profile_image_key": v.profile_image_key,
                "cover_image_key": v.cover_image_key,
                "username": v.username,
                "display_name": v.display_name,
                "reaction_type": v.reaction_type if v.reaction_type else None,
            }
            for v in viewers_with_reactions
        ]

        if user_id in story.viewers:
            view_count = len(story.viewers) - 1
        else:
            view_count = len(story.viewers)

        highlight_name = None
        if story.highlight:
            highlight = s.query(Highlight).filter(Highlight.story_id == story.id).first()
            highlight_name = highlight.name
            return response_json({
                "story_id": str(story.id),
                "user_id": user.id,
                "username": user.username,
                "profile_image_key": user.profile_image_key,
                "media_url": story_url,
                "media_type": story.media_type,
                "caption": getattr(story, "caption", ""),
                "created_at": story.created_at.isoformat(),
                "view_count": view_count,
                "reaction_count": reaction_count,
                "viewers": viewer_list,
                "reply_count": reply_count,
                "location": story.location or {},
                "mentions": story.mentions or [],
                "hashtags": story.hashtags or [],
                "music": story.music or {},
                "stickers": story.stickers or [],
                "other_stickers": get_sticker_aggregates(s, story.id),
                "allow_replies": story.allow_replies,
                "allow_sharing": story.allow_sharing,
                "privacy": story.privacy,
                "is_highlight": story.highlight,
                "highlight_name": highlight_name
            })
        else:
            return response_json({
                "story_id": str(story.id),
                "username": user.username,
                "profile_image_key": user.profile_image_key,
                "media_url": story_url,
                "media_type": story.media_type,
                "caption": getattr(story, "caption", ""),
                "created_at": story.created_at.isoformat(),
                "view_count": view_count,
                "reaction_count": reaction_count,
                "viewers": viewer_list,
                "reply_count": reply_count,
                "location": story.location or {},
                "mentions": story.mentions or [],
                "hashtags": story.hashtags or [],
                "music": story.music or {},
                "stickers": story.stickers or [],
                "other_stickers": get_sticker_aggregates(s, story.id),
                "allow_replies": story.allow_replies,
                "allow_sharing": story.allow_sharing,
                "privacy": story.privacy,
                "is_highlight": story.highlight
            })

    except Exception as e:
        return bad_response(str(e))
    finally:
        s.close()


def delete_story_from_archive(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    story_id = query_params.get("id")

    if not story_id:
        return bad_request("'id' (story_id) is required")

    now = datetime.now(timezone.utc)

    s = SessionLocal()
    try:
        # Validate UUID format
        try:
            sid = uuid.UUID(story_id)
        except Exception:
            return bad_request("invalid story_id")

        # Fetch story
        story = (
            s.query(Story)
            .filter(Story.id == sid, Story.user_id == user_id, Story.archive == True)
            .first()
        )

        if not story:
            return not_found("archived story not found")

        story.expires_at = now - timedelta(days=1)

        # ✅ Mark as deleted
        story.deleted_at = now

        s.commit()
        return response_json({"message": "story unarchived successfully"})

    except Exception as e:
        return bad_response(str(e))
    finally:
        s.close()


def get_user_reactions(event, user_id):
    """
    Fetch reactions (likes) made by user_id on videos, with filtering/sorting/pagination.
    Query params:
      - page (int), per_page (int)
      - sort: "newest" (default) | "oldest"
      - date_from (ISO string), date_to (ISO string)
      - reaction_type (comma-separated) e.g. like,heart
      - visibility (comma-separated) e.g. public,unlisted,private
      - authors (comma-separated author user_ids)
    """
    query_params = event.get("queryStringParameters") or {}
    page = int(query_params.get("page") or 1)
    per_page = min(int(query_params.get("per_page") or 20), 100)
    sort = query_params.get("sort", "newest")
    date_from = parse_iso_datetime(query_params.get("date_from"))
    date_to = parse_iso_datetime(query_params.get("date_to"))
    reaction_types = [rt.strip() for rt in (query_params.get("reaction_type") or "").split(",") if rt.strip()]
    visibilities = [v.strip() for v in (query_params.get("visibility") or "").split(",") if v.strip()]
    authors = parse_int_list(query_params.get("authors"))  # list of author ids

    s = SessionLocal()
    try:
        # ✅ Base query (explicit join, no relationships)
        base_q = (
            s.query(Reaction, Video)
            .join(Video, Reaction.video_id == Video.id)
            .filter(Reaction.user_id == int(user_id))
        )

        # Filters: reaction type
        if reaction_types:
            base_q = base_q.filter(Reaction.reaction_type.in_(reaction_types))

        # Filters: date range applied to Reaction.created_at
        if date_from:
            base_q = base_q.filter(Reaction.created_at >= date_from)
        if date_to:
            base_q = base_q.filter(Reaction.created_at <= date_to)

        # Filters on video attributes
        if visibilities:
            base_q = base_q.filter(Video.visibility.in_(visibilities))

        if authors:
            base_q = base_q.filter(Video.user_id.in_(authors))

        # Sorting
        if sort == "oldest":
            order_clause = Reaction.created_at.asc()
        else:
            order_clause = Reaction.created_at.desc()

        base_q = base_q.order_by(order_clause)

        # Pagination
        offset = (page - 1) * per_page
        total = base_q.count()
        results = base_q.offset(offset).limit(per_page).all()

        # Serialize results
        items = []
        for r, video in results:
            thumbnail_url = ""
            # generate_presigned_get(S3_BUCKET, video.thumbnail_key) if video.thumbnail_key else None
            video_url = ""
            # generate_presigned_get(S3_BUCKET, video.s3_key)

            items.append({
                "reaction_id": r.id,
                "reaction_type": r.reaction_type,
                "reacted_at": r.created_at.isoformat() if r.created_at else None,
                "video": {
                    "id": str(video.id),
                    "title": video.title,
                    "description": video.description,
                    "thumbnail_key": video.thumbnail_key,
                    "thumbnail_url": thumbnail_url,
                    "video_s3_key": video.s3_key,
                    "video_url": video_url,
                    "visibility": video.visibility,
                    "author_id": video.user_id,
                    "created_at": video.created_at.isoformat() if video.created_at else None
                }
            })

        return response_json({
            "page": page,
            "per_page": per_page,
            "total": total,
            "items": items
        })

    except Exception as e:
        s.rollback()
        return bad_request(str(e))
    finally:
        s.close()


def get_user_comments(event, user_id):
    """
    Fetch all comments made by the given user (like Instagram 'Your Activity → Comments'),
    including video info, commenter info, and full parent chain (parent, grandparent, etc.).
    """

    query_params = event.get("queryStringParameters") or {}
    sort = query_params.get("sort", "newest")
    date_from = parse_iso_datetime(query_params.get("date_from"))
    date_to = parse_iso_datetime(query_params.get("date_to"))
    authors = parse_int_list(query_params.get("authors"))
    page = int(query_params.get("page") or 1)
    per_page = min(int(query_params.get("per_page") or 20), 100)

    s = SessionLocal()
    try:
        VideoAuthor = aliased(UserDB)

        # Base query → comments made by the user
        q = (
            s.query(Comment, Video, VideoAuthor)
            .join(Video, Comment.video_id == Video.id)
            .join(VideoAuthor, Video.user_id == VideoAuthor.id)
            .filter(Comment.user_id == user_id)
        )

        # Filters
        if date_from:
            q = q.filter(Comment.created_at >= date_from)
        if date_to:
            q = q.filter(Comment.created_at <= date_to)
        if authors:
            q = q.filter(Video.user_id.in_(authors))

        # Sorting
        order = Comment.created_at.asc() if sort == "oldest" else Comment.created_at.desc()
        q = q.order_by(order)

        # Pagination
        offset = (page - 1) * per_page
        total = q.count()
        rows = q.offset(offset).limit(per_page).all()

        # Get the current user's info (for their own profile)
        me = s.query(UserDB).get(user_id)

        def get_comment_chain(comment_id):
            """Recursively fetch all parent comments."""
            chain = []
            current_id = comment_id
            while current_id:
                parent = (
                    s.query(Comment, UserDB)
                    .join(UserDB, Comment.user_id == UserDB.id)
                    .filter(Comment.id == current_id)
                    .first()
                )
                if not parent:
                    break
                parent_comment, parent_user = parent
                chain.append({
                    "comment_id": parent_comment.id,
                    "text": parent_comment.text,
                    "gif_url": parent_comment.gif_url,
                    "gif_data": parent_comment.gif_data,
                    "created_at": parent_comment.created_at.isoformat(),
                    "username": parent_user.username,
                    "profile_picture_url": generate_presigned_get(S3_BUCKET, parent_user.profile_picture_key)
                    if getattr(parent_user, "profile_picture_key", None)
                    else None,
                })
                current_id = parent_comment.parent_id
            return chain[::-1]  # reverse so oldest parent is first

        # Build result
        results = []
        for comment, video, video_author in rows:
            video_thumb_url = generate_presigned_get(S3_BUCKET, video.thumbnail_key) if video.thumbnail_key else None

            results.append({
                "comment_id": comment.id,
                "comment_text": comment.text,
                "comment_gif_url": comment.gif_url,
                "comment_gif_data": comment.gif_data,
                "comment_created_at": comment.created_at.isoformat(),
                "my_username": me.username,
                "my_profile_picture_url": generate_presigned_get(S3_BUCKET, me.profile_picture_key)
                if getattr(me, "profile_picture_key", None)
                else None,

                "video": {
                    "id": str(video.id),
                    "description": video.description,
                    "created_at": video.created_at.isoformat(),
                    "thumbnail_url": video_thumb_url,
                    "author": {
                        "id": video_author.id,
                        "username": video_author.username,
                        "profile_picture_url": generate_presigned_get(S3_BUCKET, video_author.profile_picture_key)
                        if getattr(video_author, "profile_picture_key", None)
                        else None,
                    },
                },

                "parent_chain": get_comment_chain(comment.parent_id) if comment.parent_id else [],
            })

        return response_json({
            "page": page,
            "per_page": per_page,
            "total": total,
            "items": results,
        })

    except Exception as e:
        s.rollback()
        return bad_response(str(e))
    finally:
        s.close()


def get_sticker_responses(event, user_id):
    """
    Returns all sticker responses for the given user (Instagram-style 'Your Activity -> Sticker Responses').
    
    Supports filters:
        - order: newest | oldest
        - start_date, end_date: YYYY-MM-DD
        - authors: filter by owners of the stickers
    """

    s = SessionLocal()
    try:
        # Parse filters
        query_params = event.get("queryStringParameters") or {}

        order = query_params.get("order", "newest")  # newest | oldest
        authors = parse_int_list(query_params.get("authors"))    # filter by sticker owners
        start_date = parse_iso_datetime(query_params.get("start_date"))
        end_date = parse_iso_datetime(query_params.get("end_date"))
        page = int(query_params.get("page") or 1)
        per_page = min(int(query_params.get("per_page") or 20), 100)

        # Base Query: All responses made by this user
        q = (
            s.query(
                StickerResponse,
                Sticker,
                Story.user_id.label("sticker_owner_id"),
                UserDB.username.label("sticker_owner_username"),
            )
            .join(Sticker, Sticker.id == StickerResponse.sticker_id)
            .join(Story, Story.id == Sticker.story_id)
            .join(UserDB, UserDB.id == Story.user_id)
            .filter(StickerResponse.user_id == user_id)
        )

        user = s.query(UserDB).filter(UserDB.id == user_id).first()

        # --- FILTER: AUTHORS (sticker owners) ---
        if authors:
            q = q.filter(Story.user_id.in_(authors))

        # --- FILTER: DATE RANGE ---
        if start_date:
            q = q.filter(StickerResponse.created_at >= start_date)
        if end_date:
            q = q.filter(StickerResponse.created_at <= end_date)

        # --- ORDERING ---
        if order == "oldest":
            q = q.order_by(StickerResponse.created_at.asc())
        else:
            q = q.order_by(StickerResponse.created_at.desc())

        # Pagination
        offset = (page - 1) * per_page
        results = q.offset(offset).limit(per_page).all()

        response_list = []
        for sr, sticker, owner_id, owner_username in results:

            # Parse sticker options if JSON string stored
            options = None
            if sticker.options:
                try:
                    options = json.loads(sticker.options)
                except Exception:
                    options = sticker.options

            response_entry = {
                "response_id": str(sr.id),
                "created_at": sr.created_at.isoformat(),

                # Sticker owner (story owner)
                "sticker_owner": {
                    "user_id": owner_id,
                    "username": owner_username,
                },

                # Sticker metadata
                "sticker": {
                    "sticker_id": str(sticker.id),
                    "type": sticker.type if not sticker.type == "slider" else "reaction_bar",
                    "question_text": sticker.question_text,
                    "emoji_icon": sticker.emoji_icon,
                    "options": options,
                    "correct_option": sticker.correct_option,
                },

                # User's actual response
                "user_response": {
                    "selected_option": sr.selected_option,
                    "slider_value": sr.slider_value,
                }
            }

            response_list.append(response_entry)

        return response_json({
            "total": len(response_list),
            "responder": {
                "user_id": user_id,
                "username": user.username,
                "profile_image_key": user.profile_image_key,
            },
            "responses": response_list
        }, 200)

    except Exception as e:
        print("StickerResponses error:", e)
        return response_json({"error": str(e)}, 500)

    finally:
        s.close()


def get_recently_deleted(event, user_id):
    now = datetime.now(timezone.utc)

    s = SessionLocal()
    try:
        thirty_days_ago = now - timedelta(days=30)

        # Fetch stories deleted within the last 30 days
        deleted_stories = (
            s.query(Story)
            .filter(
                Story.user_id == user_id,
                Story.deleted_at != None,
                Story.deleted_at >= thirty_days_ago
            )
            .all()
        )

        # Build response
        items = []
        for story in deleted_stories:
            items.append({
                "id": str(story.id),
                "thumbnail_key": story.thumbnail_key,
                "thumbnail_url": generate_presigned_get(S3_BUCKET, story.thumbnail_key),
                "deleted_at": story.deleted_at.isoformat()
            })

        return {
            "success": True,
            "count": len(items),
            "recently_deleted": items
        }

    finally:
        s.close()


def view_story_for_activity(event, user_id, story_id):
    s = SessionLocal()

    # Validate story_id
    try:
        sid = uuid.UUID(story_id)
    except Exception:
        return bad_request("invalid story_id")

    try:
        # Load user info
        user = s.query(UserDB).get(user_id)
        if not user:
            return not_found("user not found")

        story = s.query(Story).filter(Story.id == sid).first()

        if not story:
            return not_found("story not found")

        if not story.user_id == user_id:
            return bad_request("the user does not have access to the story")

        # Count reactions
        reaction_count = len(story.reactions) if story.reactions else 0

        # Count replies (story comments)
        reply_count = s.query(StoryComment).filter(StoryComment.story_id == story.id).count()

        viewer_ids = [v for v in story.viewers if v != user_id]

        viewers_with_reactions = (
            s.query(
                UserDB.profile_image_key,
                UserDB.cover_image_key,
                UserDB.username,
                UserDB.display_name,
                StoryReaction.reaction_type,
            )
            .outerjoin(
                StoryReaction,
                (StoryReaction.user_id == UserDB.id) & (StoryReaction.story_id == story.id),
            )
            .filter(UserDB.id.in_(viewer_ids))
            .all()
        )

        viewer_list = [
            {
                "profile_image_key": v.profile_image_key,
                "cover_image_key": v.cover_image_key,
                "username": v.username,
                "display_name": v.display_name,
                "reaction_type": v.reaction_type if v.reaction_type else None,
            }
            for v in viewers_with_reactions
        ]

        if user_id in story.viewers:
            view_count = len(story.viewers) - 1
        else:
            view_count = len(story.viewers)

        highlight_name = None
        if story.highlight:
            highlight = s.query(Highlight).filter(Highlight.story_id == story.id).first()
            highlight_name = highlight.name

            story_details = {
                "story_id": str(story.id),
                "user_id": user.id,
                "username": user.username,
                "profile_pic": getattr(user, "profile_image_key", ""),
                "media_url": generate_presigned_get(S3_BUCKET, story.s3_key),
                "media_type": story.media_type,
                "caption": getattr(story, "caption", ""),
                "created_at": story.created_at.isoformat(),
                "expires_at": story.expires_at.isoformat(),
                "view_count": view_count,
                "reaction_count": reaction_count,
                "viewers": viewer_list,
                "reply_count": reply_count,
                "location": story.location or {},
                "mentions": story.mentions or [],
                "hashtags": story.hashtags or [],
                "music": story.music or {},
                "stickers": story.stickers or [],
                "other_stickers": get_sticker_aggregates(s, story.id),
                "allow_replies": story.allow_replies,
                "allow_sharing": story.allow_sharing,
                "privacy": story.privacy,
                "is_highlight": story.highlight,
                "highlight_name": highlight_name
            }
        else:
            story_details = {
                "story_id": str(story.id),
                "user_id": user.id,
                "username": user.username,
                "profile_pic": getattr(user, "profile_image_key", ""),
                "media_url": generate_presigned_get(S3_BUCKET, story.s3_key),
                "media_type": story.media_type,
                "caption": getattr(story, "caption", ""),
                "created_at": story.created_at.isoformat(),
                "expires_at": story.expires_at.isoformat(),
                "view_count": view_count,
                "reaction_count": reaction_count,
                "viewers": viewer_list,
                "reply_count": reply_count,
                "location": story.location or {},
                "mentions": story.mentions or [],
                "hashtags": story.hashtags or [],
                "music": story.music or {},
                "stickers": story.stickers or [],
                "other_stickers": get_sticker_aggregates(s, story.id),
                "allow_replies": story.allow_replies,
                "allow_sharing": story.allow_sharing,
                "privacy": story.privacy,
                "is_highlight": story.highlight
            }

        return response_json(story_details)

    except Exception as e:
        return bad_response(e)

    finally:
        s.close()


def delete_story_from_recently_deleted(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    story_id = query_params.get("story_id")

    try:
        sid = uuid.UUID(story_id)
    except Exception:
        return bad_request("invalid story_id")

    s = SessionLocal()
    try:
        story = s.query(Story).filter(Story.id == sid, Story.deleted_at != None).first()

        if not story:
            return bad_request("story not found")

        if not story.user_id == user_id:
            return bad_request("user does not have access to the story")

        story.deleted_at = None
        s.add(story)

        s.commit()
        return response_json({"message": "story deleted successfully"})

    except Exception as e:
        s.rollback()
        return bad_response(str(e))

    finally:
        s.close()


def restore_story_from_recently_deleted(event, user_id):
    query_params = event.get("queryStringParameters") or {}
    story_id = query_params.get("story_id")

    try:
        sid = uuid.UUID(story_id)
    except Exception:
        return bad_request("invalid story_id")

    s = SessionLocal()
    try:
        story = s.query(Story).filter(Story.id == sid, Story.deleted_at != None).first()

        if not story:
            return bad_request("story not found")

        if not story.user_id == user_id:
            return bad_request("user does not have access to the story")

        story.deleted_at = None
        story.expires_at = story.created_at + timedelta(days=1)
        s.add(story)

        s.commit()
        return response_json({"message": "story restored successfully"})

    except Exception as e:
        s.rollback()
        return bad_response(str(e))

    finally:
        s.close()


def post_video_view(event, video_id):
    query_params = event.get("queryStringParameters") or {}

    try:
        user_id = int(query_params.get("user_id"))
    except Exception:
        return bad_request("user_id is NULL or not an integer")

    # Validate video_id
    try:
        vid = uuid.UUID(video_id)
    except Exception:
        return bad_request("invalid video_id")

    s = SessionLocal()
    try:
        video = s.query(Video).filter(Video.id == vid).first()
        user = s.query(UserDB).filter(UserDB.id == user_id).first()

        if not user:
            return bad_request("user not found")

        if not video:
            return bad_request("video not found")

        existing = s.query(VideoView).filter(VideoView.video_id == vid, VideoView.user_id == user_id).first()

        if existing:
            existing.viewed_at = datetime.now(timezone.utc)
            s.add(existing)
            s.commit()
            return response_json({"message": "view updated"}, 201)

        # Insert view record
        view = VideoView(
            id=uuid.uuid4(),
            user_id=user_id,
            video_id=vid,
            viewed_at=datetime.now(timezone.utc)
        )

        s.add(view)
        s.commit()

        return response_json({"message": "view recorded"}, 201)

    except Exception as e:
        s.rollback()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "internal error", "details": str(e)})
        }

    finally:
        s.close()


def get_video_views(event, user_id):
    # Parse query parameters
    query_params = event.get("queryStringParameters") or {}

    try:
        user_id = int(user_id)
    except:
        return bad_request("invalid user_id")

    # Pagination + filters
    try:
        page = int(query_params.get("page", 1))
        per_page = int(query_params.get("per_page", 20))
        offset = (page - 1) * per_page

        order = query_params.get("order", "newest")  # newest | oldest
        
        authors = parse_int_list(query_params.get("authors"))  # filter by viewer user_ids
        start_date = parse_iso_datetime(query_params.get("start_date"))
        end_date = parse_iso_datetime(query_params.get("end_date"))

    except:
        return bad_request("invalid page/per_page")

    s = SessionLocal()
    try:
        # Base query
        q = (
            s.query(VideoView, Video)
            .join(Video, Video.id == VideoView.video_id)
            .filter(VideoView.user_id == user_id)  # views by this user
        )

        # Filter by viewer_ids (authors)
        if authors:
            q = q.filter(VideoView.user_id.in_(authors))

        # Filter by date range
        if start_date:
            q = q.filter(VideoView.viewed_at >= start_date)
        if end_date:
            q = q.filter(VideoView.viewed_at <= end_date)

        # Apply ordering
        if order == "oldest":
            q = q.order_by(VideoView.viewed_at.asc())
        else:  # newest
            q = q.order_by(VideoView.viewed_at.desc())

        # Total count
        total = q.count()

        # Pagination
        rows = q.limit(per_page).offset(offset).all()

        # Format response
        viewed = []
        for view, video in rows:
            viewed.append({
                "view_id": str(view.id),
                "video_id": str(video.id),
                "video_s3_key": video.s3_key,
                "thumbnail_key": video.thumbnail_key,
                "thumbnail_url": video.thumbnail_key,
                "viewed_at": view.viewed_at.isoformat()
            })

        return response_json({
            "total": total,
            "page": page,
            "per_page": per_page,
            "count": len(viewed),
            "viewed": viewed,
        })

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "internal server error", "details": str(e)})
        }

    finally:
        s.close()


def post_account_history(event):
    data = parse_body(event)
    user_id = data.get("user_id")
    event_type = data.get("event_type")
    data_ = data.get("data")
    ip_address = data.get("ip_address")
    device = data.get("device")

    # Validate required fields
    if not user_id or not event_type or not data_:
        return bad_request("user_id, event_type, and data are required")

    try:
        user_id = int(user_id)
    except:
        return bad_request("invalid user_id")

    if not isinstance(data_, dict):
        return bad_request("data must be a JSON object")

    s = SessionLocal()
    try:
        user = s.query(UserDB).filter(UserDB.id == user_id)
        
        if not user:
            return bad_request("user not found")

        new_record = AccountHistory(
            user_id=user_id,
            event_type=event_type,
            data=data_,
            ip_address=ip_address,
            device=device,
        )

        s.add(new_record)
        s.commit()

        return response_json({
            "message": "Account history recorded",
            "id": str(new_record.id)
        })

    except Exception as e:
        s.rollback()
        return response_json({
            "error": "Internal server error",
            "exception": str(e)
        }, status=500)

    finally:
        s.close()


def get_account_history(event, user_id):
    query_params = event.get("queryStringParameters") or {}

    # Pagination
    try:
        page = int(query_params.get("page", 1))
        per_page = int(query_params.get("per_page", 20))
        offset = (page - 1) * per_page
        order = query_params.get("order", "newest")  # newest | oldest
    except:
        return bad_request("invalid page/per_page")

    # Filters
    event_types = query_params.get("event_types")  # comma-separated string
    if event_types:
        event_types = [et.strip() for et in event_types.split(",")]
    start_date = parse_iso_datetime(query_params.get("start_date"))
    end_date = parse_iso_datetime(query_params.get("end_date"))

    s = SessionLocal()
    try:
        query = s.query(AccountHistory).filter(AccountHistory.user_id == user_id)

        # Apply event type filter
        if event_types:
            query = query.filter(AccountHistory.event_type.in_(event_types))

        # Apply date range filter
        if start_date:
            query = query.filter(AccountHistory.created_at >= start_date)
        if end_date:
            query = query.filter(AccountHistory.created_at <= end_date)

        # Sorting
        if order == "newest":
            query = query.order_by(desc(AccountHistory.created_at))
        else:
            query = query.order_by(asc(AccountHistory.created_at))

        total = query.count()
        results = query.offset(offset).limit(per_page).all()

        history_list = []
        for record in results:
            history_list.append({
                "id": str(record.id),
                "user_id": record.user_id,
                "event_type": record.event_type,
                "metadata": record.data,
                "ip_address": record.ip_address,
                "device": record.device,
                "created_at": record.created_at.isoformat() if record.created_at else None
            })

        return response_json({
            "page": page,
            "per_page": per_page,
            "total": total,
            "history": history_list
        })

    except Exception as e:
        s.rollback()
        return response_json({
            "error": "Internal server error",
            "exception": str(e)
        }, status=500)

    finally:
        s.close()