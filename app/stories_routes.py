import os
import tempfile
import subprocess
import json
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import or_, and_, func, desc
from botocore.exceptions import ClientError

from app.db import SessionLocal
from app.models import UserDB, Story, StoryReaction
from app.models import StoryComment, CloseFriend, Follower, Highlight, Sticker, StickerResponse
from app.utils import forbidden, bad_response, response_json, bad_request, not_found, split_path
from app.utils import parse_body, parse_cursor

from app.config import IMAGE_MAX_BYTES, VIDEO_MAX_BYTES, S3_BUCKET, VIDEO_MAX_DURATION
from app.s3_utils import generate_presigned_post, head_object, generate_presigned_get, delete_object
from app.s3_utils import get_video_duration_from_s3, copy_object, upload_file, download_file


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