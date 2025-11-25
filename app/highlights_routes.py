import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import desc
from botocore.exceptions import ClientError

from app.db import SessionLocal
from app.models import UserDB, Story, StoryReaction
from app.models import StoryComment, CloseFriend, Follower, Highlight
from app.utils import forbidden, bad_response, response_json, bad_request, not_found
from app.utils import parse_body
from app.stories_routes import get_sticker_aggregates

from app.config import IMAGE_MAX_BYTES, S3_BUCKET
from app.s3_utils import generate_presigned_post, head_object, generate_presigned_get, delete_object



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