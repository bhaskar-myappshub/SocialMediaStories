import uuid
from datetime import datetime, timezone, timedelta

from app.db import SessionLocal
from app.models import UserDB, Story, StoryReaction
from app.models import StoryComment, Highlight
from app.utils import bad_response, response_json, bad_request, not_found
from app.stories_routes import get_sticker_aggregates

from app.config import S3_BUCKET
from app.s3_utils import generate_presigned_get



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