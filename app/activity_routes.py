import json
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import asc, desc
from sqlalchemy.orm import aliased

from app.db import SessionLocal
from app.models import UserDB, Story, StoryReaction, Reaction, VideoView, AccountHistory
from app.models import Video, Comment, StoryComment, Highlight, Sticker, StickerResponse
from app.utils import bad_response, response_json, bad_request, not_found
from app.utils import parse_body
from app.utils import parse_int_list, parse_iso_datetime
from app.stories_routes import get_sticker_aggregates

from app.config import S3_BUCKET
from app.s3_utils import generate_presigned_get



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