import json
from datetime import datetime, timezone, timedelta
from app.db import SessionLocal
from app.models import Story, UserDB
from app.s3_utils import delete_object
from app.config import S3_BUCKET

from app.stories_routes import generate_presigned_puts, confirm_story, get_feed, list_user_stories, delete_story, archive_story, react_to_story
from app.stories_routes import delete_story_reaction, add_to_highlights, remove_story_from_highlights, comment_on_story, vote_poll
from app.stories_routes import  record_answer_for_quiz, update_reaction_bar, upload_default_cover_image

from app.highlights_routes import list_existing_highlights_folders, generate_presigned_put, get_updated_cover_url, list_archived_stories_for_highlights
from app.highlights_routes import create_highlight, get_highlights_folders, get_highlights, delete_highlight_folder, remove_highlight_from_highlights
from app.highlights_routes import archive_highlight_folder, unarchive_highight_folder, get_selected_and_story_archives, edit_highlight_folder

from app.archives_routes import list_archived_stories, get_archived_highlight_folders, view_archived_story, delete_story_from_archive

from app.activity_routes import get_user_reactions, get_user_comments, get_sticker_responses, get_recently_deleted, view_story_for_activity
from app.activity_routes import delete_story_from_recently_deleted, restore_story_from_recently_deleted, post_video_view, get_video_views
from app.activity_routes import post_account_history, get_account_history



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


# Helper response builders
def response_json(body, status=200):
    return {"statusCode": status, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}


# Simple path parsing
def split_path(path):
    if not path:
        return []
    p = path
    if p.startswith("/"):
        p = p[1:]
    if p.endswith("/"):
        p = p[:-1]
    return p.split("/") if p else []


# Router dispatcher
def lambda_handler(event, context):
    
    run_cleanup()

    method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("path") or event.get("rawPath") or "/"

    # Normalize path
    parts = split_path(path)

    try:
        # routing table (method, path pattern)
        if method == "POST" and parts == ["presign"]:
            return generate_presigned_puts(event)
        if method == "POST" and parts == ["stories", "confirm"]:
            return confirm_story(event)
        if method == "GET" and len(parts) == 2 and parts[0] == "stories" and parts[1] == "feed":
            return get_feed(event)
        if method == "GET" and len(parts) == 3 and parts[0] == "users" and parts[2] == "stories":
            return list_user_stories(event, int(parts[1]))
        if method == "DELETE" and len(parts) == 2 and parts[0] == "stories":
            return delete_story(event, parts[1])
        if method == "PATCH" and parts == ["stories", "archive"]:
            return archive_story(event)
        if method == "POST" and parts == ["presign_url"]:
            return generate_presigned_put(event)
        if method == "GET" and len(parts) == 3 and parts[0] == "users" and parts[2] == "cover_image":
            return get_updated_cover_url(event, int(parts[1]))
        if method == "POST" and len(parts) == 1:
            return react_to_story(event, parts[0])
        if method == "DELETE" and len(parts) == 3 and parts[0] == "stories" and parts[2] == "reaction":
            return delete_story_reaction(event, parts[1])
        if method == "GET" and parts == ["users", "existing_highlights_profiles"]:
            return list_existing_highlights_folders(event)
        if method == "POST" and len(parts) == 3 and parts[0] == "users" and parts[2] == "highlights":
            return add_to_highlights(event, int(parts[1]))
        if method == "DELETE" and len(parts) == 3 and parts[0] == "users" and parts[2] == "highlights":
            return remove_story_from_highlights(event, int(parts[1]))
        if method == "POST" and len(parts) == 2 and parts[0] == "storycomment":
            return comment_on_story(event, parts[1])
        if method == "POST" and len(parts) == 3 and parts[0] == "stories" and parts[2] == "poll":
            return vote_poll(event, parts[1])
        if method == "POST" and len(parts) == 3 and parts[0] == "stories" and parts[2] == "quiz":
            return record_answer_for_quiz(event, parts[1])
        if method == "POST" and len(parts) == 3 and parts[0] == "stories" and parts[2] == "bar":
            return update_reaction_bar(event, parts[1])
        if method == "GET" and len(parts) == 3 and parts[0] == "stories" and parts[1] == "archived":
            return list_archived_stories_for_highlights(event, int(parts[2]))
        if method == "POST" and parts == ["highlights", "cover_image", "default"]:
            return upload_default_cover_image(event)
        if method == "POST" and parts == ["highlights", "create"]:
            return create_highlight(event)
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "highlights" and parts[3] == "folders":
            return get_highlights_folders(event, int(parts[1]))
        if method == "GET" and len(parts) == 3 and parts[0] == "users" and parts[2] == "highlights":
            return get_highlights(event, int(parts[1]))
        if method == "DELETE" and len(parts) == 4 and parts[0] == "users" and parts[2] == "highlights" and parts[3] == "delete":
            return delete_highlight_folder(event, int(parts[1]))
        if method == "DELETE" and len(parts) == 2 and parts[0] == "highlights":
            return remove_highlight_from_highlights(event, parts[1])
        if method == "PATCH" and len(parts) == 4 and parts[0] == "users" and parts[2] == "highlights" and parts[3] == "archive":
            return archive_highlight_folder(event, int(parts[1]))
        if method == "PATCH" and len(parts) == 4 and parts[0] == "users" and parts[2] == "highlights" and parts[3] == "unarchive":
            return unarchive_highight_folder(event, int(parts[1]))
        if method == "GET" and len(parts) == 2 and parts[0] == "users":
            return get_selected_and_story_archives(event, int(parts[1]))
        if method == "PATCH" and len(parts) == 4 and parts[0] == "users" and parts[2] == "highlights" and parts[3] == "edit":
            return edit_highlight_folder(event, int(parts[1]))
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "stories" and parts[3] == "archive":
            return list_archived_stories(event, int(parts[1]))
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "highlights" and parts[3] == "archive":
            return get_archived_highlight_folders(event, int(parts[1]))
        if method == "GET" and len(parts) == 3 and parts[0] == "users" and parts[2] == "archive":
            return view_archived_story(event, int(parts[1]))
        if method == "PATCH" and len(parts) == 3 and parts[0] == "users" and parts[2] == "archive":
            return delete_story_from_archive(event, parts[1])
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "activity" and parts[3] == "reactions":
            return get_user_reactions(event, int(parts[1]))
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "activity" and parts[3] == "comments":
            return get_user_comments(event, int(parts[1]))
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "activity" and parts[3] == "sticker_responses":
            return get_sticker_responses(event, int(parts[1]))
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "activity" and parts[3] == "recently_deleted":
            return get_recently_deleted(event, int(parts[1]))
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "view":
            return view_story_for_activity(event, int(parts[1]), parts[3])
        if method == "PATCH" and len(parts) == 5 and parts[0] == "users" and parts[2] == "activity" and parts[3] == "story" and parts[4] == "delete":
            return delete_story_from_recently_deleted(event, int(parts[1]))
        if method == "PATCH" and len(parts) == 5 and parts[0] == "users" and parts[2] == "activity" and parts[3] == "story" and parts[4] == "restore":
            return restore_story_from_recently_deleted(event, int(parts[1]))
        if method == "POST" and parts[0] == "videos" and parts[2] == "view":
            return post_video_view(event, parts[1])
        if method == "GET" and len(parts) == 4 and parts[0] == "users" and parts[2] == "activity" and parts[3] == "watch_history":
            return get_video_views(event, int(parts[1]))
        if method == "POST" and parts[0] == "account" and parts[1] == "history":
            return post_account_history(event)
        if method == "GET" and parts[0] == "users" and parts[2] == "activity" and parts[3] == "account_history":
            return get_account_history(event, int(parts[1]))

        return response_json({"error": "not found"}, status=404)
    except Exception as e:
        return response_json({"error": "internal server error", "exception": str(e)}, status=500)