import json
from app.routes import confirm_story, list_user_stories, generate_presigned_puts, delete_story, remove_story_from_highlights, create_highlight
from app.routes import react_to_story, delete_story_reaction, generate_presigned_put, upload_default_cover_image, get_user_reactions
from app.routes import get_user_comments, comment_on_story, get_feed, add_to_highlights, vote_poll, list_existing_highlights_folders
from app.routes import get_highlights_folders, archive_story, run_cleanup, get_updated_cover_url, get_highlights, list_archived_stories_for_highlights
from app.routes import delete_highlight_folder, update_reaction_bar, remove_highlight_from_highlights, archive_highlight_folder
from app.routes import get_selected_and_story_archives, edit_highlight_folder, list_archived_stories, get_archived_highlight_folders
from app.routes import view_archived_story, delete_story_from_archive, record_answer_for_quiz, unarchive_highight_folder, get_sticker_responses
from app.routes import get_recently_deleted, view_story_for_activity, delete_story_from_recently_deleted, restore_story_from_recently_deleted
from app.routes import post_video_view, get_video_views, post_account_history, get_account_history

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