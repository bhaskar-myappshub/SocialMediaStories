import json
from urllib.parse import parse_qs

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