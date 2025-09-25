import io
from pyrogram import Client
from app.config import API_ID, API_HASH
from app.session import upload_session, download_session
import asyncio

async def get_telegram_client():
    session_path = download_session()
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    client = Client(session_path, api_id=API_ID, api_hash=API_HASH)
    logger.info("hai2")
    await client.start()
    me = await client.get_me()
    logger.info(f"✅ Logged in as {me.id} ({me.first_name})")
    logger.info("hai3")
    session_bytes = io.BytesIO()
    client.session.save(session_bytes)
    upload_session(session_bytes)
    
    return client