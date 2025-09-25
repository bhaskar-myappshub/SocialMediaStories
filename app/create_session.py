from pyrogram import Client


API_ID = 15940223      # get from https://my.telegram.org
API_HASH = "91e9197b4e0038d73d8864aa0a2c7eb2"

app = Client("my_session", api_id=API_ID, api_hash=API_HASH)

with app:
    print("Session created successfully!")
