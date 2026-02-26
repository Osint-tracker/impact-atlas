import asyncio
from telethon import TelegramClient

async def main():
    api_id = 24856485
    api_hash = 'ccb58f4b099129d8e899d973b4fbf336'
    session_file = 'ingestion/telegram_session'
    
    print(f"Connecting to {session_file}...")
    client = TelegramClient(session_file, api_id, api_hash)
    await client.connect()
    
    auth = await client.is_user_authorized()
    print(f"Authorized? {auth}")
    
    if auth:
        me = await client.get_me()
        print(f"Logged in as: {me.username or me.first_name}")

if __name__ == '__main__':
    asyncio.run(main())
