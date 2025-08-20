# config.py
import os
from dotenv import load_dotenv

load_dotenv()

ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CENTRAL_BOT_TOKEN = os.getenv("CENTRAL_BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
VERIFY_KEY = os.getenv("VERIFY_KEY")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
