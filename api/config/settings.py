# api/config/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    SUPABASE_URL = os.getenv("SUPABASE_URL") 
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

settings = Settings()